/*-------------------------------------------------------------------------
 *
 * deadlock.c
 *
 * Basic implementation of distributed deadlock detection.  The algorithm used
 * is quite naive, in the hope that that's sufficient.  The approach basically is:
 *
 * 1) regularly poll all workers if they've any sessions blocked longer than
 *    deadlock_timeout * fuzz_factor.  This avoids triggering more expensive
 *    work if there's no potential for deadlocks. (currently unimplemented)
 *
 * 2) If 1) finds there's potential for deadlock, query each worker node for a
 *    wait-graphs for transactions originating on the corresponding
 *    coordinator.  This graph can include non-distributed transactions,
 *    transactions originating on another systems, etc., just not as the
 *    "starting points".  All the nodes are identified by their distributed
 *    transaction id (inventing one for non-distributed transactions).
 *
 * 3) The coordinator combines the wait-graphs from each worker, and builds a
 *    wait-graph spanning all of these.
 *
 * 4) Perform cycle detection for each distributed transaction started on the
 *    local node.  The algorithm used is a simple graph traversal that finds a
 *    deadlock when the starting node is reached again.  As already visited
 *    nodes are skipped, this is O(edges) for a single distributed
 *    transaction.  Meaning the total cost is O(edges *
 *    distributed_transactions), where distributed_transactions is limited by
 *    max_connections.  The cost can be limited by only considering
 *    transactions that have been running for longer than deadlock_timeout
 *    (unimplemented).
 *    If necessary this could be implemented in O(transactions + edges).
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/connection_management.h"
#include "distributed/deadlock.h"
#include "distributed/hash_helpers.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_management.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

typedef struct LockDepNode
{
	TmgmtTransactionId transactionId;
	List *deps;
	int initial_pid;
	bool *deadlocked;
	bool visited;
} LockDepNode;

/*
 * Describes an edge in a waiting-for graph of locks.  This isn't used for
 * deadlock-checking itself, but to gather the information necessary to do so.
 * It can be acquired locally, or from a remote node.
 *
 * The datatypes here are a bit looser than strictly necessary, because
 * they're transported as the return type from an SQL function.
 */
typedef struct WaitEdge
{
	int waitingPid;
	int waitingNodeId;
	int64 waitingTransactionId;
	TimestampTz waitingTransactionStamp;

	int blockingPid;
	int blockingNodeId;
	int64 blockingTransactionId;
	TimestampTz blockingTransactionStamp;
	bool blockingAlsoBlocked;
} WaitEdge;

/* FIXME: better name? More adjacency list than graph... */
typedef struct WaitGraph
{
	size_t numAllocated;
	size_t numUsed;
	WaitEdge *edges;
} WaitGraph;

#define NUM_DUMP_LOCKS_COLS 9

/*
 * Columns:
 * 00: waiting_pid
 * 01: waiting_node_id
 * 02: waiting_transaction_id
 * 03: waiting_transactionstamp
 * 04: blocking_pid
 * 05: blocking_node_id
 * 06: blocking_transaction_id
 * 07: blocking_transactionstamp
 * 08: blocking_also_blocked
 */

static WaitGraph * BuildLocalWaitForGraph(int sourceNodeId, int localNodeId);
static WaitEdge * AllocWaitEdge(WaitGraph *waitGraph);

static void ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo);
static void LoadRemoteEdges(WaitGraph *waitGraph);
static void WaitEdgeFromResult(WaitEdge *edge, PGresult *result, size_t rowIndex);
static int64 ParseIntField(PGresult *result, int rowIndex, int colIndex, bool *isNull);
static TimestampTz ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex,
										 bool *isNull);

static uint32 TransactionIdHash(const void *key, Size keysize);
static int TransactionIdCompare(const void *a, const void *b, Size keysize);
static LockDepNode * LookupDepNode(HTAB *lockDepNodeHash,
								   TmgmtTransactionId *transactionId);

PG_FUNCTION_INFO_V1(this_machine_kills_deadlocks);
PG_FUNCTION_INFO_V1(dump_local_wait_edges);
PG_FUNCTION_INFO_V1(dump_all_wait_edges);


Datum
this_machine_kills_deadlocks(PG_FUNCTION_ARGS)
{
	WaitGraph *waitGraph = NULL;
	HASHCTL info;
	uint32 hashFlags = 0;
	HTAB *lockDepNodeHash = NULL;
	int localNodeId = 0; /* FIXME: get correct local node id */
	int curBackend = 0;
	size_t curEdgeNum;

	/* create (host,port,user,database) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TmgmtTransactionId);
	info.entrysize = sizeof(LockDepNode);
	info.hash = TransactionIdHash;
	info.match = TransactionIdCompare;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	lockDepNodeHash = hash_create("deadlock detection",
								  64, &info, hashFlags);

	waitGraph = BuildLocalWaitForGraph(0, 0);
	LoadRemoteEdges(waitGraph);

	/*
	 * FIXME: should be doable without additional lock now, could just do this
	 * in the hash, after building it?
	 */
	LWLockAcquire(&TmgmtShmemControl->lock, LW_SHARED);
	for (curBackend = 0; curBackend < MaxBackends; curBackend++)
	{
		TmgmtBackendData *CurBackendData = &TmgmtShmemControl->sessions[curBackend];
		PGPROC *curProc;
		LockDepNode *initialNode = NULL;

		curProc = &ProcGlobal->allProcs[curBackend];

		/* Skip if unused (FIXME: prepared xacts!) */
		if (curProc->pid == 0)
		{
			continue;
		}

		/* Skip if not a distributed transaction. */
		if (CurBackendData->transactionId.transactionId == 0)
		{
			continue;
		}

		/* Skip if not not originating from interesting node */
		if (localNodeId != CurBackendData->transactionId.nodeId)
		{
			continue;
		}

		/*
		 * It'd be nice if we could only start at transactions that are
		 * actually blocked, but we do not know that at this point.
		 */

		initialNode = LookupDepNode(lockDepNodeHash, &CurBackendData->transactionId);
		initialNode->initial_pid = curProc->pid;
		initialNode->deadlocked = &CurBackendData->deadlockKilled;
	}
	LWLockRelease(&TmgmtShmemControl->lock);

	/*
	 * Flatten wait edges, which includes wait-edges between processes on each
	 * node, to a graph only including wait-for dependencies between
	 * distributed transactions.
	 */
	for (curEdgeNum = 0; curEdgeNum < waitGraph->numUsed; curEdgeNum++)
	{
		WaitEdge *curEdge = &waitGraph->edges[curEdgeNum];
		TmgmtTransactionId from;
		TmgmtTransactionId to;
		LockDepNode *fromNode;
		LockDepNode *toNode;

		from.nodeId = curEdge->waitingNodeId;
		from.transactionId = curEdge->waitingTransactionId;
		from.timestamp = curEdge->waitingTransactionStamp;

		to.nodeId = curEdge->blockingNodeId;
		to.transactionId = curEdge->blockingTransactionId;
		to.timestamp = curEdge->blockingTransactionStamp;

		fromNode = LookupDepNode(lockDepNodeHash, &from);
		toNode = LookupDepNode(lockDepNodeHash, &to);

		fromNode->deps = lappend(fromNode->deps, toNode);
	}

	/* avoids the need need to call hash_seq_term */
	hash_freeze(lockDepNodeHash);

	/*
	 * Find lock cycles by doing a tree traversal for each node that starts
	 * with one of the local ones.
	 */
	{
		LockDepNode *curNode;
		HASH_SEQ_STATUS initNodeSeq;

		hash_seq_init(&initNodeSeq, lockDepNodeHash);
		while ((curNode = (LockDepNode *) hash_seq_search(&initNodeSeq)) != 0)
		{
			HASH_SEQ_STATUS resetVisitedSeq;
			LockDepNode *resetVisitedNode;
			List *todoList = NIL;

			/* Only start at transactions originating locally and blocked. */
			if (curNode->initial_pid == 0)
			{
				continue;
			}

			elog(WARNING, "doing deadlock detection for %d", curNode->initial_pid);

			/* reset visited fields */
			hash_seq_init(&resetVisitedSeq, lockDepNodeHash);
			while ((resetVisitedNode = (LockDepNode *) hash_seq_search(
						&resetVisitedSeq)) != 0)
			{
				resetVisitedNode->visited = false;
			}

			todoList = list_copy(curNode->deps);

			while (todoList)
			{
				LockDepNode *visitNode = linitial(todoList);
				ListCell *recurseCell = NULL;

				todoList = list_delete_first(todoList);

				/* There's a deadlock if we found our original node */
				if (visitNode == curNode)
				{
					elog(WARNING, "found deadlock, killing: %d", curNode->initial_pid);
					*curNode->deadlocked = true;
					kill(curNode->initial_pid, SIGINT);
					PG_RETURN_BOOL(true);
				}

				/* don't do redundant work (or cycle in indep. deadlocks) */
				if (visitNode->visited)
				{
					continue;
				}
				visitNode->visited = true;

				foreach(recurseCell, visitNode->deps)
				{
					todoList = lappend(todoList, lfirst(recurseCell));
				}
			}
		}
	}

	PG_RETURN_BOOL(false);
}


Datum
dump_local_wait_edges(PG_FUNCTION_ARGS)
{
	int32 sourceNodeId = PG_GETARG_INT32(0);
	int32 localNodeId = PG_GETARG_INT32(1);
	WaitGraph *waitGraph = NULL;

	waitGraph = BuildLocalWaitForGraph(sourceNodeId, localNodeId);

	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


Datum
dump_all_wait_edges(PG_FUNCTION_ARGS)
{
	WaitGraph *waitGraph = NULL;

	waitGraph = BuildLocalWaitForGraph(0, 0);
	LoadRemoteEdges(waitGraph);

	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


static void
RecordWaitEdge(WaitGraph *waitGraph,
			   int localNodeId, bool blockingAlsoBlocked,
			   TmgmtBackendData *fromData, PGPROC *fromProc,
			   TmgmtBackendData *toData, PGPROC *toProc)
{
	WaitEdge *curEdge = AllocWaitEdge(waitGraph);

	curEdge->waitingPid = fromProc->pid;
	if (fromData->transactionId.transactionId == 0)
	{
		curEdge->waitingNodeId = localNodeId;
		curEdge->waitingTransactionId = 0;
		curEdge->waitingTransactionStamp = 0;
	}
	else
	{
		curEdge->waitingNodeId = fromData->transactionId.nodeId;
		curEdge->waitingTransactionId = fromData->transactionId.transactionId;
		curEdge->waitingTransactionStamp = fromData->transactionId.timestamp;
	}

	curEdge->blockingPid = toProc->pid;
	if (toData->transactionId.transactionId == 0)
	{
		curEdge->blockingNodeId = localNodeId;
		curEdge->blockingTransactionId = 0;
		curEdge->blockingTransactionStamp = 0;
	}
	else
	{
		curEdge->blockingNodeId = toData->transactionId.nodeId;
		curEdge->blockingTransactionId = toData->transactionId.transactionId;
		curEdge->blockingTransactionStamp = toData->transactionId.timestamp;
	}
	curEdge->blockingAlsoBlocked = blockingAlsoBlocked;
}


static WaitGraph *
BuildLocalWaitForGraph(int sourceNodeId, int localNodeId)
{
	WaitGraph *waitGraph = (WaitGraph *) palloc(sizeof(WaitGraph));
	int curBackend;
	int partitionNum;
	List *todoList = NIL;
	bool *visited = NULL;

	/*
	 * Try hard to avoid allocations while holding lock. Thus we pre-allocate
	 * space for locks in large batches - for common scenarios this should be
	 * more than enough space to build the list of wait edges without a single
	 * allocation.
	 *
	 * FIXME: Better todoList datatype, pg style list is a bad idea, way too
	 * many allocations.
	 */
	waitGraph->numAllocated = MaxBackends * 3;
	waitGraph->numUsed = 0;
	waitGraph->edges = (WaitEdge *) palloc(waitGraph->numAllocated * sizeof(WaitEdge));

	visited = palloc0(sizeof(bool) * MaxBackends);

	/*
	 * Build lock-graph.  We do so by first finding all procs which we are
	 * interested in (originating on our source system, and blocked).  Once
	 * those are collected, do depth first search over all procs blocking
	 * those.  To avoid redundantly dropping procs, keep track of which procs
	 * already have been visisted in a pgproc indexed visited[] array.
	 */

	LWLockAcquire(&TmgmtShmemControl->lock, LW_SHARED);
	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
	{
		LWLockAcquire(LockHashPartitionLockByIndex(partitionNum), LW_SHARED);
	}

	/* build list of starting procs */
	for (curBackend = 0; curBackend < MaxBackends; curBackend++)
	{
		TmgmtBackendData *CurBackendData = &TmgmtShmemControl->sessions[curBackend];
		PGPROC *curProc;

		curProc = &ProcGlobal->allProcs[curBackend];

		/* Skip if unused (FIXME: prepared xacts!) */
		if (curProc->pid == 0)
		{
			continue;
		}

		/* Skip if not a distributed transaction. */
		if (CurBackendData->transactionId.transactionId == 0)
		{
			continue;
		}

		/* Skip if not not originating from interesting node */
		if (sourceNodeId != CurBackendData->transactionId.nodeId)
		{
			continue;
		}

		/* Skip if not blocked */
		if (curProc->links.next == NULL || curProc->waitLock == NULL)
		{
			continue;
		}

		todoList = lappend(todoList, curProc);
	}


	while (todoList)
	{
		PGPROC *curProc = (PGPROC *) linitial(todoList);
		TmgmtBackendData *curBackendData =
			&TmgmtShmemControl->sessions[curProc->pgprocno];

		/* pop from todo list */
		todoList = list_delete_first(todoList);

		/* avoid redundant (potentially cyclic!) iteration */
		if (visited[curProc->pgprocno])
		{
			continue;
		}
		visited[curProc->pgprocno] = true;

		/* FIXME: deal with group locking */

		/* FIXME: move body to different function */
		if (curProc->links.next != NULL && curProc->waitLock != NULL)
		{
			LOCK *lock = curProc->waitLock;
			SHM_QUEUE *procLocks = &(lock->procLocks);
			PROCLOCK *proclock;
			int conflictMask;
			int numLockModes;
			LockMethod lockMethodTable;

			lockMethodTable = GetLocksMethodTable(lock);
			numLockModes = lockMethodTable->numLockModes;
			conflictMask = lockMethodTable->conflictTab[curProc->waitLockMode];

			/*
			 * Record an edge for everyone already holding the lock in a
			 * conflicting manner ("hard edges" in postgres parlance).
			 */
			proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												 offsetof(PROCLOCK, lockLink));

			while (proclock)
			{
				PGPROC *leader;
				TmgmtBackendData *blockingBackendData = NULL;
				PGPROC *nextProc;

				nextProc = proclock->tag.myProc;
				leader = nextProc->lockGroupLeader == NULL ? nextProc :
						 nextProc->lockGroupLeader;

				blockingBackendData = &TmgmtShmemControl->sessions[leader->pgprocno];

				/* A proc never blocks itself or any other lock group member */
				if (leader != curProc)
				{
					int lockMethodOff;

					/* Have to check conflicts with every locktype held. */
					for (lockMethodOff = 1; lockMethodOff <= numLockModes;
						 lockMethodOff++)
					{
						if ((proclock->holdMask & LOCKBIT_ON(lockMethodOff)) &&
							(conflictMask & LOCKBIT_ON(lockMethodOff)))
						{
							bool blockingAlsoBlocked = nextProc->links.next != NULL &&
													   nextProc->waitLock != NULL;

							RecordWaitEdge(waitGraph,
										   localNodeId, blockingAlsoBlocked,
										   curBackendData, curProc,
										   blockingBackendData, nextProc);

							if (blockingAlsoBlocked)
							{
								todoList = lappend(todoList, nextProc);
							}
						}
					}
				}
				proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
													 offsetof(PROCLOCK, lockLink));
			}


			/*
			 * Record an edge for everyone in front of us in the wait-queue
			 * for the lock ("soft edges" in postgres parlance).  Postgres can
			 * re-order the wait-queue, but that seems pretty hard to do in a
			 * distributed context.
			 */
			{
				PROC_QUEUE *waitQueue = &(lock->waitProcs);
				int queueSize = waitQueue->size;
				PGPROC *nextProc = (PGPROC *) waitQueue->links.next;

				while (queueSize-- > 0)
				{
					PGPROC *leader;

					/* done when we reached ourselves */
					if (curProc == nextProc)
					{
						break;
					}

					leader = nextProc->lockGroupLeader == NULL ? nextProc :
							 nextProc->lockGroupLeader;

					if ((LOCKBIT_ON(leader->waitLockMode) & conflictMask) != 0)
					{
						bool blockingAlsoBlocked = nextProc->links.next != NULL &&
												   nextProc->waitLock != NULL;
						TmgmtBackendData *blockingBackendData = NULL;

						blockingBackendData =
							&TmgmtShmemControl->sessions[leader->pgprocno];

						RecordWaitEdge(waitGraph,
									   localNodeId, blockingAlsoBlocked,
									   curBackendData, curProc,
									   blockingBackendData, nextProc);

						if (blockingAlsoBlocked)
						{
							todoList = lappend(todoList, nextProc);
						}
					}

					nextProc = (PGPROC *) nextProc->links.next;
				}
			}
		}
	}

	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
	{
		LWLockRelease(LockHashPartitionLockByIndex(partitionNum));
	}

	LWLockRelease(&TmgmtShmemControl->lock);

	return waitGraph;
}


static WaitEdge *
AllocWaitEdge(WaitGraph *waitGraph)
{
	/* ensure space for new edge */
	if (waitGraph->numAllocated == waitGraph->numUsed)
	{
		waitGraph->numAllocated *= 2;
		waitGraph->edges = (WaitEdge *)
						   repalloc(waitGraph->edges, sizeof(WaitEdge) *
									waitGraph->numAllocated);
	}

	return &waitGraph->edges[waitGraph->numUsed++];
}


static int64
ParseIntField(PGresult *result, int rowIndex, int colIndex, bool *isNull)
{
	if (PQgetisnull(result, rowIndex, colIndex))
	{
		if (!isNull)
		{
			ereport(ERROR, (errmsg("remote field unexpectedly is NULL")));
		}
		*isNull = true;
		return 0;
	}

	if (isNull)
	{
		*isNull = false;
	}

	return pg_strtouint64(PQgetvalue(result, rowIndex, colIndex), NULL, 10);
}


static TimestampTz
ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex, bool *isNull)
{
	Datum timestampDatum;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		if (!isNull)
		{
			ereport(ERROR, (errmsg("remote field unexpectedly is NULL")));
		}
		*isNull = true;
		return 0;
	}

	if (isNull)
	{
		*isNull = false;
	}

	timestampDatum =
		DirectFunctionCall3(timestamptz_in, CStringGetDatum(PQgetvalue(result, rowIndex,
																	   colIndex)),
							0, -1);
	return DatumGetTimestampTz(timestampDatum);
}


static void
ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupdesc = NULL;
	Tuplestorestate *tupstore = NULL;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	size_t curEdgeNum;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}
	if (!(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Columns:
	 * 00: waiting_pid
	 * 01: waiting_node_id
	 * 02: waiting_transaction_id
	 * 03: waiting_transactionstamp
	 * 04: blocking_pid
	 * 05: blocking_node_id
	 * 06: blocking_transaction_id
	 * 07: blocking_transactionstamp
	 * 08: blocking_also_blocked
	 */
	for (curEdgeNum = 0; curEdgeNum < waitGraph->numUsed; curEdgeNum++)
	{
		Datum values[NUM_DUMP_LOCKS_COLS];
		bool nulls[NUM_DUMP_LOCKS_COLS];
		WaitEdge *curEdge = &waitGraph->edges[curEdgeNum];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(curEdge->waitingPid);
		values[1] = Int32GetDatum(curEdge->waitingNodeId);
		if (curEdge->waitingTransactionId != 0)
		{
			values[2] = Int64GetDatum(curEdge->waitingTransactionId);
			values[3] = TimestampTzGetDatum(curEdge->waitingTransactionStamp);
		}
		else
		{
			nulls[2] = true;
			nulls[3] = true;
		}

		values[4] = Int32GetDatum(curEdge->blockingPid);
		values[5] = Int32GetDatum(curEdge->blockingNodeId);
		if (curEdge->blockingTransactionId != 0)
		{
			values[6] = Int64GetDatum(curEdge->blockingTransactionId);
			values[7] = TimestampTzGetDatum(curEdge->blockingTransactionStamp);
		}
		else
		{
			nulls[6] = true;
			nulls[7] = true;
		}
		values[8] = BoolGetDatum(curEdge->blockingAlsoBlocked);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}


static void
WaitEdgeFromResult(WaitEdge *edge, PGresult *result, size_t rowIndex)
{
	bool isNull = false;

	/*
	 * Remote Columns:
	 * 00: waiting_pid
	 * 01: waiting_node_id
	 * 02: waiting_transaction_id
	 * 03: waiting_transactionstamp
	 * 04: blocking_pid
	 * 05: blocking_node_id
	 * 06: blocking_transaction_id
	 * 07: blocking_transactionstamp
	 * 08: blocking_also_blocked
	 */
	edge->waitingPid = ParseIntField(result, rowIndex, 0, NULL);
	edge->waitingNodeId = ParseIntField(result, rowIndex, 1, NULL);
	edge->waitingTransactionId = ParseIntField(result, rowIndex, 2, &isNull);
	if (isNull)
	{
		edge->waitingTransactionId = 0;
	}
	edge->waitingTransactionStamp = ParseTimestampTzField(result, rowIndex, 3, &isNull);
	if (isNull)
	{
		edge->waitingTransactionStamp = 0;
	}

	edge->blockingPid = ParseIntField(result, rowIndex, 4, NULL);
	edge->blockingNodeId = ParseIntField(result, rowIndex, 5, &isNull);
	edge->blockingTransactionId = ParseIntField(result, rowIndex, 6, &isNull);
	if (isNull)
	{
		edge->blockingTransactionId = 0;
	}
	edge->blockingTransactionStamp = ParseTimestampTzField(result, rowIndex, 7, &isNull);
	if (isNull)
	{
		edge->blockingTransactionStamp = 0;
	}
	edge->blockingAlsoBlocked = ParseIntField(result, rowIndex, 8, &isNull) != 0;
}


static void
LoadRemoteEdges(WaitGraph *waitGraph)
{
	List *workerNodeList = ActiveWorkerNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = CitusExtensionOwnerName();
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;


	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	/* finish opening connections */
	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	forboth(workerNodeCell, workerNodeList, connectionCell, connectionList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;
		char *command = NULL;
		const char *params[2];


		params[0] = psprintf("%d", 0); /* FIXME: better id for master */
		params[1] = psprintf("%d", workerNode->nodeId);
		command = "SELECT * FROM dump_local_wait_edges($1, $2)";

		querySent = SendRemoteCommandParams(connection, command, 2,
											NULL, params);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;
		int64 rowIndex = 0;
		int64 rowCount = 0;
		int64 colCount = 0;

		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

#define NUM_DUMP_LOCKS_COLS 9
		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		if (colCount != NUM_DUMP_LOCKS_COLS)
		{
			elog(ERROR, "hey mister");
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			WaitEdge *curEdge = AllocWaitEdge(waitGraph);

			WaitEdgeFromResult(curEdge, result, rowIndex);
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


static uint32
TransactionIdHash(const void *key, Size keysize)
{
	TmgmtTransactionId *entry = (TmgmtTransactionId *) key;
	uint32 hash = 0;

	hash = hash_any((unsigned char *) &entry->nodeId, sizeof(int64));
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->transactionId,
									   sizeof(int64)));
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->timestamp,
									   sizeof(TimestampTz)));

	return hash;
}


static int
TransactionIdCompare(const void *a, const void *b, Size keysize)
{
	TmgmtTransactionId *ta = (TmgmtTransactionId *) a;
	TmgmtTransactionId *tb = (TmgmtTransactionId *) b;

	/* NB: Not used for sorting, just equality... */
	if (ta->nodeId != tb->nodeId ||
		ta->transactionId != tb->transactionId ||
		ta->timestamp != tb->timestamp)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


static LockDepNode *
LookupDepNode(HTAB *lockDepNodeHash, TmgmtTransactionId *transactionId)
{
	bool found = false;
	LockDepNode *node = (LockDepNode *)
						hash_search(lockDepNodeHash, transactionId, HASH_ENTER, &found);

	if (!found)
	{
		node->deps = NIL;
		node->initial_pid = false;
		node->visited = false;
	}

	return node;
}


void
DeadlockLogHook(ErrorData *edata)
{
	if (edata->elevel != ERROR ||
		edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
	{
		return;
	}

	if (MyTmgmtBackendData->deadlockKilled)
	{
		edata->sqlerrcode = ERRCODE_T_R_DEADLOCK_DETECTED;
		edata->message = "deadlock detected";
		edata->detail = "Check server log for detail.";
	}
}
