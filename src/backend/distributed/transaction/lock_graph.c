/*-------------------------------------------------------------------------
 *
 * lock_graph.c
 *
 *  Functions for obtaining local and global lock graphs in which each
 *  node is a distributed transaction, and an edge represent a waiting-for
 *  relationship.
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
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


/*
 * PROCStack is a stack of PGPROC pointers used to perform a depth-first search
 * through the lock graph. It also keeps track of which processes have been
 * added to the stack to avoid visiting the same process multiple times.
 */
typedef struct PROCStack
{
	int procCount;
	PGPROC **procs;
	bool *procAdded;
} PROCStack;


static void AddWaitEdgeFromResult(WaitGraph *waitGraph, PGresult *result, int rowIndex);
static void ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo);
static WaitGraph * BuildLocalWaitGraph(void);
static bool IsProcessWaitingForSafeOperations(PGPROC *proc);
static void LockLockData(void);
static void UnlockLockData(void);
static void AddEdgesForLockWaits(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddEdgesForWaitQueue(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddWaitEdge(WaitGraph *waitGraph, PGPROC *waitingProc, PGPROC *blockingProc,
						PROCStack *remaining);
static WaitEdge * AllocWaitEdge(WaitGraph *waitGraph);
static void AddProcToVisit(PROCStack *remaining, PGPROC *proc);
static bool IsSameLockGroup(PGPROC *leftProc, PGPROC *rightProc);
static bool IsConflictingLockMask(int holdMask, int conflictMask);


PG_FUNCTION_INFO_V1(dump_local_wait_edges);
PG_FUNCTION_INFO_V1(dump_global_wait_edges);


/*
 * dump_global_wait_edges returns global wait edges for distributed transactions
 * originating from the node on which it is started.
 */
Datum
dump_global_wait_edges(PG_FUNCTION_ARGS)
{
	WaitGraph *waitGraph = NULL;

	waitGraph = BuildGlobalWaitGraph();

	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * BuildGlobalWaitGraph builds a wait graph for distributed transactions
 * that originate from this node, including edges from all (other) worker
 * nodes.
 */
WaitGraph *
BuildGlobalWaitGraph(void)
{
	List *workerNodeList = ActiveReadableNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = CitusExtensionOwnerName();
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;
	int localNodeId = GetLocalGroupId();

	WaitGraph *waitGraph = BuildLocalWaitGraph();

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		if (workerNode->groupId == localNodeId)
		{
			/* we already have local wait edges */
			continue;
		}

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;
		const char *command = "SELECT * FROM dump_local_wait_edges()";

		querySent = SendRemoteCommand(connection, command);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive dump_local_wait_edges results */
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
			ReportResultError(connection, result, WARNING);
			continue;
		}

		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		if (colCount != 9)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "dump_local_wait_edges")));
			continue;
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			AddWaitEdgeFromResult(waitGraph, result, rowIndex);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	return waitGraph;
}


/*
 * AddWaitEdgeFromResult adds an edge to the wait graph that is read from
 * a PGresult.
 */
static void
AddWaitEdgeFromResult(WaitGraph *waitGraph, PGresult *result, int rowIndex)
{
	WaitEdge *waitEdge = AllocWaitEdge(waitGraph);

	waitEdge->waitingPid = ParseIntField(result, rowIndex, 0);
	waitEdge->waitingNodeId = ParseIntField(result, rowIndex, 1);
	waitEdge->waitingTransactionNum = ParseIntField(result, rowIndex, 2);
	waitEdge->waitingTransactionStamp = ParseTimestampTzField(result, rowIndex, 3);
	waitEdge->blockingPid = ParseIntField(result, rowIndex, 4);
	waitEdge->blockingNodeId = ParseIntField(result, rowIndex, 5);
	waitEdge->blockingTransactionNum = ParseIntField(result, rowIndex, 6);
	waitEdge->blockingTransactionStamp = ParseTimestampTzField(result, rowIndex, 7);
	waitEdge->isBlockingXactWaiting = ParseBoolField(result, rowIndex, 8);
}


/*
 * ParseIntField parses a int64 from a remote result or returns 0 if the
 * result is NULL.
 */
int64
ParseIntField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);

	return pg_strtouint64(resultString, NULL, 10);
}


/*
 * ParseBoolField parses a bool from a remote result or returns false if the
 * result is NULL.
 */
bool
ParseBoolField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return false;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	if (strlen(resultString) != 1)
	{
		return false;
	}

	return resultString[0] == 't';
}


/*
 * ParseTimestampTzField parses a timestamptz from a remote result or returns
 * 0 if the result is NULL.
 */
TimestampTz
ParseTimestampTzField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum timestampDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return DT_NOBEGIN;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	timestampDatum = DirectFunctionCall3(timestamptz_in, resultStringDatum, 0, -1);

	return DatumGetTimestampTz(timestampDatum);
}


/*
 * dump_local_wait_edges returns wait edges for distributed transactions
 * running on the node on which it is called, which originate from the source node.
 */
Datum
dump_local_wait_edges(PG_FUNCTION_ARGS)
{
	WaitGraph *waitGraph = BuildLocalWaitGraph();
	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * ReturnWaitGraph returns a wait graph for a set returning function.
 */
static void
ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupleDesc = NULL;
	Tuplestorestate *tupleStore = NULL;
	MemoryContext per_query_ctx = NULL;
	MemoryContext oldContext = NULL;
	size_t curEdgeNum = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (resultInfo == NULL || !IsA(resultInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}
	if (!(resultInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	per_query_ctx = resultInfo->econtext->ecxt_per_query_memory;
	oldContext = MemoryContextSwitchTo(per_query_ctx);

	tupleStore = tuplestore_begin_heap(true, false, work_mem);
	resultInfo->returnMode = SFRM_Materialize;
	resultInfo->setResult = tupleStore;
	resultInfo->setDesc = tupleDesc;
	MemoryContextSwitchTo(oldContext);

	/*
	 * Columns:
	 * 00: waiting_pid
	 * 01: waiting_node_id
	 * 02: waiting_transaction_num
	 * 03: waiting_transaction_stamp
	 * 04: blocking_pid
	 * 05: blocking__node_id
	 * 06: blocking_transaction_num
	 * 07: blocking_transaction_stamp
	 * 08: blocking_transaction_waiting
	 */
	for (curEdgeNum = 0; curEdgeNum < waitGraph->edgeCount; curEdgeNum++)
	{
		Datum values[9];
		bool nulls[9];
		WaitEdge *curEdge = &waitGraph->edges[curEdgeNum];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(curEdge->waitingPid);
		values[1] = Int32GetDatum(curEdge->waitingNodeId);
		if (curEdge->waitingTransactionNum != 0)
		{
			values[2] = Int64GetDatum(curEdge->waitingTransactionNum);
			values[3] = TimestampTzGetDatum(curEdge->waitingTransactionStamp);
		}
		else
		{
			nulls[2] = true;
			nulls[3] = true;
		}

		values[4] = Int32GetDatum(curEdge->blockingPid);
		values[5] = Int32GetDatum(curEdge->blockingNodeId);
		if (curEdge->blockingTransactionNum != 0)
		{
			values[6] = Int64GetDatum(curEdge->blockingTransactionNum);
			values[7] = TimestampTzGetDatum(curEdge->blockingTransactionStamp);
		}
		else
		{
			nulls[6] = true;
			nulls[7] = true;
		}
		values[8] = BoolGetDatum(curEdge->isBlockingXactWaiting);

		tuplestore_putvalues(tupleStore, tupleDesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);
}


/*
 * BuildLocalWaitGraph builds a wait graph for distributed transactions
 * that originate from the local node.
 */
static WaitGraph *
BuildLocalWaitGraph(void)
{
	WaitGraph *waitGraph = NULL;
	int curBackend = 0;
	PROCStack remaining;

	/*
	 * Try hard to avoid allocations while holding lock. Thus we pre-allocate
	 * space for locks in large batches - for common scenarios this should be
	 * more than enough space to build the list of wait edges without a single
	 * allocation.
	 */
	waitGraph = (WaitGraph *) palloc0(sizeof(WaitGraph));
	waitGraph->localNodeId = GetLocalGroupId();
	waitGraph->allocatedSize = TotalProcs * 3;
	waitGraph->edgeCount = 0;
	waitGraph->edges = (WaitEdge *) palloc(waitGraph->allocatedSize * sizeof(WaitEdge));

	remaining.procs = (PGPROC **) palloc(sizeof(PGPROC *) * TotalProcs);
	remaining.procAdded = (bool *) palloc0(sizeof(bool *) * TotalProcs);
	remaining.procCount = 0;

	LockLockData();

	/*
	 * Build lock-graph.  We do so by first finding all procs which we are
	 * interested in (in a distributed transaction, and blocked).  Once
	 * those are collected, do depth first search over all procs blocking
	 * those.
	 */

	/* build list of starting procs */
	for (curBackend = 0; curBackend < TotalProcs; curBackend++)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[curBackend];
		BackendData currentBackendData;

		/* skip if the PGPROC slot is unused */
		if (currentProc->pid == 0)
		{
			continue;
		}

		GetBackendDataForProc(currentProc, &currentBackendData);

		/*
		 * Only start searching from distributed transactions, since we only
		 * care about distributed transactions for the purpose of distributed
		 * deadlock detection.
		 */
		if (!IsInDistributedTransaction(&currentBackendData))
		{
			continue;
		}

		/* skip if the process is not blocked */
		if (!IsProcessWaitingForLock(currentProc))
		{
			continue;
		}

		/* skip if the process is blocked for relation extension */
		if (IsProcessWaitingForSafeOperations(currentProc))
		{
			continue;
		}

		AddProcToVisit(&remaining, currentProc);
	}

	while (remaining.procCount > 0)
	{
		PGPROC *waitingProc = remaining.procs[--remaining.procCount];

		/* only blocked processes result in wait edges */
		if (!IsProcessWaitingForLock(waitingProc))
		{
			continue;
		}

		/* skip if the process is blocked for relation extension */
		if (IsProcessWaitingForSafeOperations(waitingProc))
		{
			continue;
		}

		/*
		 * Record an edge for everyone already holding the lock in a
		 * conflicting manner ("hard edges" in postgres parlance).
		 */
		AddEdgesForLockWaits(waitGraph, waitingProc, &remaining);

		/*
		 * Record an edge for everyone in front of us in the wait-queue
		 * for the lock ("soft edges" in postgres parlance).
		 */
		AddEdgesForWaitQueue(waitGraph, waitingProc, &remaining);
	}

	UnlockLockData();

	return waitGraph;
}


/*
 * IsProcessWaitingForSafeOperations returns true if the given PROC
 * waiting on relation extension locks, page locks or speculative locks.
 *
 * The function also returns true if the waiting process is an autovacuum
 * process given that autovacuum cannot contribute to any distributed
 * deadlocks.
 *
 * In general for the purpose of distributed deadlock detection, we should
 * skip if the process blocked on the locks that may not be part of deadlocks.
 * Those locks are held for a short duration while the relation or the index
 * is actually  extended on the disk and released as soon as the extension is
 * done, even before the execution of the command that triggered the extension
 * finishes. Thus, recording such waits on our lock graphs could yield detecting
 * wrong distributed deadlocks.
 */
static bool
IsProcessWaitingForSafeOperations(PGPROC *proc)
{
	PROCLOCK *waitProcLock = NULL;
	LOCK *waitLock = NULL;
	PGXACT *pgxact = NULL;

	if (proc->waitStatus != STATUS_WAITING)
	{
		return false;
	}

	/* get the transaction that the backend associated with */
	pgxact = &ProcGlobal->allPgXact[proc->pgprocno];
	if (pgxact->vacuumFlags & PROC_IS_AUTOVACUUM)
	{
		return true;
	}

	waitProcLock = proc->waitProcLock;
	waitLock = waitProcLock->tag.myLock;

	return waitLock->tag.locktag_type == LOCKTAG_RELATION_EXTEND ||
		   waitLock->tag.locktag_type == LOCKTAG_PAGE ||
		   waitLock->tag.locktag_type == LOCKTAG_SPECULATIVE_TOKEN;
}


/*
 * LockLockData takes locks the shared lock data structure, which prevents
 * concurrent lock acquisitions/releases.
 *
 * The function also acquires lock on the backend shared memory to prevent
 * new backends to start.
 */
static void
LockLockData(void)
{
	int partitionNum = 0;

	LockBackendSharedMemory(LW_SHARED);

	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
	{
		LWLockAcquire(LockHashPartitionLockByIndex(partitionNum), LW_SHARED);
	}
}


/*
 * UnlockLockData unlocks the locks on the shared lock data structure in reverse
 * order since LWLockRelease searches the given lock from the end of the
 * held_lwlocks array.
 *
 * The function also releases the shared memory lock to allow new backends to
 * start.
 */
static void
UnlockLockData(void)
{
	int partitionNum = 0;

	for (partitionNum = NUM_LOCK_PARTITIONS - 1; partitionNum >= 0; partitionNum--)
	{
		LWLockRelease(LockHashPartitionLockByIndex(partitionNum));
	}

	UnlockBackendSharedMemory();
}


/*
 * AddEdgesForLockWaits adds an edge to the wait graph for every granted lock
 * that waitingProc is waiting for.
 *
 * This function iterates over the procLocks data structure in shared memory,
 * which also contains entries for locks which have not been granted yet, but
 * it does not reflect the order of the wait queue. We therefore handle the
 * wait queue separately.
 */
static void
AddEdgesForLockWaits(WaitGraph *waitGraph, PGPROC *waitingProc, PROCStack *remaining)
{
	/* the lock for which this process is waiting */
	LOCK *waitLock = waitingProc->waitLock;

	/* determine the conflict mask for the lock level used by the process */
	LockMethod lockMethodTable = GetLocksMethodTable(waitLock);
	int conflictMask = lockMethodTable->conflictTab[waitingProc->waitLockMode];

	/* iterate through the queue of processes holding the lock */
	SHM_QUEUE *procLocks = &waitLock->procLocks;
	PROCLOCK *procLock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												   offsetof(PROCLOCK, lockLink));

	while (procLock != NULL)
	{
		PGPROC *currentProc = procLock->tag.myProc;

		/*
		 * Skip processes from the same lock group, processes that don't conflict,
		 * and processes that are waiting on safe operations.
		 */
		if (!IsSameLockGroup(waitingProc, currentProc) &&
			IsConflictingLockMask(procLock->holdMask, conflictMask) &&
			!IsProcessWaitingForSafeOperations(currentProc))
		{
			AddWaitEdge(waitGraph, waitingProc, currentProc, remaining);
		}

		procLock = (PROCLOCK *) SHMQueueNext(procLocks, &procLock->lockLink,
											 offsetof(PROCLOCK, lockLink));
	}
}


/*
 * AddEdgesForWaitQueue adds an edge to the wait graph for processes in front of
 * waitingProc in the wait queue that are trying to acquire a conflicting lock.
 */
static void
AddEdgesForWaitQueue(WaitGraph *waitGraph, PGPROC *waitingProc, PROCStack *remaining)
{
	/* the lock for which this process is waiting */
	LOCK *waitLock = waitingProc->waitLock;

	/* determine the conflict mask for the lock level used by the process */
	LockMethod lockMethodTable = GetLocksMethodTable(waitLock);
	int conflictMask = lockMethodTable->conflictTab[waitingProc->waitLockMode];

	/* iterate through the wait queue */
	PROC_QUEUE *waitQueue = &(waitLock->waitProcs);
	int queueSize = waitQueue->size;
	PGPROC *currentProc = (PGPROC *) waitQueue->links.next;

	/*
	 * Iterate through the queue from the start until we encounter waitingProc,
	 * since we only care about processes in front of waitingProc in the queue.
	 */
	while (queueSize-- > 0 && currentProc != waitingProc)
	{
		int awaitMask = LOCKBIT_ON(currentProc->waitLockMode);

		/*
		 * Skip processes from the same lock group, processes that don't conflict,
		 * and processes that are waiting on safe operations.
		 */
		if (!IsSameLockGroup(waitingProc, currentProc) &&
			IsConflictingLockMask(awaitMask, conflictMask) &&
			!IsProcessWaitingForSafeOperations(currentProc))
		{
			AddWaitEdge(waitGraph, waitingProc, currentProc, remaining);
		}

		currentProc = (PGPROC *) currentProc->links.next;
	}
}


/*
 * AddWaitEdge adds a new wait edge to a wait graph. The nodes in the graph are
 * transactions and an edge indicates the "waiting" process is blocked on a lock
 * held by the "blocking" process.
 *
 * If the blocking process is itself waiting then it is added to the remaining
 * stack.
 */
static void
AddWaitEdge(WaitGraph *waitGraph, PGPROC *waitingProc, PGPROC *blockingProc,
			PROCStack *remaining)
{
	WaitEdge *curEdge = AllocWaitEdge(waitGraph);
	BackendData waitingBackendData;
	BackendData blockingBackendData;

	GetBackendDataForProc(waitingProc, &waitingBackendData);
	GetBackendDataForProc(blockingProc, &blockingBackendData);

	curEdge->isBlockingXactWaiting =
		IsProcessWaitingForLock(blockingProc) &&
		!IsProcessWaitingForSafeOperations(blockingProc);
	if (curEdge->isBlockingXactWaiting)
	{
		AddProcToVisit(remaining, blockingProc);
	}

	curEdge->waitingPid = waitingProc->pid;

	if (IsInDistributedTransaction(&waitingBackendData))
	{
		DistributedTransactionId *waitingTransactionId =
			&waitingBackendData.transactionId;

		curEdge->waitingNodeId = waitingTransactionId->initiatorNodeIdentifier;
		curEdge->waitingTransactionNum = waitingTransactionId->transactionNumber;
		curEdge->waitingTransactionStamp = waitingTransactionId->timestamp;
	}
	else
	{
		curEdge->waitingNodeId = waitGraph->localNodeId;
		curEdge->waitingTransactionNum = 0;
		curEdge->waitingTransactionStamp = 0;
	}

	curEdge->blockingPid = blockingProc->pid;

	if (IsInDistributedTransaction(&blockingBackendData))
	{
		DistributedTransactionId *blockingTransactionId =
			&blockingBackendData.transactionId;

		curEdge->blockingNodeId = blockingTransactionId->initiatorNodeIdentifier;
		curEdge->blockingTransactionNum = blockingTransactionId->transactionNumber;
		curEdge->blockingTransactionStamp = blockingTransactionId->timestamp;
	}
	else
	{
		curEdge->blockingNodeId = waitGraph->localNodeId;
		curEdge->blockingTransactionNum = 0;
		curEdge->blockingTransactionStamp = 0;
	}
}


/*
 * AllocWaitEdge allocates a wait edge as part of the given wait graph.
 * If the wait graph has insufficient space its size is doubled using
 * repalloc.
 */
static WaitEdge *
AllocWaitEdge(WaitGraph *waitGraph)
{
	/* ensure space for new edge */
	if (waitGraph->allocatedSize == waitGraph->edgeCount)
	{
		waitGraph->allocatedSize *= 2;
		waitGraph->edges = (WaitEdge *)
						   repalloc(waitGraph->edges, sizeof(WaitEdge) *
									waitGraph->allocatedSize);
	}

	return &waitGraph->edges[waitGraph->edgeCount++];
}


/*
 * AddProcToVisit adds a process to the stack of processes to visit
 * in the depth-first search, unless it was already added.
 */
static void
AddProcToVisit(PROCStack *remaining, PGPROC *proc)
{
	if (remaining->procAdded[proc->pgprocno])
	{
		return;
	}

	Assert(remaining->procCount < TotalProcs);

	remaining->procs[remaining->procCount++] = proc;
	remaining->procAdded[proc->pgprocno] = true;
}


/*
 * IsProcessWaitingForLock returns whether a given process is waiting for a lock.
 */
bool
IsProcessWaitingForLock(PGPROC *proc)
{
	return proc->waitStatus == STATUS_WAITING;
}


/*
 * IsSameLockGroup returns whether two processes are part of the same lock group,
 * meaning they are either the same process, or have the same lock group leader.
 */
static bool
IsSameLockGroup(PGPROC *leftProc, PGPROC *rightProc)
{
	return leftProc == rightProc ||
		   (leftProc->lockGroupLeader != NULL &&
			leftProc->lockGroupLeader == rightProc->lockGroupLeader);
}


/*
 * IsConflictingLockMask returns whether the given conflict mask conflicts with the
 * holdMask.
 *
 * holdMask is a bitmask with the i-th bit turned on if a lock mode i is held.
 *
 * conflictMask is a bitmask with the j-th bit turned on if it conflicts with
 * lock mode i.
 */
static bool
IsConflictingLockMask(int holdMask, int conflictMask)
{
	return (holdMask & conflictMask) != 0;
}


/*
 * IsInDistributedTransaction returns whether the given backend is in a
 * distributed transaction.
 */
bool
IsInDistributedTransaction(BackendData *backendData)
{
	return backendData->transactionId.transactionNumber != 0;
}
