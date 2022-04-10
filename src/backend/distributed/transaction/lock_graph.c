/*-------------------------------------------------------------------------
 *
 * lock_graph.c
 *
 *  Functions for obtaining local and global lock graphs in which each
 *  node is a distributed transaction, and an edge represent a waiting-for
 *  relationship.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
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
static void AddWaitEdgeFromBlockedProcessResult(WaitGraph *waitGraph, PGresult *result,
												int rowIndex);
static void ReturnBlockedProcessGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo);
static WaitGraph * BuildLocalWaitGraph(bool onlyDistributedTx);
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

/*
 * We almost have 2 sets of identical functions. The first set (e.g., dump_wait_edges)
 * functions are intended for distributed deadlock detection purposes.
 *
 * The second set of functions (e.g., citus_internal_local_blocked_processes) are
 * intended for citus_lock_waits view.
 *
 * The main difference is that the former functions only show processes that are blocked
 * inside a distributed transaction (e.g., see AssignDistributedTransactionId()).
 * The latter functions return a superset, where any blocked process is returned.
 *
 * We kept two different set of functions for two purposes. First, the deadlock detection
 * is a performance critical code-path happening very frequently and we don't add any
 * performance overhead. Secondly, to be able to do rolling upgrades, we cannot change
 * the API of dump_global_wait_edges/dump_local_wait_edges such that they take a boolean
 * parameter. If we do that, until all nodes are upgraded, the deadlock detection would fail,
 * which is not acceptable.
 */
PG_FUNCTION_INFO_V1(dump_local_wait_edges);
PG_FUNCTION_INFO_V1(dump_global_wait_edges);

PG_FUNCTION_INFO_V1(citus_internal_local_blocked_processes);
PG_FUNCTION_INFO_V1(citus_internal_global_blocked_processes);


/*
 * dump_global_wait_edges returns global wait edges for distributed transactions
 * originating from the node on which it is started.
 */
Datum
dump_global_wait_edges(PG_FUNCTION_ARGS)
{
	bool onlyDistributedTx = true;

	WaitGraph *waitGraph = BuildGlobalWaitGraph(onlyDistributedTx);

	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * citus_internal_global_blocked_processes returns global wait edges
 * including all processes running on the cluster.
 */
Datum
citus_internal_global_blocked_processes(PG_FUNCTION_ARGS)
{
	bool onlyDistributedTx = false;

	WaitGraph *waitGraph = BuildGlobalWaitGraph(onlyDistributedTx);

	ReturnBlockedProcessGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * BuildGlobalWaitGraph builds a wait graph for distributed transactions
 * that originate from this node, including edges from all (other) worker
 * nodes.
 *
 *
 * If onlyDistributedTx is true, we only return distributed transactions
 * (e.g., AssignDistributedTransaction() or assign_distributed_transactions())
 * has been called for the process. Distributed deadlock detection only
 * interested in these processes.
 */
WaitGraph *
BuildGlobalWaitGraph(bool onlyDistributedTx)
{
	List *workerNodeList = ActiveReadableNodeList();
	char *nodeUser = CitusExtensionOwnerName();
	List *connectionList = NIL;
	int32 localGroupId = GetLocalGroupId();

	/* deadlock detection is only interested in distributed transactions */
	WaitGraph *waitGraph = BuildLocalWaitGraph(onlyDistributedTx);

	/* open connections in parallel */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = 0;

		if (workerNode->groupId == localGroupId)
		{
			/* we already have local wait edges */
			continue;
		}

		MultiConnection *connection = StartNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		StringInfo queryString = makeStringInfo();

		if (onlyDistributedTx)
		{
			appendStringInfo(queryString,
							 "SELECT waiting_pid, waiting_node_id, "
							 "waiting_transaction_num, waiting_transaction_stamp, "
							 "blocking_pid, blocking_node_id, blocking_transaction_num, "
							 "blocking_transaction_stamp, blocking_transaction_waiting "
							 "FROM dump_local_wait_edges()");
		}
		else
		{
			appendStringInfo(queryString,
							 "SELECT waiting_global_pid, waiting_pid, "
							 "waiting_node_id, waiting_transaction_num, waiting_transaction_stamp, "
							 "blocking_global_pid,blocking_pid, blocking_node_id, "
							 "blocking_transaction_num, blocking_transaction_stamp, blocking_transaction_waiting "
							 "FROM citus_internal_local_blocked_processes()");
		}

		int querySent = SendRemoteCommand(connection, queryString->data);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive dump_local_wait_edges results */
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupts = true;

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		int64 rowCount = PQntuples(result);
		int64 colCount = PQnfields(result);

		if (onlyDistributedTx && colCount != 9)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "dump_local_wait_edges")));
			continue;
		}
		else if (!onlyDistributedTx && colCount != 11)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "citus_internal_local_blocked_processes")));
			continue;
		}

		for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			if (onlyDistributedTx)
			{
				AddWaitEdgeFromResult(waitGraph, result, rowIndex);
			}
			else
			{
				AddWaitEdgeFromBlockedProcessResult(waitGraph, result, rowIndex);
			}
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

	waitEdge->waitingGPid = 0; /* not requested for deadlock detection */
	waitEdge->waitingPid = ParseIntField(result, rowIndex, 0);
	waitEdge->waitingNodeId = ParseIntField(result, rowIndex, 1);
	waitEdge->waitingTransactionNum = ParseIntField(result, rowIndex, 2);
	waitEdge->waitingTransactionStamp = ParseTimestampTzField(result, rowIndex, 3);
	waitEdge->blockingGPid = 0; /* not requested for deadlock detection */
	waitEdge->blockingPid = ParseIntField(result, rowIndex, 4);
	waitEdge->blockingNodeId = ParseIntField(result, rowIndex, 5);
	waitEdge->blockingTransactionNum = ParseIntField(result, rowIndex, 6);
	waitEdge->blockingTransactionStamp = ParseTimestampTzField(result, rowIndex, 7);
	waitEdge->isBlockingXactWaiting = ParseBoolField(result, rowIndex, 8);
}


/*
 * AddWaitEdgeFromBlockedProcessResult adds an edge to the wait graph that
 * is read from a PGresult.
 */
static void
AddWaitEdgeFromBlockedProcessResult(WaitGraph *waitGraph, PGresult *result, int rowIndex)
{
	WaitEdge *waitEdge = AllocWaitEdge(waitGraph);

	waitEdge->waitingGPid = ParseIntField(result, rowIndex, 0);
	waitEdge->waitingPid = ParseIntField(result, rowIndex, 1);
	waitEdge->waitingNodeId = ParseIntField(result, rowIndex, 2);
	waitEdge->waitingTransactionNum = ParseIntField(result, rowIndex, 3);
	waitEdge->waitingTransactionStamp = ParseTimestampTzField(result, rowIndex, 4);
	waitEdge->blockingGPid = ParseIntField(result, rowIndex, 5);
	waitEdge->blockingPid = ParseIntField(result, rowIndex, 6);
	waitEdge->blockingNodeId = ParseIntField(result, rowIndex, 7);
	waitEdge->blockingTransactionNum = ParseIntField(result, rowIndex, 8);
	waitEdge->blockingTransactionStamp = ParseTimestampTzField(result, rowIndex, 9);
	waitEdge->isBlockingXactWaiting = ParseBoolField(result, rowIndex, 10);
}


/*
 * ParseIntField parses a int64 from a remote result or returns 0 if the
 * result is NULL.
 */
int64
ParseIntField(PGresult *result, int rowIndex, int colIndex)
{
	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	char *resultString = PQgetvalue(result, rowIndex, colIndex);

	return strtou64(resultString, NULL, 10);
}


/*
 * ParseBoolField parses a bool from a remote result or returns false if the
 * result is NULL.
 */
bool
ParseBoolField(PGresult *result, int rowIndex, int colIndex)
{
	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return false;
	}

	char *resultString = PQgetvalue(result, rowIndex, colIndex);
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
	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return DT_NOBEGIN;
	}

	char *resultString = PQgetvalue(result, rowIndex, colIndex);
	Datum resultStringDatum = CStringGetDatum(resultString);
	Datum timestampDatum = DirectFunctionCall3(timestamptz_in, resultStringDatum, 0, -1);

	return DatumGetTimestampTz(timestampDatum);
}


/*
 * dump_local_wait_edges returns wait edges for distributed transactions
 * running on the node on which it is called, which originate from the source node.
 */
Datum
dump_local_wait_edges(PG_FUNCTION_ARGS)
{
	bool onlyDistributedTx = true;

	WaitGraph *waitGraph = BuildLocalWaitGraph(onlyDistributedTx);
	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * citus_internal_local_blocked_processes returns global wait edges
 * including all processes running on the node.
 */
Datum
citus_internal_local_blocked_processes(PG_FUNCTION_ARGS)
{
	bool onlyDistributedTx = false;

	WaitGraph *waitGraph = BuildLocalWaitGraph(onlyDistributedTx);
	ReturnBlockedProcessGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * ReturnWaitGraph returns a wait graph for a set returning function.
 */
static void
ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	TupleDesc tupleDesc;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDesc);

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
	for (size_t curEdgeNum = 0; curEdgeNum < waitGraph->edgeCount; curEdgeNum++)
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
}


/*
 * ReturnBlockedProcessGraph returns a wait graph for a set returning function.
 */
static void
ReturnBlockedProcessGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	TupleDesc tupleDesc;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDesc);

	/*
	 * Columns:
	 * 00: waiting_global_pid
	 * 01: waiting_pid
	 * 02: waiting_node_id
	 * 03: waiting_transaction_num
	 * 04: waiting_transaction_stamp
	 * 05: blocking_global_pid
	 * 06: blocking_pid
	 * 07: blocking__node_id
	 * 08: blocking_transaction_num
	 * 09: blocking_transaction_stamp
	 * 10: blocking_transaction_waiting
	 */
	for (size_t curEdgeNum = 0; curEdgeNum < waitGraph->edgeCount; curEdgeNum++)
	{
		Datum values[11];
		bool nulls[11];
		WaitEdge *curEdge = &waitGraph->edges[curEdgeNum];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = UInt64GetDatum(curEdge->waitingGPid);
		values[1] = Int32GetDatum(curEdge->waitingPid);
		values[2] = Int32GetDatum(curEdge->waitingNodeId);
		if (curEdge->waitingTransactionNum != 0)
		{
			values[3] = Int64GetDatum(curEdge->waitingTransactionNum);
			values[4] = TimestampTzGetDatum(curEdge->waitingTransactionStamp);
		}
		else
		{
			nulls[3] = true;
			nulls[4] = true;
		}

		values[5] = UInt64GetDatum(curEdge->blockingGPid);
		values[6] = Int32GetDatum(curEdge->blockingPid);
		values[7] = Int32GetDatum(curEdge->blockingNodeId);
		if (curEdge->blockingTransactionNum != 0)
		{
			values[8] = Int64GetDatum(curEdge->blockingTransactionNum);
			values[9] = TimestampTzGetDatum(curEdge->blockingTransactionStamp);
		}
		else
		{
			nulls[8] = true;
			nulls[9] = true;
		}
		values[10] = BoolGetDatum(curEdge->isBlockingXactWaiting);

		tuplestore_putvalues(tupleStore, tupleDesc, values, nulls);
	}
}


/*
 * BuildLocalWaitGraph builds a wait graph for distributed transactions
 * that originate from the local node.
 *
 * If onlyDistributedTx is true, we only return distributed transactions
 * (e.g., AssignDistributedTransaction() or assign_distributed_transactions())
 * has been called for the process. Distributed deadlock detection only
 * interested in these processes.
 */
static WaitGraph *
BuildLocalWaitGraph(bool onlyDistributedTx)
{
	PROCStack remaining;
	int totalProcs = TotalProcCount();

	/*
	 * Try hard to avoid allocations while holding lock. Thus we pre-allocate
	 * space for locks in large batches - for common scenarios this should be
	 * more than enough space to build the list of wait edges without a single
	 * allocation.
	 */
	WaitGraph *waitGraph = (WaitGraph *) palloc0(sizeof(WaitGraph));
	waitGraph->localNodeId = GetLocalGroupId();
	waitGraph->allocatedSize = totalProcs * 3;
	waitGraph->edgeCount = 0;
	waitGraph->edges = (WaitEdge *) palloc(waitGraph->allocatedSize * sizeof(WaitEdge));

	remaining.procs = (PGPROC **) palloc(sizeof(PGPROC *) * totalProcs);
	remaining.procAdded = (bool *) palloc0(sizeof(bool *) * totalProcs);
	remaining.procCount = 0;

	LockLockData();

	/*
	 * Build lock-graph.  We do so by first finding all procs which we are
	 * interested in (in a distributed transaction, and blocked).  Once
	 * those are collected, do depth first search over all procs blocking
	 * those.
	 */

	/* build list of starting procs */
	for (int curBackend = 0; curBackend < totalProcs; curBackend++)
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
		if (onlyDistributedTx &&
			!IsInDistributedTransaction(&currentBackendData))
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
 * is actually extended on the disk and released as soon as the extension is
 * done, even before the execution of the command that triggered the extension
 * finishes. Thus, recording such waits on our lock graphs could yield detecting
 * wrong distributed deadlocks.
 */
static bool
IsProcessWaitingForSafeOperations(PGPROC *proc)
{
	if (proc->waitStatus != PROC_WAIT_STATUS_WAITING)
	{
		return false;
	}

	if (pgproc_statusflags_compat(proc) & PROC_IS_AUTOVACUUM)
	{
		return true;
	}

	PROCLOCK *waitProcLock = proc->waitProcLock;
	LOCK *waitLock = waitProcLock->tag.myLock;

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
	LockBackendSharedMemory(LW_SHARED);

	for (int partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
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
	for (int partitionNum = NUM_LOCK_PARTITIONS - 1; partitionNum >= 0; partitionNum--)
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
	curEdge->waitingGPid = waitingBackendData.globalPID;

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
	curEdge->blockingGPid = blockingBackendData.globalPID;

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

	Assert(remaining->procCount < TotalProcCount());

	remaining->procs[remaining->procCount++] = proc;
	remaining->procAdded[proc->pgprocno] = true;
}


/*
 * IsProcessWaitingForLock returns whether a given process is waiting for a lock.
 */
bool
IsProcessWaitingForLock(PGPROC *proc)
{
	return proc->waitStatus == PROC_WAIT_STATUS_WAITING;
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
