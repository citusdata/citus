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
#include "distributed/hash_helpers.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


/*
 * PROCStack is a stack of PGPROC pointers used to perform a depth-first search
 * through the lock graph.
 */
typedef struct PROCStack
{
	int procCount;
	PGPROC **procs;
} PROCStack;


static void ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo);
static WaitGraph * BuildWaitGraphForSourceNode(int sourceNodeId);
static void LockLockData(void);
static void UnlockLockData(void);
static void AddEdgesForLockWaits(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddEdgesForWaitQueue(WaitGraph *waitGraph, PGPROC *waitingProc,
								 PROCStack *remaining);
static void AddWaitEdge(WaitGraph *waitGraph, PGPROC *waitingProc, PGPROC *blockingProc,
						PROCStack *remaining);
static WaitEdge * AllocWaitEdge(WaitGraph *waitGraph);
static bool IsProcessWaitingForLock(PGPROC *proc);
static bool IsSameLockGroup(PGPROC *leftProc, PGPROC *rightProc);
static bool IsConflictingLockMask(int holdMask, int conflictMask);
static bool IsInDistributedTransaction(BackendData *backendData);


PG_FUNCTION_INFO_V1(dump_local_wait_edges);


/*
 * dump_local_wait_edges returns wait edges for distributed transactions
 * running on the node on which it is called, which originate from the source node.
 */
Datum
dump_local_wait_edges(PG_FUNCTION_ARGS)
{
	int32 sourceNodeId = PG_GETARG_INT32(0);

	WaitGraph *waitGraph = BuildWaitGraphForSourceNode(sourceNodeId);
	ReturnWaitGraph(waitGraph, fcinfo);

	return (Datum) 0;
}


/*
 * ReturnWaitGraph returns a wait graph for a set returning function.
 */
static void
ReturnWaitGraph(WaitGraph *waitGraph, FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupdesc = NULL;
	Tuplestorestate *tupstore = NULL;
	MemoryContext per_query_ctx = NULL;
	MemoryContext oldcontext = NULL;
	size_t curEdgeNum = 0;

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

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}


/*
 * BuildWaitGraphForSourceNode builds a wait graph for distributed transactions
 * that originate from the given source node.
 */
static WaitGraph *
BuildWaitGraphForSourceNode(int sourceNodeId)
{
	WaitGraph *waitGraph = NULL;
	int curBackend = 0;
	bool visitedProcs[MaxBackends];
	PROCStack remaining;

	memset(visitedProcs, 0, MaxBackends);

	/*
	 * Try hard to avoid allocations while holding lock. Thus we pre-allocate
	 * space for locks in large batches - for common scenarios this should be
	 * more than enough space to build the list of wait edges without a single
	 * allocation.
	 */
	waitGraph = (WaitGraph *) palloc0(sizeof(WaitGraph));
	waitGraph->localNodeId = GetLocalGroupId();
	waitGraph->allocatedSize = MaxBackends * 3;
	waitGraph->edgeCount = 0;
	waitGraph->edges = (WaitEdge *) palloc(waitGraph->allocatedSize * sizeof(WaitEdge));

	remaining.procs = (PGPROC **) palloc(sizeof(PGPROC *) * MaxBackends);
	remaining.procCount = 0;

	LockLockData();

	/*
	 * Build lock-graph.  We do so by first finding all procs which we are
	 * interested in (originating on our source system, and blocked).  Once
	 * those are collected, do depth first search over all procs blocking
	 * those.  To avoid redundantly visiting procs, keep track of which procs
	 * already have been visited in a pgproc-indexed visitedProcs[] array.
	 */

	/* build list of starting procs */
	for (curBackend = 0; curBackend < MaxBackends; curBackend++)
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
		 * Only start searching from distributed transactions originating on the source
		 * node. Other deadlocks may exist, but the source node can only resolve those
		 * that involve its own transactions.
		 */
		if (sourceNodeId != currentBackendData.transactionId.initiatorNodeIdentifier ||
			!IsInDistributedTransaction(&currentBackendData))
		{
			continue;
		}

		/* skip if the process is not blocked */
		if (!IsProcessWaitingForLock(currentProc))
		{
			continue;
		}

		remaining.procs[remaining.procCount++] = currentProc;
	}

	while (remaining.procCount > 0)
	{
		PGPROC *waitingProc = remaining.procs[--remaining.procCount];

		/*
		 * We might find a process again if multiple distributed transactions are
		 * waiting for it, but we add all edges on the first visit so we don't need
		 * to visit it again. This also avoids getting into an infinite loop in
		 * case of a local deadlock.
		 */
		if (visitedProcs[waitingProc->pgprocno])
		{
			continue;
		}

		visitedProcs[waitingProc->pgprocno] = true;

		/* only blocked processes result in wait edges */
		if (!IsProcessWaitingForLock(waitingProc))
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
 * LockLockData takes locks the shared lock data structure, which prevents
 * concurrent lock acquisitions/releases.
 */
static void
LockLockData(void)
{
	int partitionNum = 0;

	for (partitionNum = 0; partitionNum < NUM_LOCK_PARTITIONS; partitionNum++)
	{
		LWLockAcquire(LockHashPartitionLockByIndex(partitionNum), LW_SHARED);
	}
}


/*
 * UnlockLockData unlocks the locks on the shared lock data structure in reverse
 * order since LWLockRelease searches the given lock from the end of the
 * held_lwlocks array.
 */
static void
UnlockLockData(void)
{
	int partitionNum = 0;

	for (partitionNum = NUM_LOCK_PARTITIONS - 1; partitionNum >= 0; partitionNum--)
	{
		LWLockRelease(LockHashPartitionLockByIndex(partitionNum));
	}
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

		/* skip processes from the same lock group and ones that don't conflict */
		if (!IsSameLockGroup(waitingProc, currentProc) &&
			IsConflictingLockMask(procLock->holdMask, conflictMask))
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

		/* skip processes from the same lock group and ones that don't conflict */
		if (!IsSameLockGroup(waitingProc, currentProc) &&
			IsConflictingLockMask(awaitMask, conflictMask))
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

	curEdge->isBlockingXactWaiting = IsProcessWaitingForLock(blockingProc);
	if (curEdge->isBlockingXactWaiting)
	{
		remaining->procs[remaining->procCount++] = blockingProc;
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
 * IsProcessWaitingForLock returns whether a given process is waiting for a lock.
 */
static bool
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
static bool
IsInDistributedTransaction(BackendData *backendData)
{
	return backendData->transactionId.transactionNumber != 0;
}
