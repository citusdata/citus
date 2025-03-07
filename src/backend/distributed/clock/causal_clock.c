/*-------------------------------------------------------------------------
 * causal_clock.c
 *
 * Core function defintions to implement hybrid logical clock.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "catalog/namespace.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "executor/spi.h"
#include "nodes/pg_list.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/typcache.h"

#include "distributed/causal_clock.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"

#define SAVE_AND_PERSIST(c) \
	do { \
		Oid savedUserId = InvalidOid; \
		int savedSecurityContext = 0; \
		LogicalClockShmem->clusterClockValue = *(c); \
		GetUserIdAndSecContext(&savedUserId, &savedSecurityContext); \
		SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE); \
		DirectFunctionCall2(setval_oid, \
							ObjectIdGetDatum(DistClockLogicalSequenceId()), \
							Int64GetDatum((c)->logical)); \
		SetUserIdAndSecContext(savedUserId, savedSecurityContext); \
	} while (0)

PG_FUNCTION_INFO_V1(citus_get_node_clock);
PG_FUNCTION_INFO_V1(citus_internal_adjust_local_clock_to_remote);
PG_FUNCTION_INFO_V1(citus_is_clock_after);
PG_FUNCTION_INFO_V1(citus_get_transaction_clock);

/*
 * Current state of the logical clock
 */
typedef enum ClockState
{
	CLOCKSTATE_INITIALIZED,
	CLOCKSTATE_UNINITIALIZED
} ClockState;

/*
 * Holds the cluster clock variables in shared memory.
 */
typedef struct LogicalClockShmemData
{
	NamedLWLockTranche namedLockTranche;
	LWLock clockLock;

	/* Current logical clock value of this node */
	ClusterClock clusterClockValue;

	/* Tracks initialization at boot */
	ClockState clockInitialized;
} LogicalClockShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static LogicalClockShmemData *LogicalClockShmem = NULL;
static void AdjustLocalClock(ClusterClock *remoteClock);
static void GetNextNodeClockValue(ClusterClock *nextClusterClockValue);
static ClusterClock * GetHighestClockInTransaction(List *nodeConnectionList);
static void AdjustClocksToTransactionHighest(List *nodeConnectionList,
											 ClusterClock *transactionClockValue);
static void InitClockAtFirstUse(void);
static void IncrementClusterClock(ClusterClock *clusterClock);
static ClusterClock * LargerClock(ClusterClock *clock1, ClusterClock *clock2);
static ClusterClock * PrepareAndSetTransactionClock(void);
bool EnableClusterClock = true;


/*
 * GetEpochTimeAsClock returns the epoch value milliseconds used as logical
 * value in ClusterClock.
 */
ClusterClock *
GetEpochTimeAsClock(void)
{
	struct timeval tp = { 0 };

	gettimeofday(&tp, NULL);

	uint64 result = (uint64) (tp.tv_sec) * 1000;
	result = result + (uint64) (tp.tv_usec) / 1000;

	ClusterClock *epochClock = (ClusterClock *) palloc(sizeof(ClusterClock));
	epochClock->logical = result;
	epochClock->counter = 0;

	return epochClock;
}


/*
 * LogicalClockShmemSize returns the size that should be allocated
 * in the shared memory for logical clock management.
 */
size_t
LogicalClockShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(LogicalClockShmemData));

	return size;
}


/*
 * InitializeClusterClockMem reserves shared-memory space needed to
 * store LogicalClockShmemData, and sets the hook for initialization
 * of the same.
 */
void
InitializeClusterClockMem(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = LogicalClockShmemInit;
}


/*
 * LogicalClockShmemInit Allocates and initializes shared memory for
 * cluster clock related variables.
 */
void
LogicalClockShmemInit(void)
{
	bool alreadyInitialized = false;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	LogicalClockShmem = (LogicalClockShmemData *)
						ShmemInitStruct("Logical Clock Shmem",
										LogicalClockShmemSize(),
										&alreadyInitialized);

	if (!alreadyInitialized)
	{
		/* A zero value indicates that the clock is not adjusted yet */
		memset(&LogicalClockShmem->clusterClockValue, 0, sizeof(ClusterClock));

		LogicalClockShmem->namedLockTranche.trancheName = "Cluster Clock Setup Tranche";
		LogicalClockShmem->namedLockTranche.trancheId = LWLockNewTrancheId();
		LWLockRegisterTranche(LogicalClockShmem->namedLockTranche.trancheId,
							  LogicalClockShmem->namedLockTranche.trancheName);
		LWLockInitialize(&LogicalClockShmem->clockLock,
						 LogicalClockShmem->namedLockTranche.trancheId);

		LogicalClockShmem->clockInitialized = CLOCKSTATE_UNINITIALIZED;
	}

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * IncrementClusterClock increments the ClusterClock by 1.
 */
static void
IncrementClusterClock(ClusterClock *clusterClock)
{
	/*
	 * It's the counter that always ticks, once it reaches the maximum, reset
	 * the counter to 1 and increment the logical clock.
	 */
	if (clusterClock->counter == MAX_COUNTER)
	{
		clusterClock->logical++;
		clusterClock->counter = 0;
		return;
	}

	clusterClock->counter++;
}


/*
 * LargerClock compares two ClusterClock(s) and returns pointer to the larger one.
 * Note: If equal or one of the clock is NULL, non NULL clock is copied.
 */
static ClusterClock *
LargerClock(ClusterClock *clock1, ClusterClock *clock2)
{
	/* Check if one of the paramater is NULL */
	if (!clock1 || !clock2)
	{
		Assert(clock1 || clock2);
		return (!clock1 ? clock2 : clock1);
	}

	if (cluster_clock_cmp_internal(clock1, clock2) > 0)
	{
		return clock1;
	}
	else
	{
		return clock2;
	}
}


/*
 * GetNextNodeClock implements the internal guts of the UDF citus_get_node_clock()
 */
static void
GetNextNodeClockValue(ClusterClock *nextClusterClockValue)
{
	static bool isClockInitChecked = false; /* serves as a local cache */
	ClusterClock *epochValue = GetEpochTimeAsClock();

	/* If this backend already checked for initialization, skip it */
	if (!isClockInitChecked)
	{
		InitClockAtFirstUse();

		/* We reach here only if CLOCKSTATE_INITIALIZED, all other cases error out. */
		isClockInitChecked = true;
	}

	LWLockAcquire(&LogicalClockShmem->clockLock, LW_EXCLUSIVE);

	Assert(LogicalClockShmem->clockInitialized == CLOCKSTATE_INITIALIZED);

	/* Tick the clock */
	IncrementClusterClock(&LogicalClockShmem->clusterClockValue);

	/* Pick the larger of the two, wallclock and logical clock. */
	ClusterClock *clockValue = LargerClock(&LogicalClockShmem->clusterClockValue,
										   epochValue);

	/*
	 * Save the returned value in both the shared memory and sequences.
	 */
	SAVE_AND_PERSIST(clockValue);

	LWLockRelease(&LogicalClockShmem->clockLock);

	/* Return the clock */
	*nextClusterClockValue = *clockValue;
}


/*
 * AdjustLocalClock Adjusts the local shared memory clock to the
 * received value from the remote node.
 */
void
AdjustLocalClock(ClusterClock *remoteClock)
{
	LWLockAcquire(&LogicalClockShmem->clockLock, LW_EXCLUSIVE);

	ClusterClock *localClock = &LogicalClockShmem->clusterClockValue;

	/* local clock is ahead or equal, do nothing */
	if (cluster_clock_cmp_internal(localClock, remoteClock) >= 0)
	{
		LWLockRelease(&LogicalClockShmem->clockLock);
		return;
	}

	SAVE_AND_PERSIST(remoteClock);

	LWLockRelease(&LogicalClockShmem->clockLock);

	ereport(DEBUG1, (errmsg("adjusted to remote clock: "
							"<logical(%lu) counter(%u)>",
							remoteClock->logical,
							remoteClock->counter)));
}


/*
 * GetHighestClockInTransaction takes the connection list of participating nodes in the
 * current transaction and polls the logical clock value of all the nodes. Returns the
 * highest logical clock value of all the nodes in the current distributed transaction,
 * which may be used as commit order for individual objects in the transaction.
 */
static ClusterClock *
GetHighestClockInTransaction(List *nodeConnectionList)
{
	MultiConnection *connection = NULL;

	foreach_declared_ptr(connection, nodeConnectionList)
	{
		int querySent =
			SendRemoteCommand(connection, "SELECT citus_get_node_clock();");

		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* Check for the current node */
	ClusterClock *globalClockValue = (ClusterClock *) palloc(sizeof(ClusterClock));

	GetNextNodeClockValue(globalClockValue);

	ereport(DEBUG1, (errmsg("node(%u) transaction clock %lu:%u",
							PostPortNumber, globalClockValue->logical,
							globalClockValue->counter)));

	/* fetch the results and pick the highest clock value of all the nodes */
	foreach_declared_ptr(connection, nodeConnectionList)
	{
		bool raiseInterrupts = true;

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			ereport(ERROR, (errmsg("connection to %s:%d failed when "
								   "fetching logical clock value",
								   connection->hostname, connection->port)));
		}

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		ClusterClock *nodeClockValue = ParseClusterClockPGresult(result, 0, 0);

		ereport(DEBUG1, (errmsg("node(%u) transaction clock %lu:%u",
								connection->port, nodeClockValue->logical,
								nodeClockValue->counter)));

		globalClockValue = LargerClock(globalClockValue, nodeClockValue);

		PQclear(result);
		ForgetResults(connection);
	}

	ereport(DEBUG1, (errmsg("final global transaction clock %lu:%u",
							globalClockValue->logical,
							globalClockValue->counter)));
	return globalClockValue;
}


/*
 * AdjustClocksToTransactionHighest Sets the clock value of all the nodes, participated
 * in the PREPARE of the transaction, to the highest clock value of all the nodes.
 */
static void
AdjustClocksToTransactionHighest(List *nodeConnectionList,
								 ClusterClock *transactionClockValue)
{
	StringInfo queryToSend = makeStringInfo();

	/* Set the clock value on participating worker nodes */
	appendStringInfo(queryToSend,
					 "SELECT pg_catalog.citus_internal_adjust_local_clock_to_remote"
					 "('(%lu, %u)'::pg_catalog.cluster_clock);",
					 transactionClockValue->logical, transactionClockValue->counter);

	ExecuteRemoteCommandInConnectionList(nodeConnectionList, queryToSend->data);
	AdjustLocalClock(transactionClockValue);
}


/*
 * PrepareAndSetTransactionClock polls all the transaction-nodes for their respective clocks,
 * picks the highest clock and returns it via UDF citus_get_transaction_clock. All the nodes
 * will now move to this newly negotiated clock.
 */
static ClusterClock *
PrepareAndSetTransactionClock(void)
{
	if (!EnableClusterClock)
	{
		/* citus.enable_cluster_clock is false */
		ereport(WARNING, (errmsg("GUC enable_cluster_clock is off")));
		return NULL;
	}

	dlist_iter iter;
	List *transactionNodeList = NIL;
	List *nodeList = NIL;

	/* Prepare the connection list */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		WorkerNode *workerNode = FindWorkerNode(connection->hostname, connection->port);

		/* Skip the node if we already in the list */
		if (list_member_int(nodeList, workerNode->groupId))
		{
			continue;
		}

		RemoteTransaction *transaction = &connection->remoteTransaction;

		Assert(transaction->transactionState != REMOTE_TRANS_NOT_STARTED);

		/* Skip a transaction that failed */
		if (transaction->transactionFailed)
		{
			continue;
		}

		nodeList = lappend_int(nodeList, workerNode->groupId);
		transactionNodeList = lappend(transactionNodeList, connection);
	}

	/* Pick the highest logical clock value among all transaction-nodes */
	ClusterClock *transactionClockValue =
		GetHighestClockInTransaction(transactionNodeList);

	/* Adjust all the nodes with the new clock value */
	AdjustClocksToTransactionHighest(transactionNodeList, transactionClockValue);

	return transactionClockValue;
}


/*
 * InitClockAtFirstUse Initializes the shared memory clock value to the highest clock
 * persisted. This will protect from any clock drifts.
 */
static void
InitClockAtFirstUse(void)
{
	LWLockAcquire(&LogicalClockShmem->clockLock, LW_EXCLUSIVE);

	/* Avoid repeated and parallel initialization */
	if (LogicalClockShmem->clockInitialized == CLOCKSTATE_INITIALIZED)
	{
		LWLockRelease(&LogicalClockShmem->clockLock);
		return;
	}

	if (DistClockLogicalSequenceId() == InvalidOid)
	{
		ereport(ERROR, (errmsg("Clock related sequence is missing")));
	}

	/* Start with the wall clock value */
	ClusterClock *epochValue = GetEpochTimeAsClock();
	LogicalClockShmem->clusterClockValue = *epochValue;

	/*  Retrieve the highest clock value persisted in the sequence */
	ClusterClock persistedMaxClock = { 0 };

	/*
	 * We will get one more than the persisted value, but that's harmless and
	 * also very _crucial_ in below scenarios
	 *
	 * 1) As sequences are not transactional, this will protect us from crashes
	 *    after the logical increment and before the counter increment.
	 *
	 * 2) If a clock drifts backwards, we should always start one clock above
	 *    the previous value, though we are not persisting the counter as the
	 *    logical value supersedes the counter, a simple increment of it will
	 *    protect us.
	 *
	 * Note: The first (and every 32nd) call to nextval() consumes 32 values in the
	 * WAL. This is an optimization that postgres does to only have to write a WAL
	 * entry every 32 invocations. Normally this is harmless, however, if the database
	 * gets in a crashloop it could outrun the wall clock, if the database crashes at
	 * a higher rate than once every 32 seconds.
	 *
	 */
	Oid saveUserId = InvalidOid;
	int savedSecurityCtx = 0;

	GetUserIdAndSecContext(&saveUserId, &savedSecurityCtx);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	persistedMaxClock.logical =
		DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(DistClockLogicalSequenceId()));

	SetUserIdAndSecContext(saveUserId, savedSecurityCtx);

	/*
	 * Sequence 1 indicates no prior clock timestamps on this server, retain
	 * the wall clock i.e. no adjustment needed.
	 */
	if (persistedMaxClock.logical != 1)
	{
		ereport(DEBUG1, (errmsg("adjusting the clock with persisted value: "
								"<logical(%lu) and counter(%u)>",
								persistedMaxClock.logical,
								persistedMaxClock.counter)));

		/*
		 * Adjust the local clock according to the most recent
		 * clock stamp value persisted in the catalog.
		 */
		if (cluster_clock_cmp_internal(&persistedMaxClock, epochValue) > 0)
		{
			SAVE_AND_PERSIST(&persistedMaxClock);
			ereport(NOTICE, (errmsg("clock drifted backwards, adjusted to: "
									"<logical(%lu) counter(%u)>",
									persistedMaxClock.logical,
									persistedMaxClock.counter)));
		}
	}

	LogicalClockShmem->clockInitialized = CLOCKSTATE_INITIALIZED;

	LWLockRelease(&LogicalClockShmem->clockLock);
}


/*
 * citus_get_node_clock() is an UDF that returns a monotonically increasing
 * logical clock. Clock guarantees to never go back in value after restarts, and
 * makes best attempt to keep the value close to unix epoch time in milliseconds.
 */
Datum
citus_get_node_clock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ClusterClock *nodeClockValue = (ClusterClock *) palloc(sizeof(ClusterClock));

	GetNextNodeClockValue(nodeClockValue);

	PG_RETURN_POINTER(nodeClockValue);
}


/*
 * citus_internal_adjust_local_clock_to_remote is an internal UDF to adjust
 * the local clock to the highest in the cluster.
 */
Datum
citus_internal_adjust_local_clock_to_remote(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ClusterClock *remoteClock = (ClusterClock *) PG_GETARG_POINTER(0);
	AdjustLocalClock(remoteClock);

	PG_RETURN_VOID();
}


/*
 * citus_is_clock_after is an UDF that accepts logical clock timestamps of
 * two causally related events and returns true if the argument1 happened
 * before argument2.
 */
Datum
citus_is_clock_after(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* Fetch both the arguments */
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	ereport(DEBUG1, (errmsg(
						 "clock1 @ LC:%lu, C:%u, "
						 "clock2 @ LC:%lu, C:%u",
						 clock1->logical, clock1->counter,
						 clock2->logical, clock2->counter)));

	bool result = (cluster_clock_cmp_internal(clock1, clock2) > 0);

	PG_RETURN_BOOL(result);
}


/*
 * citus_get_transaction_clock() is an UDF that returns a transaction timestamp
 * logical clock. Clock returned is the maximum of all transaction-nodes and the
 * all the nodes adjust to the this new clock value.
 */
Datum
citus_get_transaction_clock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ClusterClock *clusterClockValue = PrepareAndSetTransactionClock();

	PG_RETURN_POINTER(clusterClockValue);
}
