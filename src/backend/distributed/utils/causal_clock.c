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
#include "miscadmin.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/typcache.h"

#include "nodes/pg_list.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/s_lock.h"

#include "distributed/causal_clock.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_commit_transaction.h"
#include "distributed/remote_commands.h"
#include "distributed/citus_safe_lib.h"

PG_FUNCTION_INFO_V1(citus_get_cluster_clock);
PG_FUNCTION_INFO_V1(citus_internal_adjust_local_clock_to_remote);
PG_FUNCTION_INFO_V1(citus_is_clock_after);

/*
 * Current state of the logical clock
 */
typedef enum ClockState
{
	CLOCKSTATE_INITIALIZED,
	CLOCKSTATE_UNINITIALIZED,
	CLOCKSTATE_INIT_INPROGRESS,
} ClockState;

/*
 * Holds the cluster clock variables in shared memory.
 */
typedef struct LogicalClockShmemData
{
	slock_t clockMutex;

	/* Current logical clock value of this node */
	ClusterClock clusterClockValue;

	/* Tracks initialization at boot */
	ClockState clockInitialized;
} LogicalClockShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static LogicalClockShmemData *logicalClockShmem = NULL;
static uint64 GetEpochTimeMs(void);
static void AdjustLocalClock(ClusterClock *remoteClock);
static void GetNextClusterClockValue(ClusterClock *nextClusterClockValue);
static ClusterClock * GetHighestClockInTransaction(List *nodeConnectionList);
static void AdjustClocksToTransactionHighest(List *nodeConnectionList,
											 ClusterClock *transactionClockValue);
static void LogTransactionCommitClock(char *transactionId, ClusterClock
									  *transactionClockValue);
static char * ExecuteQueryAndReturnStringResult(char *query, int spiok_type);
static void InitClockAtFirstUse(void);
static void IncrementClusterClock(ClusterClock *clusterClock);
static ClusterClock * LargerClock(ClusterClock *clusterClock1,
								  ClusterClock *clusterClock2);


bool EnableClusterClock = false;


/*
 * GetEpochTimeAsClock returns the epoch value milliseconds used as logical value in ClusterClock.
 */
ClusterClock *
GetEpochTimeAsClock(void)
{
	struct timeval tp;

	gettimeofday(&tp, NULL);

	uint64 result = (uint64) (tp.tv_sec) * 1000;
	result = result + (uint64) (tp.tv_usec) / 1000;

	ClusterClock *epochClock = (ClusterClock *) palloc(sizeof(ClusterClock));
	epochClock->logical = result;
	epochClock->counter = 0;

	return epochClock;
}


/*
 * GetEpochTimeMs returns the epoch value in milliseconds, used as wall-clock timestamp.
 */
static uint64
GetEpochTimeMs(void)
{
	struct timeval tp;

	gettimeofday(&tp, NULL);

	uint64 result = (uint64) (tp.tv_sec) * 1000;
	result = result + (uint64) (tp.tv_usec) / 1000;
	return result;
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
	/* On PG 15 and above, we use shmem_request_hook_type */
	#if PG_VERSION_NUM < PG_VERSION_15

	/* allocate shared memory for pre PG-15 versions */
	if (!IsUnderPostmaster)
	{
		RequestAddinShmemSpace(LogicalClockShmemSize());
	}

	#endif

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

	logicalClockShmem = (LogicalClockShmemData *)
						ShmemInitStruct("Logical Clock Shmem",
										LogicalClockShmemSize(),
										&alreadyInitialized);

	if (!alreadyInitialized)
	{
		/* A zero value indicates that the clock is not adjusted yet */
		memset(&logicalClockShmem->clusterClockValue, 0, sizeof(ClusterClock));
		SpinLockInit(&logicalClockShmem->clockMutex);
		logicalClockShmem->clockInitialized = CLOCKSTATE_UNINITIALIZED;
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
	 * It's the counter that always ticks, once it reaches the maximum, reset the
	 * counter and increment the logical clock.
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
 * LargerClock compares two ClusterClock(s) and returns a pointer to the copy of the larger one.
 */
static ClusterClock *
LargerClock(ClusterClock *clusterClock1, ClusterClock *clusterClock2)
{
	ClusterClock *maxClock = (ClusterClock *) palloc(sizeof(ClusterClock));
	ClusterClock *sourceClock = NULL;

	if (clusterClock1->logical != clusterClock2->logical)
	{
		sourceClock = (clusterClock1->logical > clusterClock2->logical) ? clusterClock1 :
					  clusterClock2;
	}
	else
	{
		sourceClock = (clusterClock1->counter > clusterClock2->counter) ? clusterClock1 :
					  clusterClock2;
	}

	memcpy(maxClock, sourceClock, sizeof(ClusterClock));
	return maxClock;
}


/*
 * GetNextClusterClock implements the internal guts of the UDF citus_get_cluster_clock()
 */
static void
GetNextClusterClockValue(ClusterClock *nextClusterClockValue)
{
	ClusterClock *epochValue = GetEpochTimeAsClock();

	SpinLockAcquire(&logicalClockShmem->clockMutex);

	/* Check if the clock is adjusted after the boot */
	if (logicalClockShmem->clockInitialized == CLOCKSTATE_UNINITIALIZED)
	{
		SpinLockRelease(&logicalClockShmem->clockMutex);
		Assert(logicalClockShmem->clusterClockValue.logical == 0);
		InitClockAtFirstUse();

		SpinLockAcquire(&logicalClockShmem->clockMutex);
		if (logicalClockShmem->clockInitialized == CLOCKSTATE_INIT_INPROGRESS ||
			logicalClockShmem->clockInitialized == CLOCKSTATE_UNINITIALIZED)
		{
			/* Either we lost the initialization-race or there was an exception */
			SpinLockRelease(&logicalClockShmem->clockMutex);
			ereport(ERROR, (errmsg("Clock is in the process of getting initialized, "
								   "please retry")));
		}
		SpinLockRelease(&logicalClockShmem->clockMutex);
	}

	Assert(logicalClockShmem->clockInitialized == CLOCKSTATE_INITIALIZED);

	IncrementClusterClock(&logicalClockShmem->clusterClockValue);
	ClusterClock *clockValue = LargerClock(&logicalClockShmem->clusterClockValue,
										   epochValue);
	logicalClockShmem->clusterClockValue = *clockValue;

	SpinLockRelease(&logicalClockShmem->clockMutex);

	nextClusterClockValue->logical = clockValue->logical;
	nextClusterClockValue->counter = clockValue->counter;
}


/*
 * AdjustLocalClock Adjusts the local shared memory clock to the
 * received value from the remote node.
 */
void
AdjustLocalClock(ClusterClock *remoteClock)
{
	SpinLockAcquire(&logicalClockShmem->clockMutex);

	if (remoteClock->logical < logicalClockShmem->clusterClockValue.logical)
	{
		/* local clock is ahead, do nothing */
		SpinLockRelease(&logicalClockShmem->clockMutex);
		return;
	}

	if (remoteClock->logical > logicalClockShmem->clusterClockValue.logical)
	{
		ereport(DEBUG1, (errmsg("adjusting to remote clock "
								"logical(%lu) counter(%u)",
								remoteClock->logical,
								remoteClock->counter)));

		/* Pick the remote value */
		logicalClockShmem->clusterClockValue.logical = remoteClock->logical;
		logicalClockShmem->clusterClockValue.counter = remoteClock->counter;
		SpinLockRelease(&logicalClockShmem->clockMutex);
		return;
	}

	/*
	 * Both the logical clock values are equal, pick the larger counter.
	 */
	if (remoteClock->counter > logicalClockShmem->clusterClockValue.counter)
	{
		ereport(DEBUG1, (errmsg("both logical clock values are "
								"equal(%lu), pick remote's counter (%u) "
								"since it's greater",
								remoteClock->logical, remoteClock->counter)));
		logicalClockShmem->clusterClockValue.logical = remoteClock->logical;
		logicalClockShmem->clusterClockValue.counter = remoteClock->counter;
	}

	SpinLockRelease(&logicalClockShmem->clockMutex);
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
	/* get clock value from each node including the transaction coordinator */
	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags, LocalHostName,
													PostPortNumber);
	nodeConnectionList = lappend(nodeConnectionList, connection);

	foreach_ptr(connection, nodeConnectionList)
	{
		int querySent = SendRemoteCommand(connection,
										  "SELECT citus_get_cluster_clock();");
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	ClusterClock *globalClockValue = NULL;

	/* fetch the results and pick the highest clock value of all the nodes */
	foreach_ptr(connection, nodeConnectionList)
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

		int32 rowCount = PQntuples(result);
		int32 colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != 1 || rowCount != 1)
		{
			ereport(ERROR,
					(errmsg("unexpected result from citus_get_cluster_clock()")));
		}

		ClusterClock *nodeClockValue = ParseClusterClockPGresult(result, 0, 0);

		ereport(DEBUG1, (errmsg(
							 "node(%lu:%u) transaction clock %lu:%u",
							 connection->connectionId, connection->port,
							 nodeClockValue->logical, nodeClockValue->counter)));

		if (globalClockValue)
		{
			globalClockValue = LargerClock(globalClockValue, nodeClockValue);
		}
		else
		{
			globalClockValue = nodeClockValue;
		}

		PQclear(result);
		ForgetResults(connection);
	}

	ereport(DEBUG1,
			(errmsg("final global transaction clock %lu:%u",
					globalClockValue->logical,
					globalClockValue->counter)));
	return globalClockValue;
}


/*
 * AdjustClocksToTransactionHighest Sets the clock value of all the nodes, participated
 * in the PREPARE of the transaction, to the highest clock value of all the nodes.
 */
static void
AdjustClocksToTransactionHighest(List *nodeConnectionList, ClusterClock
								 *transactionClockValue)
{
	StringInfo queryToSend = makeStringInfo();

	/* Set the adjusted value locally */
	AdjustLocalClock(transactionClockValue);

	/* Set the clock value on participating worker nodes */
	MultiConnection *connection = NULL;
	appendStringInfo(queryToSend,
					 "SELECT pg_catalog.citus_internal_adjust_local_clock_to_remote"
					 "('(%lu, %u)'::pg_catalog.cluster_clock);",
					 transactionClockValue->logical, transactionClockValue->counter);

	foreach_ptr(connection, nodeConnectionList)
	{
		int querySent = SendRemoteCommand(connection, queryToSend->data);

		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* Process the result */
	foreach_ptr(connection, nodeConnectionList)
	{
		bool raiseInterrupts = true;
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);

		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


/*
 * During prepare, once all the nodes acknowledge commit, persist the current
 * transaction id along with the clock value in the catalog.
 */
void
PrepareAndSetTransactionClock(List *transactionNodeList)
{
	if (!EnableClusterClock)
	{
		/* citus.enable_cluster_clock is false */
		return;
	}

	char *transactionId = GetCurrentTransactionIdString();

	/* Pick the highest logical clock value among all transaction-nodes */
	ClusterClock *transactionClockValue =
		GetHighestClockInTransaction(transactionNodeList);

	/* Persist the transactionId along with the logical commit-clock timestamp */
	LogTransactionCommitClock(transactionId, transactionClockValue);

	/* Adjust all the nodes with the new clock value */
	AdjustClocksToTransactionHighest(transactionNodeList, transactionClockValue);
}


/*
 * LogTransactionCommitClock registers the committed transaction along
 * with the commit clock.
 */
static void
LogTransactionCommitClock(char *transactionId, ClusterClock *transactionClockValue)
{
	ereport(DEBUG1, (errmsg("persisting transaction %s with "
							"clock logical (%lu) and counter(%u)",
							transactionId, transactionClockValue->logical,
							transactionClockValue->counter)));

	uint64 transactionTimestamp = GetEpochTimeMs();
	char *clockString = psprintf("(%lu,%u)",
								 transactionClockValue->logical,
								 transactionClockValue->counter);

	Datum values[Natts_pg_dist_commit_transaction] = { 0 };
	bool isNulls[Natts_pg_dist_commit_transaction] = { 0 };
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_commit_transaction_transaction_id - 1] =
		CStringGetTextDatum(transactionId);
	values[Anum_pg_dist_commit_transaction_cluster_clock - 1] =
		CStringGetDatum(clockString);
	values[Anum_pg_dist_commit_transaction_timestamp - 1] =
		Int64GetDatum(transactionTimestamp);

	/* open pg_dist_commit_transaction and insert new tuple */
	Relation pgDistCommitTransaction =
		table_open(DistCommitTransactionRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistCommitTransaction);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistCommitTransaction, heapTuple);

	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	table_close(pgDistCommitTransaction, NoLock);
}


/*
 * Initialize the shared memory clock value to the highest clock
 * persisted. This will protect from any clock drifts.
 */
static void
InitClockAtFirstUse(void)
{
	if (creating_extension)
	{
		/* No catalog present yet */
		return;
	}

	SpinLockAcquire(&logicalClockShmem->clockMutex);

	/* Avoid repeated and parallel initialization */
	if (logicalClockShmem->clockInitialized == CLOCKSTATE_INITIALIZED ||
		logicalClockShmem->clockInitialized == CLOCKSTATE_INIT_INPROGRESS)
	{
		SpinLockRelease(&logicalClockShmem->clockMutex);
		return;
	}

	Assert(logicalClockShmem->clockInitialized == CLOCKSTATE_UNINITIALIZED);

	/*
	 * Set the flag before executing a distributed query
	 * (else it might trigger this routine recursively.)
	 */
	logicalClockShmem->clockInitialized = CLOCKSTATE_INIT_INPROGRESS;

	SpinLockRelease(&logicalClockShmem->clockMutex);

	PG_TRY();
	{
		/* Start with the wall clock value */
		ClusterClock *epochValue = GetEpochTimeAsClock();

		logicalClockShmem->clusterClockValue = *epochValue;

		/*
		 * Select the highest clock value persisted in the catalog.
		 */
		char *results = ExecuteQueryAndReturnStringResult(
			"SELECT cluster_clock_value FROM "
			"pg_catalog.pg_dist_commit_transaction ORDER BY 1 DESC LIMIT 1;",
			SPI_OK_SELECT);

		if (results != NULL)
		{
			ClusterClock *persistedMaxClock = ParseClusterClockFields(results);

			ereport(DEBUG1, (errmsg("adjusted the clock with value persisted"
									"logical(%lu) and counter(%u)",
									persistedMaxClock->logical,
									persistedMaxClock->counter)));

			/*
			 * Adjust the local clock according to the most recent
			 * clock stamp value persisted in the catalog.
			 */
			AdjustLocalClock(persistedMaxClock);
		}

		/*
		 * NULL results indicate no prior commit timestamps on this node, retain
		 * the wall clock.
		 */
	}
	PG_CATCH();
	{
		SpinLockAcquire(&logicalClockShmem->clockMutex);
		logicalClockShmem->clockInitialized = CLOCKSTATE_UNINITIALIZED;
		memset(&logicalClockShmem->clusterClockValue, 0, sizeof(ClusterClock));
		SpinLockRelease(&logicalClockShmem->clockMutex);

		PG_RE_THROW();
	}
	PG_END_TRY();

	SpinLockAcquire(&logicalClockShmem->clockMutex);
	logicalClockShmem->clockInitialized = CLOCKSTATE_INITIALIZED;
	SpinLockRelease(&logicalClockShmem->clockMutex);
}


/*
 * ExecuteQueryAndReturnResults connects to SPI, executes the query and checks
 * if the SPI returned the correct type. Returns an array of int64 results
 * in the caller's memory context.
 */
static char *
ExecuteQueryAndReturnStringResult(char *query, int spiok_type)
{
	/*
	 * Allocate in caller's context, should hold max value
	 * of 44 bits(13 chars) and 22 bits(7 chars) + 3 delimiters.
	 */
	char *clockString = (char *) palloc(24 * sizeof(char));

	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	spiResult = SPI_execute(query, false, 0);
	if (spiResult != spiok_type)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

	if (SPI_processed > 1)
	{
		ereport(ERROR, (errmsg("query(%s) unexpectedly returned "
							   "more than one row", query)));
	}

	if (SPI_processed != 1)
	{
		/* No rows found, it's up to the caller to handle it */
		SPI_finish();
		return NULL;
	}

	char *results = DatumGetCString(
		SPI_getvalue(SPI_tuptable->vals[0],
					 SPI_tuptable->tupdesc, 1));
	strcpy_s(clockString, 24, results);

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}

	return clockString;
}


/*
 * citus_get_cluster_clock() is an UDF that returns a monotonically increasing
 * logical clock. Clock guarantees to never go back in value after restarts, and
 * makes best attempt to keep the value close to unix epoch time in milliseconds.
 */
Datum
citus_get_cluster_clock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	ClusterClock *clusterClockValue = (ClusterClock *) palloc(sizeof(ClusterClock));

	GetNextClusterClockValue(clusterClockValue);

	PG_RETURN_POINTER(clusterClockValue);
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
