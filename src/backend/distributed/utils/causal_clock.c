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

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/typcache.h"

#include "nodes/pg_list.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
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
#include "distributed/type_utils.h"

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
	uint64 clusterClockValue;

	/* Tracks initialization at boot */
	ClockState clockInitialized;
} LogicalClockShmemData;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static LogicalClockShmemData *logicalClockShmem = NULL;
static uint64 GetEpochTimeMs(void);
static void AdjustLocalClock(uint64 remoteLogicalClock,
							 uint32 remoteCounterClock);
static uint64 GetNextClusterClockValue(void);
static uint64 GetHighestClockInTransaction(List *nodeConnectionList);
static void AdjustClocksToTransactionHighest(List *nodeConnectionList,
											 uint64 transactionClockValue);
static void LogTransactionCommitClock(char *transactionId, uint64 transactionClockValue);
static uint64 * ExecuteQueryAndReturnBigIntCols(char *query, int resultSize, int
												spiok_type);
static bool IsClockAfter(uint64 logicalClock1, uint32 counterClock1,
						 uint64 logicalClock2, uint32 counterClock2);
bool EnableClusterClock = false;


/*
 * GetEpochTimeMs returns the epoch value in milliseconds.
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
		logicalClockShmem->clusterClockValue = 0;
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
 * GetNextClusterClock implements the internal guts of the UDF citus_get_cluster_clock()
 */
uint64
GetNextClusterClockValue(void)
{
	uint64 wallClockValue;
	uint64 epochValue = GetEpochTimeMs();

	SpinLockAcquire(&logicalClockShmem->clockMutex);

	/* Check if the clock is adjusted after the boot */
	if (logicalClockShmem->clockInitialized == CLOCKSTATE_UNINITIALIZED)
	{
		SpinLockRelease(&logicalClockShmem->clockMutex);
		Assert(logicalClockShmem->clusterClockValue == 0);
		ereport(ERROR, (errmsg("backend never adjusted the clock please retry")));
	}

	/* Set the epoch in (lc, c) format */
	SET_CLOCK(wallClockValue, epochValue, 0);

	uint64 nextClusterClockValue = Max(logicalClockShmem->clusterClockValue + 1,
									   wallClockValue);

	logicalClockShmem->clusterClockValue = nextClusterClockValue;

	SpinLockRelease(&logicalClockShmem->clockMutex);

	return nextClusterClockValue;
}


/*
 * IsClockAfter implements the internal guts of the UDF citus_is_clock_after()
 */
static bool
IsClockAfter(uint64 logicalClock1, uint32 counterClock1,
			 uint64 logicalClock2, uint32 counterClock2)
{
	ereport(DEBUG1, (errmsg(
						 "clock1 @ LC:%lu, C:%u, "
						 "clock2 @ LC:%lu, C:%u",
						 logicalClock1, counterClock1,
						 logicalClock2, counterClock2)));

	if (logicalClock1 != logicalClock2)
	{
		return (logicalClock1 > logicalClock2);
	}
	else
	{
		return (counterClock1 > counterClock2);
	}
}


/*
 * AdjustLocalClock Adjusts the local shared memory clock to the
 * received value from the remote node.
 */
void
AdjustLocalClock(uint64 remoteLogicalClock, uint32 remoteCounterClock)
{
	uint64 remoteClusterClock;

	SET_CLOCK(remoteClusterClock, remoteLogicalClock, remoteCounterClock);

	SpinLockAcquire(&logicalClockShmem->clockMutex);

	uint64 currentLogicalClock = GET_LOGICAL(logicalClockShmem->clusterClockValue);
	uint64 currentCounterClock = GET_COUNTER(logicalClockShmem->clusterClockValue);

	if (remoteLogicalClock < currentLogicalClock)
	{
		/* local clock is ahead, do nothing */
		SpinLockRelease(&logicalClockShmem->clockMutex);
		return;
	}

	if (remoteLogicalClock > currentLogicalClock)
	{
		ereport(DEBUG1, (errmsg("adjusting to remote clock "
								"logical(%lu) counter(%u)",
								remoteLogicalClock,
								remoteCounterClock)));

		/* Pick the remote value */
		logicalClockShmem->clusterClockValue = remoteClusterClock;
		SpinLockRelease(&logicalClockShmem->clockMutex);
		return;
	}

	/*
	 * Both the logical clock values are equal, pick the larger counter.
	 */
	if (remoteCounterClock > currentCounterClock)
	{
		ereport(DEBUG1, (errmsg("both logical clock values are "
								"equal(%lu), pick remote's counter (%u) "
								"since it's greater",
								remoteLogicalClock, remoteCounterClock)));
		logicalClockShmem->clusterClockValue = remoteClusterClock;
	}

	SpinLockRelease(&logicalClockShmem->clockMutex);
}


/*
 * GetHighestClockInTransaction takes the connection list of participating nodes in the
 * current transaction and polls the logical clock value of all the nodes. Returns the
 * highest logical clock value of all the nodes in the current distributed transaction,
 * which may be used as commit order for individual objects in the transaction.
 */
static uint64
GetHighestClockInTransaction(List *nodeConnectionList)
{
	/* get clock value of the local node */
	uint64 globalClockValue = GetNextClusterClockValue();

	ereport(DEBUG1, (errmsg("coordinator transaction clock %lu:%u",
							GET_LOGICAL(globalClockValue),
							(uint32) GET_COUNTER(globalClockValue))));

	/* get clock value from each node */
	MultiConnection *connection = NULL;

	foreach_ptr(connection, nodeConnectionList)
	{
		int querySent = SendRemoteCommand(connection,
										  "SELECT logical, counter FROM citus_get_cluster_clock();");
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

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
		if (colCount != 2 || rowCount != 1)
		{
			ereport(ERROR,
					(errmsg("unexpected result from citus_get_cluster_clock()")));
		}

		uint64 logical = ParseIntField(result, 0, 0);
		uint32 counter = ParseIntField(result, 0, 1);
		uint64 nodeClockValue;

		SET_CLOCK(nodeClockValue, logical, counter);

		ereport(DEBUG1, (errmsg(
							 "node(%lu:%u) transaction clock %lu:%u",
							 connection->connectionId, connection->port,
							 logical, counter)));

		if (nodeClockValue > globalClockValue)
		{
			globalClockValue = nodeClockValue;
		}

		PQclear(result);
		ForgetResults(connection);
	}

	ereport(DEBUG1,
			(errmsg("final global transaction clock %lu:%u",
					GET_LOGICAL(globalClockValue),
					(uint32) GET_COUNTER(globalClockValue))));

	return globalClockValue;
}


/*
 * AdjustClocksToTransactionHighest Sets the clock value of all the nodes, participated
 * in the PREPARE of the transaction, to the highest clock value of all the nodes.
 */
static void
AdjustClocksToTransactionHighest(List *nodeConnectionList, uint64 transactionClockValue)
{
	StringInfo queryToSend = makeStringInfo();
	uint64 transactionLogicalClock = GET_LOGICAL(transactionClockValue);
	uint32 transactionCounterClock = GET_COUNTER(transactionClockValue);

	/* Set the adjusted value locally */
	AdjustLocalClock(transactionLogicalClock, transactionCounterClock);

	/* Set the clock value on participating worker nodes */
	MultiConnection *connection = NULL;
	appendStringInfo(queryToSend,
					 "SELECT pg_catalog.citus_internal_adjust_local_clock_to_remote"
					 "((%lu, %u)::pg_catalog.cluster_clock);",
					 transactionLogicalClock, transactionCounterClock);

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
	uint64 transactionClockValue = GetHighestClockInTransaction(transactionNodeList);

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
LogTransactionCommitClock(char *transactionId, uint64 transactionClockValue)
{
	Datum values[Natts_pg_dist_commit_transaction];
	bool isNulls[Natts_pg_dist_commit_transaction];
	uint64 clockLogical = GET_LOGICAL(transactionClockValue);
	uint32 clockCounter = GET_COUNTER(transactionClockValue);

	ereport(DEBUG1, (errmsg("persisting transaction %s with "
							"clock logical (%lu) and counter(%u)",
							transactionId, clockLogical, clockCounter)));

	/* form new transaction tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	uint64 transactionTimestamp = GetEpochTimeMs();

	/* Fetch the tuple description of "cluster_clock" type */
	Oid schemaId = get_namespace_oid("pg_catalog", false);
	Oid clusterClockTypeOid = TypeOid(schemaId, "cluster_clock");
	TupleDesc clockTupDesc = lookup_rowtype_tupdesc_copy(clusterClockTypeOid, -1);

	values[0] = Int64GetDatum(clockLogical);
	values[1] = Int32GetDatum(clockCounter);
	HeapTuple clockTuple = heap_form_tuple(clockTupDesc, values, isNulls);

	values[Anum_pg_dist_commit_transaction_transaction_id - 1] =
		CStringGetTextDatum(transactionId);
	values[Anum_pg_dist_commit_transaction_cluster_clock - 1] =
		HeapTupleGetDatum(clockTuple);
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
void
InitClockAtBoot(void)
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

	/*Assert(logicalClockShmem->clockInitialized == CLOCKSTATE_UNINITIALIZED);*/

	/*
	 * Set the flag before executing a distributed query
	 * (else it might trigger this routine recursively.)
	 */
	logicalClockShmem->clockInitialized = CLOCKSTATE_INIT_INPROGRESS;

	SpinLockRelease(&logicalClockShmem->clockMutex);

	PG_TRY();
	{
		/* Start with the wall clock value */
		uint64 epochValue = GetEpochTimeMs();

		SpinLockAcquire(&logicalClockShmem->clockMutex);
		SET_CLOCK(logicalClockShmem->clusterClockValue, epochValue, 0);
		SpinLockRelease(&logicalClockShmem->clockMutex);

		/*
		 * Version checking is a must for the below code as it accesses the
		 * new catalog created in the new version.
		 * Note: If we move this code to the top of the routine it will cause an
		 * infinite loop with CitusHasBeenLoaded() calling this routine again.
		 */
		bool savedEnableVersionChecks = EnableVersionChecks;
		EnableVersionChecks = true;

		if (!CheckCitusVersion(NOTICE))
		{
			/* Reset the CLOCKSTATE_INIT_INPROGRESS */
			SpinLockAcquire(&logicalClockShmem->clockMutex);
			logicalClockShmem->clockInitialized = CLOCKSTATE_UNINITIALIZED;
			SET_CLOCK(logicalClockShmem->clusterClockValue, 0, 0);
			SpinLockRelease(&logicalClockShmem->clockMutex);

			EnableVersionChecks = savedEnableVersionChecks;

			return;
		}

		EnableVersionChecks = savedEnableVersionChecks;

		/*
		 * Select the highest clock value persisted in the catalog.
		 *
		 * SELECT i, MAX(j) FROM <tab>
		 * WHERE i = (SELECT MAX(i) FROM <tab>)
		 * GROUP BY i
		 */
		uint32 numCols = 2;
		uint64 *results = ExecuteQueryAndReturnBigIntCols(
			"SELECT (cluster_clock_value).logical, (cluster_clock_value).counter FROM "
			"pg_catalog.pg_dist_commit_transaction ORDER BY 1 DESC, "
			"2 DESC LIMIT 1;", numCols, SPI_OK_SELECT);

		if (results != NULL)
		{
			uint64 logicalMaxClock = results[0];
			uint32 counterMaxClock = results[1];
			ereport(DEBUG1, (errmsg("adjusted the clock with value persisted"
									"logical(%lu) and counter(%u)",
									logicalMaxClock, counterMaxClock)));

			/*
			 * Adjust the local clock according to the most recent
			 * clock stamp value persisted in the catalog.
			 */
			AdjustLocalClock(logicalMaxClock, counterMaxClock);
		}

		/*
		 * NULL results indicate no prior commit timestamps on this node, start
		 * from the wall clock.
		 */
	}
	PG_CATCH();
	{
		SpinLockAcquire(&logicalClockShmem->clockMutex);
		logicalClockShmem->clockInitialized = CLOCKSTATE_UNINITIALIZED;
		SET_CLOCK(logicalClockShmem->clusterClockValue, 0, 0);
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
static uint64 *
ExecuteQueryAndReturnBigIntCols(char *query, int resultSize, int spiok_type)
{
	/* Allocate in caller's context */
	uint64 *results = (uint64 *) palloc(resultSize * sizeof(uint64));

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

	for (int i = 0; i < resultSize; i++)
	{
		bool isNull = false;
		results[i] = DatumGetInt64(
			SPI_getbinval(SPI_tuptable->vals[0],         /* First row */
						  SPI_tuptable->tupdesc,
						  i + 1, /* 'i+1' column */
						  &isNull));
	}

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}

	return results;
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

	TupleDesc tupleDescriptor = NULL;
	Datum values[2];
	bool isNull[2];

	/* build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	uint64 clusterClockValue = GetNextClusterClockValue();

	memset(values, 0, sizeof(values));
	memset(isNull, false, sizeof(isNull));

	values[0] = Int64GetDatum(GET_LOGICAL(clusterClockValue)); /* LC */
	values[1] = Int32GetDatum(GET_COUNTER(clusterClockValue)); /* C */

	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNull);
	PG_RETURN_DATUM(HeapTupleGetDatum(heapTuple));
}


/*
 * citus_internal_adjust_local_clock_to_remote is an internal UDF to adjust
 * the local clock to the highest in the cluster.
 */
Datum
citus_internal_adjust_local_clock_to_remote(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
	bool isNull[2];

	memset(isNull, false, sizeof(isNull));

	/* Argument is of complex type (logical, counter) */
	uint64 logicalClock =
		DatumGetInt64(GetAttributeByName(record, "logical", &isNull[0]));
	uint32 counterClock =
		DatumGetUInt32(GetAttributeByName(record, "counter", &isNull[1]));

	if (isNull[0] || isNull[1])
	{
		ereport(ERROR, (errmsg("parameters can't be NULL")));
	}

	AdjustLocalClock(logicalClock, counterClock);

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
	HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
	bool isNull[2];

	memset(isNull, false, sizeof(isNull));

	/* Argument is of complex type (logical, counter) */
	uint64 logical1 = DatumGetInt64(GetAttributeByName(record1, "logical", &isNull[0]));
	uint32 counter1 = DatumGetUInt32(GetAttributeByName(record1, "counter", &isNull[1]));

	if (isNull[0] || isNull[1])
	{
		ereport(ERROR, (errmsg("parameters can't be NULL")));
	}

	/* Argument is of complex type (logical, counter) */
	uint64 logical2 = DatumGetInt64(GetAttributeByName(record2, "logical", &isNull[0]));
	uint32 counter2 = DatumGetUInt32(GetAttributeByName(record2, "counter", &isNull[1]));

	if (isNull[0] || isNull[1])
	{
		ereport(ERROR, (errmsg("parameters can't be NULL")));
	}

	bool result = IsClockAfter(logical1, counter1, logical2, counter2);

	PG_RETURN_BOOL(result);
}
