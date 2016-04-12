/*-------------------------------------------------------------------------
 *
 * multi_copy.c
 *     This file contains implementation of COPY utility for distributed
 *     tables.
 *
 * The CitusCopyFrom function should be called from the utility hook to
 * process COPY ... FROM commands on distributed tables. CitusCopyFrom
 * parses the input from stdin, a program executed on the master, or a file
 * on the master, and decides into which shard to put the data. It opens a
 * new connection for every shard placement and uses the PQputCopyData
 * function to copy the data. Because PQputCopyData transmits data,
 * asynchronously, the workers will ingest data at least partially in
 * parallel.
 *
 * When failing to connect to a worker, the master marks the placement for
 * which it was trying to open a connection as inactive, similar to the way
 * DML statements are handled. If a failure occurs after connecting, the
 * transaction is rolled back on all the workers.
 *
 * By default, COPY uses normal transactions on the workers. This can cause
 * a problem when some of the transactions fail to commit while others have
 * succeeded. To ensure no data is lost, COPY can use two-phase commit, by
 * increasing max_prepared_transactions on the worker and setting
 * citus.copy_transaction_manager to '2pc'. The default is '1pc'.
 *
 * Parsing options are processed and enforced on the master, while
 * constraints are enforced on the worker. In either case, failure causes
 * the whole COPY to roll back.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * With contributions from Postgres Professional.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "plpgsql.h"

#include <string.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "access/sdir.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "commands/extension.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_transaction.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_protocol.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001


/* the transaction manager to use for COPY commands */
int CopyTransactionManager = TRANSACTION_MANAGER_1PC;

/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;
	List *connectionList;
} ShardConnections;


/* Local functions forward declarations */
static void LockAllShards(List *shardIntervalList);
static HTAB * CreateShardConnectionHash(void);
static int CompareShardIntervalsById(const void *leftElement, const void *rightElement);
static bool IsUniformHashDistribution(ShardInterval **shardIntervalArray,
									  int shardCount);
static FmgrInfo * ShardIntervalCompareFunction(Var *partitionColumn, char
											   partitionMethod);
static ShardInterval * FindShardInterval(Datum partitionColumnValue,
										 ShardInterval **shardIntervalCache,
										 int shardCount, char partitionMethod,
										 FmgrInfo *compareFunction,
										 FmgrInfo *hashFunction, bool useBinarySearch);
static ShardInterval * SearchCachedShardInterval(Datum partitionColumnValue,
												 ShardInterval **shardIntervalCache,
												 int shardCount,
												 FmgrInfo *compareFunction);
static ShardConnections * GetShardConnections(HTAB *shardConnectionHash,
											  int64 shardId,
											  bool *shardConnectionsFound);
static void OpenCopyTransactions(CopyStmt *copyStatement,
								 ShardConnections *shardConnections);
static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId);
static void SendCopyDataToAll(StringInfo dataBuffer, List *connectionList);
static void SendCopyDataToPlacement(StringInfo dataBuffer, PGconn *connection,
									int64 shardId);
static List * ConnectionList(HTAB *connectionHash);
static void EndRemoteCopy(List *connectionList, bool stopOnFailure);
static void ReportCopyError(PGconn *connection, PGresult *result);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);


/*
 * CitusCopyFrom implements the COPY table_name FROM ... for hash-partitioned
 * and range-partitioned tables.
 */
void
CitusCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	Oid tableId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
	char *relationName = get_rel_name(tableId);
	Relation distributedRelation = NULL;
	char partitionMethod = '\0';
	Var *partitionColumn = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TypeCacheEntry *typeEntry = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;

	int shardCount = 0;
	List *shardIntervalList = NULL;
	ShardInterval **shardIntervalCache = NULL;
	bool useBinarySearch = false;

	HTAB *shardConnectionHash = NULL;
	ShardConnections *shardConnections = NULL;
	List *connectionList = NIL;

	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;

	CopyState copyState = NULL;
	CopyOutState copyOutState = NULL;
	FmgrInfo *columnOutputFunctions = NULL;
	uint64 processedRowCount = 0;

	/* disallow COPY to/from file or program except for superusers */
	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
	}

	partitionColumn = PartitionColumn(tableId, 0);
	partitionMethod = PartitionMethod(tableId);
	if (partitionMethod != DISTRIBUTE_BY_RANGE && partitionMethod != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("COPY is only supported for hash- and "
							   "range-partitioned tables")));
	}

	/* resolve hash function for partition column */
	typeEntry = lookup_type_cache(partitionColumn->vartype, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);

	/* resolve compare function for shard intervals */
	compareFunction = ShardIntervalCompareFunction(partitionColumn, partitionMethod);

	/* allocate column values and nulls arrays */
	distributedRelation = heap_open(tableId, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(distributedRelation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/* load the list of shards and verify that we have shards to copy into */
	shardIntervalList = LoadShardIntervalList(tableId);
	if (shardIntervalList == NIL)
	{
		if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find any shards into which to copy"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName),
							errhint("Run master_create_worker_shards to create shards "
									"and try again.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find any shards into which to copy"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName)));
		}
	}

	/* prevent concurrent placement changes and non-commutative DML statements */
	LockAllShards(shardIntervalList);

	/* initialize the shard interval cache */
	shardCount = list_length(shardIntervalList);
	shardIntervalCache = SortedShardIntervalArray(shardIntervalList);

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH ||
		!IsUniformHashDistribution(shardIntervalCache, shardCount))
	{
		useBinarySearch = true;
	}

	/* initialize copy state to read from COPY data source */
	copyState = BeginCopyFrom(distributedRelation,
							  copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	executorState = CreateExecutorState();
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->binary = true;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/*
	 * Create a mapping of shard id to a connection for each of its placements.
	 * The hash should be initialized before the PG_TRY, since it is used and
	 * PG_CATCH. Otherwise, it may be undefined in the PG_CATCH (see sigsetjmp
	 * documentation).
	 */
	shardConnectionHash = CreateShardConnectionHash();

	/* we use a PG_TRY block to roll back on errors (e.g. in NextCopyFrom) */
	PG_TRY();
	{
		ErrorContextCallback errorCallback;

		/* set up callback to identify error line number */
		errorCallback.callback = CopyFromErrorCallback;
		errorCallback.arg = (void *) copyState;
		errorCallback.previous = error_context_stack;
		error_context_stack = &errorCallback;

		/* ensure transactions have unique names on worker nodes */
		InitializeDistributedTransaction();

		while (true)
		{
			bool nextRowFound = false;
			Datum partitionColumnValue = 0;
			ShardInterval *shardInterval = NULL;
			int64 shardId = 0;
			bool shardConnectionsFound = false;
			MemoryContext oldContext = NULL;

			ResetPerTupleExprContext(executorState);

			oldContext = MemoryContextSwitchTo(executorTupleContext);

			/* parse a row from the input */
			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);

			if (!nextRowFound)
			{
				MemoryContextSwitchTo(oldContext);
				break;
			}

			CHECK_FOR_INTERRUPTS();

			/* find the partition column value */

			if (columnNulls[partitionColumn->varattno - 1])
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg("cannot copy row with NULL value "
									   "in partition column")));
			}

			partitionColumnValue = columnValues[partitionColumn->varattno - 1];

			/* find the shard interval and id for the partition column value */
			shardInterval = FindShardInterval(partitionColumnValue, shardIntervalCache,
											  shardCount, partitionMethod,
											  compareFunction, hashFunction,
											  useBinarySearch);
			if (shardInterval == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("could not find shard for partition column "
									   "value")));
			}

			shardId = shardInterval->shardId;

			MemoryContextSwitchTo(oldContext);

			/* get existing connections to the shard placements, if any */
			shardConnections = GetShardConnections(shardConnectionHash,
												   shardId,
												   &shardConnectionsFound);
			if (!shardConnectionsFound)
			{
				/* open connections and initiate COPY on shard placements */
				OpenCopyTransactions(copyStatement, shardConnections);

				/* send binary headers to shard placements */
				resetStringInfo(copyOutState->fe_msgbuf);
				AppendCopyBinaryHeaders(copyOutState);
				SendCopyDataToAll(copyOutState->fe_msgbuf,
								  shardConnections->connectionList);
			}

			/* replicate row to shard placements */
			resetStringInfo(copyOutState->fe_msgbuf);
			AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
							  copyOutState, columnOutputFunctions);
			SendCopyDataToAll(copyOutState->fe_msgbuf, shardConnections->connectionList);

			processedRowCount += 1;
		}

		connectionList = ConnectionList(shardConnectionHash);

		/* send binary footers to all shard placements */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyBinaryFooters(copyOutState);
		SendCopyDataToAll(copyOutState->fe_msgbuf, connectionList);

		/* all lines have been copied, stop showing line number in errors */
		error_context_stack = errorCallback.previous;

		/* close the COPY input on all shard placements */
		EndRemoteCopy(connectionList, true);

		if (CopyTransactionManager == TRANSACTION_MANAGER_2PC)
		{
			PrepareRemoteTransactions(connectionList);
		}

		EndCopyFrom(copyState);
		heap_close(distributedRelation, NoLock);

		/* check for cancellation one last time before committing */
		CHECK_FOR_INTERRUPTS();
	}
	PG_CATCH();
	{
		List *abortConnectionList = NIL;

		/* roll back all transactions */
		abortConnectionList = ConnectionList(shardConnectionHash);
		EndRemoteCopy(abortConnectionList, false);
		AbortRemoteTransactions(abortConnectionList);
		CloseConnections(abortConnectionList);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Ready to commit the transaction, this code is below the PG_TRY block because
	 * we do not want any of the transactions rolled back if a failure occurs. Instead,
	 * they should be rolled forward.
	 */
	CommitRemoteTransactions(connectionList);
	CloseConnections(connectionList);

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * LockAllShards takes shared locks on the metadata and the data of all shards in
 * shardIntervalList. This prevents concurrent placement changes and concurrent
 * DML statements that require an exclusive lock.
 */
static void
LockAllShards(List *shardIntervalList)
{
	ListCell *shardIntervalCell = NULL;

	/* lock shards in order of shard id to prevent deadlock */
	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->shardId;

		/* prevent concurrent changes to number of placements */
		LockShardDistributionMetadata(shardId, ShareLock);

		/* prevent concurrent update/delete statements */
		LockShardResource(shardId, ShareLock);
	}
}


/*
 * CreateShardConnectionHash constructs a hash table used for shardId->Connection
 * mapping.
 */
static HTAB *
CreateShardConnectionHash(void)
{
	HTAB *shardConnectionsHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;

	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;
	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
}


/*
 * CompareShardIntervalsById is a comparison function for sort shard
 * intervals by their shard ID.
 */
static int
CompareShardIntervalsById(const void *leftElement, const void *rightElement)
{
	ShardInterval *leftInterval = *((ShardInterval **) leftElement);
	ShardInterval *rightInterval = *((ShardInterval **) rightElement);
	int64 leftShardId = leftInterval->shardId;
	int64 rightShardId = rightInterval->shardId;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * ShardIntervalCompareFunction returns the appropriate compare function for the
 * partition column type. In case of hash-partitioning, it always returns the compare
 * function for integers.
 */
static FmgrInfo *
ShardIntervalCompareFunction(Var *partitionColumn, char partitionMethod)
{
	FmgrInfo *compareFunction = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		compareFunction = GetFunctionInfo(INT4OID, BTREE_AM_OID, BTORDER_PROC);
	}
	else if (partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		compareFunction = GetFunctionInfo(partitionColumn->vartype,
										  BTREE_AM_OID, BTORDER_PROC);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported partition method %d", partitionMethod)));
	}

	return compareFunction;
}


/*
 * IsUniformHashDistribution determines whether the given list of sorted shards
 * has a uniform hash distribution, as produced by master_create_worker_shards.
 */
static bool
IsUniformHashDistribution(ShardInterval **shardIntervalArray, int shardCount)
{
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;
	int shardIndex = 0;

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);

		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		if (DatumGetInt32(shardInterval->minValue) != shardMinHashToken ||
			DatumGetInt32(shardInterval->maxValue) != shardMaxHashToken)
		{
			return false;
		}
	}

	return true;
}


/*
 * FindShardInterval finds a single shard interval in the cache for the
 * given partition column value.
 */
static ShardInterval *
FindShardInterval(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
				  int shardCount, char partitionMethod, FmgrInfo *compareFunction,
				  FmgrInfo *hashFunction, bool useBinarySearch)
{
	ShardInterval *shardInterval = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		int hashedValue = DatumGetInt32(FunctionCall1(hashFunction,
													  partitionColumnValue));
		if (useBinarySearch)
		{
			shardInterval = SearchCachedShardInterval(Int32GetDatum(hashedValue),
													  shardIntervalCache, shardCount,
													  compareFunction);
		}
		else
		{
			uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;
			int shardIndex = (uint32) (hashedValue - INT32_MIN) / hashTokenIncrement;

			Assert(shardIndex <= shardCount);

			/*
			 * If the shard count is not power of 2, the range of the last
			 * shard becomes larger than others. For that extra piece of range,
			 * we still need to use the last shard.
			 */
			if (shardIndex == shardCount)
			{
				shardIndex = shardCount - 1;
			}

			shardInterval = shardIntervalCache[shardIndex];
		}
	}
	else
	{
		shardInterval = SearchCachedShardInterval(partitionColumnValue,
												  shardIntervalCache, shardCount,
												  compareFunction);
	}

	return shardInterval;
}


/*
 * SearchCachedShardInterval performs a binary search for a shard interval matching a
 * given partition column value and returns it.
 */
static ShardInterval *
SearchCachedShardInterval(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
						  int shardCount, FmgrInfo *compareFunction)
{
	int lowerBoundIndex = 0;
	int upperBoundIndex = shardCount;

	while (lowerBoundIndex < upperBoundIndex)
	{
		int middleIndex = (lowerBoundIndex + upperBoundIndex) / 2;
		int maxValueComparison = 0;
		int minValueComparison = 0;

		minValueComparison = FunctionCall2Coll(compareFunction,
											   DEFAULT_COLLATION_OID,
											   partitionColumnValue,
											   shardIntervalCache[middleIndex]->minValue);

		if (DatumGetInt32(minValueComparison) < 0)
		{
			upperBoundIndex = middleIndex;
			continue;
		}

		maxValueComparison = FunctionCall2Coll(compareFunction,
											   DEFAULT_COLLATION_OID,
											   partitionColumnValue,
											   shardIntervalCache[middleIndex]->maxValue);

		if (DatumGetInt32(maxValueComparison) <= 0)
		{
			return shardIntervalCache[middleIndex];
		}

		lowerBoundIndex = middleIndex + 1;
	}

	return NULL;
}


/*
 * GetShardConnections finds existing connections for a shard in the hash
 * or opens new connections to each active placement and starts a (binary) COPY
 * transaction on each of them.
 */
static ShardConnections *
GetShardConnections(HTAB *shardConnectionHash, int64 shardId,
					bool *shardConnectionsFound)
{
	ShardConnections *shardConnections = NULL;

	shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
														&shardId,
														HASH_ENTER,
														shardConnectionsFound);
	if (!*shardConnectionsFound)
	{
		shardConnections->shardId = shardId;
		shardConnections->connectionList = NIL;
	}

	return shardConnections;
}


/*
 * OpenCopyTransactions opens a connection for each placement of a shard and
 * starts a COPY transaction. If a connection cannot be opened, then the shard
 * placement is marked as inactive and the COPY continues with the remaining
 * shard placements.
 */
static void
OpenCopyTransactions(CopyStmt *copyStatement, ShardConnections *shardConnections)
{
	List *finalizedPlacementList = NIL;
	List *failedPlacementList = NIL;
	ListCell *placementCell = NULL;
	ListCell *failedPlacementCell = NULL;
	List *connectionList = NULL;

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "OpenCopyTransactions",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);

	/* release finalized placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	finalizedPlacementList = FinalizedShardPlacementList(shardConnections->shardId);

	MemoryContextSwitchTo(oldContext);

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;
		TransactionConnection *transactionConnection = NULL;
		StringInfo copyCommand = NULL;
		PGresult *result = NULL;

		PGconn *connection = ConnectToNode(nodeName, nodePort);

		/* release failed placement list and copy command at the end of this function */
		oldContext = MemoryContextSwitchTo(localContext);

		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, placement);
			continue;
		}

		result = PQexec(connection, "BEGIN");
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, placement);
			continue;
		}

		copyCommand = ConstructCopyStatement(copyStatement, shardConnections->shardId);

		result = PQexec(connection, copyCommand->data);
		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			ReportRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, placement);
			continue;
		}

		/* preserve transaction connection in regular memory context */
		MemoryContextSwitchTo(oldContext);

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->connectionId = shardConnections->shardId;
		transactionConnection->transactionState = TRANSACTION_STATE_COPY_STARTED;
		transactionConnection->connection = connection;

		connectionList = lappend(connectionList, transactionConnection);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errmsg("could not find any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
		uint64 shardLength = 0;

		DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
								failedPlacement->nodePort);
		InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
								failedPlacement->nodeName, failedPlacement->nodePort);
	}

	shardConnections->connectionList = connectionList;

	MemoryContextReset(localContext);
}


/*
 * ConstructCopyStatement constructs the text of a COPY statement for a particular
 * shard.
 */
static StringInfo
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId)
{
	StringInfo command = makeStringInfo();
	char *qualifiedName = NULL;

	qualifiedName = quote_qualified_identifier(copyStatement->relation->schemaname,
											   copyStatement->relation->relname);

	appendStringInfo(command, "COPY %s_%ld ", qualifiedName, shardId);

	appendStringInfoString(command, "FROM STDIN WITH (FORMAT BINARY)");

	return command;
}


/*
 * SendCopyDataToAll sends copy data to all connections in a list.
 */
static void
SendCopyDataToAll(StringInfo dataBuffer, List *connectionList)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 shardId = transactionConnection->connectionId;

		SendCopyDataToPlacement(dataBuffer, connection, shardId);
	}
}


/*
 * SendCopyDataToPlacement sends serialized COPY data to a specific shard placement
 * over the given connection.
 */
static void
SendCopyDataToPlacement(StringInfo dataBuffer, PGconn *connection, int64 shardId)
{
	int copyResult = PQputCopyData(connection, dataBuffer->data, dataBuffer->len);
	if (copyResult != 1)
	{
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to shard %ld on %s:%s",
							   shardId, nodeName, nodePort)));
	}
}


/*
 * ConnectionList flattens the connection hash to a list of placement connections.
 */
static List *
ConnectionList(HTAB *connectionHash)
{
	List *connectionList = NIL;
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;

	hash_seq_init(&status, connectionHash);

	shardConnections = (ShardConnections *) hash_seq_search(&status);
	while (shardConnections != NULL)
	{
		List *shardConnectionsList = list_copy(shardConnections->connectionList);
		connectionList = list_concat(connectionList, shardConnectionsList);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
	}

	return connectionList;
}


/*
 * EndRemoteCopy ends the COPY input on all connections. If stopOnFailure
 * is true, then EndRemoteCopy reports an error on failure, otherwise it
 * reports a warning or continues.
 */
static void
EndRemoteCopy(List *connectionList, bool stopOnFailure)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 shardId = transactionConnection->connectionId;
		int copyEndResult = 0;
		PGresult *result = NULL;

		if (transactionConnection->transactionState != TRANSACTION_STATE_COPY_STARTED)
		{
			/* a failure occurred after having previously called EndRemoteCopy */
			continue;
		}

		/* end the COPY input */
		copyEndResult = PQputCopyEnd(connection, NULL);
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;

		if (copyEndResult != 1)
		{
			if (stopOnFailure)
			{
				char *nodeName = ConnectionGetOptionValue(connection, "host");
				char *nodePort = ConnectionGetOptionValue(connection, "port");

				ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
								errmsg("failed to COPY to shard %ld on %s:%s",
									   shardId, nodeName, nodePort)));
			}

			continue;
		}

		/* check whether there were any COPY errors */
		result = PQgetResult(connection);
		if (PQresultStatus(result) != PGRES_COMMAND_OK && stopOnFailure)
		{
			ReportCopyError(connection, result);
		}

		PQclear(result);
	}
}


/*
 * ReportCopyError tries to report a useful error message for the user from
 * the remote COPY error messages.
 */
static void
ReportCopyError(PGconn *connection, PGresult *result)
{
	char *remoteMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);

	if (remoteMessage != NULL)
	{
		/* probably a constraint violation, show remote message and detail */
		char *remoteDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);

		ereport(ERROR, (errmsg("%s", remoteMessage),
						errdetail("%s", remoteDetail)));
	}
	else
	{
		/* probably a connection problem, get the message from the connection */
		char *lastNewlineIndex = NULL;
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");

		remoteMessage = PQerrorMessage(connection);
		lastNewlineIndex = strrchr(remoteMessage, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to complete COPY on %s:%s", nodeName, nodePort),
						errdetail("%s", remoteMessage)));
	}
}


/*
 * ColumnOutputFunctions walks over a table's columns, and finds each column's
 * type information. The function then resolves each type's output function,
 * and stores and returns these output functions in an array.
 */
FmgrInfo *
ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat)
{
	uint32 columnCount = (uint32) rowDescriptor->natts;
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Oid columnTypeId = currentColumn->atttypid;
		Oid outputFunctionId = InvalidOid;
		bool typeVariableLength = false;

		if (currentColumn->attisdropped)
		{
			/* dropped column, leave the output function NULL */
			continue;
		}
		else if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}


/*
 * AppendCopyRowData serializes one row using the column output functions,
 * and appends the data to the row output state object's message buffer.
 * This function is modeled after the CopyOneRowTo() function in
 * commands/copy.c, but only implements a subset of that functionality.
 * Note that the caller of this function should reset row memory context
 * to not bloat memory usage.
 */
void
AppendCopyRowData(Datum *valueArray, bool *isNullArray, TupleDesc rowDescriptor,
				  CopyOutState rowOutputState, FmgrInfo *columnOutputFunctions)
{
	MemoryContext oldContext = NULL;
	uint32 totalColumnCount = (uint32) rowDescriptor->natts;
	uint32 columnIndex = 0;
	uint32 availableColumnCount = AvailableColumnCount(rowDescriptor);
	uint32 appendedColumnCount = 0;

	oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

	if (rowOutputState->binary)
	{
		CopySendInt16(rowOutputState, availableColumnCount);
	}

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (currentColumn->attisdropped)
		{
			continue;
		}
		else if (rowOutputState->binary)
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				bytea *outputBytes = SendFunctionCall(outputFunctionPointer, value);

				CopySendInt32(rowOutputState, VARSIZE(outputBytes) - VARHDRSZ);
				CopySendData(rowOutputState, VARDATA(outputBytes),
							 VARSIZE(outputBytes) - VARHDRSZ);
			}
			else
			{
				CopySendInt32(rowOutputState, -1);
			}
		}
		else
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				char *columnText = OutputFunctionCall(outputFunctionPointer, value);

				CopyAttributeOutText(rowOutputState, columnText);
			}
			else
			{
				CopySendString(rowOutputState, rowOutputState->null_print_client);
			}

			lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
			if (!lastColumn)
			{
				CopySendChar(rowOutputState, rowOutputState->delim[0]);
			}
		}

		appendedColumnCount++;
	}

	if (!rowOutputState->binary)
	{
		/* append default line termination string depending on the platform */
#ifndef WIN32
		CopySendChar(rowOutputState, '\n');
#else
		CopySendString(rowOutputState, "\r\n");
#endif
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * AvailableColumnCount returns the number of columns in a tuple descriptor, excluding
 * columns that were dropped.
 */
static uint32
AvailableColumnCount(TupleDesc tupleDescriptor)
{
	uint32 columnCount = 0;
	uint32 columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];

		if (!currentColumn->attisdropped)
		{
			columnCount++;
		}
	}

	return columnCount;
}


/*
 * AppendCopyBinaryHeaders appends binary headers to the copy buffer in
 * headerOutputState.
 */
void
AppendCopyBinaryHeaders(CopyOutState headerOutputState)
{
	const int32 zero = 0;

	/* Signature */
	CopySendData(headerOutputState, BinarySignature, 11);

	/* Flags field (no OIDs) */
	CopySendInt32(headerOutputState, zero);

	/* No header extension */
	CopySendInt32(headerOutputState, zero);
}


/*
 * AppendCopyBinaryFooters appends binary footers to the copy buffer in
 * footerOutputState.
 */
void
AppendCopyBinaryFooters(CopyOutState footerOutputState)
{
	int16 negative = -1;

	CopySendInt16(footerOutputState, negative);
}


/* *INDENT-OFF* */
/* Append data to the copy buffer in outputState */
static void
CopySendData(CopyOutState outputState, const void *databuf, int datasize)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, databuf, datasize);
}


/* Append a striong to the copy buffer in outputState. */
static void
CopySendString(CopyOutState outputState, const char *str)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, str, strlen(str));
}


/* Append a char to the copy buffer in outputState. */
static void
CopySendChar(CopyOutState outputState, char c)
{
	appendStringInfoCharMacro(outputState->fe_msgbuf, c);
}


/* Append an int32 to the copy buffer in outputState. */
static void
CopySendInt32(CopyOutState outputState, int32 val)
{
	uint32 buf = htonl((uint32) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/* Append an int16 to the copy buffer in outputState. */
static void
CopySendInt16(CopyOutState outputState, int16 val)
{
	uint16 buf = htons((uint16) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/*
 * Send text representation of one column, with conversion and escaping.
 *
 * NB: This function is based on commands/copy.c and doesn't fully conform to
 * our coding style. The function should be kept in sync with copy.c.
 */
static void
CopyAttributeOutText(CopyOutState cstate, char *string)
{
	char *pointer = NULL;
	char *start = NULL;
	char c = '\0';
	char delimc = cstate->delim[0];

	if (cstate->need_transcoding)
	{
		pointer = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	}
	else
	{
		pointer = string;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "pointer"
	 * can be sent literally, but hasn't yet been.
	 *
	 * As all encodings here are safe, i.e. backend supported ones, we can
	 * skip doing pg_encoding_mblen(), because in valid backend encodings,
	 * extra bytes of a multibyte character never look like ASCII.
	 */
	start = pointer;
	while ((c = *pointer) != '\0')
	{
		if ((unsigned char) c < (unsigned char) 0x20)
		{
			/*
			 * \r and \n must be escaped, the others are traditional. We
			 * prefer to dump these using the C-like notation, rather than
			 * a backslash and the literal character, because it makes the
			 * dump file a bit more proof against Microsoftish data
			 * mangling.
			 */
			switch (c)
			{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (c == delimc)
						break;
					/* All ASCII control chars are length 1 */
					pointer++;
					continue;		/* fall to end of loop */
			}
			/* if we get here, we need to convert the control char */
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			CopySendChar(cstate, c);
			start = ++pointer;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			start = pointer++;	/* we include char in next run */
		}
		else
		{
			pointer++;
		}
	}

	CopyFlushOutput(cstate, start, pointer);
}


/* *INDENT-ON* */
/* Helper function to send pending copy output */
static inline void
CopyFlushOutput(CopyOutState cstate, char *start, char *pointer)
{
	if (pointer > start)
	{
		CopySendData(cstate, start, pointer - start);
	}
}
