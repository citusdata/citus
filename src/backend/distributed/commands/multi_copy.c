/*-------------------------------------------------------------------------
 *
 * multi_copy.c
 *     This file contains implementation of COPY utility for distributed
 *     tables.
 *
 * Contributed by Konstantin Knizhnik, Postgres Professional
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
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


/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;
	List *connectionList;
} ShardConnections;


/* Local functions forward declarations */
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
static void OpenCopyTransactions(CopyStmt *copyStatement,
								 ShardConnections *shardConnections,
								 int64 shardId);
static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId);
static void SendCopyDataToPlacements(StringInfo dataBuffer,
									 ShardConnections *shardConnections);
static List * ConnectionList(HTAB *connectionHash);
static void EndRemoteCopy(List *connectionList, bool stopOnFailure);
static void ReportCopyError(PGconn *connection, PGresult *result);
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
	RangeVar *relation = copyStatement->relation;
	Oid tableId = RangeVarGetRelid(relation, NoLock, false);
	char *relationName = get_rel_name(tableId);
	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;
	List *shardIntervalList = NULL;
	ListCell *shardIntervalCell = NULL;
	char partitionMethod = '\0';
	Var *partitionColumn = NULL;
	HTAB *shardConnectionHash = NULL;
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;
	List *connectionList = NIL;
	CopyState copyState = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	Relation rel = NULL;
	ShardInterval **shardIntervalCache = NULL;
	bool useBinarySearch = false;
	TypeCacheEntry *typeEntry = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;
	int shardCount = 0;
	uint64 processedRowCount = 0;
	ErrorContextCallback errorCallback;
	CopyOutState copyOutState = NULL;
	FmgrInfo *columnOutputFunctions = NULL;

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
	rel = heap_open(tableId, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(rel);
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
							errmsg("could not find any shards for query"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName),
							errhint("Run master_create_worker_shards to create shards "
									"and try again.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find any shards for query"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName)));
		}
	}

	/* create a mapping of shard id to a connection for each of its placements */
	shardConnectionHash = CreateShardConnectionHash();

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
	copyState = BeginCopyFrom(rel, copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

	executorState = CreateExecutorState();
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->binary = true;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/* we use a PG_TRY block to roll back on errors (e.g. in NextCopyFrom) */
	PG_TRY();
	{
		while (true)
		{
			bool nextRowFound = false;
			Datum partitionColumnValue = 0;
			ShardInterval *shardInterval = NULL;
			int64 shardId = 0;
			bool found = false;
			MemoryContext oldContext = NULL;

			ResetPerTupleExprContext(executorState);

			oldContext = MemoryContextSwitchTo(executorTupleContext);

			/* parse a row from the input */
			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues,columnNulls, NULL);

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
								errmsg("no shard for partition column value")));
			}

			shardId = shardInterval->shardId;

			MemoryContextSwitchTo(oldContext);

			/* find the connections to the shard placements */
			shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
																&shardId,
																HASH_ENTER,
																&found);
			if (!found)
			{
				/* intialize COPY transactions on shard placements */
				shardConnections->shardId = shardId;
				shardConnections->connectionList = NIL;

				OpenCopyTransactions(copyStatement, shardConnections, shardId);

				BuildCopyBinaryHeaders(copyOutState);
				SendCopyDataToPlacements(copyOutState->fe_msgbuf, shardConnections);
			}

			/* Replicate row to all shard placements */
			BuildCopyRowData(columnValues, columnNulls, tupleDescriptor,
							 copyOutState, columnOutputFunctions);
			SendCopyDataToPlacements(copyOutState->fe_msgbuf, shardConnections);

			processedRowCount += 1;
		}

		/* send binary footers to all shards */
		hash_seq_init(&status, shardConnectionHash);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
		while (shardConnections != NULL)
		{
			List *shardConnectionsList = list_copy(shardConnections->connectionList);
			connectionList = list_concat(connectionList, shardConnectionsList);

			BuildCopyBinaryFooters(copyOutState);
			SendCopyDataToPlacements(copyOutState->fe_msgbuf, shardConnections);

			shardConnections = (ShardConnections *) hash_seq_search(&status);
		}

		EndRemoteCopy(connectionList, true);

		if (CopyTransactionManager == TRANSACTION_MANAGER_2PC)
		{
			PrepareTransactions(connectionList);
		}

		CHECK_FOR_INTERRUPTS();
	}
	PG_CATCH();
	{
		/* roll back all transactions */
		connectionList = ConnectionList(shardConnectionHash);
		EndRemoteCopy(connectionList, false);
		AbortTransactions(connectionList);
		CloseConnections(connectionList);

		PG_RE_THROW();
	}
	PG_END_TRY();

	EndCopyFrom(copyState);
	heap_close(rel, NoLock);

	error_context_stack = errorCallback.previous;

	CommitTransactions(connectionList);
	CloseConnections(connectionList);

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
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
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;

	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   HASH_ELEM | HASH_FUNCTION);

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
	else
	{
		compareFunction = GetFunctionInfo(partitionColumn->vartype,
										  BTREE_AM_OID, BTORDER_PROC);
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
		int middleIndex = (lowerBoundIndex + upperBoundIndex) >> 1;
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
 * OpenCopyTransactions opens a connection for each placement of a shard and
 * starts a COPY transaction. If a connection cannot be opened, then the shard
 * placement is marked as inactive and the COPY continues with the remaining
 * shard placements.
 */
static void
OpenCopyTransactions(CopyStmt *copyStatement, ShardConnections *shardConnections,
					 int64 shardId)
{
	List *finalizedPlacementList = NIL;
	List *failedPlacementList = NIL;
	ListCell *placementCell = NULL;
	ListCell *failedPlacementCell = NULL;
	List *connectionList = NIL;

	finalizedPlacementList = FinalizedShardPlacementList(shardId);

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;
		TransactionConnection *transactionConnection = NULL;
		StringInfo copyCommand = NULL;
		PGresult *result = NULL;

		PGconn *connection = ConnectToNode(nodeName, nodePort);
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

		copyCommand = ConstructCopyStatement(copyStatement, shardId);

		result = PQexec(connection, copyCommand->data);
		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			ReportRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, placement);
			continue;
		}

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->connectionId = shardId;
		transactionConnection->transactionState = TRANSACTION_STATE_COPY_STARTED;
		transactionConnection->connection = connection;

		connectionList = lappend(connectionList, transactionConnection);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
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
}


/*
 * ConstructCopyStattement constructs the text of a COPY statement for a particular
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
 * SendCopyDataToPlacements copies given copy data to a list of placements for
 * a shard.
 */
static void
SendCopyDataToPlacements(StringInfo dataBuffer, ShardConnections *shardConnections)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, shardConnections->connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 shardId = shardConnections->shardId;

		/* copy the line buffer into the placement */
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
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");
		int copyEndResult = 0;
		PGresult *result = NULL;

		if (transactionConnection->transactionState != TRANSACTION_STATE_COPY_STARTED)
		{
			/* COPY already ended during the prepare phase */
			continue;
		}

		/* end the COPY input */
		copyEndResult = PQputCopyEnd(connection, NULL);
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;

		if (copyEndResult != 1)
		{
			if (stopOnFailure)
			{
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

		remoteMessage = PQerrorMessage(connection);
		lastNewlineIndex = strrchr(remoteMessage, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}

		ereport(ERROR, (errmsg("%s", remoteMessage)));
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

		if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}

		Assert(currentColumn->attisdropped == false);

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}


/*
 * BuildCopyRowData serializes one row using the column output functions,
 * and appends the data to the row output state object's message buffer.
 * This function is modeled after the CopyOneRowTo() function in
 * commands/copy.c, but only implements a subset of that functionality.
 * Note that the caller of this function should reset row memory context
 * to not bloat memory usage.
 */
void
BuildCopyRowData(Datum *valueArray, bool *isNullArray, TupleDesc rowDescriptor,
				 CopyOutState rowOutputState, FmgrInfo *columnOutputFunctions)
{
	MemoryContext oldContext = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = 0;

	/* reset previous tuple's output data */
	resetStringInfo(rowOutputState->fe_msgbuf);

	oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

	if (rowOutputState->binary)
	{
		CopySendInt16(rowOutputState, rowDescriptor->natts);
	}

	columnCount = (uint32) rowDescriptor->natts;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (rowOutputState->binary)
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

			lastColumn = ((columnIndex + 1) == columnCount);
			if (!lastColumn)
			{
				CopySendChar(rowOutputState, rowOutputState->delim[0]);
			}
		}
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


/* Append binary headers to the copy buffer in headerOutputState. */
void
BuildCopyBinaryHeaders(CopyOutState headerOutputState)
{
	const int32 zero = 0;

	resetStringInfo(headerOutputState->fe_msgbuf);

	/* Signature */
	CopySendData(headerOutputState, BinarySignature, 11);

	/* Flags field (no OIDs) */
	CopySendInt32(headerOutputState, zero);

	/* No header extension */
	CopySendInt32(headerOutputState, zero);
}


/* Append binary footers to the copy buffer in footerOutputState. */
void
BuildCopyBinaryFooters(CopyOutState footerOutputState)
{
	int16 negative = -1;

	resetStringInfo(footerOutputState->fe_msgbuf);
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
