/*-------------------------------------------------------------------------
 *
 * multi_copy.c
 *     This file contains implementation of COPY utility for distributed
 *     tables.
 *
 * The CitusCopyFrom function should be called from the utility hook to process
 * COPY ... FROM commands on distributed tables. CitusCopyFrom parses the input
 * from stdin, a program, or a file, and decides to copy new rows to existing
 * shards or new shards based on the partition method of the distributed table.
 * If copy is run a worker node, CitusCopyFrom calls CopyFromWorkerNode which
 * parses the master node copy options and handles communication with the master
 * node.
 *
 * It opens a new connection for every shard placement and uses the PQputCopyData
 * function to copy the data. Because PQputCopyData transmits data, asynchronously,
 * the workers will ingest data at least partially in parallel.
 *
 * For hash-partitioned tables, if it fails to connect to a worker, the master
 * marks the placement for which it was trying to open a connection as inactive,
 * similar to the way DML statements are handled. If a failure occurs after
 * connecting, the transaction is rolled back on all the workers. Note that,
 * in the case of append-partitioned tables, if a fail occurs, immediately
 * metadata changes are rolled back on the master node, but shard placements
 * are left on the worker nodes.
 *
 * By default, COPY uses normal transactions on the workers. In the case of
 * hash or range-partitioned tables, this can cause a problem when some of the
 * transactions fail to commit while others have succeeded. To ensure no data
 * is lost, COPY can use two-phase commit, by increasing max_prepared_transactions
 * on the worker and setting citus.multi_shard_commit_protocol to '2pc'. The default
 * is '1pc'. This is not a problem for append-partitioned tables because new
 * shards are created and in the case of failure, metadata changes are rolled
 * back on the master node.
 *
 * Parsing options are processed and enforced on the node where copy command
 * is run, while constraints are enforced on the worker. In either case,
 * failure causes the whole COPY to roll back.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * With contributions from Postgres Professional.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <arpa/inet.h> /* for htons */
#include <netinet/in.h> /* for htons */
#include <string.h>

#include "access/htup_details.h"
#include "access/htup.h"
#include "access/sdir.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_pruning.h"
#include "executor/executor.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/memutils.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* use a global connection to the master node in order to skip passing it around */
static MultiConnection *masterConnection = NULL;


/* Local functions forward declarations */
static void CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag);
static void CopyToExistingShards(CopyStmt *copyStatement, char *completionTag);
static void CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId);
static char MasterPartitionMethod(RangeVar *relation);
static void RemoveMasterOptions(CopyStmt *copyStatement);
static void OpenCopyConnections(CopyStmt *copyStatement,
								ShardConnections *shardConnections, bool stopOnFailure,
								bool useBinaryCopyFormat);

static bool CanUseBinaryCopyFormat(TupleDesc tupleDescription);
static bool BinaryOutputFunctionDefined(Oid typeId);
static List * MasterShardPlacementList(uint64 shardId);
static List * RemoteFinalizedShardPlacementList(uint64 shardId);

static void SendCopyBinaryHeaders(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);
static void SendCopyBinaryFooters(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);

static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId,
										 bool useBinaryCopyFormat);
static void SendCopyDataToAll(StringInfo dataBuffer, int64 shardId, List *connectionList);
static void SendCopyDataToPlacement(StringInfo dataBuffer, int64 shardId,
									MultiConnection *connection);
static void EndRemoteCopy(int64 shardId, List *connectionList, bool stopOnFailure);
static void ReportCopyError(MultiConnection *connection, PGresult *result);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor);
static int64 StartCopyToNewShard(ShardConnections *shardConnections,
								 CopyStmt *copyStatement, bool useBinaryCopyFormat);
static int64 MasterCreateEmptyShard(char *relationName);
static int64 CreateEmptyShard(char *relationName);
static int64 RemoteCreateEmptyShard(char *relationName);
static void MasterUpdateShardStatistics(uint64 shardId);
static void RemoteUpdateShardStatistics(uint64 shardId);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);


/*
 * CitusCopyFrom implements the COPY table_name FROM. It dispacthes the copy
 * statement to related subfunctions based on where the copy command is run
 * and the partition method of the distributed table.
 */
void
CitusCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	bool isCopyFromWorker = false;

	BeginOrContinueCoordinatedTransaction();
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

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

	masterConnection = NULL; /* reset, might still be set after error */
	isCopyFromWorker = IsCopyFromWorker(copyStatement);
	if (isCopyFromWorker)
	{
		CopyFromWorkerNode(copyStatement, completionTag);
	}
	else
	{
		Oid relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
		char partitionMethod = PartitionMethod(relationId);

		if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
			DISTRIBUTE_BY_RANGE || partitionMethod == DISTRIBUTE_BY_NONE)
		{
			CopyToExistingShards(copyStatement, completionTag);
		}
		else if (partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			CopyToNewShards(copyStatement, completionTag, relationId);
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported partition method")));
		}
	}

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * IsCopyFromWorker checks if the given copy statement has the master host option.
 */
bool
IsCopyFromWorker(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "master_host", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * CopyFromWorkerNode implements the COPY table_name FROM ... from worker nodes
 * for append-partitioned tables.
 */
static void
CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag)
{
	NodeAddress *masterNodeAddress = MasterNodeAddress(copyStatement);
	char *nodeName = masterNodeAddress->nodeName;
	int32 nodePort = masterNodeAddress->nodePort;
	Oid relationId = InvalidOid;
	char partitionMethod = 0;
	char *schemaName = NULL;
	uint32 connectionFlags = FOR_DML;

	masterConnection = GetNodeConnection(connectionFlags, nodeName, nodePort);
	ClaimConnectionExclusively(masterConnection);

	RemoteTransactionBeginIfNecessary(masterConnection);

	/* strip schema name for local reference */
	schemaName = copyStatement->relation->schemaname;
	copyStatement->relation->schemaname = NULL;

	relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);

	/* put schema name back */
	copyStatement->relation->schemaname = schemaName;
	partitionMethod = MasterPartitionMethod(copyStatement->relation);
	if (partitionMethod != DISTRIBUTE_BY_APPEND)
	{
		ereport(ERROR, (errmsg("copy from worker nodes is only supported "
							   "for append-partitioned tables")));
	}

	/*
	 * Remove master node options from the copy statement because they are not
	 * recognized by PostgreSQL machinery.
	 */
	RemoveMasterOptions(copyStatement);

	CopyToNewShards(copyStatement, completionTag, relationId);

	UnclaimConnection(masterConnection);
	masterConnection = NULL;
}


/*
 * CopyToExistingShards implements the COPY table_name FROM ... for hash or
 * range-partitioned tables where there are already shards into which to copy
 * rows.
 */
static void
CopyToExistingShards(CopyStmt *copyStatement, char *completionTag)
{
	Oid tableId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
	char *relationName = get_rel_name(tableId);
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(tableId);
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	List *shardIntervalList = NULL;
	HTAB *shardConnectionHash = NULL;
	ShardConnections *shardConnections = NULL;
	List *shardConnectionsList = NIL;
	ListCell *shardConnectionsCell = NULL;

	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;

	CopyState copyState = NULL;
	CopyOutState copyOutState = NULL;
	FmgrInfo *columnOutputFunctions = NULL;
	uint64 processedRowCount = 0;

	Var *partitionColumn = PartitionColumn(tableId, 0);
	char partitionMethod = cacheEntry->partitionMethod;

	ErrorContextCallback errorCallback;

	/* allocate column values and nulls arrays */
	distributedRelation = heap_open(tableId, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(distributedRelation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/* we don't support copy to reference tables from workers */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		EnsureCoordinator();
	}

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

	/* error if any shard missing min/max values for non reference tables */
	if (partitionMethod != DISTRIBUTE_BY_NONE &&
		cacheEntry->hasUninitializedShardInterval)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not start copy"),
						errdetail("Distributed relation \"%s\" has shards "
								  "with missing shardminvalue/shardmaxvalue.",
								  relationName)));
	}

	/* prevent concurrent placement changes and non-commutative DML statements */
	LockShardListMetadata(shardIntervalList, ShareLock);
	LockShardListResources(shardIntervalList, ShareLock);

	if (cacheEntry->replicationModel == REPLICATION_MODEL_2PC)
	{
		CoordinatedTransactionUse2PC();
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
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/* create a mapping of shard id to a connection for each of its placements */
	shardConnectionHash = CreateShardConnectionHash(TopTransactionContext);

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

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

		/*
		 * Find the partition column value and corresponding shard interval
		 * for non-reference tables.
		 * Get the existing (and only a single) shard interval for the reference
		 * tables. Note that, reference tables has NULL partition column values so
		 * skip the check.
		 */
		if (partitionColumn != NULL)
		{
			if (columnNulls[partitionColumn->varattno - 1])
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg("cannot copy row with NULL value "
									   "in partition column")));
			}

			partitionColumnValue = columnValues[partitionColumn->varattno - 1];
		}

		/*
		 * Find the shard interval and id for the partition column value for
		 * non-reference tables.
		 * For reference table, this function blindly returns the tables single
		 * shard.
		 */
		shardInterval = FindShardInterval(partitionColumnValue, cacheEntry);

		if (shardInterval == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find shard for partition column "
								   "value")));
		}

		shardId = shardInterval->shardId;

		MemoryContextSwitchTo(oldContext);

		/* get existing connections to the shard placements, if any */
		shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
												   &shardConnectionsFound);
		if (!shardConnectionsFound)
		{
			bool stopOnFailure = false;

			if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
			{
				stopOnFailure = true;
			}

			/* open connections and initiate COPY on shard placements */
			OpenCopyConnections(copyStatement, shardConnections, stopOnFailure,
								copyOutState->binary);

			/* send copy binary headers to shard placements */
			if (copyOutState->binary)
			{
				SendCopyBinaryHeaders(copyOutState, shardId,
									  shardConnections->connectionList);
			}
		}

		/* replicate row to shard placements */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
						  copyOutState, columnOutputFunctions);
		SendCopyDataToAll(copyOutState->fe_msgbuf, shardId,
						  shardConnections->connectionList);

		processedRowCount += 1;
	}

	/* all lines have been copied, stop showing line number in errors */
	error_context_stack = errorCallback.previous;

	shardConnectionsList = ShardConnectionList(shardConnectionHash);
	foreach(shardConnectionsCell, shardConnectionsList)
	{
		ShardConnections *shardConnections = (ShardConnections *) lfirst(
			shardConnectionsCell);

		/* send copy binary footers to all shard placements */
		if (copyOutState->binary)
		{
			SendCopyBinaryFooters(copyOutState, shardConnections->shardId,
								  shardConnections->connectionList);
		}

		/* close the COPY input on all shard placements */
		EndRemoteCopy(shardConnections->shardId, shardConnections->connectionList, true);
	}

	EndCopyFrom(copyState);
	heap_close(distributedRelation, NoLock);

	/* mark failed placements as inactive */
	MarkFailedShardPlacements();

	CHECK_FOR_INTERRUPTS();

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * CopyToNewShards implements the COPY table_name FROM ... for append-partitioned
 * tables where we create new shards into which to copy rows.
 */
static void
CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId)
{
	FmgrInfo *columnOutputFunctions = NULL;

	/* allocate column values and nulls arrays */
	Relation distributedRelation = heap_open(relationId, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);
	uint32 columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	ErrorContextCallback errorCallback;

	int64 currentShardId = INVALID_SHARD_ID;
	uint64 shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;
	uint64 copiedDataSizeInBytes = 0;
	uint64 processedRowCount = 0;

	ShardConnections *shardConnections =
		(ShardConnections *) palloc0(sizeof(ShardConnections));

	/* initialize copy state to read from COPY data source */
	CopyState copyState = BeginCopyFrom(distributedRelation,
										copyStatement->filename,
										copyStatement->is_program,
										copyStatement->attlist,
										copyStatement->options);

	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;

	while (true)
	{
		bool nextRowFound = false;
		MemoryContext oldContext = NULL;
		uint64 messageBufferSize = 0;

		ResetPerTupleExprContext(executorState);

		/* switch to tuple memory context and start showing line number in errors */
		error_context_stack = &errorCallback;
		oldContext = MemoryContextSwitchTo(executorTupleContext);

		/* parse a row from the input */
		nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
									columnValues, columnNulls, NULL);

		if (!nextRowFound)
		{
			/* switch to regular memory context and stop showing line number in errors */
			MemoryContextSwitchTo(oldContext);
			error_context_stack = errorCallback.previous;
			break;
		}

		CHECK_FOR_INTERRUPTS();

		/* switch to regular memory context and stop showing line number in errors */
		MemoryContextSwitchTo(oldContext);
		error_context_stack = errorCallback.previous;

		/*
		 * If copied data size is zero, this means either this is the first
		 * line in the copy or we just filled the previous shard up to its
		 * capacity. Either way, we need to create a new shard and
		 * start copying new rows into it.
		 */
		if (copiedDataSizeInBytes == 0)
		{
			/* create shard and open connections to shard placements */
			currentShardId = StartCopyToNewShard(shardConnections, copyStatement,
												 copyOutState->binary);

			/* send copy binary headers to shard placements */
			if (copyOutState->binary)
			{
				SendCopyBinaryHeaders(copyOutState, currentShardId,
									  shardConnections->connectionList);
			}
		}

		/* replicate row to shard placements */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
						  copyOutState, columnOutputFunctions);
		SendCopyDataToAll(copyOutState->fe_msgbuf, currentShardId,
						  shardConnections->connectionList);

		messageBufferSize = copyOutState->fe_msgbuf->len;
		copiedDataSizeInBytes = copiedDataSizeInBytes + messageBufferSize;

		/*
		 * If we filled up this shard to its capacity, send copy binary footers
		 * to shard placements, and update shard statistics.
		 */
		if (copiedDataSizeInBytes > shardMaxSizeInBytes)
		{
			Assert(currentShardId != INVALID_SHARD_ID);

			if (copyOutState->binary)
			{
				SendCopyBinaryFooters(copyOutState, currentShardId,
									  shardConnections->connectionList);
			}

			EndRemoteCopy(currentShardId, shardConnections->connectionList, true);
			MasterUpdateShardStatistics(shardConnections->shardId);

			copiedDataSizeInBytes = 0;
			currentShardId = INVALID_SHARD_ID;
		}

		processedRowCount += 1;
	}

	/*
	 * For the last shard, send copy binary footers to shard placements,
	 * and update shard statistics. If no row is send, there is no shard
	 * to finalize the copy command.
	 */
	if (copiedDataSizeInBytes > 0)
	{
		Assert(currentShardId != INVALID_SHARD_ID);

		if (copyOutState->binary)
		{
			SendCopyBinaryFooters(copyOutState, currentShardId,
								  shardConnections->connectionList);
		}
		EndRemoteCopy(currentShardId, shardConnections->connectionList, true);
		MasterUpdateShardStatistics(shardConnections->shardId);
	}

	EndCopyFrom(copyState);
	heap_close(distributedRelation, NoLock);

	/* check for cancellation one last time before returning */
	CHECK_FOR_INTERRUPTS();

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * MasterNodeAddress gets the master node address from copy options and returns
 * it. Note that if the master_port is not provided, we use 5432 as the default
 * port.
 */
NodeAddress *
MasterNodeAddress(CopyStmt *copyStatement)
{
	NodeAddress *masterNodeAddress = (NodeAddress *) palloc0(sizeof(NodeAddress));
	char *nodeName = NULL;

	/* set default port to 5432 */
	int32 nodePort = 5432;

	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "master_host", NAMEDATALEN) == 0)
		{
			nodeName = defGetString(defel);
		}
		else if (strncmp(defel->defname, "master_port", NAMEDATALEN) == 0)
		{
			nodePort = defGetInt32(defel);
		}
	}

	masterNodeAddress->nodeName = nodeName;
	masterNodeAddress->nodePort = nodePort;

	return masterNodeAddress;
}


/*
 * MasterPartitionMethod gets the partition method of the given relation from
 * the master node and returns it.
 */
static char
MasterPartitionMethod(RangeVar *relation)
{
	char partitionMethod = '\0';
	PGresult *queryResult = NULL;

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);

	StringInfo partitionMethodCommand = makeStringInfo();
	appendStringInfo(partitionMethodCommand, PARTITION_METHOD_QUERY, qualifiedName);

	queryResult = PQexec(masterConnection->pgConn, partitionMethodCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		char *partitionMethodString = PQgetvalue((PGresult *) queryResult, 0, 0);
		if (partitionMethodString == NULL || (*partitionMethodString) == '\0')
		{
			ereport(ERROR, (errmsg("could not find a partition method for the "
								   "table %s", relationName)));
		}

		partitionMethod = partitionMethodString[0];
	}
	else
	{
		ReportResultError(masterConnection, queryResult, WARNING);
		ereport(ERROR, (errmsg("could not get the partition method of the "
							   "distributed table")));
	}

	PQclear(queryResult);

	return partitionMethod;
}


/*
 * RemoveMasterOptions removes master node related copy options from the option
 * list of the copy statement.
 */
static void
RemoveMasterOptions(CopyStmt *copyStatement)
{
	List *newOptionList = NIL;
	ListCell *optionCell = NULL;

	/* walk over the list of all options */
	foreach(optionCell, copyStatement->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		/* skip master related options */
		if ((strncmp(option->defname, "master_host", NAMEDATALEN) == 0) ||
			(strncmp(option->defname, "master_port", NAMEDATALEN) == 0))
		{
			continue;
		}

		newOptionList = lappend(newOptionList, option);
	}

	copyStatement->options = newOptionList;
}


/*
 * OpenCopyConnections opens a connection for each placement of a shard and
 * starts a COPY transaction if necessary. If a connection cannot be opened,
 * then the shard placement is marked as inactive and the COPY continues with the remaining
 * shard placements.
 */
static void
OpenCopyConnections(CopyStmt *copyStatement, ShardConnections *shardConnections,
					bool stopOnFailure, bool useBinaryCopyFormat)
{
	List *finalizedPlacementList = NIL;
	int failedPlacementCount = 0;
	ListCell *placementCell = NULL;
	List *connectionList = NULL;
	int64 shardId = shardConnections->shardId;

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "OpenCopyConnections",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);

	/* release finalized placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	finalizedPlacementList = MasterShardPlacementList(shardId);

	MemoryContextSwitchTo(oldContext);

	if (XactModificationLevel > XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("distributed copy operations must not appear in "
							   "transaction blocks containing other distributed "
							   "modifications")));
	}

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeUser = CurrentUserName();
		MultiConnection *connection = NULL;
		uint32 connectionFlags = FOR_DML;
		StringInfo copyCommand = NULL;
		PGresult *result = NULL;

		connection = GetPlacementConnection(connectionFlags, placement, nodeUser);

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			if (stopOnFailure)
			{
				ReportConnectionError(connection, ERROR);
			}
			else
			{
				ReportConnectionError(connection, WARNING);
				MarkRemoteTransactionFailed(connection, true);

				failedPlacementCount++;
				continue;
			}
		}

		/*
		 * Errors are supposed to cause immediate aborts (i.e. we don't
		 * want to/can't invalidate placements), mark the connection as
		 * critical so later errors cause failures.
		 */
		MarkRemoteTransactionCritical(connection);
		ClaimConnectionExclusively(connection);
		RemoteTransactionBeginIfNecessary(connection);
		copyCommand = ConstructCopyStatement(copyStatement, shardConnections->shardId,
											 useBinaryCopyFormat);
		result = PQexec(connection->pgConn, copyCommand->data);

		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);
		connectionList = lappend(connectionList, connection);
	}

	/* if all placements failed, error out */
	if (failedPlacementCount == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errmsg("could not connect to any active placements")));
	}

	/*
	 * If stopOnFailure is true, we just error out and code execution should
	 * never reach to this point. This is the case for reference tables and
	 * copy from worker nodes.
	 */
	Assert(!stopOnFailure || failedPlacementCount == 0);

	shardConnections->connectionList = connectionList;

	MemoryContextReset(localContext);
}


/*
 * CanUseBinaryCopyFormat iterates over columns of the relation and looks for a
 * column whose type is array of user-defined type or composite type. If it finds
 * such column, that means we cannot use binary format for COPY, because binary
 * format sends Oid of the types, which are generally not same in master and
 * worker nodes for user-defined types. If the function can not detect a binary
 * output function for any of the column, it returns false.
 */
static bool
CanUseBinaryCopyFormat(TupleDesc tupleDescription)
{
	bool useBinaryCopyFormat = true;
	int totalColumnCount = tupleDescription->natts;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescription->attrs[columnIndex];
		Oid typeId = InvalidOid;
		char typeCategory = '\0';
		bool typePreferred = false;
		bool binaryOutputFunctionDefined = false;

		if (currentColumn->attisdropped)
		{
			continue;
		}

		typeId = currentColumn->atttypid;

		/* built-in types may also don't have binary output function */
		binaryOutputFunctionDefined = BinaryOutputFunctionDefined(typeId);
		if (!binaryOutputFunctionDefined)
		{
			useBinaryCopyFormat = false;
			break;
		}

		if (typeId >= FirstNormalObjectId)
		{
			get_type_category_preferred(typeId, &typeCategory, &typePreferred);
			if (typeCategory == TYPCATEGORY_ARRAY ||
				typeCategory == TYPCATEGORY_COMPOSITE)
			{
				useBinaryCopyFormat = false;
				break;
			}
		}
	}

	return useBinaryCopyFormat;
}


/*
 * BinaryOutputFunctionDefined checks whether binary output function is defined
 * for the given type.
 */
static bool
BinaryOutputFunctionDefined(Oid typeId)
{
	Oid typeFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	int16 typeLength = 0;
	bool typeByVal = false;
	char typeAlign = 0;
	char typeDelim = 0;

	get_type_io_data(typeId, IOFunc_send, &typeLength, &typeByVal,
					 &typeAlign, &typeDelim, &typeIoParam, &typeFunctionId);

	if (OidIsValid(typeFunctionId))
	{
		return true;
	}

	return false;
}


/*
 * MasterShardPlacementList dispatches the finalized shard placements call
 * between local or remote master node according to the master connection state.
 */
static List *
MasterShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	if (masterConnection == NULL)
	{
		finalizedPlacementList = FinalizedShardPlacementList(shardId);
	}
	else
	{
		finalizedPlacementList = RemoteFinalizedShardPlacementList(shardId);
	}

	return finalizedPlacementList;
}


/*
 * RemoteFinalizedShardPlacementList gets the finalized shard placement list
 * for the given shard id from the remote master node.
 */
static List *
RemoteFinalizedShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	PGresult *queryResult = NULL;

	StringInfo shardPlacementsCommand = makeStringInfo();
	appendStringInfo(shardPlacementsCommand, FINALIZED_SHARD_PLACEMENTS_QUERY, shardId);

	queryResult = PQexec(masterConnection->pgConn, shardPlacementsCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int rowCount = PQntuples(queryResult);
		int rowIndex = 0;

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			char *placementIdString = PQgetvalue(queryResult, rowIndex, 0);
			char *nodeName = PQgetvalue(queryResult, rowIndex, 1);
			char *nodePortString = PQgetvalue(queryResult, rowIndex, 2);
			uint32 nodePort = atoi(nodePortString);
			uint64 placementId = atoll(placementIdString);

			ShardPlacement *shardPlacement =
				(ShardPlacement *) palloc0(sizeof(ShardPlacement));

			shardPlacement->placementId = placementId;
			shardPlacement->nodeName = nodeName;
			shardPlacement->nodePort = nodePort;

			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("could not get shard placements from the master node")));
	}

	return finalizedPlacementList;
}


/* Send copy binary headers to given connections */
static void
SendCopyBinaryHeaders(CopyOutState copyOutState, int64 shardId, List *connectionList)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryHeaders(copyOutState);
	SendCopyDataToAll(copyOutState->fe_msgbuf, shardId, connectionList);
}


/* Send copy binary footers to given connections */
static void
SendCopyBinaryFooters(CopyOutState copyOutState, int64 shardId, List *connectionList)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryFooters(copyOutState);
	SendCopyDataToAll(copyOutState->fe_msgbuf, shardId, connectionList);
}


/*
 * ConstructCopyStatement constructs the text of a COPY statement for a particular
 * shard.
 */
static StringInfo
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId, bool useBinaryCopyFormat)
{
	StringInfo command = makeStringInfo();

	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;

	char *shardName = pstrdup(relationName);
	char *shardQualifiedName = NULL;
	const char *copyFormat = NULL;

	AppendShardIdToName(&shardName, shardId);

	shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	if (useBinaryCopyFormat)
	{
		copyFormat = "BINARY";
	}
	else
	{
		copyFormat = "TEXT";
	}
	appendStringInfo(command, "COPY %s FROM STDIN WITH (FORMAT %s)", shardQualifiedName,
					 copyFormat);

	return command;
}


/*
 * SendCopyDataToAll sends copy data to all connections in a list.
 */
static void
SendCopyDataToAll(StringInfo dataBuffer, int64 shardId, List *connectionList)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		SendCopyDataToPlacement(dataBuffer, shardId, connection);
	}
}


/*
 * SendCopyDataToPlacement sends serialized COPY data to a specific shard placement
 * over the given connection.
 */
static void
SendCopyDataToPlacement(StringInfo dataBuffer, int64 shardId, MultiConnection *connection)
{
	int copyResult = PQputCopyData(connection->pgConn, dataBuffer->data, dataBuffer->len);
	if (copyResult != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to shard %ld on %s:%d",
							   shardId, connection->hostname, connection->port),
						errdetail("failed to send %d bytes %s", dataBuffer->len,
								  dataBuffer->data)));
	}
}


/*
 * EndRemoteCopy ends the COPY input on all connections, and unclaims connections.
 * If stopOnFailure is true, then EndRemoteCopy reports an error on failure,
 * otherwise it reports a warning or continues.
 */
static void
EndRemoteCopy(int64 shardId, List *connectionList, bool stopOnFailure)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int copyEndResult = 0;
		PGresult *result = NULL;

		/* end the COPY input */
		copyEndResult = PQputCopyEnd(connection->pgConn, NULL);

		if (copyEndResult != 1)
		{
			if (stopOnFailure)
			{
				ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
								errmsg("failed to COPY to shard %ld on %s:%d",
									   shardId, connection->hostname, connection->port)));
			}

			continue;
		}

		/* check whether there were any COPY errors */
		result = PQgetResult(connection->pgConn);
		if (PQresultStatus(result) != PGRES_COMMAND_OK && stopOnFailure)
		{
			ReportCopyError(connection, result);
		}

		PQclear(result);
		ForgetResults(connection);
		UnclaimConnection(connection);
	}
}


/*
 * ReportCopyError tries to report a useful error message for the user from
 * the remote COPY error messages.
 */
static void
ReportCopyError(MultiConnection *connection, PGresult *result)
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

		remoteMessage = PQerrorMessage(connection->pgConn);
		lastNewlineIndex = strrchr(remoteMessage, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to complete COPY on %s:%d", connection->hostname,
							   connection->port),
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
	uint32 totalColumnCount = (uint32) rowDescriptor->natts;
	uint32 availableColumnCount = AvailableColumnCount(rowDescriptor);
	uint32 appendedColumnCount = 0;
	uint32 columnIndex = 0;

	MemoryContext oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

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
	MemoryContext oldContext = MemoryContextSwitchTo(headerOutputState->rowcontext);

	/* Signature */
	CopySendData(headerOutputState, BinarySignature, 11);

	/* Flags field (no OIDs) */
	CopySendInt32(headerOutputState, zero);

	/* No header extension */
	CopySendInt32(headerOutputState, zero);

	MemoryContextSwitchTo(oldContext);
}


/*
 * AppendCopyBinaryFooters appends binary footers to the copy buffer in
 * footerOutputState.
 */
void
AppendCopyBinaryFooters(CopyOutState footerOutputState)
{
	int16 negative = -1;
	MemoryContext oldContext = MemoryContextSwitchTo(footerOutputState->rowcontext);

	CopySendInt16(footerOutputState, negative);

	MemoryContextSwitchTo(oldContext);
}


/*
 * StartCopyToNewShard creates a new shard and related shard placements and
 * opens connections to shard placements.
 */
static int64
StartCopyToNewShard(ShardConnections *shardConnections, CopyStmt *copyStatement,
					bool useBinaryCopyFormat)
{
	char *relationName = copyStatement->relation->relname;
	char *schemaName = copyStatement->relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);
	int64 shardId = MasterCreateEmptyShard(qualifiedName);
	bool stopOnFailure = true;

	shardConnections->shardId = shardId;

	shardConnections->connectionList = NIL;

	/* connect to shards placements and start transactions */
	OpenCopyConnections(copyStatement, shardConnections, stopOnFailure,
						useBinaryCopyFormat);

	return shardId;
}


/*
 * MasterCreateEmptyShard dispatches the create empty shard call between local or
 * remote master node according to the master connection state.
 */
static int64
MasterCreateEmptyShard(char *relationName)
{
	int64 shardId = 0;
	if (masterConnection == NULL)
	{
		shardId = CreateEmptyShard(relationName);
	}
	else
	{
		shardId = RemoteCreateEmptyShard(relationName);
	}

	return shardId;
}


/*
 * CreateEmptyShard creates a new shard and related shard placements from the
 * local master node.
 */
static int64
CreateEmptyShard(char *relationName)
{
	int64 shardId = 0;

	text *relationNameText = cstring_to_text(relationName);
	Datum relationNameDatum = PointerGetDatum(relationNameText);
	Datum shardIdDatum = DirectFunctionCall1(master_create_empty_shard,
											 relationNameDatum);
	shardId = DatumGetInt64(shardIdDatum);

	return shardId;
}


/*
 * RemoteCreateEmptyShard creates a new shard and related shard placements from
 * the remote master node.
 */
static int64
RemoteCreateEmptyShard(char *relationName)
{
	int64 shardId = 0;
	PGresult *queryResult = NULL;

	StringInfo createEmptyShardCommand = makeStringInfo();
	appendStringInfo(createEmptyShardCommand, CREATE_EMPTY_SHARD_QUERY, relationName);

	queryResult = PQexec(masterConnection->pgConn, createEmptyShardCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		char *shardIdString = PQgetvalue((PGresult *) queryResult, 0, 0);
		char *shardIdStringEnd = NULL;
		shardId = strtoul(shardIdString, &shardIdStringEnd, 0);
	}
	else
	{
		ReportResultError(masterConnection, queryResult, WARNING);
		ereport(ERROR, (errmsg("could not create a new empty shard on the remote node")));
	}

	PQclear(queryResult);

	return shardId;
}


/*
 * MasterUpdateShardStatistics dispatches the update shard statistics call
 * between local or remote master node according to the master connection state.
 */
static void
MasterUpdateShardStatistics(uint64 shardId)
{
	if (masterConnection == NULL)
	{
		UpdateShardStatistics(shardId);
	}
	else
	{
		RemoteUpdateShardStatistics(shardId);
	}
}


/*
 * RemoteUpdateShardStatistics updates shard statistics on the remote master node.
 */
static void
RemoteUpdateShardStatistics(uint64 shardId)
{
	PGresult *queryResult = NULL;

	StringInfo updateShardStatisticsCommand = makeStringInfo();
	appendStringInfo(updateShardStatisticsCommand, UPDATE_SHARD_STATISTICS_QUERY,
					 shardId);

	queryResult = PQexec(masterConnection->pgConn, updateShardStatisticsCommand->data);
	if (PQresultStatus(queryResult) != PGRES_TUPLES_OK)
	{
		ereport(ERROR, (errmsg("could not update shard statistics")));
	}

	PQclear(queryResult);
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
