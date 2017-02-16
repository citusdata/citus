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
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"

#include <arpa/inet.h> /* for htons */
#include <netinet/in.h> /* for htons */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zmq.h>

#include "access/htup_details.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "access/sdir.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/bload.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/placement_connection.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/typcache.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* use a global connection to the master node in order to skip passing it around */
static MultiConnection *masterConnection = NULL;

static int MaxEvents = 64; /* up to MaxEvents returned by epoll_wait() */
static int EpollTimeout = 100; /* wait for a maximum time of EpollTimeout ms */
static int ZeromqPortCount = 100; /* number of available zeromq ports */
static int ZeromqStartPort = 10240; /* start port of zeormq */

/* Local functions forward declarations */
static void CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag);
static void CopyToExistingShards(CopyStmt *copyStatement, char *completionTag, Oid relationId);
static void CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId);
static char MasterPartitionMethod(RangeVar *relation);
static void RemoveMasterOptions(CopyStmt *copyStatement);
static void OpenCopyConnections(CopyStmt *copyStatement,
								ShardConnections *shardConnections, bool stopOnFailure,
								bool useBinaryCopyFormat);

static bool CanUseBinaryCopyFormat(TupleDesc tupleDescription,
								   CopyOutState rowOutputState);
static List * MasterShardPlacementList(uint64 shardId);
static List * RemoteFinalizedShardPlacementList(uint64 shardId);

static void SendCopyBinaryHeaders(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);
static void SendCopyBinaryFooters(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);

static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId,
										 bool useBinaryCopyFormat, bool useFreeze);
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

/* Functions for bulkload copy */
static void RemoveBulkloadOptions(CopyStmt *copyStatement);

static StringInfo ConstructBulkloadCopyStmt(CopyStmt *copyStatement,
		NodeAddress *masterNodeAddress, char *nodeName, uint32 nodePort);
static void RebuildBulkloadCopyStatement(CopyStmt *copyStatement,
		NodeAddress *bulkloadServer);
static StringInfo DeparseCopyStatementOptions(List *options);

static NodeAddress * LocalAddress(void);
static NodeAddress * BulkloadServerAddress(CopyStmt *copyStatement);

static void BulkloadCopyToNewShards(CopyStmt *copyStatement, char *completionTag,
		Oid relationId);
static void BulkloadCopyToExistingShards(CopyStmt *copyStatement, char *completionTag,
		Oid relationId);
static void BulkloadCopyServer(CopyStmt *copyStatement, char *completionTag,
		NodeAddress *masterNodeAddress, Oid relationId);

static List * MasterWorkerNodeList(void);
static List * RemoteWorkerNodeList(void);
static DistTableCacheEntry * MasterDistributedTableCacheEntry(RangeVar *relation);

static void StartZeroMQServer(ZeroMQServer *zeromqServer, bool is_program, bool binary,
		int natts);
static void SendMessage(ZeroMQServer *zeromqServer, char *buf, size_t len, bool kill);
static void StopZeroMQServer(ZeroMQServer *zeromqServer);

static int CopyGetAttnums(Oid relationId, List *attnamelist);
static PGconn * GetConnectionBySock(List *connList, int sock, int *connIdx);


/*
 * CitusCopyFrom implements the COPY table_name FROM. It dispacthes the copy
 * statement to related subfunctions based on where the copy command is run
 * and the partition method of the distributed table.
 */
void
CitusCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	bool isCopyFromWorker = false;
	bool isBulkloadCopy = false;

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
	isBulkloadCopy = IsBulkloadCopy(copyStatement);
	isCopyFromWorker = IsCopyFromWorker(copyStatement);
	if (isBulkloadCopy)
	{
		CitusBulkloadCopy(copyStatement, completionTag);
	}
	else if (isCopyFromWorker)
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
			CopyToExistingShards(copyStatement, completionTag, InvalidOid);
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
CopyToExistingShards(CopyStmt *copyStatement, char *completionTag, Oid relationId)
{
	Oid tableId = InvalidOid;
	if (relationId != InvalidOid)
	{
		tableId = relationId;
	}
	else
	{
		tableId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
	}
	char *relationName = get_rel_name(tableId);
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;
	bool hasUniformHashDistribution = false;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(tableId);
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	int shardCount = 0;
	List *shardIntervalList = NULL;
	ShardInterval **shardIntervalCache = NULL;
	bool useBinarySearch = false;

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
	char partitionMethod = PartitionMethod(tableId);

	ErrorContextCallback errorCallback;

	/* get hash function for partition column */
	hashFunction = cacheEntry->hashFunction;

	/* get compare function for shard intervals */
	compareFunction = cacheEntry->shardIntervalCompareFunction;

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

	/* initialize the shard interval cache */
	shardCount = cacheEntry->shardIntervalArrayLength;
	shardIntervalCache = cacheEntry->sortedShardIntervalArray;
	hasUniformHashDistribution = cacheEntry->hasUniformHashDistribution;

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH || !hasUniformHashDistribution)
	{
		useBinarySearch = true;
	}

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
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor, copyOutState);
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
		shardInterval = FindShardInterval(partitionColumnValue,
										  shardIntervalCache,
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
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor, copyOutState);
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
											 useBinaryCopyFormat, false);
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
 * CanUseBinaryCopyFormat iterates over columns of the relation given in rowOutputState
 * and looks for a column whose type is array of user-defined type or composite type.
 * If it finds such column, that means we cannot use binary format for COPY, because
 * binary format sends Oid of the types, which are generally not same in master and
 * worker nodes for user-defined types.
 */
static bool
CanUseBinaryCopyFormat(TupleDesc tupleDescription, CopyOutState rowOutputState)
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

		if (currentColumn->attisdropped)
		{
			continue;
		}

		typeId = currentColumn->atttypid;
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
			shardPlacement->nodeName = (char *) palloc0(strlen(nodeName) + 1);
			strcpy(shardPlacement->nodeName, nodeName);
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
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId, bool useBinaryCopyFormat,
		bool useFreeze)
{
	StringInfo command = makeStringInfo();

	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;

	char *shardName = pstrdup(relationName);
	char *shardQualifiedName = NULL;
	const char *copyFormat = NULL;
	const char *freeze = NULL;

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
	if (useFreeze)
	{
		freeze = "TRUE";
	}
	else
	{
		freeze = "FALSE";
	}
	appendStringInfo(command, "COPY %s FROM STDIN WITH (FORMAT %s, FREEZE %s)",
			shardQualifiedName, copyFormat, freeze);

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

/*
 * CitusBulkloadCopy implements the COPY table_name FROM xxx WITH(method 'bulkload').
 * For bulkload server, it dispatches the copy statement and records from FROM to all
 * workers, and waits them finish. For bulkload clients, they pull records from server
 * and copy them into shards. Bulkload client handles differently against append and
 * hash distributed table.
 * For APPEND distributed table, there are two copy policies:
 * 1. bulkload client would create a shard for each tablespace and insert records to
 *    these shards in round-robin policy, and if any shard reaches ShardMaxSize, it
 *    would create a new shard in the tablespace and so on. In this policy, since DDL
 *    commands of create shard and DML commands of copy are running in one transaction,
 *    we can use COPY FREEZE and Lazy-Indexing to improve ingestion performance.
 * 2. each bulkload client acts just like CopyToNewShards(), calling master to create a
 *    new shard and insert records into this new shard, when the new shard reaches the
 *    ShardMaxSize, it would call master to create another new shard and so on.
 * For HASH distributed table, clients get metadata of the table from master node and
 * send records to different shards according to hash value.
 */
void
CitusBulkloadCopy(CopyStmt *copyStatement, char *completionTag)
{
	bool isCopyFromWorker = false;
	bool isBulkloadClient = false;
	NodeAddress *masterNodeAddress = NULL;
	Oid relationId = InvalidOid;
	char partitionMethod = 0;
	char *nodeName = NULL;
	uint32 nodePort = 0;
	char *nodeUser = NULL;
	char *schemaName = NULL;

	/*
	 * from postgres/src/bin/psql/copy.c:handleCopyIn(), we know that a pq_message
	 * contains exactly one record for csv and text format, but not for binary.
	 * since in StartZeroMQServer() it's hard to handle pq_message which may contain
	 * incomplete records, currently, we don't support COPY FROM STDIN with binary
	 * format for bulkload copy.
	 */
	if (copyStatement->filename == NULL && IsBinaryCopy(copyStatement))
	{
		elog(ERROR, "bulkload doesn't support copy from stdin with binary format");
	}

	isCopyFromWorker = IsCopyFromWorker(copyStatement);
	if (isCopyFromWorker)
	{
		masterNodeAddress = MasterNodeAddress(copyStatement);
		nodeName = masterNodeAddress->nodeName;
		nodePort = masterNodeAddress->nodePort;
		nodeUser = CurrentUserName();
		masterConnection = GetNodeConnection(FORCE_NEW_CONNECTION, nodeName, nodePort);
		if (masterConnection == NULL)
		{
			elog(ERROR, "Can't connect to master server %s:%d as user %s",
					nodeName, nodePort, nodeUser);
		}
		RemoveMasterOptions(copyStatement);

		/* strip schema name for local reference */
		schemaName = copyStatement->relation->schemaname;
		copyStatement->relation->schemaname = NULL;
		relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
		/* put schema name back */
		copyStatement->relation->schemaname = schemaName;
		partitionMethod = MasterPartitionMethod(copyStatement->relation);
	}
	else
	{
		masterNodeAddress = LocalAddress();
		relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
		partitionMethod = PartitionMethod(relationId);
	}

	isBulkloadClient = IsBulkloadClient(copyStatement);
	PG_TRY();
	{
		if (isBulkloadClient)
		{
			if (partitionMethod == DISTRIBUTE_BY_APPEND
					|| partitionMethod == DISTRIBUTE_BY_RANGE)
			{
				PGresult *queryResult = NULL;
				Assert(masterConnection != NULL);
				/* run all metadata commands in a transaction */
				queryResult = PQexec(masterConnection->pgConn, "BEGIN");
				if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
				{
					elog(ERROR, "could not start to update master node metadata");
				}
				PQclear(queryResult);

				/* there are two policies for copying into new shard */
				// BulkloadCopyToNewShardsV1(copyStatement, completionTag, masterNodeAddress,
				//							relationId);
				BulkloadCopyToNewShards(copyStatement, completionTag, relationId);

				/* commit metadata transactions */
				queryResult = PQexec(masterConnection->pgConn, "COMMIT");
				if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
				{
					elog(ERROR, "could not commit master node metadata changes");
				}
				PQclear(queryResult);
			}
			else if (partitionMethod == DISTRIBUTE_BY_HASH)
			{
				BulkloadCopyToExistingShards(copyStatement, completionTag, relationId);
			}
			else
			{
				elog(ERROR, "Unknown partition method: %d", partitionMethod);
			}
		}
		else
		{
			BulkloadCopyServer(copyStatement, completionTag, masterNodeAddress, relationId);
		}
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * CopyGetAttnums - get the number of non-dropped columns of relation.
 * copy from postgresql/src/backend/commands/copy.c
 */
static int
CopyGetAttnums(Oid relationId, List *attnamelist)
{
	int attnums = list_length(attnamelist);
	if (attnums != 0)
	{
		return attnums;
	}
	else
	{
		Relation rel = heap_open(relationId, AccessShareLock);
		TupleDesc tupDesc = RelationGetDescr(rel);
		Form_pg_attribute *attr = tupDesc->attrs;
		int										 attr_count = tupDesc->natts;
		int										 i;
		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
				continue;
			attnums++;
		}
		heap_close(rel, NoLock);
		return attnums;
	}
}

/*
 * BulkloadCopyServer rebuild a COPY statement with 'bulkload_host' and 'bulkload_port'
 * options and dispatches it to all worker nodes for asynchronous executing. It also
 * starts a zeromq server to dispatches records from FROM clause to all worker nodes.
 */
static void
BulkloadCopyServer(CopyStmt *copyStatement, char *completionTag,
		NodeAddress *masterNodeAddress, Oid relationId)
{
	List *workerNodeList = NULL;
	ListCell *workerCell = NULL;
	List *workerConnectionList = NIL;
	NodeAddress *serverAddress = NULL;
	StringInfo clientCopyCommand = NULL;
	struct ZeroMQServer *zeromqServer = NULL;
	uint64 processedRowCount = 0;
	int loopIndex;
	char *nodeName = NULL;
	uint32 nodePort = 0;
	WorkerNode *workerNode = NULL;
	MultiConnection *multiConn = NULL;
	PGconn *conn = NULL;
	PGresult *res = NULL;
	int workerConnectionCount = 0;
	int finishCount = 0;
	int failCount = 0;
	int *finish = NULL;
	int rc;
	int efd;
	int nevents;
	int sock;
	int connIdx;
	struct epoll_event *event = NULL;
	struct epoll_event *events = NULL;

	workerNodeList = MasterWorkerNodeList();
	serverAddress = LocalAddress();

	zeromqServer = (struct ZeroMQServer *) palloc0(sizeof(ZeroMQServer));
	strcpy(zeromqServer->host, serverAddress->nodeName);
	/*
	 * use port number between ZeromqStartPort and ZeromqStartPort+ZeromqPortCount
	 * as zeromq server port
	 */
	zeromqServer->port = random() % ZeromqPortCount + ZeromqStartPort;

	if (copyStatement->filename != NULL)
	{
		strcpy(zeromqServer->file, copyStatement->filename);
	}

	clientCopyCommand = ConstructBulkloadCopyStmt(copyStatement, masterNodeAddress,
			serverAddress->nodeName, zeromqServer->port);

	events = (struct epoll_event *) palloc0(MaxEvents * sizeof(struct epoll_event));
	efd = epoll_create1(0);
	if (efd == -1)
	{
		elog(ERROR, "epoll_create failed");
	}

	foreach(workerCell, workerNodeList)
	{
		workerNode = (WorkerNode *) lfirst(workerCell);
		nodeName = workerNode->workerName;
		nodePort = workerNode->workerPort;
		multiConn = GetNodeConnection(FOR_DML, nodeName, nodePort);
		conn = multiConn->pgConn;
		if (conn == NULL)
		{
			elog(WARNING, "connect to %s:%d failed", nodeName, nodePort);
		}
		else
		{
			int querySent = PQsendQuery(conn, clientCopyCommand->data);
			if (querySent == 0)
			{
				elog(WARNING, "send bulkload copy to %s:%d failed: %s", nodeName, nodePort,
						PQerrorMessage(conn));
			}
			else
			{
				if (PQsetnonblocking(conn, 1) == -1)
				{
					/*
					 * make sure it wouldn't cause to fatal error even in blocking mode
					 */
					elog(WARNING, "%s:%d set non-blocking failed", nodeName, nodePort);
				}
				sock = PQsocket(conn);
				if (sock < 0)
				{
					elog(WARNING, "%s:%d get socket failed", nodeName, nodePort);
				}
				else
				{
					event = (struct epoll_event *) palloc0(sizeof(struct epoll_event));
					event->events = EPOLLIN | EPOLLERR | EPOLLET;
					event->data.fd = sock;
					if (epoll_ctl(efd, EPOLL_CTL_ADD, sock, event) != 0)
					{
						elog(WARNING, "epoll_ctl add socket of %s:%d failed", nodeName, nodePort);
					}
					else
					{
						/*
						 * finally we append the connection which we have sent query and got it's
						 * socket file descriptor successfully to connection list.
						 */
						workerConnectionList = lappend(workerConnectionList, conn);
					}
				}
			}
		}
	}
	workerConnectionCount = list_length(workerConnectionList);
	if (workerConnectionCount == 0)
	{
		elog(ERROR, "Can't send bulkload copy to any worker");
	}

	/*
	 * array representing the status of worker connection:
	 * -1: worker failed
	 *	0: worker still running
	 *	1: worker succeed
	 */
	finish = (int *) palloc0(workerConnectionCount * sizeof(int));

	PG_TRY();
	{
		int natts = CopyGetAttnums(relationId, copyStatement->attlist);
		/*
		 * check status of workers before starting zeromq server in case
		 * of unproper bulkload copy command.
		 * TODO@luoyuanhao: if error occurs after EpollTimeOut, there's
		 * still possibility of deadlock, refactoring this code properly.
		 */
		do
		{
			nevents = epoll_wait(efd, events, MaxEvents, EpollTimeout * 10);
			if (nevents == -1)
			{
				elog(ERROR, "epoll_wait error(%d): %s", errno, strerror(errno));
			}
			for (loopIndex = 0; loopIndex	< nevents; loopIndex++)
			{
				conn = GetConnectionBySock(workerConnectionList, events[loopIndex].data.fd,
						&connIdx);
				Assert(conn != NULL);
				if (finish[connIdx] != 0) continue;
				/*
				 * if bulkload copy command is okay, there should be neither output nor error
				 * message in socket, otherwise, bulkload copy command is wrong.
				 */
				elog(WARNING, "bulkload copy in %s:%s fail, read log file to get error message",
						PQhost(conn), PQport(conn));
				finish[connIdx] = -1;
				finishCount++;
			}
		} while(nevents != 0);
		if (finishCount == workerConnectionCount)
		{
			elog(ERROR, "bulkload copy commands fail in all workers");
		}

		StartZeroMQServer(zeromqServer, copyStatement->is_program,
				IsBinaryCopy(copyStatement), natts);

		while (finishCount < workerConnectionCount)
		{
			CHECK_FOR_INTERRUPTS();

			/* send EOF message */
			SendMessage(zeromqServer, "KILL", 4, true);

			/*
			 * wait indefinitely may cause to dead-lock: we send a 'KILL' signal,
			 * but bload may not catch it and wait indefinitely, so COPY command
			 * wouldn't finish and therefore there are no responses(events) from
			 * pg connection.
			 */
			nevents = epoll_wait(efd, events, MaxEvents, EpollTimeout);
			if (nevents == -1)
			{
				elog(ERROR, "epoll_wait error(%d): %s", errno, strerror(errno));
			}
			for (loopIndex = 0; loopIndex	< nevents; loopIndex++)
			{
				conn = GetConnectionBySock(workerConnectionList, events[loopIndex].data.fd,
						&connIdx);
				Assert(conn != NULL);
				if (finish[connIdx] != 0) continue;
				if (events[loopIndex].events & EPOLLERR)
				{
					elog(WARNING, "socket of %s:%s error", PQhost(conn), PQport(conn));
					finish[connIdx] = -1;
					finishCount++;
					continue;
				}
				if (events[loopIndex].events & EPOLLIN)
				{
					rc = PQconsumeInput(conn);
					if (rc == 0)
					{
						elog(WARNING, "%s:%s error:%s", PQhost(conn), PQport(conn),
								PQerrorMessage(conn));
						finish[connIdx] = -1;
						finishCount++;
					}
					else
					{
						if (!PQisBusy(conn))
						{
							res = PQgetResult(conn);
							if (res == NULL)
							{
								finish[connIdx] = 1;
							}
							else
							{
								if (PQresultStatus(res) != PGRES_COMMAND_OK &&
										PQresultStatus(res) != PGRES_TUPLES_OK)
								{
									elog(WARNING, "%s:%s error:%s", PQhost(conn), PQport(conn),
											PQresultErrorMessage(res));
									finish[connIdx] = -1;
								}
								else
								{
									processedRowCount += atol(PQcmdTuples(res));
									finish[connIdx] = 1;
								}
								PQclear(res);
							}
							finishCount++;
						}
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		char *errbuf = (char *) palloc0(NAMEDATALEN);
		foreach(workerCell, workerConnectionList)
		{
			PGconn *conn = (PGconn *) lfirst(workerCell);
			PGcancel *cancel = PQgetCancel(conn);
			if (cancel != NULL && PQcancel(cancel, errbuf, NAMEDATALEN) != 1)
			{
				elog(WARNING, "%s", errbuf);
			}
			PQfreeCancel(cancel);
		}
		StopZeroMQServer(zeromqServer);

		PG_RE_THROW();
	}
	PG_END_TRY();

	for (loopIndex = 0; loopIndex < workerConnectionCount; loopIndex++)
	{
		if (finish[loopIndex] == -1)
		{
			failCount++;
		}
	}
	/*
	 * TODO@luoyuanhao: two phase commit, if failCount > 0, rollback.
	 */
	StopZeroMQServer(zeromqServer);
	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				"COPY " UINT64_FORMAT, processedRowCount);
	}
}

/*
 * IsBulkloadCopy checks if the given copy statement has the 'method' option
 * and the value is 'bulkload'.
 */
bool
IsBulkloadCopy(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	DefElem *defel = NULL;
	foreach(optionCell, copyStatement->options)
	{
		defel = (DefElem *) lfirst(optionCell);
		if (strcasecmp(defel->defname, "method") == 0)
		{
			char *method = defGetString(defel);
			if (strcasecmp(method, "bulkload") != 0)
			{
				elog(ERROR, "Unsupported method: %s. Valid values('bulkload')", method);
			}
			else
			{
				return true;
			}
		}
	}
	return false;
}

/*
 * IsBinaryCopy checks if the given copy statement has the 'format' option
 * and the value is 'binary'.
 */
	bool
IsBinaryCopy(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	DefElem *defel = NULL;
	foreach(optionCell, copyStatement->options)
	{
		defel = (DefElem *) lfirst(optionCell);
		if (strcasecmp(defel->defname, "format") == 0)
		{
			char *method = defGetString(defel);
			if (strcasecmp(method, "binary") == 0)
			{
				return true;
			}
		}
	}
	return false;
}

/*
 * IsBulkloadClient checks if the given copy statement has the 'bulkload_host' option.
 */
	bool
IsBulkloadClient(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	DefElem *defel = NULL;
	foreach(optionCell, copyStatement->options)
	{
		defel = (DefElem *) lfirst(optionCell);
		if (strcasecmp(defel->defname, "bulkload_host") == 0)
		{
			return true;
		}
	}
	return false;
}

/*
 * RemoveBulkloadOptions removes bulkload related copy options from the option
 * list of the copy statement.
 */
static void
RemoveBulkloadOptions(CopyStmt *copyStatement)
{
	List *newOptionList = NIL;
	ListCell *optionCell = NULL;

	/* walk over the list of all options */
	foreach(optionCell, copyStatement->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		/* skip master related options */
		if ((strcmp(option->defname, "bulkload_host") == 0) ||
				(strcmp(option->defname, "bulkload_port") == 0) ||
				(strcmp(option->defname, "method") == 0))
		{
			continue;
		}

		newOptionList = lappend(newOptionList, option);
	}

	copyStatement->options = newOptionList;
}

/*
 * BulkloadServerAddress gets the bulkload zeromq server address from copy options
 * and returns it. Note that if the bulkload_port is not provided, we use 5557 as
 * the default port.
 */
static NodeAddress *
BulkloadServerAddress(CopyStmt *copyStatement)
{
	NodeAddress *bulkloadServer = (NodeAddress *) palloc0(sizeof(NodeAddress));
	char *nodeName = NULL;

	/* set default port to 5557 */
	uint32 nodePort = 5557;

	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "bulkload_host", NAMEDATALEN) == 0)
		{
			nodeName = defGetString(defel);
		}
		else if (strncmp(defel->defname, "bulkload_port", NAMEDATALEN) == 0)
		{
			nodePort = defGetInt32(defel);
		}
	}

	bulkloadServer->nodeName = nodeName;
	bulkloadServer->nodePort = nodePort;
	return bulkloadServer;
}

/*
 * ConstructBulkloadCopyStmt constructs the text of a Bulkload COPY statement for
 * executing in bulkload copy client.
 */
static StringInfo
ConstructBulkloadCopyStmt(CopyStmt *copyStatement, NodeAddress *masterNodeAddress,
		char *nodeName, uint32 nodePort)
{
	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);
	List *attlist = copyStatement->attlist;
	ListCell *lc = NULL;
	char *binaryPath = NULL;
	StringInfo optionsString = NULL;
	StringInfo command = NULL;
	int res;
	bool isfirst = true;

	RemoveBulkloadOptions(copyStatement);

	binaryPath = (char *) palloc0(NAMEDATALEN);
	res = readlink("/proc/self/exe", binaryPath, NAMEDATALEN);
	if (res == -1)
	{
		elog(ERROR, "%s", "Can't get absolute path of PG_HOME");
	}
	else
	{
		/*
		 * original string would be "/path_of_pg_home/bin/postgres"
		 * after cutting it turns to be "/path_of_pg_home/bin/"
		 */
		binaryPath[res - 8] = '\0';
		/* append 'bload' */
		strcat(binaryPath, "bload");
	}

	optionsString = DeparseCopyStatementOptions(copyStatement->options);
	command = makeStringInfo();
	appendStringInfo(command, "COPY %s", qualifiedName);
	if (list_length(attlist) != 0)
	{
		appendStringInfoChar(command, '(');
		foreach(lc, attlist)
		{
			if (isfirst)
			{
				isfirst = false;
			}
			else
			{
				appendStringInfoString(command, ", ");
			}
			appendStringInfoString(command, strVal(lfirst(lc)));
		}
		appendStringInfoChar(command, ')');
	}
	appendStringInfo(command, " FROM PROGRAM '%s' WITH(master_host '%s', "
			"master_port %d, method 'bulkload', bulkload_host '%s', bulkload_port %d",
			binaryPath,
			masterNodeAddress->nodeName,
			masterNodeAddress->nodePort,
			nodeName,
			nodePort);
	if (strlen(optionsString->data) != 0)
	{
		appendStringInfo(command, ", %s)", optionsString->data);
	}
	else
	{
		appendStringInfoChar(command, ')');
	}
	return command;
}

/*
 * DeparseCopyStatementOptions construct the text command in WITH clause of COPY stmt.
 */
static StringInfo
DeparseCopyStatementOptions(List *options)
{
	StringInfo optionsStr = makeStringInfo();
	ListCell *option;
	bool isfirst = true;
	DefElem *defel = NULL;
	foreach(option, options)
	{
		if (isfirst) isfirst = false;
		else appendStringInfoString(optionsStr, ", ");

		defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "format") == 0)
		{
			appendStringInfo(optionsStr, "format %s", defGetString(defel));
		}
		else if (strcmp(defel->defname, "oids") == 0)
		{
			appendStringInfo(optionsStr, "oids %s", defGetBoolean(defel) ? "true" : "false");
		}
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			appendStringInfo(optionsStr, "freeze %s", defGetBoolean(defel) ? "true" : "false");
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			appendStringInfo(optionsStr, "delimiter '%s'", defGetString(defel));
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			appendStringInfo(optionsStr, "null '%s'", defGetString(defel));
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			appendStringInfo(optionsStr, "header %s", defGetBoolean(defel) ? "true" : "false");
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			appendStringInfo(optionsStr, "quote '%s'", defGetString(defel));
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (strcmp(defGetString(defel), "\\") == 0)
			{
				appendStringInfo(optionsStr, "quote '\\%s'", defGetString(defel));
			}
			else
			{
				appendStringInfo(optionsStr, "quote '%s'", defGetString(defel));
			}
		}
		/* unhandle force_quote/force_not_null/force_null and convert_selectively*/
		//else if (strcmp(defel->defname, "force_quote") == 0)
		//{
		//			if (cstate->force_quote || cstate->force_quote_all)
		//							ereport(ERROR,
		//															(errcode(ERRCODE_SYNTAX_ERROR),
		//															 errmsg("conflicting or redundant options")));
		//			if (defel->arg && IsA(defel->arg, A_Star))
		//							cstate->force_quote_all = true;
		//			else if (defel->arg && IsA(defel->arg, List))
		//							cstate->force_quote = (List *) defel->arg;
		//			else
		//							ereport(ERROR,
		//															(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		//															 errmsg("argument to option \"%s\" must be a list of column names",
		//																			 defel->defname)));
		//}
		//else if (strcmp(defel->defname, "force_not_null") == 0)
		//{
		//			if (cstate->force_notnull)
		//							ereport(ERROR,
		//															(errcode(ERRCODE_SYNTAX_ERROR),
		//															 errmsg("conflicting or redundant options")));
		//			if (defel->arg && IsA(defel->arg, List))
		//							cstate->force_notnull = (List *) defel->arg;
		//			else
		//							ereport(ERROR,
		//															(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		//															 errmsg("argument to option \"%s\" must be a list of column names",
		//																			 defel->defname)));
		//}
		//else if (strcmp(defel->defname, "force_null") == 0)
		//{
		//			if (cstate->force_null)
		//							ereport(ERROR,
		//															(errcode(ERRCODE_SYNTAX_ERROR),
		//															 errmsg("conflicting or redundant options")));
		//			if (defel->arg && IsA(defel->arg, List))
		//							cstate->force_null = (List *) defel->arg;
		//			else
		//							ereport(ERROR,
		//															(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		//															 errmsg("argument to option \"%s\" must be a list of column names",
		//																			 defel->defname)));
		//}
		//else if (strcmp(defel->defname, "convert_selectively") == 0)
		//{
		//			/*
		//			 * Undocumented, not-accessible-from-SQL option: convert only the
		//			 * named columns to binary form, storing the rest as NULLs. It's
		//			 * allowed for the column list to be NIL.
		//			 */
		//			if (cstate->convert_selectively)
		//							ereport(ERROR,
		//															(errcode(ERRCODE_SYNTAX_ERROR),
		//															 errmsg("conflicting or redundant options")));
		//			cstate->convert_selectively = true;
		//			if (defel->arg == NULL || IsA(defel->arg, List))
		//							cstate->convert_select = (List *) defel->arg;
		//			else
		//							ereport(ERROR,
		//															(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		//															 errmsg("argument to option \"%s\" must be a list of column names",
		//																			 defel->defname)));
		//}
		//else if (strcmp(defel->defname, "encoding") == 0)
		//{
		//			appendStringInfo(optionsStr, "encoding '%s'", defGetString(defel));
		//}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
						 defel->defname)));
	}
	return optionsStr;
}
/*
 * BulkloadCopyToNewShards executes the bulkload COPY command sent by bulkload server
 * for APPEND distributed table.
 * It acts just like CopyToNewShards() but records are received from zeromq server.
 */
static void
BulkloadCopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId)
{
	NodeAddress *bulkloadServer = BulkloadServerAddress(copyStatement);
	RemoveBulkloadOptions(copyStatement);
	RebuildBulkloadCopyStatement(copyStatement, bulkloadServer);
	CopyToNewShards(copyStatement, completionTag, relationId);
}
/*
 * LocalAddress gets the host and port of current running postgres.
 */
static NodeAddress *
LocalAddress(void)
{
	NodeAddress *node = (NodeAddress *) palloc0(sizeof(NodeAddress));
	char *host = (char *) palloc0(32);
	const char *portStr = GetConfigOption("port", true, false);
	int rc = gethostname(host, 32);
	if (rc != 0)
	{
		strcpy(host, "localhost");
		elog(WARNING, "gethostname fail: %s, use 'localhost'", strerror(errno));
	}
	node->nodeName = host;
	if (portStr == NULL)
	{
		node->nodePort = 5432;
	}
	else
	{
		node->nodePort = atoi(portStr);
	}
	return node;
}

/*
 * MasterWorkerNodeList dispatches the master_get_active_worker_nodes
 * between local or remote master node according to the master connection state.
 */
static List *
MasterWorkerNodeList(void)
{
	List *workerNodeList = NIL;
	if (masterConnection == NULL)
	{
		workerNodeList = WorkerNodeList();
	}
	else
	{
		workerNodeList = RemoteWorkerNodeList();
	}

	return workerNodeList ;
}

/*
 * RemoteWorkerNodeList gets the active worker node list from the remote master node.
 */
static List *
RemoteWorkerNodeList(void)
{
	List *workerNodeList = NIL;
	PGresult *queryResult = NULL;

	StringInfo workerNodeCommand = makeStringInfo();
	appendStringInfoString(workerNodeCommand, ACTIVE_WORKER_NODE_QUERY);

	queryResult = PQexec(masterConnection->pgConn, workerNodeCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int rowCount = PQntuples(queryResult);
		int rowIndex = 0;

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			WorkerNode *workerNode =
				(WorkerNode *) palloc0(sizeof(WorkerNode));

			char *host = PQgetvalue(queryResult, rowIndex, 0);
			char *port = PQgetvalue(queryResult, rowIndex, 1);
			strcpy(workerNode->workerName, host);
			workerNode->workerPort = atoi(port);

			workerNodeList = lappend(workerNodeList, workerNode);
		}
	}
	else
	{
		elog(ERROR, "could not get active worker node list from the master node: %s",
				PQresultErrorMessage(queryResult));
	}
	PQclear(queryResult);

	return workerNodeList;
}

/*
 * StartZeroMQServer starts a zeromq socket, reads data from file, remote frontend
 * or the output of the program and then sends it to zeromq client.
 * TODO@luoyuanhao: Currently we don't support bulkload copy from stdin with binary
 */
static void
StartZeroMQServer(ZeroMQServer *zeromqServer, bool is_program, bool binary, int natts)
{
	uint64_t start = 0, read = 0;
	FILE *fp = NULL;
	char *buf = NULL;
	char zeroaddr[32];
	void *context = NULL;
	void *sender = NULL;
	void *controller = NULL;
	char *file = zeromqServer->file;
	bool pipe = (strlen(file) == 0);
	StringInfoData msgbuf;
	int16 format = binary ? 1 : 0;
	int loopIdx;
	bool copyDone = false;

	context = zmq_ctx_new ();
	Assert(context != NULL);
	//			Socket to send messages on
	sender = zmq_socket(context, ZMQ_PUSH);
	if (sender == NULL)
	{
		elog(ERROR, "zmq_socket() error(%d): %s", errno, zmq_strerror(errno));
	}
	// Socket for control signal
	controller = zmq_socket(context, ZMQ_PUB);
	if (controller == NULL)
	{
		elog(ERROR, "zmq_socket() error(%d): %s", errno, zmq_strerror(errno));
	}

	zeromqServer->context = context;
	zeromqServer->sender = sender;
	zeromqServer->controller = controller;

	sprintf(zeroaddr, "tcp://*:%d", zeromqServer->port);
	if (zmq_bind (sender, zeroaddr) != 0)
	{
		elog(ERROR, "zmq_bind() error(%d): %s", errno, zmq_strerror(errno));
	}
	sprintf(zeroaddr, "tcp://*:%d", zeromqServer->port + 1);
	if (zmq_bind (controller, zeroaddr) != 0)
	{
		elog(ERROR, "zmq_bind() error(%d): %s", errno, zmq_strerror(errno));
	}

	if (pipe)
	{
		/*
		 * inspired by ReceivedCopyBegin()
		 */
		Assert(!binary);
		pq_beginmessage(&msgbuf, 'G');
		pq_sendbyte(&msgbuf, format);					 /* overall format */
		pq_sendint(&msgbuf, natts, 2);
		for (loopIdx = 0; loopIdx < natts; loopIdx++)
			pq_sendint(&msgbuf, format, 2);				 /* per-column formats */
		pq_endmessage(&msgbuf);
		pq_flush();

		initStringInfo(&msgbuf);
		/* get records from fe */
		while (!copyDone)
		{
			int mtype;
			CHECK_FOR_INTERRUPTS();
			HOLD_CANCEL_INTERRUPTS();
			/*
			 * inspired by CopyGetData()
			 */
			pq_startmsgread();
			mtype = pq_getbyte();
			if (mtype == EOF)
				elog(ERROR, "unexpected EOF on client connection with an open transaction");
			if (pq_getmessage(&msgbuf, 0))
				elog(ERROR, "unexpected EOF on client connection with an open transaction");
			RESUME_CANCEL_INTERRUPTS();
			switch (mtype)
			{
				case 'd':							 /* CopyData */
					SendMessage(zeromqServer, msgbuf.data, msgbuf.len, false);
					break;
				case 'c':							 /* CopyDone */
					/* COPY IN correctly terminated by frontend */
					copyDone = true;
					break;
				case 'f':							 /* CopyFail */
					elog(ERROR, "COPY from stdin failed: %s", pq_getmsgstring(&msgbuf));
					break;
				case 'H':							 /* Flush */
				case 'S':							 /* Sync */
					break;
				default:
					elog(ERROR, "unexpected message type 0x%02X during COPY from stdin", mtype);
					break;
			}
		}
		return;
	}

	Assert(!pipe);
	if (is_program)
	{
		fp = popen(file, PG_BINARY_R);
		if (fp == NULL)
		{
			elog(ERROR, "could not execute command \"%s\"", file);
		}
	}
	else
	{
		struct stat st;
		fp = fopen(file, PG_BINARY_R);
		if (fp == NULL)
		{
			elog(ERROR, "could not open file \"%s\": %s", file, strerror(errno));
		}
		if (fstat(fileno(fp), &st))
		{
			elog(ERROR, "could not stat file \"%s\"", file);
		}
		if (S_ISDIR(st.st_mode))
		{
			elog(ERROR, "\"%s\" is a directory", file);
		}
	}

	buf = (char *) palloc0(BatchSize + MaxRecordSize + 1);
	Assert(buf != NULL);

	if (!binary)
	{
		while (true)
		{
			uint64_t i;
			CHECK_FOR_INTERRUPTS();
			start = 0;
			read = fread(buf + start, 1, BatchSize, fp);
			start += read;
			if (read < BatchSize) break;
			for (i = 0; i < MaxRecordSize; i++)
			{
				read = fread(buf + start, 1, 1, fp);
				if (read == 0) break;
				Assert(read == 1);
				start += read;
				if (buf[start - 1] == '\n') break;
			}
			if (i == MaxRecordSize)
			{
				char *tmp = (char*) palloc0(MaxRecordSize + 1);
				strncpy(tmp, buf + start - MaxRecordSize, MaxRecordSize);
				tmp[MaxRecordSize] = '\0';
				elog(ERROR, "Too large record: %s", tmp);
			}
			else
			{
				SendMessage(zeromqServer, buf, start, false);
			}
		}
		if (start > 0)
		{
			SendMessage(zeromqServer, buf, start, false);
		}
	}
	else
	{
		int32 flag, elen;
		int16 fld_count;

		/* Signature */
		read = fread(buf, 1, 11, fp);
		if (read != 11 || strncmp(buf, BinarySignature, 11) != 0)
		{
			elog(ERROR, "COPY file signature not recognized");
		}
		/* Flags field */
		read = fread(buf, 1, 4, fp);
		if (read != 4)
		{
			elog(ERROR, "invalid COPY file header (missing flags)");
		}
		flag = (int32) ntohl(*(uint32 *)buf);
		if ((flag & (1 << 16)) != 0)
		{
			elog(ERROR, "bulkload COPY can't set OID flag");
		}
		flag &= ~(1 << 16);
		if ((flag >> 16) != 0)
		{
			elog(ERROR, "unrecognized critical flags in COPY file header");
		}
		/* Header extension length */
		read = fread(buf, 1, 4, fp);
		if (read != 4)
		{
			elog(ERROR, "invalid COPY file header (missing length)");
		}
		elen = (int32) ntohl(*(uint32 *)buf);
		/* Skip extension header, if present */
		read = fread(buf, 1, elen, fp);
		if (read != elen)
		{
			elog(ERROR, "invalid COPY file header (wrong length)");
		}

		/* handle tuples one by one */
		while (true)
		{
			int16 fld_index;
			int32 fld_size;

			CHECK_FOR_INTERRUPTS();
			start = 0;
			read = fread(buf + start, 1, 2, fp);
			if (read != 2)
			{
				/* EOF detected (end of file, or protocol-level EOR) */
				break;
			}
			fld_count = (int16) ntohs(*(uint16 *)buf);
			if (fld_count == -1)
			{
				read = fread(buf + start, 1, 1, fp);
				if (read == 1)
				{
					elog(ERROR, "received copy data after EOF marker");
				}
				/* Received EOF marker */
				break;
			}
			start += 2;
			for (fld_index = 0; fld_index < fld_count; fld_index++)
			{
				read = fread(buf + start, 1, 4, fp);
				if (read != 4)
				{
					elog(ERROR, "unexpected EOF in COPY data");
				}
				fld_size = (int32) ntohl(*(uint32 *)(buf + start));
				if (fld_size == -1)
				{
					/* null value */
					start += 4;
				}
				else if (fld_size < 0)
				{
					elog(ERROR, "invalid field size %d", fld_size);
				}
				else
				{
					start += 4;
					read = fread(buf + start, 1, fld_size, fp);
					if (read != fld_size)
					{
						elog(ERROR, "unexpected EOF in COPY data");
					}
					else
					{
						/* skip field value */
						start += fld_size;
					}
				}

				if (start >= MaxRecordSize + BatchSize)
				{
					elog(ERROR, "Too large binary record: %s", buf);
				}
			}
			SendMessage(zeromqServer, buf, start, false);
		}
	}
	if (is_program)
	{
		int rc = pclose(fp);
		if (rc == -1)
		{
			elog(WARNING, "could not close pipe to external command \"%s\"", file);
		}
		else if (rc != 0)
		{
			elog(WARNING, "program \"%s\" failed", file);
		}
	}
	else if (fclose(fp) != 0)
	{
		elog(WARNING, "close file error: %s", strerror(errno));
	}
}

/*
 * SendMessage sends message to zeromq socket.
 * If kill is true, send KILL signal.
 */
static void
SendMessage(ZeroMQServer *zeromqServer, char *buf, size_t len, bool kill)
{
	int rc;
	if (kill)
	{
		rc = zmq_send(zeromqServer->controller, buf, len, 0);
	}
	else
	{
		rc = zmq_send(zeromqServer->sender, buf, len, 0);
	}
	if (rc != len)
	{
		elog(LOG, "zmq_send() error(%d): %s", errno, zmq_strerror(errno));
	}
}

/*
 * StopZeroMQServer stops zeromq server and releases related resources.
 */
static void
StopZeroMQServer(ZeroMQServer *zeromqServer)
{
	if (zeromqServer->sender)
	{
		zmq_close(zeromqServer->sender);
		zeromqServer->sender = NULL;
	}
	if (zeromqServer->controller)
	{
		zmq_close(zeromqServer->controller);
		zeromqServer->controller = NULL;
	}
	if (zeromqServer->context)
	{
		zmq_ctx_destroy(zeromqServer->context);
		zeromqServer->context = NULL;
	}
}

/*
 * RebuildBulkloadCopyStatement adds bulkload server address as PROGRAM's arguments.
 */
static void
RebuildBulkloadCopyStatement(CopyStmt *copyStatement, NodeAddress *bulkloadServer)
{
	StringInfo tmp = makeStringInfo();
	appendStringInfo(tmp, "%s %s %d", copyStatement->filename, bulkloadServer->nodeName,
			bulkloadServer->nodePort);
	if (IsBinaryCopy(copyStatement))
	{
		appendStringInfoString(tmp, " binary");
	}
	copyStatement->filename = tmp->data;
}
/*
 * MasterRelationId gets the relationId of relation from the master node.
 */
static Oid
MasterRelationId(char *qualifiedName)
{
	Oid relationId = 0;
	PGresult *queryResult = NULL;

	StringInfo relationIdCommand = makeStringInfo();
	appendStringInfo(relationIdCommand, RELATIONID_QUERY, qualifiedName);

	queryResult = PQexec(masterConnection->pgConn, relationIdCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		char *relationIdString = PQgetvalue(queryResult, 0, 0);
		if (relationIdString == NULL || (*relationIdString) == '\0')
		{
			elog(ERROR, "could not find relationId for the table %s", qualifiedName);
		}

		relationId = (Oid) atoi(relationIdString);
	}
	else
	{
		elog(ERROR, "could not get the relationId of the distributed table %s: %s",
				qualifiedName, PQresultErrorMessage(queryResult));
	}
	PQclear(queryResult);
	return relationId;
}

/*
 * MasterDistributedTableCacheEntry get's metadada from master node and
 * build's a DistTableCacheEntry for the relation.
 */
static DistTableCacheEntry *
MasterDistributedTableCacheEntry(RangeVar *relation)
{
	DistTableCacheEntry *cacheEntry = NULL;
	Oid relationId = 0;

	/* temporary value */
	char *partmethod = NULL;
	char *colocationid = NULL;
	char *repmodel = NULL;
	char *shardidString = NULL;
	char *storageString = NULL;
	char *minValueString = NULL;
	char *maxValueString = NULL;
	char *shardidStringEnd = NULL;

	/* members of pg_dist_partition and DistTableCacheEntry */
	char *partitionKeyString = NULL;
	char partitionMethod = 0;
	uint32 colocationId = INVALID_COLOCATION_ID;
	char replicationModel = 0;

	/* members of pg_dist_shard */
	int64 shardId;
	char storageType;
	Datum minValue = 0;
	Datum maxValue = 0;
	bool minValueExists = true;
	bool maxValueExists = true;

	/* members of DistTableCacheEntry */
	int shardIntervalArrayLength = 0;
	ShardInterval **shardIntervalArray = NULL;
	ShardInterval **sortedShardIntervalArray = NULL;
	FmgrInfo *shardIntervalCompareFunction = NULL;
	FmgrInfo *hashFunction = NULL;
	bool hasUninitializedShardInterval = false;
	bool hasUniformHashDistribution = false;

	ShardInterval *shardInterval = NULL;
	Oid intervalTypeId = INT4OID;
	int32 intervalTypeMod = -1;
	int16 intervalTypeLen = 0;
	bool intervalByVal = false;
	char intervalAlign = '0';
	char intervalDelim = '0';
	Oid typeIoParam = InvalidOid;
	Oid inputFunctionId = InvalidOid;
	PGresult *queryResult = NULL;
	StringInfo partitionKeyStringInfo = makeStringInfo();
	StringInfo queryString = makeStringInfo();

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);
	relationId = MasterRelationId(qualifiedName);

	Assert(masterConnection != NULL);

	appendStringInfo(queryString, "SELECT * FROM pg_dist_partition WHERE logicalrelid=%d",
			relationId);
	queryResult = PQexec(masterConnection->pgConn, queryString->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int rowCount = PQntuples(queryResult);
		Assert(rowCount == 1);

		partmethod = PQgetvalue(queryResult, 0, Anum_pg_dist_partition_partmethod - 1);
		partitionKeyString = PQgetvalue(queryResult, 0, Anum_pg_dist_partition_partkey - 1);
		colocationid = PQgetvalue(queryResult, 0, Anum_pg_dist_partition_colocationid - 1);
		repmodel = PQgetvalue(queryResult, 0, Anum_pg_dist_partition_repmodel - 1);

		partitionMethod = partmethod[0];
		appendStringInfoString(partitionKeyStringInfo, partitionKeyString);
		partitionKeyString = partitionKeyStringInfo->data;
		colocationId = (uint32) atoi(colocationid);
		replicationModel = repmodel[0];
	}
	else
	{
		elog(ERROR, "could not get metadata of table %s: %s",
				qualifiedName, PQresultErrorMessage(queryResult));
	}
	PQclear(queryResult);

	get_type_io_data(intervalTypeId, IOFunc_input, &intervalTypeLen, &intervalByVal,
			&intervalAlign, &intervalDelim, &typeIoParam, &inputFunctionId);

	resetStringInfo(queryString);
	appendStringInfo(queryString, "SELECT * FROM pg_dist_shard WHERE logicalrelid=%d",
			relationId);
	queryResult = PQexec(masterConnection->pgConn, queryString->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int arrayIndex = 0;

		shardIntervalArrayLength = PQntuples(queryResult);
		shardIntervalArray = (ShardInterval **) palloc0(
				shardIntervalArrayLength * sizeof(ShardInterval *));

		for (arrayIndex = 0; arrayIndex < shardIntervalArrayLength; arrayIndex++)
		{
			shardidString =
				PQgetvalue(queryResult, arrayIndex, Anum_pg_dist_shard_shardid - 1);
			storageString =
				PQgetvalue(queryResult, arrayIndex, Anum_pg_dist_shard_shardstorage - 1);
			minValueString =
				PQgetvalue(queryResult, arrayIndex, Anum_pg_dist_shard_shardminvalue - 2);
			maxValueString =
				PQgetvalue(queryResult, arrayIndex, Anum_pg_dist_shard_shardmaxvalue - 2);

			shardId = strtoul(shardidString, &shardidStringEnd, 0);
			storageType = storageString[0];
			/* finally convert min/max values to their actual types */
			minValue = OidInputFunctionCall(inputFunctionId, minValueString,
					typeIoParam, intervalTypeMod);
			maxValue = OidInputFunctionCall(inputFunctionId, maxValueString,
					typeIoParam, intervalTypeMod);

			shardInterval = CitusMakeNode(ShardInterval);
			shardInterval->relationId = relationId;
			shardInterval->storageType = storageType;
			shardInterval->valueTypeId = intervalTypeId;
			shardInterval->valueTypeLen = intervalTypeLen;
			shardInterval->valueByVal = intervalByVal;
			shardInterval->minValueExists = minValueExists;
			shardInterval->maxValueExists = maxValueExists;
			shardInterval->minValue = minValue;
			shardInterval->maxValue = maxValue;
			shardInterval->shardId = shardId;

			shardIntervalArray[arrayIndex] = shardInterval;
		}
	}
	else
	{
		elog(ERROR, "could not get metadata of table %s: %s",
				qualifiedName, PQresultErrorMessage(queryResult));
	}
	PQclear(queryResult);

	/* decide and allocate interval comparison function */
	if (shardIntervalArrayLength > 0)
	{
		shardIntervalCompareFunction = GetFunctionInfo(INT4OID, BTREE_AM_OID,
				BTORDER_PROC);
	}

	/* sort the interval array */
	sortedShardIntervalArray = SortShardIntervalArray(shardIntervalArray,
			shardIntervalArrayLength,
			shardIntervalCompareFunction);

	/* check if there exists any shard intervals with no min/max values */
	hasUninitializedShardInterval =
		HasUninitializedShardInterval(sortedShardIntervalArray, shardIntervalArrayLength);

	/* we only need hash functions for hash distributed tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		TypeCacheEntry *typeEntry = NULL;
		Node *partitionNode = stringToNode(partitionKeyString);
		Var *partitionColumn = (Var *) partitionNode;
		Assert(IsA(partitionNode, Var));
		typeEntry = lookup_type_cache(partitionColumn->vartype,
				TYPECACHE_HASH_PROC_FINFO);

		hashFunction = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

		fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo), CurrentMemoryContext);

		/* check the shard distribution for hash partitioned tables */
		hasUniformHashDistribution =
			HasUniformHashDistribution(sortedShardIntervalArray, shardIntervalArrayLength);
	}

	cacheEntry = (DistTableCacheEntry *) palloc0(sizeof(DistTableCacheEntry));
	cacheEntry->relationId = relationId;
	cacheEntry->isValid = true;
	cacheEntry->isDistributedTable = true;
	cacheEntry->partitionKeyString = partitionKeyString;
	cacheEntry->partitionMethod = partitionMethod;
	cacheEntry->colocationId = colocationId;
	cacheEntry->replicationModel = replicationModel;
	cacheEntry->shardIntervalArrayLength = shardIntervalArrayLength;
	cacheEntry->sortedShardIntervalArray = sortedShardIntervalArray;
	cacheEntry->shardIntervalCompareFunction = shardIntervalCompareFunction;
	cacheEntry->hashFunction = hashFunction;
	cacheEntry->hasUninitializedShardInterval = hasUninitializedShardInterval;
	cacheEntry->hasUniformHashDistribution = hasUniformHashDistribution;

	return cacheEntry;
}

/*
 * BulkloadCopyToExistingShards implements the COPY table_name FROM ... for HASH
 * distributed table where there are already shards into which to copy. It works just
 * like CopyToExistingShards, except that the later runs only in master node, but the
 * former runs on each worker node. So BulkloadCopyToExistingShards would be mush more
 * faster(# worker times) than CopyToExistingShards for HASH distributed table.
 */
static void
BulkloadCopyToExistingShards(CopyStmt *copyStatement, char *completionTag,
		Oid relationId)
{
	DistTableCacheEntry *cacheEntry = NULL;
	NodeAddress *bulkloadServer = NULL;

	Assert(masterConnection != NULL);
	cacheEntry = MasterDistributedTableCacheEntry(copyStatement->relation);
	InsertDistTableCacheEntry(relationId, cacheEntry);

	bulkloadServer = BulkloadServerAddress(copyStatement);
	RemoveBulkloadOptions(copyStatement);
	RebuildBulkloadCopyStatement(copyStatement, bulkloadServer);

	CopyToExistingShards(copyStatement, completionTag, relationId);
}

/*
 * Get PGconn* and it's index from PGconn* list by socket descriptor.
 */
static PGconn *
GetConnectionBySock(List *connList, int sock, int *connIdx)
{
	PGconn *conn = NULL;
	int idx;
	int n = list_length(connList);
	for (idx = 0; idx < n; idx++)
	{
		conn = (PGconn *) list_nth(connList, idx);
		if (PQsocket(conn) == sock)
		{
			*connIdx = idx;
			return conn;
		}
	}
	return NULL;
}
