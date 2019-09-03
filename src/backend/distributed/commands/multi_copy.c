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
 * If this is the first command in the transaction, we open a new connection for
 * every shard placement. Otherwise we open as many connections as we can to
 * not conflict with previous commands in transactions, in which case some shards
 * may share connections. See the comments of CopyConnectionState for how we
 * operate in that case.
 *
 * We use the PQputCopyData function to copy the data. Because PQputCopyData
 * transmits data asynchronously, the workers will ingest data at least partially
 * in parallel.
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
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/multi_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/memutils.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* use a global connection to the master node in order to skip passing it around */
static MultiConnection *masterConnection = NULL;

/*
 * Data size threshold to switch over the active placement for a connection.
 * If this is too low, overhead of starting COPY commands will hurt the
 * performance. If this is too high, buffered data will use lots of memory.
 * 4MB is a good balance between memory usage and performance. Note that this
 * is irrelevant in the common case where we open one connection per placement.
 */
#define COPY_SWITCH_OVER_THRESHOLD (4 * 1024 * 1024)

typedef struct CopyShardState CopyShardState;
typedef struct CopyPlacementState CopyPlacementState;

/*
 * Multiple shard placements can share one connection. Each connection has one
 * of those placements as the activePlacementState, and others in the
 * bufferedPlacementList. When we want to send a tuple to a CopyPlacementState,
 * we check if it is the active one in its connectionState, and in this case we
 * directly put data on wire. Otherwise, we buffer it so we can put it on wire
 * later, when copy ends or a switch-over happens. See CitusSendTupleToPlacements()
 * for more details.
 *
 * This is done so we are compatible with adaptive_executor. If a previous command
 * in the current transaction has been executed using adaptive_executor.c, then
 * CopyGetPlacementConnection() might return the same connection for multiple
 * placements. We support that case by the buffering mechanism described above.
 *
 * If no previous command in the current transaction has used adaptive_executor.c,
 * then CopyGetPlacementConnection() returns one connection per placement and no
 * buffering happens and we put the copy data directly on connection.
 */
typedef struct CopyConnectionState
{
	/* Used as hash key. Equal to PQsocket(connection->pgConn). */
	int socket;

	MultiConnection *connection;

	/*
	 * Placement for which we have an active COPY going on over connection.
	 * Can be NULL.
	 */
	CopyPlacementState *activePlacementState;

	/*
	 * Other placements that we are buffering data for. Later when a switch-over
	 * happens, we remove an item from this list and set it to activePlacementState.
	 * In this case, old activePlacementState isn't NULL, is added to this list.
	 */
	dlist_head bufferedPlacementList;
} CopyConnectionState;


struct CopyPlacementState
{
	/* Connection state to which the placemement is assigned to. */
	CopyConnectionState *connectionState;

	/* State of shard to which the placement belongs to. */
	CopyShardState *shardState;

	/*
	 * Buffered COPY data. When the placement is activePlacementState of
	 * some connection, this is empty. Because in that case we directly
	 * send the data over connection.
	 */
	StringInfo data;

	/* List node for CopyConnectionState->bufferedPlacementList. */
	dlist_node bufferedPlacementNode;
};

struct CopyShardState
{
	/* Used as hash key. */
	uint64 shardId;

	/* List of CopyPlacementStates for all active placements of the shard. */
	List *placementStateList;
};


/* Local functions forward declarations */
static void CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag);
static void CopyToExistingShards(CopyStmt *copyStatement, char *completionTag);
static void CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId);
static char MasterPartitionMethod(RangeVar *relation);
static void RemoveMasterOptions(CopyStmt *copyStatement);
static void OpenCopyConnectionsForNewShards(CopyStmt *copyStatement,
											ShardConnections *shardConnections, bool
											stopOnFailure,
											bool useBinaryCopyFormat);

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
static void ReportCopyError(MultiConnection *connection, PGresult *result);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor);
static int64 StartCopyToNewShard(ShardConnections *shardConnections,
								 CopyStmt *copyStatement, bool useBinaryCopyFormat);
static int64 MasterCreateEmptyShard(char *relationName);
static int64 CreateEmptyShard(char *relationName);
static int64 RemoteCreateEmptyShard(char *relationName);
static void MasterUpdateShardStatistics(uint64 shardId);
static void RemoteUpdateShardStatistics(uint64 shardId);

static void ConversionPathForTypes(Oid inputType, Oid destType, CopyCoercionData *result);
static Oid TypeForColumnName(Oid relationId, TupleDesc tupleDescriptor, char *columnName);
static Oid * TypeArrayFromTupleDescriptor(TupleDesc tupleDescriptor);
static CopyCoercionData * ColumnCoercionPaths(TupleDesc destTupleDescriptor,
											  TupleDesc inputTupleDescriptor,
											  Oid destRelId, List *columnNameList,
											  Oid *finalColumnTypeArray);
static FmgrInfo * TypeOutputFunctions(uint32 columnCount, Oid *typeIdArray,
									  bool binaryFormat);
static Datum CoerceColumnValue(Datum inputValue, CopyCoercionData *coercionPath);
static void CreateLocalTable(RangeVar *relation, char *nodeName, int32 nodePort);
static List * CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);
static bool CopyStatementHasFormat(CopyStmt *copyStatement, char *formatName);
static bool IsCopyFromWorker(CopyStmt *copyStatement);
static NodeAddress * MasterNodeAddress(CopyStmt *copyStatement);
static void CitusCopyFrom(CopyStmt *copyStatement, char *completionTag);
static HTAB * CreateConnectionStateHash(MemoryContext memoryContext);
static HTAB * CreateShardStateHash(MemoryContext memoryContext);
static CopyConnectionState * GetConnectionState(HTAB *connectionStateHash,
												MultiConnection *connection);
static CopyShardState * GetShardState(uint64 shardId, HTAB *shardStateHash,
									  HTAB *connectionStateHash, bool stopOnFailure,
									  bool *found);
static MultiConnection * CopyGetPlacementConnection(ShardPlacement *placement,
													bool stopOnFailure);
static List * ConnectionStateList(HTAB *connectionStateHash);
static void InitializeCopyShardState(CopyShardState *shardState,
									 HTAB *connectionStateHash,
									 uint64 shardId, bool stopOnFailure);
static void StartPlacementStateCopyCommand(CopyPlacementState *placementState,
										   CopyStmt *copyStatement,
										   CopyOutState copyOutState);
static void EndPlacementStateCopyCommand(CopyPlacementState *placementState,
										 CopyOutState copyOutState);
static void UnclaimCopyConnections(List *connectionStateList);
static void ShutdownCopyConnectionState(CopyConnectionState *connectionState,
										CitusCopyDestReceiver *copyDest);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);
static bool CitusSendTupleToPlacements(TupleTableSlot *slot,
									   CitusCopyDestReceiver *copyDest);
static uint64 ShardIdForTuple(CitusCopyDestReceiver *copyDest, Datum *columnValues,
							  bool *columnNulls);

/* CitusCopyDestReceiver functions */
static void CitusCopyDestReceiverStartup(DestReceiver *copyDest, int operation,
										 TupleDesc inputTupleDesc);
static bool CitusCopyDestReceiverReceive(TupleTableSlot *slot,
										 DestReceiver *copyDest);
static void CitusCopyDestReceiverShutdown(DestReceiver *destReceiver);
static void CitusCopyDestReceiverDestroy(DestReceiver *destReceiver);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_text_send_as_jsonb);


/*
 * CitusCopyFrom implements the COPY table_name FROM. It dispacthes the copy
 * statement to related subfunctions based on where the copy command is run
 * and the partition method of the distributed table.
 */
static void
CitusCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	bool isCopyFromWorker = false;

	BeginOrContinueCoordinatedTransaction();

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

		/* disallow modifications to a partition table which have rep. factor > 1 */
		EnsurePartitionTableNotReplicated(relationId);

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
static bool
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
	MarkRemoteTransactionCritical(masterConnection);
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

	CitusCopyDestReceiver *copyDest = NULL;
	DestReceiver *dest = NULL;

	Relation distributedRelation = NULL;
	Relation copiedDistributedRelation = NULL;
	Form_pg_class copiedDistributedRelationTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	int columnIndex = 0;
	List *columnNameList = NIL;
	Var *partitionColumn = NULL;
	int partitionColumnIndex = INVALID_PARTITION_COLUMN_INDEX;
	TupleTableSlot *tupleTableSlot = NULL;

	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;

	char partitionMethod = 0;
	bool stopOnFailure = false;

	CopyState copyState = NULL;
	uint64 processedRowCount = 0;

	ErrorContextCallback errorCallback;

	/* allocate column values and nulls arrays */
	distributedRelation = heap_open(tableId, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(distributedRelation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/* set up a virtual tuple table slot */
	tupleTableSlot = MakeSingleTupleTableSlotCompat(tupleDescriptor, &TTSOpsVirtual);
	tupleTableSlot->tts_nvalid = columnCount;
	tupleTableSlot->tts_values = columnValues;
	tupleTableSlot->tts_isnull = columnNulls;

	/* determine the partition column index in the tuple descriptor */
	partitionColumn = PartitionColumn(tableId, 0);
	if (partitionColumn != NULL)
	{
		partitionColumnIndex = partitionColumn->varattno - 1;
	}

	/* build the list of column names for remote COPY statements */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			|| currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	executorState = CreateExecutorState();
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	partitionMethod = PartitionMethod(tableId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		stopOnFailure = true;
	}

	/* set up the destination for the COPY */
	copyDest = CreateCitusCopyDestReceiver(tableId, columnNameList, partitionColumnIndex,
										   executorState, stopOnFailure, NULL);
	dest = (DestReceiver *) copyDest;
	dest->rStartup(dest, 0, tupleDescriptor);

	/*
	 * Below, we change a few fields in the Relation to control the behaviour
	 * of BeginCopyFrom. However, we obviously should not do this in relcache
	 * and therefore make a copy of the Relation.
	 */
	copiedDistributedRelation = (Relation) palloc(sizeof(RelationData));
	copiedDistributedRelationTuple = (Form_pg_class) palloc(CLASS_TUPLE_SIZE);

	/*
	 * There is no need to deep copy everything. We will just deep copy of the fields
	 * we will change.
	 */
	memcpy(copiedDistributedRelation, distributedRelation, sizeof(RelationData));
	memcpy(copiedDistributedRelationTuple, distributedRelation->rd_rel,
		   CLASS_TUPLE_SIZE);

	copiedDistributedRelation->rd_rel = copiedDistributedRelationTuple;
	copiedDistributedRelation->rd_att = CreateTupleDescCopyConstr(tupleDescriptor);

	/*
	 * BeginCopyFrom opens all partitions of given partitioned table with relation_open
	 * and it expects its caller to close those relations. We do not have direct access
	 * to opened relations, thus we are changing relkind of partitioned tables so that
	 * Postgres will treat those tables as regular relations and will not open its
	 * partitions.
	 */
	if (PartitionedTable(tableId))
	{
		copiedDistributedRelationTuple->relkind = RELKIND_RELATION;
	}

	/* initialize copy state to read from COPY data source */
	copyState = BeginCopyFrom(NULL,
							  copiedDistributedRelation,
							  copyStatement->filename,
							  copyStatement->is_program,
							  NULL,
							  copyStatement->attlist,
							  copyStatement->options);

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;
	error_context_stack = &errorCallback;

	while (true)
	{
		bool nextRowFound = false;
		MemoryContext oldContext = NULL;

		ResetPerTupleExprContext(executorState);

		oldContext = MemoryContextSwitchTo(executorTupleContext);

		/* parse a row from the input */
		nextRowFound = NextCopyFromCompat(copyState, executorExpressionContext,
										  columnValues, columnNulls);

		if (!nextRowFound)
		{
			MemoryContextSwitchTo(oldContext);
			break;
		}

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(oldContext);

		dest->receiveSlot(tupleTableSlot, dest);

		processedRowCount += 1;
	}

	EndCopyFrom(copyState);

	/* all lines have been copied, stop showing line number in errors */
	error_context_stack = errorCallback.previous;

	/* finish the COPY commands */
	dest->rShutdown(dest);

	ExecDropSingleTupleTableSlot(tupleTableSlot);
	FreeExecutorState(executorState);
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
	CopyState copyState = BeginCopyFrom(NULL,
										distributedRelation,
										copyStatement->filename,
										copyStatement->is_program,
										NULL,
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

	/*
	 * From here on we use copyStatement as the template for the command
	 * that we send to workers. This command does not have an attribute
	 * list since NextCopyFrom will generate a value for all columns.
	 */
	copyStatement->attlist = NIL;

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
		nextRowFound = NextCopyFromCompat(copyState, executorExpressionContext,
										  columnValues, columnNulls);

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
						  copyOutState, columnOutputFunctions, NULL);
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

			EndRemoteCopy(currentShardId, shardConnections->connectionList);
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
		EndRemoteCopy(currentShardId, shardConnections->connectionList);
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
static NodeAddress *
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
	bool raiseInterrupts = true;

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);

	StringInfo partitionMethodCommand = makeStringInfo();
	appendStringInfo(partitionMethodCommand, PARTITION_METHOD_QUERY, qualifiedName);

	if (!SendRemoteCommand(masterConnection, partitionMethodCommand->data))
	{
		ReportConnectionError(masterConnection, ERROR);
	}
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
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

	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	Assert(!queryResult);

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
 * OpenCopyConnectionsForNewShards opens a connection for each placement of a shard and
 * starts a COPY transaction if necessary. If a connection cannot be opened,
 * then the shard placement is marked as inactive and the COPY continues with the remaining
 * shard placements.
 */
static void
OpenCopyConnectionsForNewShards(CopyStmt *copyStatement,
								ShardConnections *shardConnections,
								bool stopOnFailure, bool useBinaryCopyFormat)
{
	List *finalizedPlacementList = NIL;
	int failedPlacementCount = 0;
	ListCell *placementCell = NULL;
	List *connectionList = NULL;
	int64 shardId = shardConnections->shardId;
	bool raiseInterrupts = true;
	MemoryContext localContext =
		AllocSetContextCreateExtended(CurrentMemoryContext,
									  "OpenCopyConnectionsForNewShards",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);


	/* release finalized placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	finalizedPlacementList = MasterShardPlacementList(shardId);

	MemoryContextSwitchTo(oldContext);

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeUser = CurrentUserName();
		MultiConnection *connection = NULL;
		uint32 connectionFlags = FOR_DML;
		StringInfo copyCommand = NULL;
		PGresult *result = NULL;

		/*
		 * For hash partitioned tables, connection establishment happens in
		 * CopyGetPlacementConnection().
		 */
		Assert(placement->partitionMethod != DISTRIBUTE_BY_HASH);

		connection = GetPlacementConnection(connectionFlags, placement, nodeUser);

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			if (stopOnFailure)
			{
				ReportConnectionError(connection, ERROR);
			}
			else
			{
				const bool raiseErrors = true;

				HandleRemoteTransactionConnectionError(connection, raiseErrors);

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

		if (!SendRemoteCommand(connection, copyCommand->data))
		{
			ReportConnectionError(connection, ERROR);
		}
		result = GetRemoteCommandResult(connection, raiseInterrupts);
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
bool
CanUseBinaryCopyFormat(TupleDesc tupleDescription)
{
	bool useBinaryCopyFormat = true;
	int totalColumnCount = tupleDescription->natts;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescription, columnIndex);
		Oid typeId = InvalidOid;

		if (currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			|| currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
		{
			continue;
		}

		typeId = currentColumn->atttypid;
		if (!CanUseBinaryCopyFormatForType(typeId))
		{
			useBinaryCopyFormat = false;
			break;
		}
	}

	return useBinaryCopyFormat;
}


/*
 * CanUseBinaryCopyFormatForType determines whether it is safe to use the
 * binary copy format for the given type. The binary copy format cannot
 * be used for arrays or composite types that contain user-defined types,
 * or when there is no binary output function defined.
 */
bool
CanUseBinaryCopyFormatForType(Oid typeId)
{
	if (!BinaryOutputFunctionDefined(typeId))
	{
		return false;
	}

	if (typeId >= FirstNormalObjectId)
	{
		char typeCategory = '\0';
		bool typePreferred = false;

		get_type_category_preferred(typeId, &typeCategory, &typePreferred);
		if (typeCategory == TYPCATEGORY_ARRAY ||
			typeCategory == TYPCATEGORY_COMPOSITE)
		{
			return false;
		}
	}

	return true;
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
	bool raiseInterrupts = true;

	StringInfo shardPlacementsCommand = makeStringInfo();
	appendStringInfo(shardPlacementsCommand, FINALIZED_SHARD_PLACEMENTS_QUERY, shardId);

	if (!SendRemoteCommand(masterConnection, shardPlacementsCommand->data))
	{
		ReportConnectionError(masterConnection, ERROR);
	}
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int rowCount = PQntuples(queryResult);
		int rowIndex = 0;

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			char *placementIdString = PQgetvalue(queryResult, rowIndex, 0);
			char *nodeName = pstrdup(PQgetvalue(queryResult, rowIndex, 1));
			char *nodePortString = pstrdup(PQgetvalue(queryResult, rowIndex, 2));
			uint32 nodePort = atoi(nodePortString);
			uint64 placementId = atoll(placementIdString);

			ShardPlacement *shardPlacement =
				(ShardPlacement *) palloc0(sizeof(ShardPlacement));

			shardPlacement->placementId = placementId;
			shardPlacement->nodeName = nodeName;
			shardPlacement->nodePort = nodePort;

			/*
			 * We cannot know the nodeId, but it is not necessary at this point either.
			 * This is only used to to look up the connection for a group of co-located
			 * placements, but append-distributed tables are never co-located.
			 */
			shardPlacement->nodeId = -1;

			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("could not get shard placements from the master node")));
	}

	PQclear(queryResult);
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	Assert(!queryResult);

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

	AppendShardIdToName(&shardName, shardId);

	shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	appendStringInfo(command, "COPY %s ", shardQualifiedName);

	if (copyStatement->attlist != NIL)
	{
		ListCell *columnNameCell = NULL;
		bool appendedFirstName = false;

		foreach(columnNameCell, copyStatement->attlist)
		{
			char *columnName = (char *) lfirst(columnNameCell);

			if (!appendedFirstName)
			{
				appendStringInfo(command, "(%s", columnName);
				appendedFirstName = true;
			}
			else
			{
				appendStringInfo(command, ", %s", columnName);
			}
		}

		appendStringInfoString(command, ") ");
	}

	appendStringInfo(command, "FROM STDIN WITH ");

	if (IsCopyResultStmt(copyStatement))
	{
		appendStringInfoString(command, "(FORMAT RESULT)");
	}
	else if (useBinaryCopyFormat)
	{
		appendStringInfoString(command, "(FORMAT BINARY)");
	}
	else
	{
		appendStringInfoString(command, "(FORMAT TEXT)");
	}

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
	if (!PutRemoteCopyData(connection, dataBuffer->data, dataBuffer->len))
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to shard " INT64_FORMAT " on %s:%d",
							   shardId, connection->hostname, connection->port),
						errdetail("failed to send %d bytes %s", dataBuffer->len,
								  dataBuffer->data)));
	}
}


/*
 * EndRemoteCopy ends the COPY input on all connections, and unclaims connections.
 * This reports an error on failure.
 */
void
EndRemoteCopy(int64 shardId, List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;

		/* end the COPY input */
		if (!PutRemoteCopyEnd(connection, NULL))
		{
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("failed to COPY to shard " INT64_FORMAT " on %s:%d",
								   shardId, connection->hostname, connection->port)));
		}

		/* check whether there were any COPY errors */
		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
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
		bool haveDetail = remoteDetail != NULL;

		ereport(ERROR, (errmsg("%s", remoteMessage),
						haveDetail ? errdetail("%s", ApplyLogRedaction(remoteDetail)) :
						0));
	}
	else
	{
		/* trim the trailing characters */
		remoteMessage = pchomp(PQerrorMessage(connection->pgConn));

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to complete COPY on %s:%d", connection->hostname,
							   connection->port),
						errdetail("%s", ApplyLogRedaction(remoteMessage))));
	}
}


/*
 * ConversionPathForTypes fills *result with all the data necessary for converting
 * Datums of type inputType to Datums of type destType.
 */
static void
ConversionPathForTypes(Oid inputType, Oid destType, CopyCoercionData *result)
{
	Oid coercionFuncId = InvalidOid;
	CoercionPathType coercionType = COERCION_PATH_RELABELTYPE;

	if (destType == inputType)
	{
		result->coercionType = COERCION_PATH_RELABELTYPE;
		return;
	}

	coercionType = find_coercion_pathway(destType, inputType,
										 COERCION_EXPLICIT,
										 &coercionFuncId);

	switch (coercionType)
	{
		case COERCION_PATH_NONE:
		{
			ereport(ERROR, (errmsg("cannot cast %d to %d", inputType, destType)));
			return;
		}

		case COERCION_PATH_ARRAYCOERCE:
		{
			Oid inputBaseType = get_base_element_type(inputType);
			Oid destBaseType = get_base_element_type(destType);
			CoercionPathType baseCoercionType = COERCION_PATH_NONE;

			if (inputBaseType != InvalidOid && destBaseType != InvalidOid)
			{
				baseCoercionType = find_coercion_pathway(inputBaseType, destBaseType,
														 COERCION_EXPLICIT,
														 &coercionFuncId);
			}

			if (baseCoercionType != COERCION_PATH_COERCEVIAIO)
			{
				ereport(ERROR, (errmsg("can not run query which uses an implicit coercion"
									   " between array types")));
			}
		}

		/* fallthrough */

		case COERCION_PATH_COERCEVIAIO:
		{
			result->coercionType = COERCION_PATH_COERCEVIAIO;

			{
				bool typisvarlena = false; /* ignored */
				Oid iofunc = InvalidOid;
				getTypeOutputInfo(inputType, &iofunc, &typisvarlena);
				fmgr_info(iofunc, &(result->outputFunction));
			}

			{
				Oid iofunc = InvalidOid;
				getTypeInputInfo(destType, &iofunc, &(result->typioparam));
				fmgr_info(iofunc, &(result->inputFunction));
			}

			return;
		}

		case COERCION_PATH_FUNC:
		{
			result->coercionType = COERCION_PATH_FUNC;
			fmgr_info(coercionFuncId, &(result->coerceFunction));
			return;
		}

		case COERCION_PATH_RELABELTYPE:
		{
			result->coercionType = COERCION_PATH_RELABELTYPE;
			return; /* the types are binary compatible, no need to call a function */
		}

		default:
			Assert(false); /* there are no other options for this enum */
	}
}


/*
 * Returns the type of the provided column of the provided tuple. Throws an error if the
 * column does not exist or is dropped.
 *
 * tupleDescriptor and relationId must refer to the same table.
 */
static Oid
TypeForColumnName(Oid relationId, TupleDesc tupleDescriptor, char *columnName)
{
	AttrNumber destAttrNumber = get_attnum(relationId, columnName);
	Form_pg_attribute attr = NULL;

	if (destAttrNumber == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("invalid attr? %s", columnName)));
	}

	attr = TupleDescAttr(tupleDescriptor, destAttrNumber - 1);
	return attr->atttypid;
}


/*
 * Walks a TupleDesc and returns an array of the types of each attribute. Will return
 * InvalidOid in the place of dropped attributes.
 */
static Oid *
TypeArrayFromTupleDescriptor(TupleDesc tupleDescriptor)
{
	int columnCount = tupleDescriptor->natts;
	Oid *typeArray = palloc0(columnCount * sizeof(Oid));
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, columnIndex);
		if (attr->attisdropped)
		{
			typeArray[columnIndex] = InvalidOid;
		}
		else
		{
			typeArray[columnIndex] = attr->atttypid;
		}
	}

	return typeArray;
}


/*
 * ColumnCoercionPaths scans the input and output tuples looking for mismatched types,
 * it then returns an array of coercion functions to use on the input tuples, and an
 * array of types which descript the output tuple
 */
static CopyCoercionData *
ColumnCoercionPaths(TupleDesc destTupleDescriptor, TupleDesc inputTupleDescriptor,
					Oid destRelId, List *columnNameList,
					Oid *finalColumnTypeArray)
{
	int columnIndex = 0;
	int columnCount = inputTupleDescriptor->natts;
	CopyCoercionData *coercePaths = palloc0(columnCount * sizeof(CopyCoercionData));
	Oid *inputTupleTypes = TypeArrayFromTupleDescriptor(inputTupleDescriptor);
	ListCell *currentColumnName = list_head(columnNameList);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Oid destTupleType = InvalidOid;
		Oid inputTupleType = inputTupleTypes[columnIndex];
		char *columnName = lfirst(currentColumnName);

		if (inputTupleType == InvalidOid)
		{
			/* this was a dropped column and will not be in the incoming tuples */
			continue;
		}

		destTupleType = TypeForColumnName(destRelId, destTupleDescriptor, columnName);

		finalColumnTypeArray[columnIndex] = destTupleType;

		ConversionPathForTypes(inputTupleType, destTupleType,
							   &coercePaths[columnIndex]);

		currentColumnName = lnext(currentColumnName);

		if (currentColumnName == NULL)
		{
			/* the rest of inputTupleDescriptor are dropped columns, return early! */
			break;
		}
	}

	return coercePaths;
}


/*
 * TypeOutputFunctions takes an array of types and returns an array of output functions
 * for those types.
 */
static FmgrInfo *
TypeOutputFunctions(uint32 columnCount, Oid *typeIdArray, bool binaryFormat)
{
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Oid columnTypeId = typeIdArray[columnIndex];
		bool typeVariableLength = false;
		Oid outputFunctionId = InvalidOid;

		/* If there are any dropped columns it'll show up as a NULL */
		if (columnTypeId == InvalidOid)
		{
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
 * ColumnOutputFunctions is a wrapper around TypeOutputFunctions, it takes a
 * tupleDescriptor and returns an array of output functions, one for each column in
 * the tuple.
 */
FmgrInfo *
ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat)
{
	uint32 columnCount = (uint32) rowDescriptor->natts;
	Oid *columnTypes = TypeArrayFromTupleDescriptor(rowDescriptor);
	FmgrInfo *outputFunctions =
		TypeOutputFunctions(columnCount, columnTypes, binaryFormat);

	return outputFunctions;
}


/*
 * citus_text_send_as_jsonb sends a text as if it was a JSONB. This should only
 * be used if the text is indeed valid JSON.
 */
Datum
citus_text_send_as_jsonb(PG_FUNCTION_ARGS)
{
	text *inputText = PG_GETARG_TEXT_PP(0);
	StringInfoData buf;
	int version = 1;

	pq_begintypsend(&buf);
	pq_sendint(&buf, version, 1);
	pq_sendtext(&buf, VARDATA_ANY(inputText), VARSIZE_ANY_EXHDR(inputText));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
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
				  CopyOutState rowOutputState, FmgrInfo *columnOutputFunctions,
				  CopyCoercionData *columnCoercionPaths)
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
		Form_pg_attribute currentColumn = TupleDescAttr(rowDescriptor, columnIndex);
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (!isNull && columnCoercionPaths != NULL)
		{
			value = CoerceColumnValue(value, &columnCoercionPaths[columnIndex]);
		}

		if (currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			|| currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
#endif
			)
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
 * CoerceColumnValue follows the instructions in *coercionPath and uses them to convert
 * inputValue into a Datum of the correct type.
 */
static Datum
CoerceColumnValue(Datum inputValue, CopyCoercionData *coercionPath)
{
	switch (coercionPath->coercionType)
	{
		case 0:
		{
			return inputValue; /* this was a dropped column */
		}

		case COERCION_PATH_RELABELTYPE:
		{
			return inputValue; /* no need to do anything */
		}

		case COERCION_PATH_FUNC:
		{
			FmgrInfo *coerceFunction = &(coercionPath->coerceFunction);
			Datum outputValue = FunctionCall1(coerceFunction, inputValue);
			return outputValue;
		}

		case COERCION_PATH_COERCEVIAIO:
		{
			FmgrInfo *outFunction = &(coercionPath->outputFunction);
			Datum textRepr = FunctionCall1(outFunction, inputValue);

			FmgrInfo *inFunction = &(coercionPath->inputFunction);
			Oid typioparam = coercionPath->typioparam;
			Datum outputValue = FunctionCall3(inFunction, textRepr, typioparam,
											  Int32GetDatum(-1));

			return outputValue;
		}

		default:
		{
			/* this should never happen */
			ereport(ERROR, (errmsg("unsupported coercion type")));
		}
	}
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
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);

		if (!currentColumn->attisdropped
#if PG_VERSION_NUM >= 120000
			&& currentColumn->attgenerated != ATTRIBUTE_GENERATED_STORED
#endif
			)
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
	OpenCopyConnectionsForNewShards(copyStatement, shardConnections, stopOnFailure,
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
	bool raiseInterrupts = true;

	StringInfo createEmptyShardCommand = makeStringInfo();
	appendStringInfo(createEmptyShardCommand, CREATE_EMPTY_SHARD_QUERY, relationName);

	if (!SendRemoteCommand(masterConnection, createEmptyShardCommand->data))
	{
		ReportConnectionError(masterConnection, ERROR);
	}
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
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
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	Assert(!queryResult);

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
	bool raiseInterrupts = true;

	StringInfo updateShardStatisticsCommand = makeStringInfo();
	appendStringInfo(updateShardStatisticsCommand, UPDATE_SHARD_STATISTICS_QUERY,
					 shardId);

	if (!SendRemoteCommand(masterConnection, updateShardStatisticsCommand->data))
	{
		ReportConnectionError(masterConnection, ERROR);
	}
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	if (PQresultStatus(queryResult) != PGRES_TUPLES_OK)
	{
		ereport(ERROR, (errmsg("could not update shard statistics")));
	}

	PQclear(queryResult);
	queryResult = GetRemoteCommandResult(masterConnection, raiseInterrupts);
	Assert(!queryResult);
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
 * CreateCitusCopyDestReceiver creates a DestReceiver that copies into
 * a distributed table.
 *
 * The caller should provide the list of column names to use in the
 * remote COPY statement, and the partition column index in the tuple
 * descriptor (*not* the column name list).
 *
 * If intermediateResultIdPrefix is not NULL, the COPY will go into a set
 * of intermediate results that are co-located with the actual table.
 * The names of the intermediate results with be of the form:
 * intermediateResultIdPrefix_<shardid>
 */
CitusCopyDestReceiver *
CreateCitusCopyDestReceiver(Oid tableId, List *columnNameList, int partitionColumnIndex,
							EState *executorState, bool stopOnFailure,
							char *intermediateResultIdPrefix)
{
	CitusCopyDestReceiver *copyDest = NULL;

	copyDest = (CitusCopyDestReceiver *) palloc0(sizeof(CitusCopyDestReceiver));

	/* set up the DestReceiver function pointers */
	copyDest->pub.receiveSlot = CitusCopyDestReceiverReceive;
	copyDest->pub.rStartup = CitusCopyDestReceiverStartup;
	copyDest->pub.rShutdown = CitusCopyDestReceiverShutdown;
	copyDest->pub.rDestroy = CitusCopyDestReceiverDestroy;
	copyDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	copyDest->distributedRelationId = tableId;
	copyDest->columnNameList = columnNameList;
	copyDest->partitionColumnIndex = partitionColumnIndex;
	copyDest->executorState = executorState;
	copyDest->stopOnFailure = stopOnFailure;
	copyDest->intermediateResultIdPrefix = intermediateResultIdPrefix;
	copyDest->memoryContext = CurrentMemoryContext;

	return copyDest;
}


/*
 * CitusCopyDestReceiverStartup implements the rStartup interface of
 * CitusCopyDestReceiver. It opens the relation, acquires necessary
 * locks, and initializes the state required for doing the copy.
 */
static void
CitusCopyDestReceiverStartup(DestReceiver *dest, int operation,
							 TupleDesc inputTupleDescriptor)
{
	CitusCopyDestReceiver *copyDest = (CitusCopyDestReceiver *) dest;

	Oid tableId = copyDest->distributedRelationId;

	char *relationName = get_rel_name(tableId);
	Oid schemaOid = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaOid);

	Relation distributedRelation = NULL;
	List *columnNameList = copyDest->columnNameList;
	List *quotedColumnNameList = NIL;

	ListCell *columnNameCell = NULL;

	char partitionMethod = '\0';
	DistTableCacheEntry *cacheEntry = NULL;

	CopyStmt *copyStatement = NULL;

	List *shardIntervalList = NULL;

	CopyOutState copyOutState = NULL;
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	/* Citus currently doesn't know how to handle COPY command locally */
	ErrorIfLocalExecutionHappened();

	/* look up table properties */
	distributedRelation = heap_open(tableId, RowExclusiveLock);
	cacheEntry = DistributedTableCacheEntry(tableId);
	partitionMethod = cacheEntry->partitionMethod;

	copyDest->distributedRelation = distributedRelation;
	copyDest->tupleDescriptor = inputTupleDescriptor;

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

	/* error if any shard missing min/max values */
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

	/*
	 * Prevent concurrent UPDATE/DELETE on replication factor >1
	 * (see AcquireExecutorMultiShardLocks() at multi_router_executor.c)
	 */
	SerializeNonCommutativeWrites(shardIntervalList, RowExclusiveLock);

	/* keep the table metadata to avoid looking it up for every tuple */
	copyDest->tableMetadata = cacheEntry;

	BeginOrContinueCoordinatedTransaction();

	if (cacheEntry->replicationModel == REPLICATION_MODEL_2PC ||
		MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	/* define how tuples will be serialised */
	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = GetPerTupleMemoryContext(copyDest->executorState);
	copyDest->copyOutState = copyOutState;
	copyDest->multiShardCopy = false;

	/* prepare functions to call on received tuples */
	{
		TupleDesc destTupleDescriptor = distributedRelation->rd_att;
		int columnCount = inputTupleDescriptor->natts;
		Oid *finalTypeArray = palloc0(columnCount * sizeof(Oid));

		copyDest->columnCoercionPaths =
			ColumnCoercionPaths(destTupleDescriptor, inputTupleDescriptor,
								tableId, columnNameList, finalTypeArray);

		copyDest->columnOutputFunctions =
			TypeOutputFunctions(columnCount, finalTypeArray, copyOutState->binary);
	}

	/* ensure the column names are properly quoted in the COPY statement */
	foreach(columnNameCell, columnNameList)
	{
		char *columnName = (char *) lfirst(columnNameCell);
		char *quotedColumnName = (char *) quote_identifier(columnName);

		quotedColumnNameList = lappend(quotedColumnNameList, quotedColumnName);
	}

	if (partitionMethod != DISTRIBUTE_BY_NONE &&
		copyDest->partitionColumnIndex == INVALID_PARTITION_COLUMN_INDEX)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("the partition column of table %s should have a value",
							   quote_qualified_identifier(schemaName, relationName))));
	}

	/* define the template for the COPY statement that is sent to workers */
	copyStatement = makeNode(CopyStmt);

	if (copyDest->intermediateResultIdPrefix != NULL)
	{
		DefElem *formatResultOption = NULL;
		copyStatement->relation = makeRangeVar(NULL, copyDest->intermediateResultIdPrefix,
											   -1);

		formatResultOption = makeDefElem("format", (Node *) makeString("result"), -1);
		copyStatement->options = list_make1(formatResultOption);
	}
	else
	{
		copyStatement->relation = makeRangeVar(schemaName, relationName, -1);
		copyStatement->options = NIL;
	}

	copyStatement->query = NULL;
	copyStatement->attlist = quotedColumnNameList;
	copyStatement->is_from = true;
	copyStatement->is_program = false;
	copyStatement->filename = NULL;
	copyDest->copyStatement = copyStatement;

	copyDest->shardStateHash = CreateShardStateHash(TopTransactionContext);
	copyDest->connectionStateHash = CreateConnectionStateHash(TopTransactionContext);

	RecordRelationAccessIfReferenceTable(tableId, PLACEMENT_ACCESS_DML);
}


/*
 * CitusCopyDestReceiverReceive implements the receiveSlot function of
 * CitusCopyDestReceiver. It takes a TupleTableSlot and sends the contents to
 * the appropriate shard placement(s).
 */
static bool
CitusCopyDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	bool result = false;
	CitusCopyDestReceiver *copyDest = (CitusCopyDestReceiver *) dest;

	PG_TRY();
	{
		result = CitusSendTupleToPlacements(slot, copyDest);
	}
	PG_CATCH();
	{
		/*
		 * We might be able to recover from errors with ROLLBACK TO SAVEPOINT,
		 * so unclaim the connections before throwing errors.
		 */
		List *connectionStateList = ConnectionStateList(copyDest->connectionStateHash);
		UnclaimCopyConnections(connectionStateList);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}


/*
 * CitusSendTupleToPlacements sends the given TupleTableSlot to the appropriate
 * shard placement(s).
 */
static bool
CitusSendTupleToPlacements(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest)
{
	TupleDesc tupleDescriptor = copyDest->tupleDescriptor;
	CopyStmt *copyStatement = copyDest->copyStatement;

	CopyShardState *shardState = NULL;
	CopyOutState copyOutState = copyDest->copyOutState;
	FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;
	CopyCoercionData *columnCoercionPaths = copyDest->columnCoercionPaths;
	ListCell *placementStateCell = NULL;
	bool cachedShardStateFound = false;
	bool firstTupleInShard = false;

	bool stopOnFailure = copyDest->stopOnFailure;

	Datum *columnValues = NULL;
	bool *columnNulls = NULL;

	int64 shardId = 0;

	EState *executorState = copyDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	columnValues = slot->tts_values;
	columnNulls = slot->tts_isnull;

	shardId = ShardIdForTuple(copyDest, columnValues, columnNulls);

	/* connections hash is kept in memory context */
	MemoryContextSwitchTo(copyDest->memoryContext);

	shardState = GetShardState(shardId, copyDest->shardStateHash,
							   copyDest->connectionStateHash, stopOnFailure,
							   &cachedShardStateFound);
	if (!cachedShardStateFound)
	{
		firstTupleInShard = true;
	}

	if (firstTupleInShard && !copyDest->multiShardCopy &&
		hash_get_num_entries(copyDest->shardStateHash) == 2)
	{
		Oid relationId = copyDest->distributedRelationId;

		/* mark as multi shard to skip doing the same thing over and over */
		copyDest->multiShardCopy = true;

		if (MultiShardConnectionType != SEQUENTIAL_CONNECTION)
		{
			/* when we see multiple shard connections, we mark COPY as parallel modify */
			RecordParallelModifyAccess(relationId);
		}
	}

	foreach(placementStateCell, shardState->placementStateList)
	{
		CopyPlacementState *currentPlacementState = lfirst(placementStateCell);
		CopyConnectionState *connectionState = currentPlacementState->connectionState;
		CopyPlacementState *activePlacementState = connectionState->activePlacementState;
		bool switchToCurrentPlacement = false;
		bool sendTupleOverConnection = false;

		if (activePlacementState == NULL)
		{
			switchToCurrentPlacement = true;
		}
		else if (currentPlacementState != activePlacementState &&
				 currentPlacementState->data->len > COPY_SWITCH_OVER_THRESHOLD)
		{
			switchToCurrentPlacement = true;

			/* before switching, make sure to finish the copy */
			EndPlacementStateCopyCommand(activePlacementState, copyOutState);
			dlist_push_head(&connectionState->bufferedPlacementList,
							&activePlacementState->bufferedPlacementNode);
		}

		if (switchToCurrentPlacement)
		{
			StartPlacementStateCopyCommand(currentPlacementState, copyStatement,
										   copyOutState);
			dlist_delete(&currentPlacementState->bufferedPlacementNode);
			connectionState->activePlacementState = currentPlacementState;

			/* send previously buffered tuples */
			SendCopyDataToPlacement(currentPlacementState->data, shardId,
									connectionState->connection);
			resetStringInfo(currentPlacementState->data);

			/* additionaly, we need to send the current tuple too */
			sendTupleOverConnection = true;
		}
		else if (currentPlacementState != activePlacementState)
		{
			/* buffer data */
			StringInfo copyBuffer = copyOutState->fe_msgbuf;
			resetStringInfo(copyBuffer);
			AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
							  copyOutState, columnOutputFunctions,
							  columnCoercionPaths);
			appendBinaryStringInfo(currentPlacementState->data, copyBuffer->data,
								   copyBuffer->len);
		}
		else
		{
			Assert(currentPlacementState == activePlacementState);
			sendTupleOverConnection = true;
		}

		if (sendTupleOverConnection)
		{
			resetStringInfo(copyOutState->fe_msgbuf);
			AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
							  copyOutState, columnOutputFunctions, columnCoercionPaths);
			SendCopyDataToPlacement(copyOutState->fe_msgbuf, shardId,
									connectionState->connection);
		}
	}

	MemoryContextSwitchTo(oldContext);

	copyDest->tuplesSent++;

	/*
	 * Release per tuple memory allocated in this function. If we're writing
	 * the results of an INSERT ... SELECT then the SELECT execution will use
	 * its own executor state and reset the per tuple expression context
	 * separately.
	 */
	ResetPerTupleExprContext(executorState);

	return true;
}


/*
 * ShardIdForTuple returns id of the shard to which the given tuple belongs to.
 */
static uint64
ShardIdForTuple(CitusCopyDestReceiver *copyDest, Datum *columnValues, bool *columnNulls)
{
	int partitionColumnIndex = copyDest->partitionColumnIndex;
	Datum partitionColumnValue = 0;
	CopyCoercionData *columnCoercionPaths = copyDest->columnCoercionPaths;
	ShardInterval *shardInterval = NULL;

	/*
	 * Find the partition column value and corresponding shard interval
	 * for non-reference tables.
	 * Get the existing (and only a single) shard interval for the reference
	 * tables. Note that, reference tables has NULL partition column values so
	 * skip the check.
	 */
	if (partitionColumnIndex != INVALID_PARTITION_COLUMN_INDEX)
	{
		CopyCoercionData *coercePath = &columnCoercionPaths[partitionColumnIndex];

		if (columnNulls[partitionColumnIndex])
		{
			Oid relationId = copyDest->distributedRelationId;
			char *relationName = get_rel_name(relationId);
			Oid schemaOid = get_rel_namespace(relationId);
			char *schemaName = get_namespace_name(schemaOid);
			char *qualifiedTableName = quote_qualified_identifier(schemaName,
																  relationName);

			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("the partition column of table %s cannot be NULL",
								   qualifiedTableName)));
		}

		/* find the partition column value */
		partitionColumnValue = columnValues[partitionColumnIndex];

		/* annoyingly this is evaluated twice, but at least we don't crash! */
		partitionColumnValue = CoerceColumnValue(partitionColumnValue, coercePath);
	}

	/*
	 * Find the shard interval and id for the partition column value for
	 * non-reference tables.
	 *
	 * For reference table, this function blindly returns the tables single
	 * shard.
	 */
	shardInterval = FindShardInterval(partitionColumnValue, copyDest->tableMetadata);
	if (shardInterval == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find shard for partition column "
							   "value")));
	}

	return shardInterval->shardId;
}


/*
 * CitusCopyDestReceiverShutdown implements the rShutdown interface of
 * CitusCopyDestReceiver. It ends the COPY on all the open connections and closes
 * the relation.
 */
static void
CitusCopyDestReceiverShutdown(DestReceiver *destReceiver)
{
	CitusCopyDestReceiver *copyDest = (CitusCopyDestReceiver *) destReceiver;

	HTAB *connectionStateHash = copyDest->connectionStateHash;
	List *connectionStateList = NIL;
	ListCell *connectionStateCell = NULL;
	Relation distributedRelation = copyDest->distributedRelation;

	connectionStateList = ConnectionStateList(connectionStateHash);

	PG_TRY();
	{
		foreach(connectionStateCell, connectionStateList)
		{
			CopyConnectionState *connectionState =
				(CopyConnectionState *) lfirst(connectionStateCell);

			ShutdownCopyConnectionState(connectionState, copyDest);
		}
	}
	PG_CATCH();
	{
		/*
		 * We might be able to recover from errors with ROLLBACK TO SAVEPOINT,
		 * so unclaim the connections before throwing errors.
		 */
		UnclaimCopyConnections(connectionStateList);

		PG_RE_THROW();
	}
	PG_END_TRY();

	heap_close(distributedRelation, NoLock);
}


/*
 * ShutdownCopyConnectionState ends the copy command for the current active
 * placement on connection, and then sends the rest of the buffers over the
 * connection.
 */
static void
ShutdownCopyConnectionState(CopyConnectionState *connectionState,
							CitusCopyDestReceiver *copyDest)
{
	CopyOutState copyOutState = copyDest->copyOutState;
	CopyStmt *copyStatement = copyDest->copyStatement;
	dlist_iter iter;

	CopyPlacementState *activePlacementState = connectionState->activePlacementState;
	if (activePlacementState != NULL)
	{
		EndPlacementStateCopyCommand(activePlacementState, copyOutState);
	}

	dlist_foreach(iter, &connectionState->bufferedPlacementList)
	{
		CopyPlacementState *placementState =
			dlist_container(CopyPlacementState, bufferedPlacementNode, iter.cur);
		uint64 shardId = placementState->shardState->shardId;

		StartPlacementStateCopyCommand(placementState, copyStatement,
									   copyOutState);
		SendCopyDataToPlacement(placementState->data, shardId,
								connectionState->connection);
		EndPlacementStateCopyCommand(placementState, copyOutState);
	}
}


static void
CitusCopyDestReceiverDestroy(DestReceiver *destReceiver)
{
	CitusCopyDestReceiver *copyDest = (CitusCopyDestReceiver *) destReceiver;

	if (copyDest->copyOutState)
	{
		pfree(copyDest->copyOutState);
	}

	if (copyDest->columnOutputFunctions)
	{
		pfree(copyDest->columnOutputFunctions);
	}

	if (copyDest->columnCoercionPaths)
	{
		pfree(copyDest->columnCoercionPaths);
	}

	pfree(copyDest);
}


/*
 * IsCopyResultStmt determines whether the given copy statement is a
 * COPY "resultkey" FROM STDIN WITH (format result) statement, which is used
 * to copy query results from the coordinator into workers.
 */
bool
IsCopyResultStmt(CopyStmt *copyStatement)
{
	return CopyStatementHasFormat(copyStatement, "result");
}


/*
 * CopyStatementHasFormat checks whether the COPY statement has the given
 * format.
 */
static bool
CopyStatementHasFormat(CopyStmt *copyStatement, char *formatName)
{
	ListCell *optionCell = NULL;
	bool hasFormat = false;

	/* extract WITH (...) options from the COPY statement */
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);

		if (strncmp(defel->defname, "format", NAMEDATALEN) == 0 &&
			strncmp(defGetString(defel), formatName, NAMEDATALEN) == 0)
		{
			hasFormat = true;
			break;
		}
	}

	return hasFormat;
}


/*
 * ProcessCopyStmt handles Citus specific concerns for COPY like supporting
 * COPYing from distributed tables and preventing unsupported actions. The
 * function returns a modified COPY statement to be executed, or NULL if no
 * further processing is needed.
 */
Node *
ProcessCopyStmt(CopyStmt *copyStatement, char *completionTag, const char *queryString)
{
	/*
	 * Handle special COPY "resultid" FROM STDIN WITH (format result) commands
	 * for sending intermediate results to workers.
	 */
	if (IsCopyResultStmt(copyStatement))
	{
		const char *resultId = copyStatement->relation->relname;

		ReceiveQueryResultViaCopy(resultId);

		return NULL;
	}

	/*
	 * We check whether a distributed relation is affected. For that, we need to open the
	 * relation. To prevent race conditions with later lookups, lock the table, and modify
	 * the rangevar to include the schema.
	 */
	if (copyStatement->relation != NULL)
	{
		bool isDistributedRelation = false;
		bool isCopyFromWorker = IsCopyFromWorker(copyStatement);

		if (isCopyFromWorker)
		{
			RangeVar *relation = copyStatement->relation;
			NodeAddress *masterNodeAddress = MasterNodeAddress(copyStatement);
			char *nodeName = masterNodeAddress->nodeName;
			int32 nodePort = masterNodeAddress->nodePort;

			CreateLocalTable(relation, nodeName, nodePort);

			/*
			 * We expect copy from worker to be on a distributed table; otherwise,
			 * it fails in CitusCopyFrom() while checking the partition method.
			 */
			isDistributedRelation = true;
		}
		else
		{
			bool isFrom = copyStatement->is_from;
			Relation copiedRelation = NULL;
			char *schemaName = NULL;
			MemoryContext relationContext = NULL;

			/* consider using RangeVarGetRelidExtended to check perms before locking */
			copiedRelation = heap_openrv(copyStatement->relation,
										 isFrom ? RowExclusiveLock : AccessShareLock);

			isDistributedRelation = IsDistributedTable(RelationGetRelid(copiedRelation));

			/* ensure future lookups hit the same relation */
			schemaName = get_namespace_name(RelationGetNamespace(copiedRelation));

			/* ensure we copy string into proper context */
			relationContext = GetMemoryChunkContext(copyStatement->relation);
			schemaName = MemoryContextStrdup(relationContext, schemaName);
			copyStatement->relation->schemaname = schemaName;

			heap_close(copiedRelation, NoLock);
		}

		if (isDistributedRelation)
		{
			if (copyStatement->is_from)
			{
#if PG_VERSION_NUM >= 120000
				if (copyStatement->whereClause)
				{
					ereport(ERROR, (errmsg(
										"Citus does not support COPY FROM with WHERE")));
				}
#endif

				/* check permissions, we're bypassing postgres' normal checks */
				if (!isCopyFromWorker)
				{
					CheckCopyPermissions(copyStatement);
				}

				CitusCopyFrom(copyStatement, completionTag);
				return NULL;
			}
			else
			{
				/*
				 * The copy code only handles SELECTs in COPY ... TO on master tables,
				 * as that can be done non-invasively. To handle COPY master_rel TO
				 * the copy statement is replaced by a generated select statement.
				 */
				ColumnRef *allColumns = makeNode(ColumnRef);
				SelectStmt *selectStmt = makeNode(SelectStmt);
				ResTarget *selectTarget = makeNode(ResTarget);

				allColumns->fields = list_make1(makeNode(A_Star));
				allColumns->location = -1;

				selectTarget->name = NULL;
				selectTarget->indirection = NIL;
				selectTarget->val = (Node *) allColumns;
				selectTarget->location = -1;

				selectStmt->targetList = list_make1(selectTarget);
				selectStmt->fromClause = list_make1(copyObject(copyStatement->relation));

				/* replace original statement */
				copyStatement = copyObject(copyStatement);
				copyStatement->relation = NULL;
				copyStatement->query = (Node *) selectStmt;
			}
		}
	}


	if (copyStatement->filename != NULL && !copyStatement->is_program)
	{
		char *filename = copyStatement->filename;

		/*
		 * We execute COPY commands issued by the task-tracker executor here
		 * because we're not normally allowed to write to a file as a regular
		 * user and we don't want to execute the query as superuser.
		 */
		if (CacheDirectoryElement(filename) && copyStatement->query != NULL &&
			!copyStatement->is_from && !is_absolute_path(filename))
		{
			bool binaryCopyFormat = CopyStatementHasFormat(copyStatement, "binary");
			int64 tuplesSent = 0;
			Query *query = NULL;
			Node *queryNode = copyStatement->query;
			List *queryTreeList = NIL;
			StringInfo userFilePath = makeStringInfo();

			RawStmt *rawStmt = makeNode(RawStmt);
			rawStmt->stmt = queryNode;

			queryTreeList = pg_analyze_and_rewrite(rawStmt, queryString, NULL, 0, NULL);

			if (list_length(queryTreeList) != 1)
			{
				ereport(ERROR, (errmsg("can only execute a single query")));
			}

			query = (Query *) linitial(queryTreeList);

			/*
			 * Add a user ID suffix to prevent other users from reading/writing
			 * the same file. We do this consistently in all functions that interact
			 * with task files.
			 */
			appendStringInfo(userFilePath, "%s.%u", filename, GetUserId());

			tuplesSent = WorkerExecuteSqlTask(query, filename, binaryCopyFormat);

			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "COPY " UINT64_FORMAT, tuplesSent);

			return NULL;
		}
	}


	return (Node *) copyStatement;
}


/*
 * CreateLocalTable gets DDL commands from the remote node for the given
 * relation. Then, it creates the local relation as temporary and on commit drop.
 */
static void
CreateLocalTable(RangeVar *relation, char *nodeName, int32 nodePort)
{
	List *ddlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedRelationName = quote_qualified_identifier(schemaName, relationName);

	/*
	 * The warning message created in TableDDLCommandList() is descriptive
	 * enough; therefore, we just throw an error which says that we could not
	 * run the copy operation.
	 */
	ddlCommandList = TableDDLCommandList(nodeName, nodePort, qualifiedRelationName);
	if (ddlCommandList == NIL)
	{
		ereport(ERROR, (errmsg("could not run copy from the worker node")));
	}

	/* apply DDL commands against the local database */
	foreach(ddlCommandCell, ddlCommandList)
	{
		StringInfo ddlCommand = (StringInfo) lfirst(ddlCommandCell);
		Node *ddlCommandNode = ParseTreeNode(ddlCommand->data);
		bool applyDDLCommand = false;

		if (IsA(ddlCommandNode, CreateStmt) ||
			IsA(ddlCommandNode, CreateForeignTableStmt))
		{
			CreateStmt *createStatement = (CreateStmt *) ddlCommandNode;

			/* create the local relation as temporary and on commit drop */
			createStatement->relation->relpersistence = RELPERSISTENCE_TEMP;
			createStatement->oncommit = ONCOMMIT_DROP;

			/* temporarily strip schema name */
			createStatement->relation->schemaname = NULL;

			applyDDLCommand = true;
		}
		else if (IsA(ddlCommandNode, CreateForeignServerStmt))
		{
			CreateForeignServerStmt *createServerStmt =
				(CreateForeignServerStmt *) ddlCommandNode;
			if (GetForeignServerByName(createServerStmt->servername, true) == NULL)
			{
				/* create server if not exists */
				applyDDLCommand = true;
			}
		}
		else if ((IsA(ddlCommandNode, CreateExtensionStmt)))
		{
			applyDDLCommand = true;
		}
		else if ((IsA(ddlCommandNode, CreateSeqStmt)))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot copy to table with serial column from worker"),
							errhint("Connect to the master node to COPY to tables which "
									"use serial column types.")));
		}

		/* run only a selected set of DDL commands */
		if (applyDDLCommand)
		{
			CitusProcessUtility(ddlCommandNode, CreateCommandTag(ddlCommandNode),
								PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);

			CommandCounterIncrement();
		}
	}
}


/*
 * Check whether the current user has the permission to execute a COPY
 * statement, raise ERROR if not. In some cases we have to do this separately
 * from postgres' copy.c, because we have to execute the copy with elevated
 * privileges.
 *
 * Copied from postgres, where it's part of DoCopy().
 */
void
CheckCopyPermissions(CopyStmt *copyStatement)
{
	/* *INDENT-OFF* */
	bool		is_from = copyStatement->is_from;
	Relation	rel;
	Oid			relid;
	List	   *range_table = NIL;
	TupleDesc	tupDesc;
	AclMode		required_access = (is_from ? ACL_INSERT : ACL_SELECT);
	List	   *attnums;
	ListCell   *cur;
	RangeTblEntry *rte;

	rel = heap_openrv(copyStatement->relation,
	                  is_from ? RowExclusiveLock : AccessShareLock);

	relid = RelationGetRelid(rel);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = relid;
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = required_access;
	range_table = list_make1(rte);

	tupDesc = RelationGetDescr(rel);

	attnums = CopyGetAttnums(tupDesc, rel, copyStatement->attlist);
	foreach(cur, attnums)
	{
		int			attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

		if (is_from)
		{
			rte->insertedCols = bms_add_member(rte->insertedCols, attno);
		}
		else
		{
			rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
	}

	ExecCheckRTPerms(range_table, true);

	/* TODO: Perform RLS checks once supported */

	heap_close(rel, NoLock);
	/* *INDENT-ON* */
}


/* Helper for CheckCopyPermissions(), copied from postgres */
static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	/* *INDENT-OFF* */
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
					        (errcode(ERRCODE_UNDEFINED_COLUMN),
							        errmsg("column \"%s\" of relation \"%s\" does not exist",
							               name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
					        (errcode(ERRCODE_UNDEFINED_COLUMN),
							        errmsg("column \"%s\" does not exist",
							               name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
				        (errcode(ERRCODE_DUPLICATE_COLUMN),
						        errmsg("column \"%s\" specified more than once",
						               name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
	/* *INDENT-ON* */
}


/*
 * CreateConnectionStateHash constructs a hash table which maps from socket
 * number to CopyConnectionState, passing the provided MemoryContext to
 * hash_create for hash allocations.
 */
static HTAB *
CreateConnectionStateHash(MemoryContext memoryContext)
{
	HTAB *connectionStateHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int);
	info.entrysize = sizeof(CopyConnectionState);
	info.hcxt = memoryContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	connectionStateHash = hash_create("Copy Connection State Hash", 128, &info,
									  hashFlags);

	return connectionStateHash;
}


/*
 * CreateShardStateHash constructs a hash table which maps from shard
 * identifier to CopyShardState, passing the provided MemoryContext to
 * hash_create for hash allocations.
 */
static HTAB *
CreateShardStateHash(MemoryContext memoryContext)
{
	HTAB *shardStateHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(CopyShardState);
	info.hcxt = memoryContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	shardStateHash = hash_create("Copy Shard State Hash", 128, &info, hashFlags);

	return shardStateHash;
}


/*
 * GetConnectionState finds existing CopyConnectionState for a connection in the
 * provided hash. If not found, then a default structure is returned.
 */
static CopyConnectionState *
GetConnectionState(HTAB *connectionStateHash, MultiConnection *connection)
{
	CopyConnectionState *connectionState = NULL;
	bool found = false;

	int socket = PQsocket(connection->pgConn);
	Assert(socket != -1);

	connectionState = (CopyConnectionState *) hash_search(connectionStateHash, &socket,
														  HASH_ENTER, &found);
	if (!found)
	{
		connectionState->socket = socket;
		connectionState->connection = connection;
		connectionState->activePlacementState = NULL;
		dlist_init(&connectionState->bufferedPlacementList);
	}

	return connectionState;
}


/*
 * ConnectionStateList returns all CopyConnectionState structures in
 * the given hash.
 */
static List *
ConnectionStateList(HTAB *connectionStateHash)
{
	List *connectionStateList = NIL;
	HASH_SEQ_STATUS status;
	CopyConnectionState *connectionState = NULL;

	hash_seq_init(&status, connectionStateHash);

	connectionState = (CopyConnectionState *) hash_seq_search(&status);
	while (connectionState != NULL)
	{
		connectionStateList = lappend(connectionStateList, connectionState);

		connectionState = (CopyConnectionState *) hash_seq_search(&status);
	}

	return connectionStateList;
}


/*
 * GetShardState finds existing CopyShardState for a shard in the provided
 * hash. If not found, then a new shard state is returned with all related
 * CopyPlacementStates initialized.
 */
static CopyShardState *
GetShardState(uint64 shardId, HTAB *shardStateHash,
			  HTAB *connectionStateHash, bool stopOnFailure, bool *found)
{
	CopyShardState *shardState = NULL;

	shardState = (CopyShardState *) hash_search(shardStateHash, &shardId,
												HASH_ENTER, found);
	if (!*found)
	{
		InitializeCopyShardState(shardState, connectionStateHash,
								 shardId, stopOnFailure);
	}

	return shardState;
}


/*
 * InitializeCopyShardState initializes the given shardState. It finds all
 * placements for the given shardId, assignes connections to them, and
 * adds them to shardState->placementStateList.
 */
static void
InitializeCopyShardState(CopyShardState *shardState,
						 HTAB *connectionStateHash, uint64 shardId,
						 bool stopOnFailure)
{
	List *finalizedPlacementList = NIL;
	ListCell *placementCell = NULL;
	int failedPlacementCount = 0;

	MemoryContext localContext =
		AllocSetContextCreateExtended(CurrentMemoryContext,
									  "InitializeCopyShardState",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);

	/* release finalized placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	finalizedPlacementList = MasterShardPlacementList(shardId);

	MemoryContextSwitchTo(oldContext);

	shardState->shardId = shardId;
	shardState->placementStateList = NIL;

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		CopyConnectionState *connectionState = NULL;
		CopyPlacementState *placementState = NULL;

		MultiConnection *connection =
			CopyGetPlacementConnection(placement, stopOnFailure);
		if (connection == NULL)
		{
			failedPlacementCount++;
			continue;
		}

		connectionState = GetConnectionState(connectionStateHash, connection);

		/*
		 * If this is the first time we are using this connection for copying a
		 * shard, send begin if necessary.
		 */
		if (connectionState->activePlacementState == NULL)
		{
			RemoteTransactionBeginIfNecessary(connection);
		}

		placementState = palloc0(sizeof(CopyPlacementState));
		placementState->shardState = shardState;
		placementState->data = makeStringInfo();
		placementState->connectionState = connectionState;

		/*
		 * We don't set connectionState->activePlacementState here even if it
		 * is NULL. Later in CitusSendTupleToPlacements() we set it at the
		 * same time as calling StartPlacementStateCopyCommand() so we actually
		 * know the COPY operation for the placement is ongoing.
		 */
		dlist_push_head(&connectionState->bufferedPlacementList,
						&placementState->bufferedPlacementNode);
		shardState->placementStateList = lappend(shardState->placementStateList,
												 placementState);
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

	MemoryContextReset(localContext);
}


/*
 * CopyGetPlacementConnection assigns a connection to the given placement. If
 * a connection has already been assigned the placement in the current transaction
 * then it reuses the connection. Otherwise, it requests a connection for placement.
 */
static MultiConnection *
CopyGetPlacementConnection(ShardPlacement *placement, bool stopOnFailure)
{
	MultiConnection *connection = NULL;
	uint32 connectionFlags = FOR_DML;
	char *nodeUser = CurrentUserName();
	ShardPlacementAccess *placementAccess = NULL;

	/*
	 * Determine whether the task has to be assigned to a particular connection
	 * due to a preceding access to the placement in the same transaction.
	 */
	placementAccess = CreatePlacementAccess(placement, PLACEMENT_ACCESS_DML);
	connection = GetConnectionIfPlacementAccessedInXact(connectionFlags,
														list_make1(placementAccess),
														NULL);
	if (connection != NULL)
	{
		return connection;
	}

	/*
	 * For placements that haven't been assigned a connection by a previous command
	 * in the current transaction, we use a separate connection per placement for
	 * hash-distributed tables in order to get the maximum performance.
	 */
	if (placement->partitionMethod == DISTRIBUTE_BY_HASH &&
		MultiShardConnectionType != SEQUENTIAL_CONNECTION)
	{
		connectionFlags |= CONNECTION_PER_PLACEMENT;
	}

	connection = GetPlacementConnection(connectionFlags, placement, nodeUser);

	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		if (stopOnFailure)
		{
			ReportConnectionError(connection, ERROR);
		}
		else
		{
			const bool raiseErrors = true;

			HandleRemoteTransactionConnectionError(connection, raiseErrors);

			return NULL;
		}
	}

	/*
	 * Errors are supposed to cause immediate aborts (i.e. we don't
	 * want to/can't invalidate placements), mark the connection as
	 * critical so later errors cause failures.
	 */
	MarkRemoteTransactionCritical(connection);

	if (MultiShardConnectionType != SEQUENTIAL_CONNECTION)
	{
		ClaimConnectionExclusively(connection);
	}

	return connection;
}


/*
 * StartPlacementStateCopyCommand sends the COPY for the given placement. It also
 * sends binary headers if this is a binary COPY.
 */
static void
StartPlacementStateCopyCommand(CopyPlacementState *placementState,
							   CopyStmt *copyStatement, CopyOutState copyOutState)
{
	StringInfo copyCommand = NULL;
	PGresult *result = NULL;
	MultiConnection *connection = placementState->connectionState->connection;
	uint64 shardId = placementState->shardState->shardId;
	bool raiseInterrupts = true;
	bool binaryCopy = copyOutState->binary;

	copyCommand = ConstructCopyStatement(copyStatement, shardId, binaryCopy);

	if (!SendRemoteCommand(connection, copyCommand->data))
	{
		ReportConnectionError(connection, ERROR);
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (PQresultStatus(result) != PGRES_COPY_IN)
	{
		ReportResultError(connection, result, ERROR);
	}

	PQclear(result);

	if (binaryCopy)
	{
		SendCopyBinaryHeaders(copyOutState, shardId, list_make1(connection));
	}
}


/*
 * EndPlacementStateCopyCommand ends the COPY for the given placement. It also
 * sends binary footers if this is a binary COPY.
 */
static void
EndPlacementStateCopyCommand(CopyPlacementState *placementState,
							 CopyOutState copyOutState)
{
	MultiConnection *connection = placementState->connectionState->connection;
	uint64 shardId = placementState->shardState->shardId;
	bool binaryCopy = copyOutState->binary;

	/* send footers and end copy command */
	if (binaryCopy)
	{
		SendCopyBinaryFooters(copyOutState, shardId, list_make1(connection));
	}

	EndRemoteCopy(shardId, list_make1(connection));
}


/*
 * UnclaimCopyConnections unclaims all the connections used for COPY.
 */
static void
UnclaimCopyConnections(List *connectionStateList)
{
	ListCell *connectionStateCell = NULL;

	foreach(connectionStateCell, connectionStateList)
	{
		CopyConnectionState *connectionState = lfirst(connectionStateCell);
		UnclaimConnection(connectionState->connection);
	}
}
