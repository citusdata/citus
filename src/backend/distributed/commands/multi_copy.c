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
 * rollbacks the distributed transaction, similar to the way DML statements
 * are handled. If a failure occurs after connecting, the transaction
 * is rolled back on all the workers. Note that,
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
 * Copyright (c) Citus Data, Inc.
 *
 * With contributions from Postgres Professional.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <arpa/inet.h> /* for htons */
#include <netinet/in.h> /* for htons */
#include <string.h>

#include "distributed/pg_version_constants.h"

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
#include "commands/progress.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/log_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/listutils.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_pruning.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "distributed/local_multi_copy.h"
#include "distributed/hash_helpers.h"
#include "distributed/transmit.h"
#include "executor/executor.h"
#include "foreign/foreign.h"

#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#if PG_VERSION_NUM >= PG_VERSION_13
#include "tcop/cmdtag.h"
#endif
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/memutils.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* custom Citus option for appending to a shard */
#define APPEND_TO_SHARD_OPTION "append_to_shard"

/*
 * Data size threshold to switch over the active placement for a connection.
 * If this is too low, overhead of starting COPY commands will hurt the
 * performance. If this is too high, buffered data will use lots of memory.
 * 4MB is a good balance between memory usage and performance. Note that this
 * is irrelevant in the common case where we open one connection per placement.
 */
int CopySwitchOverThresholdBytes = 4 * 1024 * 1024;

#define FILE_IS_OPEN(x) (x > -1)

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

	/* length of bufferedPlacementList, to avoid iterations over the list when needed */
	int bufferedPlacementCount;
} CopyConnectionState;


struct CopyPlacementState
{
	/* Connection state to which the placemement is assigned to. */
	CopyConnectionState *connectionState;

	/* State of shard to which the placement belongs to. */
	CopyShardState *shardState;

	/* node group ID of the placement */
	int32 groupId;

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

	/* used for doing local copy, either for a shard or a co-located file */
	CopyOutState copyOutState;

	/* used when copy is targeting co-located file */
	FileCompat fileDest;

	/* containsLocalPlacement is true if we have a local placement for the shard id of this state */
	bool containsLocalPlacement;

	/* List of CopyPlacementStates for all active placements of the shard. */
	List *placementStateList;
};

/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;

	/* list of MultiConnection structs */
	List *connectionList;
} ShardConnections;


/*
 * Represents the state for allowing copy via local
 * execution.
 */
typedef enum LocalCopyStatus
{
	LOCAL_COPY_REQUIRED,
	LOCAL_COPY_OPTIONAL,
	LOCAL_COPY_DISABLED
} LocalCopyStatus;


/* Local functions forward declarations */
static void CopyToExistingShards(CopyStmt *copyStatement,
								 QueryCompletionCompat *completionTag);
static List * RemoveOptionFromList(List *optionList, char *optionName);
static bool BinaryOutputFunctionDefined(Oid typeId);
static bool BinaryInputFunctionDefined(Oid typeId);
static void SendCopyBinaryHeaders(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);
static void SendCopyBinaryFooters(CopyOutState copyOutState, int64 shardId,
								  List *connectionList);
static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId);
static void SendCopyDataToAll(StringInfo dataBuffer, int64 shardId, List *connectionList);
static void SendCopyDataToPlacement(StringInfo dataBuffer, int64 shardId,
									MultiConnection *connection);
static void ReportCopyError(MultiConnection *connection, PGresult *result);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor);

static Oid TypeForColumnName(Oid relationId, TupleDesc tupleDescriptor, char *columnName);
static Oid * TypeArrayFromTupleDescriptor(TupleDesc tupleDescriptor);
static CopyCoercionData * ColumnCoercionPaths(TupleDesc destTupleDescriptor,
											  TupleDesc inputTupleDescriptor,
											  Oid destRelId, List *columnNameList,
											  Oid *finalColumnTypeArray);
static FmgrInfo * TypeOutputFunctions(uint32 columnCount, Oid *typeIdArray,
									  bool binaryFormat);
#if PG_VERSION_NUM < PG_VERSION_14
static List * CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);
#endif
static bool CopyStatementHasFormat(CopyStmt *copyStatement, char *formatName);
static void CitusCopyFrom(CopyStmt *copyStatement, QueryCompletionCompat *completionTag);
static void EnsureCopyCanRunOnRelation(Oid relationId);
static HTAB * CreateConnectionStateHash(MemoryContext memoryContext);
static HTAB * CreateShardStateHash(MemoryContext memoryContext);
static CopyConnectionState * GetConnectionState(HTAB *connectionStateHash,
												MultiConnection *connection);
static CopyShardState * GetShardState(uint64 shardId, HTAB *shardStateHash,
									  HTAB *connectionStateHash,
									  bool *found, bool shouldUseLocalCopy, CopyOutState
									  copyOutState, bool isColocatedIntermediateResult);
static MultiConnection * CopyGetPlacementConnection(HTAB *connectionStateHash,
													ShardPlacement *placement,
													bool colocatedIntermediateResult);
static bool HasReachedAdaptiveExecutorPoolSize(List *connectionStateHash);
static MultiConnection * GetLeastUtilisedCopyConnection(List *connectionStateList,
														char *nodeName, int nodePort);
static List * ConnectionStateList(HTAB *connectionStateHash);
static List * ConnectionStateListToNode(HTAB *connectionStateHash,
										const char *hostname, int32 port);
static void InitializeCopyShardState(CopyShardState *shardState,
									 HTAB *connectionStateHash,
									 uint64 shardId,
									 bool canUseLocalCopy,
									 CopyOutState copyOutState,
									 bool colocatedIntermediateResult);
static void StartPlacementStateCopyCommand(CopyPlacementState *placementState,
										   CopyStmt *copyStatement,
										   CopyOutState copyOutState);
static void EndPlacementStateCopyCommand(CopyPlacementState *placementState,
										 CopyOutState copyOutState);
static void UnclaimCopyConnections(List *connectionStateList);
static void ShutdownCopyConnectionState(CopyConnectionState *connectionState,
										CitusCopyDestReceiver *copyDest);
static SelectStmt * CitusCopySelect(CopyStmt *copyStatement);
static void CitusCopyTo(CopyStmt *copyStatement, QueryCompletionCompat *completionTag);
static int64 ForwardCopyDataFromConnection(CopyOutState copyOutState,
										   MultiConnection *connection);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void SendCopyBegin(CopyOutState cstate);
static void SendCopyEnd(CopyOutState cstate);
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopySendEndOfRow(CopyOutState cstate, bool includeEndOfLine);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);
static bool CitusSendTupleToPlacements(TupleTableSlot *slot,
									   CitusCopyDestReceiver *copyDest);
static void AddPlacementStateToCopyConnectionStateBuffer(CopyConnectionState *
														 connectionState,
														 CopyPlacementState *
														 placementState);
static void RemovePlacementStateFromCopyConnectionStateBuffer(CopyConnectionState *
															  connectionState,
															  CopyPlacementState *
															  placementState);
static uint64 ProcessAppendToShardOption(Oid relationId, CopyStmt *copyStatement);
static uint64 ShardIdForTuple(CitusCopyDestReceiver *copyDest, Datum *columnValues,
							  bool *columnNulls);

/* CitusCopyDestReceiver functions */
static void CitusCopyDestReceiverStartup(DestReceiver *copyDest, int operation,
										 TupleDesc inputTupleDesc);
static bool CitusCopyDestReceiverReceive(TupleTableSlot *slot,
										 DestReceiver *copyDest);
static void CitusCopyDestReceiverShutdown(DestReceiver *destReceiver);
static void CitusCopyDestReceiverDestroy(DestReceiver *destReceiver);
static bool ContainsLocalPlacement(int64 shardId);
static void CompleteCopyQueryTagCompat(QueryCompletionCompat *completionTag, uint64
									   processedRowCount);
static void FinishLocalCopy(CitusCopyDestReceiver *copyDest);
static void CreateLocalColocatedIntermediateFile(CitusCopyDestReceiver *copyDest,
												 CopyShardState *shardState);
static void FinishLocalColocatedIntermediateFiles(CitusCopyDestReceiver *copyDest);
static void CloneCopyOutStateForLocalCopy(CopyOutState from, CopyOutState to);
static LocalCopyStatus GetLocalCopyStatus(void);
static bool ShardIntervalListHasLocalPlacements(List *shardIntervalList);
static void LogLocalCopyToRelationExecution(uint64 shardId);
static void LogLocalCopyToFileExecution(uint64 shardId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_text_send_as_jsonb);


/*
 * CitusCopyFrom implements the COPY table_name FROM. It dispacthes the copy
 * statement to related subfunctions based on where the copy command is run
 * and the partition method of the distributed table.
 */
static void
CitusCopyFrom(CopyStmt *copyStatement, QueryCompletionCompat *completionTag)
{
	UseCoordinatedTransaction();

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


	Oid relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);

	EnsureCopyCanRunOnRelation(relationId);

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	/* disallow modifications to a partition table which have rep. factor > 1 */
	EnsurePartitionTableNotReplicated(relationId);

	if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(cacheEntry, RANGE_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(cacheEntry, APPEND_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(cacheEntry, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		CopyToExistingShards(copyStatement, completionTag);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported partition method")));
	}

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * EnsureCopyCanRunOnRelation throws error is the database in read-only mode.
 */
static void
EnsureCopyCanRunOnRelation(Oid relationId)
{
	/* first, do the regular check and give consistent errors with regular queries */
	EnsureModificationsCanRunOnRelation(relationId);

	/*
	 * We use 2PC for all COPY commands. It means that we cannot allow any COPY
	 * on replicas even if the user allows via WritableStandbyCoordinator GUC.
	 */
	if (RecoveryInProgress() && WritableStandbyCoordinator)
	{
		ereport(ERROR, (errmsg("COPY command to Citus tables is not allowed in "
							   "read-only mode"),
						errhint("All COPY commands to citus tables happen via 2PC, "
								"and 2PC requires the database to be in a writable state."),
						errdetail("the database is read-only")));
	}
}


/*
 * CopyToExistingShards implements the COPY table_name FROM ... for hash or
 * range-partitioned tables where there are already shards into which to copy
 * rows.
 */
static void
CopyToExistingShards(CopyStmt *copyStatement, QueryCompletionCompat *completionTag)
{
	Oid tableId = RangeVarGetRelid(copyStatement->relation, NoLock, false);


	List *columnNameList = NIL;
	int partitionColumnIndex = INVALID_PARTITION_COLUMN_INDEX;

	uint64 processedRowCount = 0;

	ErrorContextCallback errorCallback;

	/* allocate column values and nulls arrays */
	Relation distributedRelation = table_open(tableId, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);
	uint32 columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	/* set up a virtual tuple table slot */
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlotCompat(tupleDescriptor,
																	&TTSOpsVirtual);
	tupleTableSlot->tts_nvalid = columnCount;
	tupleTableSlot->tts_values = columnValues;
	tupleTableSlot->tts_isnull = columnNulls;

	/* determine the partition column index in the tuple descriptor */
	Var *partitionColumn = PartitionColumn(tableId, 0);
	if (partitionColumn != NULL)
	{
		partitionColumnIndex = partitionColumn->varattno - 1;
	}

	/* build the list of column names for remote COPY statements */
	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped ||
			currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
			)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);

	/* set up the destination for the COPY */
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(tableId, columnNameList,
																  partitionColumnIndex,
																  executorState, NULL);

	/* if the user specified an explicit append-to_shard option, write to it */
	uint64 appendShardId = ProcessAppendToShardOption(tableId, copyStatement);
	if (appendShardId != INVALID_SHARD_ID)
	{
		copyDest->appendShardId = appendShardId;
	}

	DestReceiver *dest = (DestReceiver *) copyDest;
	dest->rStartup(dest, 0, tupleDescriptor);

	/*
	 * Below, we change a few fields in the Relation to control the behaviour
	 * of BeginCopyFrom. However, we obviously should not do this in relcache
	 * and therefore make a copy of the Relation.
	 */
	Relation copiedDistributedRelation = (Relation) palloc(sizeof(RelationData));
	Form_pg_class copiedDistributedRelationTuple =
		(Form_pg_class) palloc(CLASS_TUPLE_SIZE);

	/*
	 * There is no need to deep copy everything. We will just deep copy of the fields
	 * we will change.
	 */
	*copiedDistributedRelation = *distributedRelation;
	*copiedDistributedRelationTuple = *distributedRelation->rd_rel;

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
	CopyFromState copyState = BeginCopyFrom_compat(NULL,
												   copiedDistributedRelation,
												   NULL,
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
		ResetPerTupleExprContext(executorState);

		MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

		/* parse a row from the input */
		bool nextRowFound = NextCopyFromCompat(copyState, executorExpressionContext,
											   columnValues, columnNulls);

		if (!nextRowFound)
		{
			MemoryContextSwitchTo(oldContext);
			break;
		}

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(oldContext);

		dest->receiveSlot(tupleTableSlot, dest);

		++processedRowCount;

#if PG_VERSION_NUM >= PG_VERSION_14
		pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processedRowCount);
#endif
	}

	EndCopyFrom(copyState);

	/* all lines have been copied, stop showing line number in errors */
	error_context_stack = errorCallback.previous;

	/* finish the COPY commands */
	dest->rShutdown(dest);
	dest->rDestroy(dest);

	ExecDropSingleTupleTableSlot(tupleTableSlot);
	FreeExecutorState(executorState);
	table_close(distributedRelation, NoLock);

	CHECK_FOR_INTERRUPTS();

	if (completionTag != NULL)
	{
		CompleteCopyQueryTagCompat(completionTag, processedRowCount);
	}
}


static void
CompleteCopyQueryTagCompat(QueryCompletionCompat *completionTag, uint64 processedRowCount)
{
	#if PG_VERSION_NUM >= PG_VERSION_13
	SetQueryCompletion(completionTag, CMDTAG_COPY, processedRowCount);
	#else
	SafeSnprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	#endif
}


/*
 * RemoveOptionFromList removes an option from a list of options in a
 * COPY .. WITH (..) statement by name and returns the resulting list.
 */
static List *
RemoveOptionFromList(List *optionList, char *optionName)
{
	ListCell *optionCell = NULL;
	#if PG_VERSION_NUM < PG_VERSION_13
	ListCell *previousCell = NULL;
	#endif
	foreach(optionCell, optionList)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (strncmp(option->defname, optionName, NAMEDATALEN) == 0)
		{
			return list_delete_cell_compat(optionList, optionCell, previousCell);
		}
		#if PG_VERSION_NUM < PG_VERSION_13
		previousCell = optionCell;
		#endif
	}

	return optionList;
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

	for (int columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescription, columnIndex);

		if (currentColumn->attisdropped ||
			currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
			)
		{
			continue;
		}

		Oid typeId = currentColumn->atttypid;
		if (!CanUseBinaryCopyFormatForType(typeId))
		{
			useBinaryCopyFormat = false;
			break;
		}
	}

	return useBinaryCopyFormat;
}


/*
 * CanUseBinaryCopyFormatForTargetList returns true if we can use binary
 * copy format for all columns of the given target list.
 */
bool
CanUseBinaryCopyFormatForTargetList(List *targetEntryList)
{
	ListCell *targetEntryCell = NULL;
	foreach(targetEntryCell, targetEntryList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *targetExpr = (Node *) targetEntry->expr;

		Oid columnType = exprType(targetExpr);
		if (!CanUseBinaryCopyFormatForType(columnType))
		{
			return false;
		}
	}

	return true;
}


/*
 * CanUseBinaryCopyFormatForType determines whether it is safe to use the
 * binary copy format for the given type. See contents of the function for
 * details of when it's safe to use binary copy.
 */
bool
CanUseBinaryCopyFormatForType(Oid typeId)
{
	if (!BinaryOutputFunctionDefined(typeId))
	{
		return false;
	}

	if (!BinaryInputFunctionDefined(typeId))
	{
		return false;
	}

	/*
	 * A row type can contain any types, possibly types that don't have
	 * the binary input and output functions defined.
	 */
	if (type_is_rowtype(typeId))
	{
		/*
		 * TODO: Inspect the types inside the record and check if all of them
		 * can be binary encoded. If so, it's safe to use binary encoding.
		 *
		 * IMPORTANT: When implementing this todo keep the following in mind:
		 *
		 * In PG versions before PG14 the record_recv function would error out
		 * more than necessary.
		 *
		 * It errors out when any of the columns in the row have a type oid
		 * that doesn't match with the oid in the received data. This happens
		 * pretty much always for non built in types, because their oids differ
		 * between postgres intallations. So for those Postgres versions we
		 * would need a check like the following for each column:
		 *
		 * if (columnType >= FirstNormalObjectId) {
		 *     return false
		 * }
		 */
		return false;
	}

	HeapTuple typeTup = typeidType(typeId);
	Form_pg_type type = (Form_pg_type) GETSTRUCT(typeTup);
	Oid elementType = type->typelem;
#if PG_VERSION_NUM < PG_VERSION_14
	char typeCategory = type->typcategory;
#endif
	ReleaseSysCache(typeTup);

#if PG_VERSION_NUM < PG_VERSION_14

	/*
	 * In PG versions before PG14 the array_recv function would error out more
	 * than necessary.
	 *
	 * It errors out when the element type its oids don't match with the oid in
	 * the received data. This happens pretty much always for non built in
	 * types, because their oids differ between postgres intallations. So we
	 * skip binary encoding when the element type is a non built in type.
	 */
	if (typeCategory == TYPCATEGORY_ARRAY && elementType >= FirstNormalObjectId)
	{
		return false;
	}
#endif

	/*
	 * Any type that is a wrapper around an element type (e.g. arrays and
	 * ranges) require the element type to also has support for binary
	 * encoding.
	 */
	if (elementType != InvalidOid)
	{
		if (!CanUseBinaryCopyFormatForType(elementType))
		{
			return false;
		}
	}

	/*
	 * For domains, make sure that the underlying type can be binary copied.
	 */
	Oid baseTypeId = getBaseType(typeId);
	if (typeId != baseTypeId)
	{
		if (!CanUseBinaryCopyFormatForType(baseTypeId))
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

	return OidIsValid(typeFunctionId);
}


/*
 * BinaryInputFunctionDefined checks whether binary output function is defined
 * for the given type.
 */
static bool
BinaryInputFunctionDefined(Oid typeId)
{
	Oid typeFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	int16 typeLength = 0;
	bool typeByVal = false;
	char typeAlign = 0;
	char typeDelim = 0;

	get_type_io_data(typeId, IOFunc_receive, &typeLength, &typeByVal,
					 &typeAlign, &typeDelim, &typeIoParam, &typeFunctionId);

	return OidIsValid(typeFunctionId);
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
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId)
{
	StringInfo command = makeStringInfo();

	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;

	char *shardName = pstrdup(relationName);

	AppendShardIdToName(&shardName, shardId);

	char *shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	appendStringInfo(command, "COPY %s ", shardQualifiedName);

	if (copyStatement->attlist != NIL)
	{
		ListCell *columnNameCell = NULL;
		bool appendedFirstName = false;

		foreach(columnNameCell, copyStatement->attlist)
		{
			char *columnName = strVal(lfirst(columnNameCell));
			const char *quotedColumnName = quote_identifier(columnName);

			if (!appendedFirstName)
			{
				appendStringInfo(command, "(%s", quotedColumnName);
				appendedFirstName = true;
			}
			else
			{
				appendStringInfo(command, ", %s", quotedColumnName);
			}
		}

		appendStringInfoString(command, ") ");
	}

	if (copyStatement->is_from)
	{
		appendStringInfoString(command, "FROM STDIN");
	}
	else
	{
		appendStringInfoString(command, "TO STDOUT");
	}

	if (copyStatement->options != NIL)
	{
		ListCell *optionCell = NULL;

		appendStringInfoString(command, " WITH (");

		foreach(optionCell, copyStatement->options)
		{
			DefElem *defel = (DefElem *) lfirst(optionCell);

			if (optionCell != list_head(copyStatement->options))
			{
				appendStringInfoString(command, ", ");
			}

			appendStringInfo(command, "%s", defel->defname);

			if (defel->arg == NULL)
			{
				/* option without value */
			}
			else if (IsA(defel->arg, String))
			{
				char *value = defGetString(defel);

				/* make sure strings are quoted (may contain reserved characters) */
				appendStringInfo(command, " %s", quote_literal_cstr(value));
			}
			else if (IsA(defel->arg, List))
			{
				List *nameList = defGetStringList(defel);

				appendStringInfo(command, " (%s)", NameListToQuotedString(nameList));
			}
			else
			{
				char *value = defGetString(defel);

				/* numeric options or * should not have quotes */
				appendStringInfo(command, " %s", value);
			}
		}

		appendStringInfoString(command, ")");
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
		bool raiseInterrupts = true;

		/* end the COPY input */
		if (!PutRemoteCopyEnd(connection, NULL))
		{
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("failed to COPY to shard " INT64_FORMAT " on %s:%d",
								   shardId, connection->hostname, connection->port)));
		}

		/* check whether there were any COPY errors */
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
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
void
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

	if (destAttrNumber == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("invalid attr? %s", columnName)));
	}

	Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, destAttrNumber - 1);
	return attr->atttypid;
}


/*
 * Walks a TupleDesc and returns an array of the types of each attribute.
 * Returns InvalidOid in the place of dropped or generated attributes.
 */
static Oid *
TypeArrayFromTupleDescriptor(TupleDesc tupleDescriptor)
{
	int columnCount = tupleDescriptor->natts;
	Oid *typeArray = palloc0(columnCount * sizeof(Oid));

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, columnIndex);
		if (attr->attisdropped ||
			attr->attgenerated == ATTRIBUTE_GENERATED_STORED
			)
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
	int columnCount = inputTupleDescriptor->natts;
	CopyCoercionData *coercePaths = palloc0(columnCount * sizeof(CopyCoercionData));
	Oid *inputTupleTypes = TypeArrayFromTupleDescriptor(inputTupleDescriptor);
	ListCell *currentColumnName = list_head(columnNameList);

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Oid inputTupleType = inputTupleTypes[columnIndex];
		char *columnName = lfirst(currentColumnName);

		if (inputTupleType == InvalidOid)
		{
			/* TypeArrayFromTupleDescriptor decided to skip this column */
			continue;
		}

		Oid destTupleType = TypeForColumnName(destRelId, destTupleDescriptor, columnName);

		finalColumnTypeArray[columnIndex] = destTupleType;

		ConversionPathForTypes(inputTupleType, destTupleType,
							   &coercePaths[columnIndex]);

		currentColumnName = lnext_compat(columnNameList, currentColumnName);

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

	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Oid columnTypeId = typeIdArray[columnIndex];
		bool typeVariableLength = false;
		Oid outputFunctionId = InvalidOid;

		if (columnTypeId == InvalidOid)
		{
			/* TypeArrayFromTupleDescriptor decided to skip this column */
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

	MemoryContext oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

	if (rowOutputState->binary)
	{
		CopySendInt16(rowOutputState, availableColumnCount);
	}
	for (uint32 columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(rowDescriptor, columnIndex);
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (!isNull && columnCoercionPaths != NULL)
		{
			value = CoerceColumnValue(value, &columnCoercionPaths[columnIndex]);
		}

		if (currentColumn->attisdropped ||
			currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED
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
Datum
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

	for (uint32 columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);

		if (!currentColumn->attisdropped &&
			currentColumn->attgenerated != ATTRIBUTE_GENERATED_STORED
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


/* *INDENT-OFF* */


/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyOutState cstate)
{
#if PG_VERSION_NUM < PG_VERSION_14
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3) {
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('H');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
		return;
	}
#endif
	StringInfoData buf;
	int			natts = list_length(cstate->attnumlist);
	int16		format = (cstate->binary ? 1 : 0);
	int			i;

	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, format);	/* overall format */
	pq_sendint16(&buf, natts);
	for (i = 0; i < natts; i++)
		pq_sendint16(&buf, format); /* per-column formats */
	pq_endmessage(&buf);
	cstate->copy_dest = COPY_FRONTEND;
}


/* End a copy stream sent to the client */
static void
SendCopyEnd(CopyOutState cstate)
{
#if PG_VERSION_NUM < PG_VERSION_14
	if (cstate->copy_dest != COPY_NEW_FE)
	{
		CopySendData(cstate, "\\.", 2);
		/* Need to flush out the trailer (this also appends a newline) */
		CopySendEndOfRow(cstate, true);
		pq_endcopyout(false);
		return;
	}
#endif
	/* Shouldn't have any unsent data */
	Assert(cstate->fe_msgbuf->len == 0);
	/* Send Copy Done message */
	pq_putemptymessage('c');
}


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


/* Send the row to the appropriate destination */
static void
CopySendEndOfRow(CopyOutState cstate, bool includeEndOfLine)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
#if PG_VERSION_NUM < PG_VERSION_14
		case COPY_OLD_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary && includeEndOfLine)
				CopySendChar(cstate, '\n');

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
			break;
#endif
		case COPY_FRONTEND:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary && includeEndOfLine)
				CopySendChar(cstate, '\n');

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_FILE:
		case COPY_CALLBACK:
			Assert(false);		/* Not yet supported. */
			break;
	}

	resetStringInfo(fe_msgbuf);
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
	char *start = pointer;
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
							EState *executorState,
							char *intermediateResultIdPrefix)
{
	CitusCopyDestReceiver *copyDest = (CitusCopyDestReceiver *) palloc0(
		sizeof(CitusCopyDestReceiver));

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
	copyDest->colocatedIntermediateResultIdPrefix = intermediateResultIdPrefix;
	copyDest->memoryContext = CurrentMemoryContext;

	return copyDest;
}


/*
 * GetLocalCopyStatus returns the status for executing copy locally.
 * If LOCAL_COPY_DISABLED or LOCAL_COPY_REQUIRED, the caller has to
 * follow that. Else, the caller may decide to use local or remote
 * execution depending on other information.
 */
static LocalCopyStatus
GetLocalCopyStatus(void)
{
	if (!EnableLocalExecution ||
		GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_DISABLED)
	{
		return LOCAL_COPY_DISABLED;
	}
	else if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_REQUIRED)
	{
		/*
		 * For various reasons, including the transaction visibility
		 * rules (e.g., read-your-own-writes), we have to use local
		 * execution again if it has already happened within this
		 * transaction block.
		 *
		 * We might error out later in the execution if it is not suitable
		 * to execute the tasks locally.
		 */
		Assert(IsMultiStatementTransaction() || InCoordinatedTransaction());

		/*
		 * TODO: A future improvement could be to keep track of which placements
		 * have been locally executed. At this point, only use local execution for
		 * those placements. That'd help to benefit more from parallelism.
		 */

		return LOCAL_COPY_REQUIRED;
	}
	else if (IsMultiStatementTransaction())
	{
		return LOCAL_COPY_REQUIRED;
	}

	return LOCAL_COPY_OPTIONAL;
}


/*
 * ShardIntervalListHasLocalPlacements returns true if any of the input
 * shard placement has a local placement;
 */
static bool
ShardIntervalListHasLocalPlacements(List *shardIntervalList)
{
	int32 localGroupId = GetLocalGroupId();
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		if (ActiveShardPlacementOnGroup(localGroupId, shardInterval->shardId) != NULL)
		{
			return true;
		}
	}

	return false;
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

	List *columnNameList = copyDest->columnNameList;
	List *attributeList = NIL;

	ListCell *columnNameCell = NULL;

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	/* look up table properties */
	Relation distributedRelation = table_open(tableId, RowExclusiveLock);
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(tableId);

	copyDest->distributedRelation = distributedRelation;
	copyDest->tupleDescriptor = inputTupleDescriptor;

	/* load the list of shards and verify that we have shards to copy into */
	List *shardIntervalList = LoadShardIntervalList(tableId);
	if (shardIntervalList == NIL)
	{
		if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED))
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
	if (cacheEntry->hasUninitializedShardInterval)
	{
		if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED) ||
			IsCitusTableTypeCacheEntry(cacheEntry, RANGE_DISTRIBUTED))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not start copy"),
							errdetail("Distributed relation \"%s\" has shards "
									  "with missing shardminvalue/shardmaxvalue.",
									  relationName)));
		}
	}

	/* prevent concurrent placement changes and non-commutative DML statements */
	LockShardListMetadata(shardIntervalList, ShareLock);

	/*
	 * Prevent concurrent UPDATE/DELETE on replication factor >1
	 * (see AcquireExecutorMultiShardLocks() at multi_router_executor.c)
	 */
	SerializeNonCommutativeWrites(shardIntervalList, RowExclusiveLock);

	UseCoordinatedTransaction();

	/* all modifications use 2PC */
	Use2PCForCoordinatedTransaction();

	/* define how tuples will be serialised */
	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
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

	/* wrap the column names as Values */
	foreach(columnNameCell, columnNameList)
	{
		char *columnName = (char *) lfirst(columnNameCell);
		String *columnNameValue = makeString(columnName);

		attributeList = lappend(attributeList, columnNameValue);
	}

	if (IsCitusTableTypeCacheEntry(cacheEntry, DISTRIBUTED_TABLE) &&
		copyDest->partitionColumnIndex == INVALID_PARTITION_COLUMN_INDEX)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("the partition column of table %s should have a value",
							   quote_qualified_identifier(schemaName, relationName))));
	}

	/* define the template for the COPY statement that is sent to workers */
	CopyStmt *copyStatement = makeNode(CopyStmt);

	bool colocatedIntermediateResults =
		copyDest->colocatedIntermediateResultIdPrefix != NULL;
	if (colocatedIntermediateResults)
	{
		copyStatement->relation = makeRangeVar(NULL,
											   copyDest->
											   colocatedIntermediateResultIdPrefix,
											   -1);

		DefElem *formatResultOption = makeDefElem("format", (Node *) makeString("result"),
												  -1);
		copyStatement->options = list_make1(formatResultOption);
	}
	else
	{
		copyStatement->relation = makeRangeVar(schemaName, relationName, -1);
		copyStatement->options = NIL;

		if (copyOutState->binary)
		{
			DefElem *binaryFormatOption =
				makeDefElem("format", (Node *) makeString("binary"), -1);

			copyStatement->options = lappend(copyStatement->options, binaryFormatOption);
		}
	}


	copyStatement->query = NULL;
	copyStatement->attlist = attributeList;
	copyStatement->is_from = true;
	copyStatement->is_program = false;
	copyStatement->filename = NULL;
	copyDest->copyStatement = copyStatement;

	copyDest->shardStateHash = CreateShardStateHash(TopTransactionContext);
	copyDest->connectionStateHash = CreateConnectionStateHash(TopTransactionContext);

	RecordRelationAccessIfNonDistTable(tableId, PLACEMENT_ACCESS_DML);

	/*
	 * Colocated intermediate results do not honor citus.max_shared_pool_size,
	 * so we don't need to reserve any connections. Each result file is sent
	 * over a single connection.
	 */
	if (!colocatedIntermediateResults)
	{
		/*
		 * For all the primary (e.g., writable) remote nodes, reserve a shared
		 * connection. We do this upfront because we cannot know which nodes
		 * are going to be accessed. Since the order of the reservation is
		 * important, we need to do it right here. For the details on why the
		 * order important, see EnsureConnectionPossibilityForNodeList().
		 *
		 * We don't need to care about local node because we either get a
		 * connection or use local connection, so it cannot be part of
		 * the starvation. As an edge case, if it cannot get a connection
		 * and cannot switch to local execution (e.g., disabled by user),
		 * COPY would fail hinting the user to change the relevant settiing.
		 */
		EnsureConnectionPossibilityForRemotePrimaryNodes();
	}

	LocalCopyStatus localCopyStatus = GetLocalCopyStatus();
	if (localCopyStatus == LOCAL_COPY_DISABLED)
	{
		copyDest->shouldUseLocalCopy = false;
	}
	else if (localCopyStatus == LOCAL_COPY_REQUIRED)
	{
		copyDest->shouldUseLocalCopy = true;
	}
	else if (localCopyStatus == LOCAL_COPY_OPTIONAL)
	{
		/*
		 * At this point, there is no requirements for doing the copy locally.
		 * However, if there are local placements, we can try to reserve
		 * a connection to local node. If we cannot reserve, we can still use
		 * local execution.
		 *
		 * NB: It is not advantageous to use remote execution just with a
		 * single remote connection. In other words, a single remote connection
		 * would not perform better than local execution. However, we prefer to
		 * do this because it is likely that the COPY would get more connections
		 * to parallelize the operation. In the future, we might relax this
		 * requirement and failover to local execution as on connection attempt
		 * failures as the executor does.
		 */
		if (ShardIntervalListHasLocalPlacements(shardIntervalList))
		{
			bool reservedConnection = TryConnectionPossibilityForLocalPrimaryNode();
			copyDest->shouldUseLocalCopy = !reservedConnection;
		}
	}
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

	CopyOutState copyOutState = copyDest->copyOutState;
	FmgrInfo *columnOutputFunctions = copyDest->columnOutputFunctions;
	CopyCoercionData *columnCoercionPaths = copyDest->columnCoercionPaths;
	ListCell *placementStateCell = NULL;
	bool cachedShardStateFound = false;
	bool firstTupleInShard = false;


	EState *executorState = copyDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	int64 shardId = ShardIdForTuple(copyDest, columnValues, columnNulls);

	/* connections hash is kept in memory context */
	MemoryContextSwitchTo(copyDest->memoryContext);
	bool isColocatedIntermediateResult =
		copyDest->colocatedIntermediateResultIdPrefix != NULL;

	CopyShardState *shardState = GetShardState(shardId, copyDest->shardStateHash,
											   copyDest->connectionStateHash,
											   &cachedShardStateFound,
											   copyDest->shouldUseLocalCopy,
											   copyDest->copyOutState,
											   isColocatedIntermediateResult);
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

	if (isColocatedIntermediateResult && copyDest->shouldUseLocalCopy &&
		shardState->containsLocalPlacement)
	{
		if (firstTupleInShard)
		{
			CreateLocalColocatedIntermediateFile(copyDest, shardState);
		}

		WriteTupleToLocalFile(slot, copyDest, shardId,
							  shardState->copyOutState, &shardState->fileDest);
	}
	else if (copyDest->shouldUseLocalCopy && shardState->containsLocalPlacement)
	{
		WriteTupleToLocalShard(slot, copyDest, shardId, shardState->copyOutState);
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
				 currentPlacementState->data->len > CopySwitchOverThresholdBytes)
		{
			switchToCurrentPlacement = true;

			/* before switching, make sure to finish the copy */
			EndPlacementStateCopyCommand(activePlacementState, copyOutState);
			AddPlacementStateToCopyConnectionStateBuffer(connectionState,
														 activePlacementState);
		}

		if (switchToCurrentPlacement)
		{
			StartPlacementStateCopyCommand(currentPlacementState, copyStatement,
										   copyOutState);

			RemovePlacementStateFromCopyConnectionStateBuffer(connectionState,
															  currentPlacementState);

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
 * AddPlacementStateToCopyConnectionStateBuffer is a helper function to add a placement
 * state to connection state's placement buffer. In addition to that, keep the counter
 * up to date.
 */
static void
AddPlacementStateToCopyConnectionStateBuffer(CopyConnectionState *connectionState,
											 CopyPlacementState *placementState)
{
	dlist_push_head(&connectionState->bufferedPlacementList,
					&placementState->bufferedPlacementNode);
	connectionState->bufferedPlacementCount++;
}


/*
 * RemovePlacementStateFromCopyConnectionStateBuffer is a helper function to removes a placement
 * state from connection state's placement buffer. In addition to that, keep the counter
 * up to date.
 */
static void
RemovePlacementStateFromCopyConnectionStateBuffer(CopyConnectionState *connectionState,
												  CopyPlacementState *placementState)
{
	dlist_delete(&placementState->bufferedPlacementNode);
	connectionState->bufferedPlacementCount--;
}


/*
 * ProcessAppendToShardOption returns the value of append_to_shard if set,
 * and removes the option from the options list.
 */
static uint64
ProcessAppendToShardOption(Oid relationId, CopyStmt *copyStatement)
{
	uint64 appendShardId = INVALID_SHARD_ID;
	bool appendToShardSet = false;

	DefElem *defel = NULL;
	foreach_ptr(defel, copyStatement->options)
	{
		if (strncmp(defel->defname, APPEND_TO_SHARD_OPTION, NAMEDATALEN) == 0)
		{
			appendShardId = defGetInt64(defel);
			appendToShardSet = true;
			break;
		}
	}

	if (appendToShardSet)
	{
		if (!IsCitusTableType(relationId, APPEND_DISTRIBUTED))
		{
			ereport(ERROR, (errmsg(APPEND_TO_SHARD_OPTION " is only valid for "
														  "append-distributed tables")));
		}

		/* throws an error if shard does not exist */
		ShardInterval *shardInterval = LoadShardInterval(appendShardId);

		/* also check whether shard belongs to table */
		if (shardInterval->relationId != relationId)
		{
			ereport(ERROR, (errmsg("shard " UINT64_FORMAT " does not belong to table %s",
								   appendShardId, get_rel_name(relationId))));
		}

		copyStatement->options =
			RemoveOptionFromList(copyStatement->options, APPEND_TO_SHARD_OPTION);
	}
	else if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
	{
		ereport(ERROR, (errmsg("COPY into append-distributed table requires using the "
							   APPEND_TO_SHARD_OPTION " option")));
	}

	return appendShardId;
}


/*
 * ContainsLocalPlacement returns true if the current node has
 * a local placement for the given shard id.
 */
static bool
ContainsLocalPlacement(int64 shardId)
{
	ListCell *placementCell = NULL;
	List *activePlacementList = ActiveShardPlacementList(shardId);
	int32 localGroupId = GetLocalGroupId();

	foreach(placementCell, activePlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

		if (placement->groupId == localGroupId)
		{
			return true;
		}
	}
	return false;
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
	CitusTableCacheEntry *cacheEntry =
		GetCitusTableCacheEntry(copyDest->distributedRelationId);

	if (IsCitusTableTypeCacheEntry(cacheEntry, APPEND_DISTRIBUTED))
	{
		return copyDest->appendShardId;
	}

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
	ShardInterval *shardInterval = FindShardInterval(partitionColumnValue, cacheEntry);
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
	ListCell *connectionStateCell = NULL;
	Relation distributedRelation = copyDest->distributedRelation;

	List *connectionStateList = ConnectionStateList(connectionStateHash);

	FinishLocalColocatedIntermediateFiles(copyDest);
	FinishLocalCopy(copyDest);

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

	table_close(distributedRelation, NoLock);
}


/*
 * FinishLocalCopy sends the remaining copies for local placements.
 */
static void
FinishLocalCopy(CitusCopyDestReceiver *copyDest)
{
	HTAB *shardStateHash = copyDest->shardStateHash;
	HASH_SEQ_STATUS status;
	CopyShardState *copyShardState;

	foreach_htab(copyShardState, &status, shardStateHash)
	{
		if (copyShardState->copyOutState != NULL &&
			copyShardState->copyOutState->fe_msgbuf->len > 0)
		{
			FinishLocalCopyToShard(copyDest, copyShardState->shardId,
								   copyShardState->copyOutState);
		}
	}
}


/*
 * CreateLocalColocatedIntermediateFile creates a co-located file for the given
 * shard, and appends the binary headers if needed. The function also modifies
 * shardState to set the fileDest and copyOutState.
 */
static void
CreateLocalColocatedIntermediateFile(CitusCopyDestReceiver *copyDest,
									 CopyShardState *shardState)
{
	/* make sure the directory exists */
	CreateIntermediateResultsDirectory();

	const int fileFlags = (O_CREAT | O_RDWR | O_TRUNC);
	const int fileMode = (S_IRUSR | S_IWUSR);

	StringInfo filePath = makeStringInfo();
	appendStringInfo(filePath, "%s_%ld", copyDest->colocatedIntermediateResultIdPrefix,
					 shardState->shardId);

	const char *fileName = QueryResultFileName(filePath->data);
	shardState->fileDest =
		FileCompatFromFileStart(FileOpenForTransmit(fileName, fileFlags, fileMode));

	CopyOutState localFileCopyOutState = shardState->copyOutState;
	bool isBinaryCopy = localFileCopyOutState->binary;
	if (isBinaryCopy)
	{
		AppendCopyBinaryHeaders(localFileCopyOutState);
	}
}


/*
 * FinishLocalColocatedIntermediateFiles iterates over all the colocated
 * intermediate files and finishes the COPY on all of them.
 */
static void
FinishLocalColocatedIntermediateFiles(CitusCopyDestReceiver *copyDest)
{
	HTAB *shardStateHash = copyDest->shardStateHash;
	HASH_SEQ_STATUS status;
	CopyShardState *copyShardState;

	foreach_htab(copyShardState, &status, shardStateHash)
	{
		if (copyShardState->copyOutState != NULL &&
			FILE_IS_OPEN(copyShardState->fileDest.fd))
		{
			FinishLocalCopyToFile(copyShardState->copyOutState,
								  &copyShardState->fileDest);
		}
	}
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


/*
 * CitusCopyDestReceiverDestroy frees the DestReceiver
 */
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

	if (copyDest->shardStateHash)
	{
		hash_destroy(copyDest->shardStateHash);
	}

	if (copyDest->connectionStateHash)
	{
		hash_destroy(copyDest->connectionStateHash);
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
ProcessCopyStmt(CopyStmt *copyStatement, QueryCompletionCompat *completionTag, const
				char *queryString)
{
	/*
	 * Handle special COPY "resultid" FROM STDIN WITH (format result) commands
	 * for sending intermediate results to workers.
	 */
	if (IsCopyResultStmt(copyStatement))
	{
		const char *resultId = copyStatement->relation->relname;

		if (copyStatement->is_from)
		{
			ReceiveQueryResultViaCopy(resultId);
		}
		else
		{
			SendQueryResultViaCopy(resultId);
		}

		return NULL;
	}

	/*
	 * We check whether a distributed relation is affected. For that, we need to open the
	 * relation. To prevent race conditions with later lookups, lock the table, and modify
	 * the rangevar to include the schema.
	 */
	if (copyStatement->relation != NULL)
	{
		bool isFrom = copyStatement->is_from;

		/* consider using RangeVarGetRelidExtended to check perms before locking */
		Relation copiedRelation = table_openrv(copyStatement->relation,
											   isFrom ? RowExclusiveLock :
											   AccessShareLock);

		bool isCitusRelation = IsCitusTable(RelationGetRelid(copiedRelation));

		/* ensure future lookups hit the same relation */
		char *schemaName = get_namespace_name(RelationGetNamespace(copiedRelation));

		/* ensure we copy string into proper context */
		MemoryContext relationContext = GetMemoryChunkContext(
			copyStatement->relation);
		schemaName = MemoryContextStrdup(relationContext, schemaName);
		copyStatement->relation->schemaname = schemaName;

		table_close(copiedRelation, NoLock);

		if (isCitusRelation)
		{
			if (copyStatement->is_from)
			{
				if (copyStatement->whereClause)
				{
					/*
					 * Update progress reporting for tuples progressed so that the
					 * progress is reflected on pg_stat_progress_copy. Citus currently
					 * does not support COPY .. WHERE clause so TUPLES_EXCLUDED is not
					 * handled. When we remove this check, we should implement progress
					 * reporting as well.
					 */
					ereport(ERROR, (errmsg(
										"Citus does not support COPY FROM with WHERE")));
				}

				/* check permissions, we're bypassing postgres' normal checks */
				CheckCopyPermissions(copyStatement);
				CitusCopyFrom(copyStatement, completionTag);
				return NULL;
			}
			else if (copyStatement->filename == NULL && !copyStatement->is_program &&
					 !CopyStatementHasFormat(copyStatement, "binary"))
			{
				/*
				 * COPY table TO STDOUT is handled by specialized logic to
				 * avoid buffering the table on the coordinator. This enables
				 * pg_dump of large tables.
				 */
				CitusCopyTo(copyStatement, completionTag);
				return NULL;
			}
			else
			{
				/*
				 * COPY table TO PROGRAM / file is handled by wrapping the table
				 * in a SELECT and going through the resulting COPY logic.
				 */
				SelectStmt *selectStmt = CitusCopySelect(copyStatement);

				/* replace original statement */
				copyStatement = copyObject(copyStatement);
				copyStatement->relation = NULL;
				copyStatement->query = (Node *) selectStmt;
			}
		}
	}
	return (Node *) copyStatement;
}


/*
 * CitusCopySelect generates a SelectStmt such that table may be replaced in
 * "COPY table FROM" for an equivalent result.
 */
static SelectStmt *
CitusCopySelect(CopyStmt *copyStatement)
{
	SelectStmt *selectStmt = makeNode(SelectStmt);
	selectStmt->fromClause = list_make1(copyObject(copyStatement->relation));

	Relation distributedRelation = table_openrv(copyStatement->relation, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);
	List *targetList = NIL;

	for (int i = 0; i < tupleDescriptor->natts; i++)
	{
		Form_pg_attribute attr = &tupleDescriptor->attrs[i];

		if (attr->attisdropped ||
			attr->attgenerated
			)
		{
			continue;
		}

		ColumnRef *column = makeNode(ColumnRef);
		column->fields = list_make1(makeString(pstrdup(attr->attname.data)));
		column->location = -1;

		ResTarget *selectTarget = makeNode(ResTarget);
		selectTarget->name = NULL;
		selectTarget->indirection = NIL;
		selectTarget->val = (Node *) column;
		selectTarget->location = -1;

		targetList = lappend(targetList, selectTarget);
	}

	table_close(distributedRelation, NoLock);

	selectStmt->targetList = targetList;
	return selectStmt;
}


/*
 * CitusCopyTo runs a COPY .. TO STDOUT command on each shard to do a full
 * table dump.
 */
static void
CitusCopyTo(CopyStmt *copyStatement, QueryCompletionCompat *completionTag)
{
	ListCell *shardIntervalCell = NULL;
	int64 tuplesSent = 0;

	Relation distributedRelation = table_openrv(copyStatement->relation, AccessShareLock);
	Oid relationId = RelationGetRelid(distributedRelation);
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);

	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->binary = false;
	copyOutState->attnumlist = CopyGetAttnums(tupleDescriptor, distributedRelation,
											  copyStatement->attlist);

	SendCopyBegin(copyOutState);

	List *shardIntervalList = LoadShardIntervalList(relationId);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = lfirst(shardIntervalCell);
		List *shardPlacementList = ActiveShardPlacementList(shardInterval->shardId);
		ListCell *shardPlacementCell = NULL;
		int placementIndex = 0;

		StringInfo copyCommand = ConstructCopyStatement(copyStatement,
														shardInterval->shardId);

		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement = lfirst(shardPlacementCell);
			int connectionFlags = 0;
			char *userName = NULL;
			const bool raiseErrors = true;

			MultiConnection *connection = GetPlacementConnection(connectionFlags,
																 shardPlacement,
																 userName);

			/*
			 * This code-path doesn't support optional connections, so we don't expect
			 * NULL connections.
			 */
			Assert(connection != NULL);

			if (placementIndex == list_length(shardPlacementList) - 1)
			{
				/* last chance for this shard */
				MarkRemoteTransactionCritical(connection);
			}

			if (PQstatus(connection->pgConn) != CONNECTION_OK)
			{
				ReportConnectionError(connection, ERROR);
				continue;
			}

			RemoteTransactionBeginIfNecessary(connection);

			if (!SendRemoteCommand(connection, copyCommand->data))
			{
				ReportConnectionError(connection, ERROR);
				continue;
			}

			PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
			if (PQresultStatus(result) != PGRES_COPY_OUT)
			{
				ReportResultError(connection, result, ERROR);
			}

			PQclear(result);

			tuplesSent += ForwardCopyDataFromConnection(copyOutState, connection);

			break;
		}

		if (shardIntervalCell == list_head(shardIntervalList))
		{
			/* remove header after the first shard */
			copyStatement->options =
				RemoveOptionFromList(copyStatement->options, "header");
		}
	}

	SendCopyEnd(copyOutState);

	table_close(distributedRelation, AccessShareLock);

	if (completionTag != NULL)
	{
		CompleteCopyQueryTagCompat(completionTag, tuplesSent);
	}
}


/*
 * ForwardCopyDataFromConnection forwards copy data received over the given connection
 * to the client or file descriptor.
 */
static int64
ForwardCopyDataFromConnection(CopyOutState copyOutState, MultiConnection *connection)
{
	char *receiveBuffer = NULL;
	const int useAsync = 0;
	bool raiseErrors = true;
	int64 tuplesSent = 0;

	/* receive copy data message in a synchronous manner */
	int receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, useAsync);
	while (receiveLength > 0)
	{
		bool includeEndOfLine = false;

		CopySendData(copyOutState, receiveBuffer, receiveLength);
		CopySendEndOfRow(copyOutState, includeEndOfLine);
		tuplesSent++;

		PQfreemem(receiveBuffer);

		receiveLength = PQgetCopyData(connection->pgConn, &receiveBuffer, useAsync);
	}

	if (receiveLength != -1)
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	PQclear(result);
	ClearResults(connection, raiseErrors);

	return tuplesSent;
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
	List	   *range_table = NIL;
	TupleDesc	tupDesc;
	AclMode		required_access = (is_from ? ACL_INSERT : ACL_SELECT);
	List	   *attnums;
	ListCell   *cur;

	rel = table_openrv(copyStatement->relation,
	                  is_from ? RowExclusiveLock : AccessShareLock);

	range_table = CreateRangeTable(rel, required_access);
	RangeTblEntry *rte = (RangeTblEntry*) linitial(range_table);
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

	table_close(rel, NoLock);
	/* *INDENT-ON* */
}


/*
 * CreateRangeTable creates a range table with the given relation.
 */
List *
CreateRangeTable(Relation rel, AclMode requiredAccess)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = rel->rd_id;
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = requiredAccess;
	return list_make1(rte);
}


#if PG_VERSION_NUM < PG_VERSION_14

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
			if (TupleDescAttr(tupDesc, i)->attgenerated)
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
					if (att->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								 errmsg("column \"%s\" is a generated column",
										name),
								 errdetail("Generated columns cannot be used in COPY.")));
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


#endif


/*
 * CreateConnectionStateHash constructs a hash table which maps from socket
 * number to CopyConnectionState, passing the provided MemoryContext to
 * hash_create for hash allocations.
 */
static HTAB *
CreateConnectionStateHash(MemoryContext memoryContext)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int);
	info.entrysize = sizeof(CopyConnectionState);
	info.hcxt = memoryContext;
	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	HTAB *connectionStateHash = hash_create("Copy Connection State Hash", 128, &info,
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
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(CopyShardState);
	info.hcxt = memoryContext;
	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	HTAB *shardStateHash = hash_create("Copy Shard State Hash", 128, &info, hashFlags);

	return shardStateHash;
}


/*
 * GetConnectionState finds existing CopyConnectionState for a connection in the
 * provided hash. If not found, then a default structure is returned.
 */
static CopyConnectionState *
GetConnectionState(HTAB *connectionStateHash, MultiConnection *connection)
{
	bool found = false;

	int sock = PQsocket(connection->pgConn);
	Assert(sock != -1);

	CopyConnectionState *connectionState = (CopyConnectionState *) hash_search(
		connectionStateHash, &sock,
		HASH_ENTER,
		&found);
	if (!found)
	{
		connectionState->socket = sock;
		connectionState->connection = connection;
		connectionState->activePlacementState = NULL;
		connectionState->bufferedPlacementCount = 0;
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

	hash_seq_init(&status, connectionStateHash);

	CopyConnectionState *connectionState = (CopyConnectionState *) hash_seq_search(
		&status);
	while (connectionState != NULL)
	{
		connectionStateList = lappend(connectionStateList, connectionState);

		connectionState = (CopyConnectionState *) hash_seq_search(&status);
	}

	return connectionStateList;
}


/*
 * ConnectionStateListToNode returns all CopyConnectionState structures in
 * the given hash for a given hostname and port values.
 */
static List *
ConnectionStateListToNode(HTAB *connectionStateHash, const char *hostname, int32 port)
{
	List *connectionStateList = NIL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, connectionStateHash);

	CopyConnectionState *connectionState =
		(CopyConnectionState *) hash_seq_search(&status);
	while (connectionState != NULL)
	{
		char *connectionHostname = connectionState->connection->hostname;
		if (strncmp(connectionHostname, hostname, MAX_NODE_LENGTH) == 0 &&
			connectionState->connection->port == port)
		{
			connectionStateList = lappend(connectionStateList, connectionState);
		}

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
			  HTAB *connectionStateHash, bool *found, bool
			  shouldUseLocalCopy, CopyOutState copyOutState,
			  bool isColocatedIntermediateResult)
{
	CopyShardState *shardState = (CopyShardState *) hash_search(shardStateHash, &shardId,
																HASH_ENTER, found);
	if (!*found)
	{
		InitializeCopyShardState(shardState, connectionStateHash,
								 shardId, shouldUseLocalCopy,
								 copyOutState, isColocatedIntermediateResult);
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
						 bool shouldUseLocalCopy,
						 CopyOutState copyOutState,
						 bool colocatedIntermediateResult)
{
	ListCell *placementCell = NULL;
	int failedPlacementCount = 0;
	bool hasRemoteCopy = false;

	MemoryContext localContext =
		AllocSetContextCreateExtended(CurrentMemoryContext,
									  "InitializeCopyShardState",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);


	/* release active placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	List *activePlacementList = ActiveShardPlacementList(shardId);

	MemoryContextSwitchTo(oldContext);

	shardState->shardId = shardId;
	shardState->placementStateList = NIL;
	shardState->copyOutState = NULL;
	shardState->containsLocalPlacement = ContainsLocalPlacement(shardId);
	shardState->fileDest.fd = -1;

	foreach(placementCell, activePlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

		if (shouldUseLocalCopy && placement->groupId == GetLocalGroupId())
		{
			shardState->copyOutState = (CopyOutState) palloc0(sizeof(*copyOutState));
			CloneCopyOutStateForLocalCopy(copyOutState, shardState->copyOutState);

			if (colocatedIntermediateResult)
			{
				LogLocalCopyToFileExecution(shardId);
			}
			else
			{
				LogLocalCopyToRelationExecution(shardId);
			}

			continue;
		}

		hasRemoteCopy = true;

		MultiConnection *connection =
			CopyGetPlacementConnection(connectionStateHash, placement,
									   colocatedIntermediateResult);
		if (connection == NULL)
		{
			failedPlacementCount++;
			continue;
		}

		CopyConnectionState *connectionState = GetConnectionState(connectionStateHash,
																  connection);

		/*
		 * If this is the first time we are using this connection for copying a
		 * shard, send begin if necessary.
		 */
		if (connectionState->activePlacementState == NULL)
		{
			RemoteTransactionBeginIfNecessary(connection);
		}

		CopyPlacementState *placementState = palloc0(sizeof(CopyPlacementState));
		placementState->shardState = shardState;
		placementState->data = makeStringInfo();
		placementState->groupId = placement->groupId;
		placementState->connectionState = connectionState;

		/*
		 * We don't set connectionState->activePlacementState here even if it
		 * is NULL. Later in CitusSendTupleToPlacements() we set it at the
		 * same time as calling StartPlacementStateCopyCommand() so we actually
		 * know the COPY operation for the placement is ongoing.
		 */
		AddPlacementStateToCopyConnectionStateBuffer(connectionState, placementState);
		shardState->placementStateList = lappend(shardState->placementStateList,
												 placementState);
	}

	/* if all placements failed, error out */
	if (failedPlacementCount == list_length(activePlacementList))
	{
		ereport(ERROR, (errmsg("could not connect to any active placements")));
	}

	if (hasRemoteCopy)
	{
		EnsureRemoteTaskExecutionAllowed();
	}

	/*
	 * We just error out and code execution should never reach to this
	 * point. This is the case for all tables.
	 */
	Assert(failedPlacementCount == 0);

	MemoryContextReset(localContext);
}


/*
 * CloneCopyOutStateForLocalCopy creates a shallow copy of the CopyOutState with a new
 * fe_msgbuf. We keep a separate CopyOutState for every local shard placement, because
 * in case of local copy we serialize and buffer incoming tuples into fe_msgbuf for each
 * placement and the serialization functions take a CopyOutState as a parameter.
 */
static void
CloneCopyOutStateForLocalCopy(CopyOutState from, CopyOutState to)
{
	to->attnumlist = from->attnumlist;
	to->binary = from->binary;
	to->copy_dest = from->copy_dest;
	to->delim = from->delim;
	to->file_encoding = from->file_encoding;
	to->need_transcoding = from->need_transcoding;
	to->null_print = from->null_print;
	to->null_print_client = from->null_print_client;
	to->rowcontext = from->rowcontext;
	to->fe_msgbuf = makeStringInfo();
}


/*
 * LogLocalCopyToRelationExecution logs that the copy will be done
 * locally for the given shard.
 */
static void
LogLocalCopyToRelationExecution(uint64 shardId)
{
	if (!(LogRemoteCommands || LogLocalCommands))
	{
		return;
	}
	ereport(NOTICE, (errmsg("executing the copy locally for shard %lu", shardId)));
}


/*
 * LogLocalCopyToFileExecution logs that the copy will be done locally for
 * a file colocated to the given shard.
 */
static void
LogLocalCopyToFileExecution(uint64 shardId)
{
	if (!(LogRemoteCommands || LogLocalCommands))
	{
		return;
	}
	ereport(NOTICE, (errmsg("executing the copy locally for colocated file with "
							"shard %lu", shardId)));
}


/*
 * CopyGetPlacementConnection assigns a connection to the given placement. If
 * a connection has already been assigned the placement in the current transaction
 * then it reuses the connection. Otherwise, it requests a connection for placement.
 */
static MultiConnection *
CopyGetPlacementConnection(HTAB *connectionStateHash, ShardPlacement *placement,
						   bool colocatedIntermediateResult)
{
	if (colocatedIntermediateResult)
	{
		/*
		 * Colocated intermediate results are just files and not required to use
		 * the same connections with their co-located shards. So, we are free to
		 * use any connection we can get.
		 *
		 * Also, the current connection re-use logic does not know how to handle
		 * intermediate results as the intermediate results always truncates the
		 * existing files. That's why we we use one connection per intermediate
		 * result.
		 *
		 * Also note that we are breaking the guarantees of citus.shared_pool_size
		 * as we cannot rely on optional connections.
		 */
		uint32 connectionFlagsForIntermediateResult = 0;
		MultiConnection *connection =
			GetNodeConnection(connectionFlagsForIntermediateResult, placement->nodeName,
							  placement->nodePort);

		/*
		 * As noted above, we want each intermediate file to go over
		 * a separate connection.
		 */
		ClaimConnectionExclusively(connection);

		/* and, we cannot afford to handle failures when anything goes wrong */
		MarkRemoteTransactionCritical(connection);

		return connection;
	}

	/*
	 * Determine whether the task has to be assigned to a particular connection
	 * due to a preceding access to the placement in the same transaction.
	 */
	ShardPlacementAccess *placementAccess = CreatePlacementAccess(placement,
																  PLACEMENT_ACCESS_DML);
	uint32 connectionFlags = FOR_DML;
	MultiConnection *connection =
		GetConnectionIfPlacementAccessedInXact(connectionFlags,
											   list_make1(placementAccess), NULL);
	if (connection != NULL)
	{
		/*
		 * Errors are supposed to cause immediate aborts (i.e. we don't
		 * want to/can't invalidate placements), mark the connection as
		 * critical so later errors cause failures.
		 */
		MarkRemoteTransactionCritical(connection);

		return connection;
	}

	/*
	 * If we exceeded citus.max_adaptive_executor_pool_size, we should re-use the
	 * existing connections to multiplex multiple COPY commands on shards over a
	 * single connection.
	 */
	char *nodeName = placement->nodeName;
	int nodePort = placement->nodePort;
	List *copyConnectionStateList =
		ConnectionStateListToNode(connectionStateHash, nodeName, nodePort);
	if (HasReachedAdaptiveExecutorPoolSize(copyConnectionStateList))
	{
		/*
		 * If we've already reached the executor pool size, there should be at
		 * least one connection to any given node.
		 *
		 * Note that we don't need to mark the connection as critical, since the
		 * connection was already returned by this function before.
		 */
		connection = GetLeastUtilisedCopyConnection(copyConnectionStateList,
													nodeName,
													nodePort);

		/*
		 * Make sure that the connection management remembers that Citus
		 * accesses this placement over the connection.
		 */
		AssignPlacementListToConnection(list_make1(placementAccess), connection);

		return connection;
	}

	if (IsReservationPossible())
	{
		/*
		 * Enforce the requirements for adaptive connection management
		 * (a.k.a., throttle connections if citus.max_shared_pool_size
		 * reached).
		 *
		 * Given that we have done reservations per node, we do not ever
		 * need to pass WAIT_FOR_CONNECTION, we are sure that there is a
		 * connection either reserved for this backend or already established
		 * by the previous commands in the same transaction block.
		 */
		int adaptiveConnectionManagementFlag = OPTIONAL_CONNECTION;
		connectionFlags |= adaptiveConnectionManagementFlag;
	}


	/*
	 * For placements that haven't been assigned a connection by a previous command
	 * in the current transaction, we use a separate connection per placement for
	 * hash-distributed tables in order to get the maximum performance.
	 */
	if (placement->partitionMethod == DISTRIBUTE_BY_HASH &&
		MultiShardConnectionType != SEQUENTIAL_CONNECTION)
	{
		/*
		 * Claiming the connection exclusively (done below) would also have the
		 * effect of opening multiple connections, but claiming the connection
		 * exclusively prevents GetConnectionIfPlacementAccessedInXact from returning
		 * the connection if it is needed for a different shard placement.
		 *
		 * By setting the REQUIRE_CLEAN_CONNECTION flag we are guaranteed to get
		 * connection that will not be returned by GetConnectionIfPlacementAccessedInXact
		 * for the remainder of the COPY, hence it safe to claim the connection
		 * exclusively. Claiming a connection exclusively prevents it from being
		 * used in other distributed queries that happen during the COPY (e.g. if
		 * the copy logic calls a function to calculate a default value, and the
		 * function does a distributed query).
		 */
		connectionFlags |= REQUIRE_CLEAN_CONNECTION;
	}

	char *nodeUser = CurrentUserName();
	connection = GetPlacementConnection(connectionFlags, placement, nodeUser);
	if (connection == NULL)
	{
		if (list_length(copyConnectionStateList) > 0)
		{
			/*
			 * The connection manager throttled any new connections, so pick an existing
			 * connection with least utilization.
			 *
			 * Note that we don't need to mark the connection as critical, since the
			 * connection was already returned by this function before.
			 */
			connection =
				GetLeastUtilisedCopyConnection(copyConnectionStateList, nodeName,
											   nodePort);

			/*
			 * Make sure that the connection management remembers that Citus
			 * accesses this placement over the connection.
			 */
			AssignPlacementListToConnection(list_make1(placementAccess), connection);
		}
		else
		{
			/*
			 * For this COPY command, we have not established any connections
			 * and adaptive connection management throttled the new connection
			 * request. This could only happen if this COPY command is the
			 * second (or later) COPY command in a transaction block as the
			 * first COPY command always gets a connection per node thanks to
			 * the connection reservation.
			 *
			 * As we know that there has been at least one COPY command happened
			 * earlier, we need to find the connection to that node, and use it.
			 */
			connection =
				ConnectionAvailableToNode(nodeName, nodePort, CurrentUserName(),
										  CurrentDatabaseName());

			/*
			 * We do not expect this to happen, but still instead of an assert,
			 * we prefer explicit error message.
			 */
			if (connection == NULL)
			{
				ereport(ERROR, (errmsg("could not find an available connection"),
								errhint("Set citus.max_shared_pool_size TO -1 to let "
										"COPY command finish")));
			}
		}

		return connection;
	}

	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(connection, ERROR);
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
 * HasReachedAdaptiveExecutorPoolSize returns true if the number of entries in input
 * connection list has greater than or equal to citus.max_adaptive_executor_pool_size.
 */
static bool
HasReachedAdaptiveExecutorPoolSize(List *connectionStateList)
{
	if (list_length(connectionStateList) >= MaxAdaptiveExecutorPoolSize)
	{
		/*
		 * We've not reached MaxAdaptiveExecutorPoolSize number of
		 * connections, so we're allowed to establish a new
		 * connection to the given node.
		 */
		return true;
	}

	return false;
}


/*
 * GetLeastUtilisedCopyConnection returns a MultiConnection to the given node
 * with the least number of placements assigned to it.
 *
 * It is assumed that there exists at least one connection to the node.
 */
static MultiConnection *
GetLeastUtilisedCopyConnection(List *connectionStateList, char *nodeName,
							   int nodePort)
{
	MultiConnection *connection = NULL;
	int minPlacementCount = PG_INT32_MAX;
	ListCell *connectionStateCell = NULL;

	/*
	 * We only pick the least utilised connection when some connection limits are
	 * reached such as max_shared_pool_size or max_adaptive_executor_pool_size.
	 *
	 * Therefore there should be some connections to choose from.
	 */
	Assert(list_length(connectionStateList) > 0);

	foreach(connectionStateCell, connectionStateList)
	{
		CopyConnectionState *connectionState = lfirst(connectionStateCell);
		int currentConnectionPlacementCount = connectionState->bufferedPlacementCount;

		if (connectionState->activePlacementState != NULL)
		{
			currentConnectionPlacementCount++;
		}

		Assert(currentConnectionPlacementCount > 0);

		if (currentConnectionPlacementCount < minPlacementCount)
		{
			minPlacementCount = currentConnectionPlacementCount;
			connection = connectionState->connection;
		}
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
	MultiConnection *connection = placementState->connectionState->connection;
	uint64 shardId = placementState->shardState->shardId;
	bool raiseInterrupts = true;
	bool binaryCopy = copyOutState->binary;

	StringInfo copyCommand = ConstructCopyStatement(copyStatement, shardId);

	if (!SendRemoteCommand(connection, copyCommand->data))
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
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
