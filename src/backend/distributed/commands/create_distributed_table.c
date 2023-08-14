/*-------------------------------------------------------------------------
 *
 * create_distributed_table.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "distributed/pg_version_constants.h"
#include "distributed/commands/utility_hook.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_split.h"
#include "distributed/shard_transfer.h"
#include "distributed/shared_library_init.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "distributed/worker_transaction.h"
#include "distributed/utils/distribution_column_map.h"
#include "distributed/version_compat.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* common params that apply to all Citus table types */
typedef struct
{
	char distributionMethod;
	char replicationModel;
} CitusTableParams;


/*
 * Params that only apply to distributed tables, i.e., the ones that are
 * known as DISTRIBUTED_TABLE by Citus metadata.
 */
typedef struct
{
	int shardCount;
	bool shardCountIsStrict;
	char *distributionColumnName;
	ColocationParam colocationParam;
} DistributedTableParams;


/*
 * once every LOG_PER_TUPLE_AMOUNT, the copy will be logged.
 */
#define LOG_PER_TUPLE_AMOUNT 1000000

/* local function forward declarations */
static void CreateDistributedTableConcurrently(Oid relationId,
											   char *distributionColumnName,
											   char distributionMethod,
											   char *colocateWithTableName,
											   int shardCount,
											   bool shardCountIsStrict);
static char DecideDistTableReplicationModel(char distributionMethod,
											char *colocateWithTableName);
static List * HashSplitPointsForShardList(List *shardList);
static List * HashSplitPointsForShardCount(int shardCount);
static List * WorkerNodesForShardList(List *shardList);
static List * RoundRobinWorkerNodeList(List *workerNodeList, int listLength);
static CitusTableParams DecideCitusTableParams(CitusTableType tableType,
											   DistributedTableParams *
											   distributedTableParams);
static void CreateCitusTable(Oid relationId, CitusTableType tableType,
							 DistributedTableParams *distributedTableParams);
static void ConvertCitusLocalTableToTableType(Oid relationId,
											  CitusTableType tableType,
											  DistributedTableParams *
											  distributedTableParams);
static uint32 SingleShardTableColocationNodeId(uint32 colocationId);
static uint32 SingleShardTableGetNodeId(Oid relationId);
static int64 NoneDistTableGetShardId(Oid relationId);
static void CreateHashDistributedTableShards(Oid relationId, int shardCount,
											 Oid colocatedTableId, bool localTableEmpty);
static void CreateSingleShardTableShard(Oid relationId, Oid colocatedTableId,
										uint32 colocationId);
static uint32 ColocationIdForNewTable(Oid relationId, CitusTableType tableType,
									  DistributedTableParams *distributedTableParams,
									  Var *distributionColumn);
static void EnsureRelationCanBeDistributed(Oid relationId, Var *distributionColumn,
										   char distributionMethod, uint32 colocationId,
										   char replicationModel);
static void EnsureLocalTableEmpty(Oid relationId);
static void EnsureRelationHasNoTriggers(Oid relationId);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static void EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod);
static bool ShouldLocalTableBeEmpty(Oid relationId, char distributionMethod);
static void EnsureCitusTableCanBeCreated(Oid relationOid);
static void PropagatePrerequisiteObjectsForDistributedTable(Oid relationId);
static void EnsureDistributedSequencesHaveOneType(Oid relationId,
												  List *seqInfoList);
static void CopyLocalDataIntoShards(Oid relationId);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);

#if (PG_VERSION_NUM >= PG_VERSION_15)
static bool DistributionColumnUsesNumericColumnNegativeScale(TupleDesc relationDesc,
															 Var *distributionColumn);
static int numeric_typmod_scale(int32 typmod);
static bool is_valid_numeric_typmod(int32 typmod);
#endif

static bool DistributionColumnUsesGeneratedStoredColumn(TupleDesc relationDesc,
														Var *distributionColumn);
static bool CanUseExclusiveConnections(Oid relationId, bool localTableEmpty);
static void DoCopyFromLocalTableIntoShards(Relation distributedRelation,
										   DestReceiver *copyDest,
										   TupleTableSlot *slot,
										   EState *estate);
static void ErrorIfTemporaryTable(Oid relationId);
static void ErrorIfForeignTable(Oid relationOid);
static void SendAddLocalTableToMetadataCommandOutsideTransaction(Oid relationId);
static void EnsureDistributableTable(Oid relationId);
static void EnsureForeignKeysForDistributedTableConcurrently(Oid relationId);
static void EnsureColocateWithTableIsValid(Oid relationId, char distributionMethod,
										   char *distributionColumnName,
										   char *colocateWithTableName);
static void WarnIfTableHaveNoReplicaIdentity(Oid relationId);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(create_distributed_table_concurrently);
PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(create_reference_table);


/*
 * master_create_distributed_table is a deprecated predecessor to
 * create_distributed_table
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("master_create_distributed_table has been deprecated")));
}


/*
 * create_distributed_table gets a table name, distribution column,
 * distribution method and colocate_with option, then it creates a
 * distributed table.
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(3))
	{
		PG_RETURN_VOID();
	}

	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(3);
	char *colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	bool shardCountIsStrict = false;
	if (distributionColumnText)
	{
		if (PG_ARGISNULL(2))
		{
			PG_RETURN_VOID();
		}

		int shardCount = ShardCount;
		if (!PG_ARGISNULL(4))
		{
			if (!IsColocateWithDefault(colocateWithTableName) &&
				!IsColocateWithNone(colocateWithTableName))
			{
				ereport(ERROR, (errmsg("Cannot use colocate_with with a table "
									   "and shard_count at the same time")));
			}

			shardCount = PG_GETARG_INT32(4);

			/*
			 * If shard_count parameter is given, then we have to
			 * make sure table has that many shards.
			 */
			shardCountIsStrict = true;
		}

		char *distributionColumnName = text_to_cstring(distributionColumnText);
		Assert(distributionColumnName != NULL);

		char distributionMethod = LookupDistributionMethod(distributionMethodOid);

		if (shardCount < 1 || shardCount > MAX_SHARD_COUNT)
		{
			ereport(ERROR, (errmsg("%d is outside the valid range for "
								   "parameter \"shard_count\" (1 .. %d)",
								   shardCount, MAX_SHARD_COUNT)));
		}

		CreateDistributedTable(relationId, distributionColumnName, distributionMethod,
							   shardCount, shardCountIsStrict, colocateWithTableName);
	}
	else
	{
		if (!PG_ARGISNULL(4))
		{
			ereport(ERROR, (errmsg("shard_count can't be specified when the "
								   "distribution column is null because in "
								   "that case it's automatically set to 1")));
		}

		if (!PG_ARGISNULL(2) &&
			LookupDistributionMethod(PG_GETARG_OID(2)) != DISTRIBUTE_BY_HASH)
		{
			/*
			 * As we do for shard_count parameter, we could throw an error if
			 * distribution_type is not NULL when creating a single-shard table.
			 * However, this requires changing the default value of distribution_type
			 * parameter to NULL and this would mean a breaking change for most
			 * users because they're mostly using this API to create sharded
			 * tables. For this reason, here we instead do nothing if the distribution
			 * method is DISTRIBUTE_BY_HASH.
			 */
			ereport(ERROR, (errmsg("distribution_type can't be specified "
								   "when the distribution column is null ")));
		}

		ColocationParam colocationParam = {
			.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
			.colocateWithTableName = colocateWithTableName,
		};
		CreateSingleShardTable(relationId, colocationParam);
	}

	PG_RETURN_VOID();
}


/*
 * create_distributed_concurrently gets a table name, distribution column,
 * distribution method and colocate_with option, then it creates a
 * distributed table.
 */
Datum
create_distributed_table_concurrently(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
	{
		PG_RETURN_VOID();
	}

	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("cannot use create_distributed_table_concurrently "
							   "to create a distributed table with a null shard "
							   "key, consider using create_distributed_table()")));
	}

	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Oid distributionMethodOid = PG_GETARG_OID(2);
	char distributionMethod = LookupDistributionMethod(distributionMethodOid);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(3);
	char *colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	bool shardCountIsStrict = false;
	int shardCount = ShardCount;
	if (!PG_ARGISNULL(4))
	{
		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0 &&
			pg_strncasecmp(colocateWithTableName, "none", NAMEDATALEN) != 0)
		{
			ereport(ERROR, (errmsg("Cannot use colocate_with with a table "
								   "and shard_count at the same time")));
		}

		shardCount = PG_GETARG_INT32(4);

		/*
		 * if shard_count parameter is given than we have to
		 * make sure table has that many shards
		 */
		shardCountIsStrict = true;
	}

	CreateDistributedTableConcurrently(relationId, distributionColumnName,
									   distributionMethod,
									   colocateWithTableName,
									   shardCount,
									   shardCountIsStrict);

	PG_RETURN_VOID();
}


/*
 * CreateDistributedTableConcurrently distributes a table by first converting
 * it to a Citus local table and then splitting the shard of the Citus local
 * table.
 *
 * If anything goes wrong during the second phase, the table is left as a
 * Citus local table.
 */
static void
CreateDistributedTableConcurrently(Oid relationId, char *distributionColumnName,
								   char distributionMethod,
								   char *colocateWithTableName,
								   int shardCount,
								   bool shardCountIsStrict)
{
	/*
	 * We disallow create_distributed_table_concurrently in transaction blocks
	 * because we cannot handle preceding writes, and we block writes at the
	 * very end of the operation so the transaction should end immediately after.
	 */
	PreventInTransactionBlock(true, "create_distributed_table_concurrently");

	/*
	 * do not allow multiple create_distributed_table_concurrently in the same
	 * transaction. We should do that check just here because concurrent local table
	 * conversion can cause issues.
	 */
	ErrorIfMultipleNonblockingMoveSplitInTheSameTransaction();

	/* do not allow concurrent CreateDistributedTableConcurrently operations */
	AcquireCreateDistributedTableConcurrentlyLock(relationId);

	if (distributionMethod != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("only hash-distributed tables can be distributed "
							   "without blocking writes")));
	}

	if (ShardReplicationFactor > 1)
	{
		ereport(ERROR, (errmsg("cannot distribute a table concurrently when "
							   "citus.shard_replication_factor > 1")));
	}

	DropOrphanedResourcesInSeparateTransaction();

	EnsureCitusTableCanBeCreated(relationId);

	EnsureValidDistributionColumn(relationId, distributionColumnName);

	/*
	 * Ensure table type is valid to be distributed. It should be either regular or citus local table.
	 */
	EnsureDistributableTable(relationId);

	/*
	 * we rely on citus_add_local_table_to_metadata, so it can generate irrelevant messages.
	 * we want to error with a user friendly message if foreign keys are not supported.
	 * We can miss foreign key violations because we are not holding locks, so relation
	 * can be modified until we acquire the lock for the relation, but we do as much as we can
	 * to be user friendly on foreign key violation messages.
	 */

	EnsureForeignKeysForDistributedTableConcurrently(relationId);

	char replicationModel = DecideDistTableReplicationModel(distributionMethod,
															colocateWithTableName);

	/*
	 * we fail transaction before local table conversion if the table could not be colocated with
	 * given table. We should make those checks after local table conversion by acquiring locks to
	 * the relation because the distribution column can be modified in that period.
	 */
	if (!IsColocateWithDefault(colocateWithTableName) && !IsColocateWithNone(
			colocateWithTableName))
	{
		EnsureColocateWithTableIsValid(relationId, distributionMethod,
									   distributionColumnName,
									   colocateWithTableName);
	}

	/*
	 * Get name of the table before possibly replacing it in
	 * citus_add_local_table_to_metadata.
	 */
	char *tableName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	RangeVar *rangeVar = makeRangeVar(schemaName, tableName, -1);

	/* If table is a regular table, then we need to add it into metadata. */
	if (!IsCitusTable(relationId))
	{
		/*
		 * Before taking locks, convert the table into a Citus local table and commit
		 * to allow shard split to see the shard.
		 */
		SendAddLocalTableToMetadataCommandOutsideTransaction(relationId);
	}

	/*
	 * Lock target relation with a shard update exclusive lock to
	 * block DDL, but not writes.
	 *
	 * If there was a concurrent drop/rename, error out by setting missingOK = false.
	 */
	bool missingOK = false;
	relationId = RangeVarGetRelid(rangeVar, ShareUpdateExclusiveLock, missingOK);

	if (PartitionedTableNoLock(relationId))
	{
		/* also lock partitions */
		LockPartitionRelations(relationId, ShareUpdateExclusiveLock);
	}

	WarnIfTableHaveNoReplicaIdentity(relationId);

	List *shardList = LoadShardIntervalList(relationId);

	/*
	 * It's technically possible for the table to have been concurrently
	 * distributed just after citus_add_local_table_to_metadata and just
	 * before acquiring the lock, so double check.
	 */
	if (list_length(shardList) != 1 ||
		!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table was concurrently modified")));
	}

	/*
	 * The table currently has one shard, we will split that shard to match the
	 * target distribution.
	 */
	ShardInterval *shardToSplit = (ShardInterval *) linitial(shardList);

	PropagatePrerequisiteObjectsForDistributedTable(relationId);

	/*
	 * we should re-evaluate distribution column values. It may have changed,
	 * because we did not lock the relation at the previous check before local
	 * table conversion.
	 */
	Var *distributionColumn = BuildDistributionKeyFromColumnName(relationId,
																 distributionColumnName,
																 NoLock);
	Oid distributionColumnType = distributionColumn->vartype;
	Oid distributionColumnCollation = distributionColumn->varcollid;

	/* get an advisory lock to serialize concurrent default group creations */
	if (IsColocateWithDefault(colocateWithTableName))
	{
		AcquireColocationDefaultLock();
	}

	/*
	 * At this stage, we only want to check for an existing co-location group.
	 * We cannot create a new co-location group until after replication slot
	 * creation in NonBlockingShardSplit.
	 */
	uint32 colocationId = FindColocateWithColocationId(relationId,
													   replicationModel,
													   distributionColumnType,
													   distributionColumnCollation,
													   shardCount,
													   shardCountIsStrict,
													   colocateWithTableName);

	if (IsColocateWithDefault(colocateWithTableName) && (colocationId !=
														 INVALID_COLOCATION_ID))
	{
		/*
		 * we can release advisory lock if there is already a default entry for given params;
		 * else, we should keep it to prevent different default coloc entry creation by
		 * concurrent operations.
		 */
		ReleaseColocationDefaultLock();
	}

	EnsureRelationCanBeDistributed(relationId, distributionColumn, distributionMethod,
								   colocationId, replicationModel);

	Oid colocatedTableId = InvalidOid;
	if (colocationId != INVALID_COLOCATION_ID)
	{
		colocatedTableId = ColocatedTableId(colocationId);
	}

	List *workerNodeList = DistributedTablePlacementNodeList(NoLock);
	if (workerNodeList == NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no worker nodes are available for placing shards"),
						errhint("Add more worker nodes.")));
	}

	List *workersForPlacementList;
	List *shardSplitPointsList;

	if (colocatedTableId != InvalidOid)
	{
		List *colocatedShardList = LoadShardIntervalList(colocatedTableId);

		/*
		 * Match the shard ranges of an existing table.
		 */
		shardSplitPointsList = HashSplitPointsForShardList(colocatedShardList);

		/*
		 * Find the node IDs of the shard placements.
		 */
		workersForPlacementList = WorkerNodesForShardList(colocatedShardList);
	}
	else
	{
		/*
		 * Generate a new set of #shardCount shards.
		 */
		shardSplitPointsList = HashSplitPointsForShardCount(shardCount);

		/*
		 * Place shards in a round-robin fashion across all data nodes.
		 */
		workersForPlacementList = RoundRobinWorkerNodeList(workerNodeList, shardCount);
	}

	/*
	 * Make sure that existing reference tables have been replicated to all the nodes
	 * such that we can create foreign keys and joins work immediately after creation.
	 * We do this after applying all essential checks to error out early in case of
	 * user error.
	 *
	 * Use force_logical since this function is meant to not block writes.
	 */
	EnsureReferenceTablesExistOnAllNodesExtended(TRANSFER_MODE_FORCE_LOGICAL);

	/*
	 * At this point, the table is a Citus local table, which means it does
	 * not have a partition column in the metadata. However, we cannot update
	 * the metadata here because that would prevent us from creating a replication
	 * slot to copy ongoing changes. Instead, we pass a hash that maps relation
	 * IDs to partition column vars.
	 */
	DistributionColumnMap *distributionColumnOverrides = CreateDistributionColumnMap();
	AddDistributionColumnForRelation(distributionColumnOverrides, relationId,
									 distributionColumnName);

	/*
	 * there is no colocation entries yet for local table, so we should
	 * check if table has any partition and add them to same colocation
	 * group
	 */
	List *sourceColocatedShardIntervalList = ListShardsUnderParentRelation(relationId);

	SplitMode splitMode = NON_BLOCKING_SPLIT;
	SplitOperation splitOperation = CREATE_DISTRIBUTED_TABLE;
	SplitShard(
		splitMode,
		splitOperation,
		shardToSplit->shardId,
		shardSplitPointsList,
		workersForPlacementList,
		distributionColumnOverrides,
		sourceColocatedShardIntervalList,
		colocationId
		);
}


/*
 * EnsureForeignKeysForDistributedTableConcurrently ensures that referenced and referencing foreign
 * keys for the given table are supported.
 *
 * We allow distributed -> reference
 *          distributed -> citus local
 *
 * We disallow reference   -> distributed
 *             citus local -> distributed
 *             regular     -> distributed
 *
 * Normally regular		-> distributed is allowed but it is not allowed when we create the
 * distributed table concurrently because we rely on conversion of regular table to citus local table,
 * which errors with an unfriendly message.
 */
static void
EnsureForeignKeysForDistributedTableConcurrently(Oid relationId)
{
	/*
	 * disallow citus local -> distributed fkeys.
	 * disallow reference   -> distributed fkeys.
	 * disallow regular     -> distributed fkeys.
	 */
	EnsureNoFKeyFromTableType(relationId, INCLUDE_CITUS_LOCAL_TABLES |
							  INCLUDE_REFERENCE_TABLES | INCLUDE_LOCAL_TABLES);

	/*
	 * disallow distributed -> regular fkeys.
	 */
	EnsureNoFKeyToTableType(relationId, INCLUDE_LOCAL_TABLES);
}


/*
 * EnsureColocateWithTableIsValid ensures given relation can be colocated with the table of given name.
 */
static void
EnsureColocateWithTableIsValid(Oid relationId, char distributionMethod,
							   char *distributionColumnName, char *colocateWithTableName)
{
	char replicationModel = DecideDistTableReplicationModel(distributionMethod,
															colocateWithTableName);

	/*
	 * we fail transaction before local table conversion if the table could not be colocated with
	 * given table. We should make those checks after local table conversion by acquiring locks to
	 * the relation because the distribution column can be modified in that period.
	 */
	Oid distributionColumnType = ColumnTypeIdForRelationColumnName(relationId,
																   distributionColumnName);

	text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
	Oid colocateWithTableId = ResolveRelationId(colocateWithTableNameText, false);
	EnsureTableCanBeColocatedWith(relationId, replicationModel,
								  distributionColumnType, colocateWithTableId);
}


/*
 * AcquireCreateDistributedTableConcurrentlyLock does not allow concurrent create_distributed_table_concurrently
 * operations.
 */
void
AcquireCreateDistributedTableConcurrentlyLock(Oid relationId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = true;

	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_CREATE_DISTRIBUTED_TABLE_CONCURRENTLY);

	LockAcquireResult lockAcquired = LockAcquire(&tag, ExclusiveLock, sessionLock,
												 dontWait);
	if (!lockAcquired)
	{
		ereport(ERROR, (errmsg("another create_distributed_table_concurrently "
							   "operation is in progress"),
						errhint("Make sure that the concurrent operation has "
								"finished and re-run the command")));
	}
}


/*
 * SendAddLocalTableToMetadataCommandOutsideTransaction executes metadata add local
 * table command locally to avoid deadlock.
 */
static void
SendAddLocalTableToMetadataCommandOutsideTransaction(Oid relationId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	/*
	 * we need to allow nested distributed execution, because we start a new distributed
	 * execution inside the pushed-down UDF citus_add_local_table_to_metadata. Normally
	 * citus does not allow that because it cannot guarantee correctness.
	 */
	StringInfo allowNestedDistributionCommand = makeStringInfo();
	appendStringInfo(allowNestedDistributionCommand,
					 "SET LOCAL citus.allow_nested_distributed_execution to ON");

	StringInfo addLocalTableToMetadataCommand = makeStringInfo();
	appendStringInfo(addLocalTableToMetadataCommand,
					 "SELECT pg_catalog.citus_add_local_table_to_metadata(%s)",
					 quote_literal_cstr(qualifiedRelationName));

	List *commands = list_make2(allowNestedDistributionCommand->data,
								addLocalTableToMetadataCommand->data);
	char *username = NULL;
	SendCommandListToWorkerOutsideTransaction(LocalHostName, PostPortNumber, username,
											  commands);
}


/*
 * WarnIfTableHaveNoReplicaIdentity notices user if the given table or its partitions (if any)
 * do not have a replica identity which is required for logical replication to replicate
 * UPDATE and DELETE commands during create_distributed_table_concurrently.
 */
void
WarnIfTableHaveNoReplicaIdentity(Oid relationId)
{
	bool foundRelationWithNoReplicaIdentity = false;

	/*
	 * Check for source relation's partitions if any. We do not need to check for the source relation
	 * because we can replicate partitioned table even if it does not have replica identity.
	 * Source table will have no data if it has partitions.
	 */
	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);
		ListCell *partitionCell = NULL;

		foreach(partitionCell, partitionList)
		{
			Oid partitionTableId = lfirst_oid(partitionCell);

			if (!RelationCanPublishAllModifications(partitionTableId))
			{
				foundRelationWithNoReplicaIdentity = true;
				break;
			}
		}
	}
	/* check for source relation if it is not partitioned */
	else
	{
		if (!RelationCanPublishAllModifications(relationId))
		{
			foundRelationWithNoReplicaIdentity = true;
		}
	}

	if (foundRelationWithNoReplicaIdentity)
	{
		char *relationName = get_rel_name(relationId);

		ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("relation %s does not have a REPLICA "
								"IDENTITY or PRIMARY KEY", relationName),
						 errdetail("UPDATE and DELETE commands on the relation will "
								   "error out during create_distributed_table_concurrently unless "
								   "there is a REPLICA IDENTITY or PRIMARY KEY. "
								   "INSERT commands will still work.")));
	}
}


/*
 * HashSplitPointsForShardList returns a list of split points which match
 * the shard ranges of the given list of shards;
 */
static List *
HashSplitPointsForShardList(List *shardList)
{
	List *splitPointList = NIL;

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardList)
	{
		int32 shardMaxValue = DatumGetInt32(shardInterval->maxValue);

		splitPointList = lappend_int(splitPointList, shardMaxValue);
	}

	/*
	 * Split point lists only include the upper boundaries.
	 */
	splitPointList = list_delete_last(splitPointList);

	return splitPointList;
}


/*
 * HashSplitPointsForShardCount returns a list of split points for a given
 * shard count with roughly equal hash ranges.
 */
static List *
HashSplitPointsForShardCount(int shardCount)
{
	List *splitPointList = NIL;

	/* calculate the split of the hash space */
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

	/*
	 * Split points lists only include the upper boundaries, so we only
	 * go up to shardCount - 1 and do not have to apply the correction
	 * for the last shardmaxvalue.
	 */
	for (int64 shardIndex = 0; shardIndex < shardCount - 1; shardIndex++)
	{
		/* initialize the hash token space for this shard */
		int32 shardMinValue = PG_INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxValue = shardMinValue + (hashTokenIncrement - 1);

		splitPointList = lappend_int(splitPointList, shardMaxValue);
	}

	return splitPointList;
}


/*
 * WorkerNodesForShardList returns a list of node ids reflecting the locations of
 * the given list of shards.
 */
static List *
WorkerNodesForShardList(List *shardList)
{
	List *nodeIdList = NIL;

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardList)
	{
		WorkerNode *workerNode = ActiveShardPlacementWorkerNode(shardInterval->shardId);
		nodeIdList = lappend_int(nodeIdList, workerNode->nodeId);
	}

	return nodeIdList;
}


/*
 * RoundRobinWorkerNodeList round robins over the workers in the worker node list
 * and adds node ids to a list of length listLength.
 */
static List *
RoundRobinWorkerNodeList(List *workerNodeList, int listLength)
{
	Assert(workerNodeList != NIL);

	List *nodeIdList = NIL;

	for (int idx = 0; idx < listLength; idx++)
	{
		int nodeIdx = idx % list_length(workerNodeList);
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, nodeIdx);
		nodeIdList = lappend_int(nodeIdList, workerNode->nodeId);
	}

	return nodeIdList;
}


/*
 * create_reference_table creates a distributed table with the given relationId. The
 * created table has one shard and replication factor is set to the active worker
 * count. In fact, the above is the definition of a reference table in Citus.
 */
Datum
create_reference_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	Oid relationId = PG_GETARG_OID(0);

	CreateReferenceTable(relationId);
	PG_RETURN_VOID();
}


/*
 * EnsureCitusTableCanBeCreated checks if
 * - we are on the coordinator
 * - the current user is the owner of the table
 * - relation kind is supported
 * - relation is not a shard
 */
static void
EnsureCitusTableCanBeCreated(Oid relationOid)
{
	EnsureCoordinator();
	EnsureRelationExists(relationOid);
	EnsureTableOwner(relationOid);
	ErrorIfTemporaryTable(relationOid);
	ErrorIfForeignTable(relationOid);

	/*
	 * We should do this check here since the codes in the following lines rely
	 * on this relation to have a supported relation kind. More extensive checks
	 * will be performed in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationOid);

	/*
	 * When coordinator is added to the metadata, or on the workers,
	 * some of the relations of the coordinator node may/will be shards.
	 * We disallow creating distributed tables from shard relations, by
	 * erroring out here.
	 */
	ErrorIfRelationIsAKnownShard(relationOid);
}


/*
 * EnsureRelationExists does a basic check on whether the OID belongs to
 * an existing relation.
 */
void
EnsureRelationExists(Oid relationId)
{
	if (!RelationExists(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation with OID %d does not exist",
							   relationId)));
	}
}


/*
 * CreateReferenceTable is a wrapper around CreateCitusTable that creates a
 * distributed table.
 */
void
CreateDistributedTable(Oid relationId, char *distributionColumnName,
					   char distributionMethod,
					   int shardCount, bool shardCountIsStrict,
					   char *colocateWithTableName)
{
	CitusTableType tableType;
	switch (distributionMethod)
	{
		case DISTRIBUTE_BY_HASH:
		{
			tableType = HASH_DISTRIBUTED;
			break;
		}

		case DISTRIBUTE_BY_APPEND:
		{
			tableType = APPEND_DISTRIBUTED;
			break;
		}

		case DISTRIBUTE_BY_RANGE:
		{
			tableType = RANGE_DISTRIBUTED;
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unexpected distribution method when "
								   "deciding Citus table type")));
			break;
		}
	}

	DistributedTableParams distributedTableParams = {
		.colocationParam = {
			.colocateWithTableName = colocateWithTableName,
			.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT
		},
		.shardCount = shardCount,
		.shardCountIsStrict = shardCountIsStrict,
		.distributionColumnName = distributionColumnName
	};
	CreateCitusTable(relationId, tableType, &distributedTableParams);
}


/*
 * CreateReferenceTable is a wrapper around CreateCitusTable that creates a
 * reference table.
 */
void
CreateReferenceTable(Oid relationId)
{
	CreateCitusTable(relationId, REFERENCE_TABLE, NULL);
}


/*
 * CreateSingleShardTable is a wrapper around CreateCitusTable that creates a
 * single shard distributed table that doesn't have a shard key.
 */
void
CreateSingleShardTable(Oid relationId, ColocationParam colocationParam)
{
	DistributedTableParams distributedTableParams = {
		.colocationParam = colocationParam,
		.shardCount = 1,
		.shardCountIsStrict = true,
		.distributionColumnName = NULL
	};
	CreateCitusTable(relationId, SINGLE_SHARD_DISTRIBUTED, &distributedTableParams);
}


/*
 * CreateCitusTable is the internal method that creates a Citus table in
 * given configuration.
 *
 * DistributedTableParams should be non-null only if we're creating a distributed
 * table.
 *
 * This functions contains all necessary logic to create distributed tables. It
 * performs necessary checks to ensure distributing the table is safe. If it is
 * safe to distribute the table, this function creates distributed table metadata,
 * creates shards and copies local data to shards. This function also handles
 * partitioned tables by distributing its partitions as well.
 */
static void
CreateCitusTable(Oid relationId, CitusTableType tableType,
				 DistributedTableParams *distributedTableParams)
{
	if ((tableType == HASH_DISTRIBUTED || tableType == APPEND_DISTRIBUTED ||
		 tableType == RANGE_DISTRIBUTED || tableType == SINGLE_SHARD_DISTRIBUTED) !=
		(distributedTableParams != NULL))
	{
		ereport(ERROR, (errmsg("distributed table params must be provided "
							   "when creating a distributed table and must "
							   "not be otherwise")));
	}

	EnsureCitusTableCanBeCreated(relationId);

	/* allow creating a Citus table on an empty cluster */
	InsertCoordinatorIfClusterEmpty();

	Relation relation = try_relation_open(relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("could not create Citus table: "
							   "relation does not exist")));
	}

	relation_close(relation, NoLock);

	if (tableType == SINGLE_SHARD_DISTRIBUTED && ShardReplicationFactor > 1)
	{
		ereport(ERROR, (errmsg("could not create single shard table: "
							   "citus.shard_replication_factor is greater than 1"),
						errhint("Consider setting citus.shard_replication_factor to 1 "
								"and try again")));
	}

	/*
	 * EnsureTableNotDistributed() errors out when relation is a Citus table.
	 *
	 * For this reason, we either undistribute the Citus Local table first
	 * and then follow the usual code-path to create distributed table; or
	 * we simply move / replicate its shard to create a single-shard table /
	 * reference table, and then we update the metadata accordingly.
	 *
	 * If we're about it to undistribute it (because we will create a distributed
	 * table soon), then we first drop foreign keys that given relation is
	 * involved because UndistributeTable() does not support undistributing
	 * relations involved in foreign key relationships. At the end of this
	 * function, we then re-create the dropped foreign keys.
	 */
	List *originalForeignKeyRecreationCommands = NIL;
	if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		if (tableType == REFERENCE_TABLE || tableType == SINGLE_SHARD_DISTRIBUTED)
		{
			ConvertCitusLocalTableToTableType(relationId, tableType,
											  distributedTableParams);
			return;
		}
		else
		{
			/* store foreign key creation commands that relation is involved */
			originalForeignKeyRecreationCommands =
				GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																	 INCLUDE_ALL_TABLE_TYPES);
			relationId = DropFKeysAndUndistributeTable(relationId);
		}
	}
	/*
	 * To support foreign keys between reference tables and local tables,
	 * we drop & re-define foreign keys at the end of this function so
	 * that ALTER TABLE hook does the necessary job, which means converting
	 * local tables to citus local tables to properly support such foreign
	 * keys.
	 */
	else if (tableType == REFERENCE_TABLE &&
			 ShouldEnableLocalReferenceForeignKeys() &&
			 HasForeignKeyWithLocalTable(relationId))
	{
		/*
		 * Store foreign key creation commands for foreign key relationships
		 * that relation has with postgres tables.
		 */
		originalForeignKeyRecreationCommands =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_LOCAL_TABLES);

		/*
		 * Soon we will convert local tables to citus local tables. As
		 * CreateCitusLocalTable needs to use local execution, now we
		 * switch to local execution beforehand so that reference table
		 * creation doesn't use remote execution and we don't error out
		 * in CreateCitusLocalTable.
		 */
		SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);

		DropFKeysRelationInvolvedWithTableType(relationId, INCLUDE_LOCAL_TABLES);
	}

	LockRelationOid(relationId, ExclusiveLock);

	EnsureTableNotDistributed(relationId);

	PropagatePrerequisiteObjectsForDistributedTable(relationId);

	Var *distributionColumn = NULL;
	if (distributedTableParams && distributedTableParams->distributionColumnName)
	{
		distributionColumn = BuildDistributionKeyFromColumnName(relationId,
																distributedTableParams->
																distributionColumnName,
																NoLock);
	}

	CitusTableParams citusTableParams = DecideCitusTableParams(tableType,
															   distributedTableParams);

	/*
	 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
	 * our caller already acquired lock on relationId.
	 */
	uint32 colocationId = INVALID_COLOCATION_ID;
	if (distributedTableParams &&
		distributedTableParams->colocationParam.colocationParamType ==
		COLOCATE_WITH_COLOCATION_ID)
	{
		colocationId = distributedTableParams->colocationParam.colocationId;
	}
	else
	{
		/*
		 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
		 * our caller already acquired lock on relationId.
		 */
		colocationId = ColocationIdForNewTable(relationId, tableType,
											   distributedTableParams,
											   distributionColumn);
	}

	EnsureRelationCanBeDistributed(relationId, distributionColumn,
								   citusTableParams.distributionMethod,
								   colocationId, citusTableParams.replicationModel);

	/*
	 * Make sure that existing reference tables have been replicated to all the nodes
	 * such that we can create foreign keys and joins work immediately after creation.
	 *
	 * This will take a lock on the nodes to make sure no nodes are added after we have
	 * verified and ensured the reference tables are copied everywhere.
	 * Although copying reference tables here for anything but creating a new colocation
	 * group, it requires significant refactoring which we don't want to perform now.
	 */
	EnsureReferenceTablesExistOnAllNodes();

	/*
	 * While adding tables to a colocation group we need to make sure no concurrent
	 * mutations happen on the colocation group with regards to its placements. It is
	 * important that we have already copied any reference tables before acquiring this
	 * lock as these are competing operations.
	 */
	LockColocationId(colocationId, ShareLock);

	/* we need to calculate these variables before creating distributed metadata */
	bool localTableEmpty = TableEmpty(relationId);
	Oid colocatedTableId = ColocatedTableId(colocationId);

	/* setting to false since this flag is only valid for citus local tables */
	bool autoConverted = false;

	/* create an entry for distributed table in pg_dist_partition */
	InsertIntoPgDistPartition(relationId, citusTableParams.distributionMethod,
							  distributionColumn,
							  colocationId, citusTableParams.replicationModel,
							  autoConverted);

	/* foreign tables do not support TRUNCATE trigger */
	if (RegularTable(relationId))
	{
		CreateTruncateTrigger(relationId);
	}

	/* create shards for hash distributed and reference tables */
	if (tableType == HASH_DISTRIBUTED)
	{
		CreateHashDistributedTableShards(relationId, distributedTableParams->shardCount,
										 colocatedTableId,
										 localTableEmpty);
	}
	else if (tableType == REFERENCE_TABLE)
	{
		CreateReferenceTableShard(relationId);
	}
	else if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		CreateSingleShardTableShard(relationId, colocatedTableId,
									colocationId);
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		SyncCitusTableMetadata(relationId);
	}

	/*
	 * We've a custom way of foreign key graph invalidation,
	 * see InvalidateForeignKeyGraph().
	 */
	if (TableReferenced(relationId) || TableReferencing(relationId))
	{
		InvalidateForeignKeyGraph();
	}

	/* if this table is partitioned table, distribute its partitions too */
	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);
		Oid partitionRelationId = InvalidOid;
		Oid namespaceId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(namespaceId);
		char *relationName = get_rel_name(relationId);
		char *parentRelationName = quote_qualified_identifier(schemaName, relationName);

		/*
		 * when there are many partitions, each call to CreateDistributedTable
		 * accumulates used memory. Create and free context for each call.
		 */
		MemoryContext citusPartitionContext =
			AllocSetContextCreate(CurrentMemoryContext,
								  "citus_per_partition_context",
								  ALLOCSET_DEFAULT_SIZES);
		MemoryContext oldContext = MemoryContextSwitchTo(citusPartitionContext);

		foreach_oid(partitionRelationId, partitionList)
		{
			MemoryContextReset(citusPartitionContext);

			DistributedTableParams childDistributedTableParams = {
				.colocationParam = {
					.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
					.colocateWithTableName = parentRelationName,
				},
				.shardCount = distributedTableParams->shardCount,
				.shardCountIsStrict = false,
				.distributionColumnName = distributedTableParams->distributionColumnName,
			};
			CreateCitusTable(partitionRelationId, tableType,
							 &childDistributedTableParams);
		}

		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(citusPartitionContext);
	}

	/* copy over data for hash distributed and reference tables */
	if (tableType == HASH_DISTRIBUTED || tableType == SINGLE_SHARD_DISTRIBUTED ||
		tableType == REFERENCE_TABLE)
	{
		if (RegularTable(relationId))
		{
			CopyLocalDataIntoShards(relationId);
		}
	}

	/*
	 * Now recreate foreign keys that we dropped beforehand. As modifications are not
	 * allowed on the relations that are involved in the foreign key relationship,
	 * we can skip the validation of the foreign keys.
	 */
	bool skip_validation = true;
	ExecuteForeignKeyCreateCommandList(originalForeignKeyRecreationCommands,
									   skip_validation);
}


/*
 * ConvertCitusLocalTableToTableType converts given Citus local table to
 * given table type.
 *
 * This only supports converting Citus local tables to reference tables
 * (by replicating the shard to workers) and single-shard distributed
 * tables (by moving the shard to appropriate worker).
 */
static void
ConvertCitusLocalTableToTableType(Oid relationId, CitusTableType tableType,
								  DistributedTableParams *distributedTableParams)
{
	if (!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("table is not a local table added to metadata")));
	}

	if (tableType != REFERENCE_TABLE && tableType != SINGLE_SHARD_DISTRIBUTED)
	{
		ereport(ERROR, (errmsg("table type is not supported for conversion")));
	}

	LockRelationOid(relationId, ExclusiveLock);

	Var *distributionColumn = NULL;
	CitusTableParams citusTableParams = DecideCitusTableParams(tableType,
															   distributedTableParams);

	uint32 colocationId = INVALID_COLOCATION_ID;
	if (distributedTableParams &&
		distributedTableParams->colocationParam.colocationParamType ==
		COLOCATE_WITH_COLOCATION_ID)
	{
		colocationId = distributedTableParams->colocationParam.colocationId;
	}
	else
	{
		colocationId = ColocationIdForNewTable(relationId, tableType,
											   distributedTableParams,
											   distributionColumn);
	}

	/* check constraints etc. on table based on new distribution params */
	EnsureRelationCanBeDistributed(relationId, distributionColumn,
								   citusTableParams.distributionMethod,
								   colocationId, citusTableParams.replicationModel);

	/*
	 * Regarding the foreign key relationships that given relation is involved,
	 * EnsureRelationCanBeDistributed() only checks the ones where the relation is
	 * the referencing table.
	 *
	 * And given that the table at hand is a Citus local table, right now it may
	 * only be referenced by a reference table or a Citus local table.
	 *
	 * However, given that neither of those two cases are not applicable for a
	 * distributed table, here we throw an error assuming that the referencing
	 * relation is a reference table or a Citus local table.
	 *
	 * While doing so, we use the same error message used in
	 * ErrorIfUnsupportedForeignConstraintExists(), which is eventually called
	 * by EnsureRelationCanBeDistributed().
	 *
	 * Note that we don't need to check the same if we're creating a reference
	 * table from a Citus local table because all the foreign keys referencing
	 * Citus local tables are supported by reference tables.
	 */
	if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		int fkeyFlags = (INCLUDE_REFERENCED_CONSTRAINTS | EXCLUDE_SELF_REFERENCES |
						 INCLUDE_ALL_TABLE_TYPES);
		List *externalReferencedFkeyIds = GetForeignKeyOids(relationId, fkeyFlags);
		if (list_length(externalReferencedFkeyIds) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint "
								   "since foreign keys from reference tables "
								   "and local tables to distributed tables "
								   "are not supported"),
							errdetail("Reference tables and local tables "
									  "can only have foreign keys to reference "
									  "tables and local tables")));
		}
	}

	EnsureReferenceTablesExistOnAllNodes();

	LockColocationId(colocationId, ShareLock);

	int64 shardId = NoneDistTableGetShardId(relationId);
	WorkerNode *sourceNode = CoordinatorNodeIfAddedAsWorkerOrError();

	if (tableType == SINGLE_SHARD_DISTRIBUTED)
	{
		uint32 targetNodeId = SingleShardTableColocationNodeId(colocationId);
		if (targetNodeId != sourceNode->nodeId)
		{
			bool missingOk = false;
			WorkerNode *targetNode = FindNodeWithNodeId(targetNodeId, missingOk);

			TransferCitusLocalTableShardInXact(shardId, sourceNode->workerName,
											   sourceNode->workerPort,
											   targetNode->workerName,
											   targetNode->workerPort,
											   SHARD_TRANSFER_MOVE);
		}
	}
	else if (tableType == REFERENCE_TABLE)
	{
		List *nodeList = ActivePrimaryNonCoordinatorNodeList(ShareLock);
		nodeList = SortList(nodeList, CompareWorkerNodes);

		WorkerNode *targetNode = NULL;
		foreach_ptr(targetNode, nodeList)
		{
			TransferCitusLocalTableShardInXact(shardId, sourceNode->workerName,
											   sourceNode->workerPort,
											   targetNode->workerName,
											   targetNode->workerPort,
											   SHARD_TRANSFER_COPY);
		}
	}

	bool autoConverted = false;
	UpdateNoneDistTableMetadataGlobally(
		relationId, citusTableParams.replicationModel,
		colocationId, autoConverted);

	/*
	 * TransferCitusLocalTableShardInXact() moves / copies partition shards
	 * to the target node too, but we still need to update the metadata
	 * for them.
	 */
	if (PartitionedTable(relationId))
	{
		Oid partitionRelationId = InvalidOid;
		List *partitionList = PartitionList(relationId);
		foreach_oid(partitionRelationId, partitionList)
		{
			UpdateNoneDistTableMetadataGlobally(
				partitionRelationId, citusTableParams.replicationModel,
				colocationId, autoConverted);
		}
	}
}


/*
 * SingleShardTableColocationNodeId takes a colocation id that is known to be
 * used / going be used to colocate a set of single-shard tables and returns
 * id of the node that should store the shards of those tables.
 */
static uint32
SingleShardTableColocationNodeId(uint32 colocationId)
{
	List *tablesInColocationGroup = ColocationGroupTableList(colocationId, 1);
	if (list_length(tablesInColocationGroup) == 0)
	{
		int workerNodeIndex =
			EmptySingleShardTableColocationDecideNodeId(colocationId);
		List *workerNodeList = DistributedTablePlacementNodeList(RowShareLock);
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);

		return workerNode->nodeId;
	}
	else
	{
		Oid colocatedTableId = linitial_oid(tablesInColocationGroup);
		return SingleShardTableGetNodeId(colocatedTableId);
	}
}


/*
 * SingleShardTableGetNodeId returns id of the node that stores shard of
 * given single-shard table.
 */
static uint32
SingleShardTableGetNodeId(Oid relationId)
{
	int64 shardId = NoneDistTableGetShardId(relationId);

	List *shardPlacementList = ShardPlacementList(shardId);
	if (list_length(shardPlacementList) != 1)
	{
		ereport(ERROR, (errmsg("table shard does not have a single shard placement")));
	}

	return ((ShardPlacement *) linitial(shardPlacementList))->nodeId;
}


/*
 * NoneDistTableGetShardId returns shard id of given table that is known
 * to be a none-distriubted table.
 */
static int64
NoneDistTableGetShardId(Oid relationId)
{
	if (HasDistributionKey(relationId))
	{
		ereport(ERROR, (errmsg("table is not a none-distributed table")));
	}

	List *shardIntervalList = LoadShardIntervalList(relationId);
	return ((ShardInterval *) linitial(shardIntervalList))->shardId;
}


/*
 * DecideCitusTableParams decides CitusTableParams based on given CitusTableType
 * and DistributedTableParams if it's a distributed table.
 *
 * DistributedTableParams should be non-null only if CitusTableType corresponds
 * to a distributed table.
 */
static
CitusTableParams
DecideCitusTableParams(CitusTableType tableType,
					   DistributedTableParams *distributedTableParams)
{
	CitusTableParams citusTableParams = { 0 };
	switch (tableType)
	{
		case HASH_DISTRIBUTED:
		{
			Assert(distributedTableParams->colocationParam.colocationParamType ==
				   COLOCATE_WITH_TABLE_LIKE_OPT);

			citusTableParams.distributionMethod = DISTRIBUTE_BY_HASH;
			citusTableParams.replicationModel =
				DecideDistTableReplicationModel(DISTRIBUTE_BY_HASH,
												distributedTableParams->colocationParam.
												colocateWithTableName);
			break;
		}

		case APPEND_DISTRIBUTED:
		{
			Assert(distributedTableParams->colocationParam.colocationParamType ==
				   COLOCATE_WITH_TABLE_LIKE_OPT);

			citusTableParams.distributionMethod = DISTRIBUTE_BY_APPEND;
			citusTableParams.replicationModel =
				DecideDistTableReplicationModel(APPEND_DISTRIBUTED,
												distributedTableParams->colocationParam.
												colocateWithTableName);
			break;
		}

		case RANGE_DISTRIBUTED:
		{
			Assert(distributedTableParams->colocationParam.colocationParamType ==
				   COLOCATE_WITH_TABLE_LIKE_OPT);

			citusTableParams.distributionMethod = DISTRIBUTE_BY_RANGE;
			citusTableParams.replicationModel =
				DecideDistTableReplicationModel(RANGE_DISTRIBUTED,
												distributedTableParams->colocationParam.
												colocateWithTableName);
			break;
		}

		case SINGLE_SHARD_DISTRIBUTED:
		{
			citusTableParams.distributionMethod = DISTRIBUTE_BY_NONE;
			citusTableParams.replicationModel = REPLICATION_MODEL_STREAMING;
			break;
		}

		case REFERENCE_TABLE:
		{
			citusTableParams.distributionMethod = DISTRIBUTE_BY_NONE;
			citusTableParams.replicationModel = REPLICATION_MODEL_2PC;
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unexpected table type when deciding Citus "
								   "table params")));
			break;
		}
	}

	return citusTableParams;
}


/*
 * PropagatePrerequisiteObjectsForDistributedTable ensures we can create shards
 * on all nodes by ensuring all dependent objects exist on all node.
 */
static void
PropagatePrerequisiteObjectsForDistributedTable(Oid relationId)
{
	/*
	 * Ensure that the sequences used in column defaults of the table
	 * have proper types
	 */
	EnsureRelationHasCompatibleSequenceTypes(relationId);

	/*
	 * distributed tables might have dependencies on different objects, since we create
	 * shards for a distributed table via multiple sessions these objects will be created
	 * via their own connection and committed immediately so they become visible to all
	 * sessions creating shards.
	 */
	ObjectAddress *tableAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*tableAddress, RelationRelationId, relationId);
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(tableAddress));
}


/*
 * EnsureSequenceTypeSupported ensures that the type of the column that uses
 * a sequence on its DEFAULT is consistent with previous uses (if any) of the
 * sequence in distributed tables.
 * If any other distributed table uses the input sequence, it checks whether
 * the types of the columns using the sequence match. If they don't, it errors out.
 * Otherwise, the condition is ensured.
 * Since the owner of the sequence may not distributed yet, it should be added
 * explicitly.
 */
void
EnsureSequenceTypeSupported(Oid seqOid, Oid attributeTypeId, Oid ownerRelationId)
{
	List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
	citusTableIdList = list_append_unique_oid(citusTableIdList, ownerRelationId);

	Oid citusTableId = InvalidOid;
	foreach_oid(citusTableId, citusTableIdList)
	{
		List *seqInfoList = NIL;
		GetDependentSequencesWithRelation(citusTableId, &seqInfoList, 0, DEPENDENCY_AUTO);

		SequenceInfo *seqInfo = NULL;
		foreach_ptr(seqInfo, seqInfoList)
		{
			AttrNumber currentAttnum = seqInfo->attributeNumber;
			Oid currentSeqOid = seqInfo->sequenceOid;

			if (!seqInfo->isNextValDefault)
			{
				/*
				 * If a sequence is not on the nextval, we don't need any check.
				 * This is a dependent sequence via ALTER SEQUENCE .. OWNED BY col
				 */
				continue;
			}

			/*
			 * If another distributed table is using the same sequence
			 * in one of its column defaults, make sure the types of the
			 * columns match
			 */
			if (currentSeqOid == seqOid)
			{
				Oid currentAttributeTypId = GetAttributeTypeOid(citusTableId,
																currentAttnum);
				if (attributeTypeId != currentAttributeTypId)
				{
					char *sequenceName = generate_qualified_relation_name(
						seqOid);
					char *citusTableName =
						generate_qualified_relation_name(citusTableId);
					ereport(ERROR, (errmsg(
										"The sequence %s is already used for a different"
										" type in column %d of the table %s",
										sequenceName, currentAttnum,
										citusTableName)));
				}
			}
		}
	}
}


/*
 * AlterSequenceType alters the given sequence's type to the given type.
 */
void
AlterSequenceType(Oid seqOid, Oid typeOid)
{
	Form_pg_sequence sequenceData = pg_get_sequencedef(seqOid);
	Oid currentSequenceTypeOid = sequenceData->seqtypid;
	if (currentSequenceTypeOid != typeOid)
	{
		AlterSeqStmt *alterSequenceStatement = makeNode(AlterSeqStmt);
		char *seqNamespace = get_namespace_name(get_rel_namespace(seqOid));
		char *seqName = get_rel_name(seqOid);
		alterSequenceStatement->sequence = makeRangeVar(seqNamespace, seqName, -1);
		Node *asTypeNode = (Node *) makeTypeNameFromOid(typeOid, -1);
		SetDefElemArg(alterSequenceStatement, "as", asTypeNode);
		ParseState *pstate = make_parsestate(NULL);
		AlterSequence(pstate, alterSequenceStatement);
		CommandCounterIncrement();
	}
}


/*
 * EnsureRelationHasCompatibleSequenceTypes ensures that sequences used for columns
 * of the table have compatible types both with the column type on that table and
 * all other distributed tables' columns they have used for
 */
void
EnsureRelationHasCompatibleSequenceTypes(Oid relationId)
{
	List *seqInfoList = NIL;

	GetDependentSequencesWithRelation(relationId, &seqInfoList, 0, DEPENDENCY_AUTO);
	EnsureDistributedSequencesHaveOneType(relationId, seqInfoList);
}


/*
 * EnsureDistributedSequencesHaveOneType first ensures that the type of the column
 * in which the sequence is used as default is supported for each sequence in input
 * dependentSequenceList, and then alters the sequence type if not the same with the column type.
 */
static void
EnsureDistributedSequencesHaveOneType(Oid relationId, List *seqInfoList)
{
	SequenceInfo *seqInfo = NULL;
	foreach_ptr(seqInfo, seqInfoList)
	{
		if (!seqInfo->isNextValDefault)
		{
			/*
			 * If a sequence is not on the nextval, we don't need any check.
			 * This is a dependent sequence via ALTER SEQUENCE .. OWNED BY col
			 */
			continue;
		}

		/*
		 * We should make sure that the type of the column that uses
		 * that sequence is supported
		 */
		Oid sequenceOid = seqInfo->sequenceOid;
		AttrNumber attnum = seqInfo->attributeNumber;
		Oid attributeTypeId = GetAttributeTypeOid(relationId, attnum);
		EnsureSequenceTypeSupported(sequenceOid, attributeTypeId, relationId);

		/*
		 * Alter the sequence's data type in the coordinator if needed.
		 *
		 * First, we should only change the sequence type if the column
		 * is a supported sequence type. For example, if a sequence is used
		 * in an expression which then becomes a text, we should not try to
		 * alter the sequence type to text. Postgres only supports int2, int4
		 * and int8 as the sequence type.
		 *
		 * A sequence's type is bigint by default and it doesn't change even if
		 * it's used in an int column. We should change the type if needed,
		 * and not allow future ALTER SEQUENCE ... TYPE ... commands for
		 * sequences used as defaults in distributed tables.
		 */
		if (attributeTypeId == INT2OID ||
			attributeTypeId == INT4OID ||
			attributeTypeId == INT8OID)
		{
			AlterSequenceType(sequenceOid, attributeTypeId);
		}
	}
}


/*
 * DecideDistTableReplicationModel function decides which replication model should be
 * used for a distributed table depending on given distribution configuration.
 */
static char
DecideDistTableReplicationModel(char distributionMethod, char *colocateWithTableName)
{
	Assert(distributionMethod != DISTRIBUTE_BY_NONE);

	if (!IsColocateWithDefault(colocateWithTableName) &&
		!IsColocateWithNone(colocateWithTableName))
	{
		text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
		Oid colocatedRelationId = ResolveRelationId(colocateWithTableNameText, false);
		CitusTableCacheEntry *targetTableEntry = GetCitusTableCacheEntry(
			colocatedRelationId);
		char replicationModel = targetTableEntry->replicationModel;

		return replicationModel;
	}
	else if (distributionMethod == DISTRIBUTE_BY_HASH &&
			 !DistributedTableReplicationIsEnabled())
	{
		return REPLICATION_MODEL_STREAMING;
	}
	else
	{
		return REPLICATION_MODEL_COORDINATOR;
	}

	/* we should not reach to this point */
	return REPLICATION_MODEL_INVALID;
}


/*
 * CreateHashDistributedTableShards creates shards of given hash distributed table.
 */
static void
CreateHashDistributedTableShards(Oid relationId, int shardCount,
								 Oid colocatedTableId, bool localTableEmpty)
{
	bool useExclusiveConnection = false;

	/*
	 * Decide whether to use exclusive connections per placement or not. Note that
	 * if the local table is not empty, we cannot use sequential mode since the COPY
	 * operation that'd load the data into shards currently requires exclusive
	 * connections.
	 */
	if (RegularTable(relationId))
	{
		useExclusiveConnection = CanUseExclusiveConnections(relationId,
															localTableEmpty);
	}

	if (colocatedTableId != InvalidOid)
	{
		/*
		 * We currently allow concurrent distribution of colocated tables (which
		 * we probably should not be allowing because of foreign keys /
		 * partitioning etc).
		 *
		 * We also prevent concurrent shard moves / copy / splits) while creating
		 * a colocated table.
		 */
		AcquirePlacementColocationLock(colocatedTableId, ShareLock,
									   "colocate distributed table");

		CreateColocatedShards(relationId, colocatedTableId, useExclusiveConnection);
	}
	else
	{
		/*
		 * This path is only reached by create_distributed_table for the distributed
		 * tables which will not be part of an existing colocation group. Therefore,
		 * we can directly use ShardReplicationFactor global variable here.
		 */
		CreateShardsWithRoundRobinPolicy(relationId, shardCount, ShardReplicationFactor,
										 useExclusiveConnection);
	}
}


/*
 * CreateHashDistributedTableShards creates the shard of given single-shard
 * distributed table.
 */
static void
CreateSingleShardTableShard(Oid relationId, Oid colocatedTableId,
							uint32 colocationId)
{
	if (colocatedTableId != InvalidOid)
	{
		/*
		 * We currently allow concurrent distribution of colocated tables (which
		 * we probably should not be allowing because of foreign keys /
		 * partitioning etc).
		 *
		 * We also prevent concurrent shard moves / copy / splits) while creating
		 * a colocated table.
		 */
		AcquirePlacementColocationLock(colocatedTableId, ShareLock,
									   "colocate distributed table");

		/*
		 * We don't need to force using exclusive connections because we're anyway
		 * creating a single shard.
		 */
		bool useExclusiveConnection = false;
		CreateColocatedShards(relationId, colocatedTableId, useExclusiveConnection);
	}
	else
	{
		CreateSingleShardTableShardWithRoundRobinPolicy(relationId, colocationId);
	}
}


/*
 * ColocationIdForNewTable returns a colocation id for given table
 * according to given configuration. If there is no such configuration, it
 * creates one and returns colocation id of newly the created colocation group.
 * Note that DistributedTableParams and the distribution column Var should be
 * non-null only if CitusTableType corresponds to a distributed table.
 *
 * For append and range distributed tables, this function errors out if
 * colocateWithTableName parameter is not NULL, otherwise directly returns
 * INVALID_COLOCATION_ID.
 *
 * For reference tables, returns the common reference table colocation id.
 *
 * This function assumes its caller take necessary lock on relationId to
 * prevent possible changes on it.
 */
static uint32
ColocationIdForNewTable(Oid relationId, CitusTableType tableType,
						DistributedTableParams *distributedTableParams,
						Var *distributionColumn)
{
	CitusTableParams citusTableParams = DecideCitusTableParams(tableType,
															   distributedTableParams);

	uint32 colocationId = INVALID_COLOCATION_ID;

	if (tableType == APPEND_DISTRIBUTED || tableType == RANGE_DISTRIBUTED)
	{
		Assert(distributedTableParams->colocationParam.colocationParamType ==
			   COLOCATE_WITH_TABLE_LIKE_OPT);
		char *colocateWithTableName =
			distributedTableParams->colocationParam.colocateWithTableName;
		if (!IsColocateWithDefault(colocateWithTableName))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot distribute relation"),
							errdetail("Currently, colocate_with option is not supported "
									  "for append / range distributed tables.")));
		}

		return colocationId;
	}
	else if (tableType == REFERENCE_TABLE)
	{
		return CreateReferenceTableColocationId();
	}
	else
	{
		/*
		 * Get an exclusive lock on the colocation system catalog. Therefore, we
		 * can be sure that there will no modifications on the colocation table
		 * until this transaction is committed.
		 */

		Oid distributionColumnType =
			distributionColumn ? distributionColumn->vartype : InvalidOid;
		Oid distributionColumnCollation =
			distributionColumn ? get_typcollation(distributionColumnType) : InvalidOid;

		Assert(distributedTableParams->colocationParam.colocationParamType ==
			   COLOCATE_WITH_TABLE_LIKE_OPT);
		char *colocateWithTableName =
			distributedTableParams->colocationParam.colocateWithTableName;

		/* get an advisory lock to serialize concurrent default group creations */
		if (IsColocateWithDefault(colocateWithTableName))
		{
			AcquireColocationDefaultLock();
		}

		colocationId = FindColocateWithColocationId(relationId,
													citusTableParams.replicationModel,
													distributionColumnType,
													distributionColumnCollation,
													distributedTableParams->shardCount,
													distributedTableParams->
													shardCountIsStrict,
													colocateWithTableName);

		if (IsColocateWithDefault(colocateWithTableName) &&
			(colocationId != INVALID_COLOCATION_ID))
		{
			/*
			 * we can release advisory lock if there is already a default entry for given params;
			 * else, we should keep it to prevent different default coloc entry creation by
			 * concurrent operations.
			 */
			ReleaseColocationDefaultLock();
		}

		if (colocationId == INVALID_COLOCATION_ID)
		{
			if (IsColocateWithDefault(colocateWithTableName))
			{
				/*
				 * Generate a new colocation ID and insert a pg_dist_colocation
				 * record.
				 */
				colocationId = CreateColocationGroup(distributedTableParams->shardCount,
													 ShardReplicationFactor,
													 distributionColumnType,
													 distributionColumnCollation);
			}
			else if (IsColocateWithNone(colocateWithTableName))
			{
				/*
				 * Generate a new colocation ID and insert a pg_dist_colocation
				 * record.
				 */
				colocationId = CreateColocationGroup(distributedTableParams->shardCount,
													 ShardReplicationFactor,
													 distributionColumnType,
													 distributionColumnCollation);
			}
		}
	}

	return colocationId;
}


/*
 * EnsureRelationCanBeDistributed checks whether Citus can safely distribute given
 * relation with the given configuration. We perform almost all safety checks for
 * distributing table here. If there is an unsatisfied requirement, we error out
 * and do not distribute the table.
 *
 * This function assumes, callers have already acquired necessary locks to ensure
 * there will not be any change in the given relation.
 */
static void
EnsureRelationCanBeDistributed(Oid relationId, Var *distributionColumn,
							   char distributionMethod, uint32 colocationId,
							   char replicationModel)
{
	Oid parentRelationId = InvalidOid;

	EnsureLocalTableEmptyIfNecessary(relationId, distributionMethod);

	/* user really wants triggers? */
	if (EnableUnsafeTriggers)
	{
		ErrorIfRelationHasUnsupportedTrigger(relationId);
	}
	else
	{
		EnsureRelationHasNoTriggers(relationId);
	}

	/* we assume callers took necessary locks */
	Relation relation = relation_open(relationId, NoLock);
	TupleDesc relationDesc = RelationGetDescr(relation);
	char *relationName = RelationGetRelationName(relation);

	ErrorIfTableIsACatalogTable(relation);


	/* verify target relation is not distributed by a generated stored column
	 */
	if (distributionMethod != DISTRIBUTE_BY_NONE &&
		DistributionColumnUsesGeneratedStoredColumn(relationDesc, distributionColumn))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distribution column must not use GENERATED ALWAYS "
								  "AS (...) STORED.")));
	}

#if (PG_VERSION_NUM >= PG_VERSION_15)

	/* verify target relation is not distributed by a column of type numeric with negative scale */
	if (distributionMethod != DISTRIBUTE_BY_NONE &&
		DistributionColumnUsesNumericColumnNegativeScale(relationDesc,
														 distributionColumn))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distribution column must not use numeric type "
								  "with negative scale")));
	}
#endif

	/* check for support function needed by specified partition method */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(distributionColumn,
														   HASH_AM_OID,
														   HASHSTANDARD_PROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(distributionColumn->vartype)),
							errdatatype(distributionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}

		if (distributionColumn->varcollid != InvalidOid &&
			!get_collation_isdeterministic(distributionColumn->varcollid))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Hash distributed partition columns may not use "
								   "a non deterministic collation")));
		}
	}
	else if (distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		Oid btreeSupportFunction = SupportFunctionForColumn(distributionColumn,
															BTREE_AM_OID, BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(distributionColumn->vartype)),
					 errdatatype(distributionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}

	if (PartitionTableNoLock(relationId))
	{
		parentRelationId = PartitionParentOid(relationId);
	}

	/* partitions cannot be distributed if their parent is not distributed */
	if (PartitionTableNoLock(relationId) && !IsCitusTable(parentRelationId))
	{
		char *parentRelationName = get_rel_name(parentRelationId);

		ereport(ERROR, (errmsg("cannot distribute relation \"%s\" which is partition of "
							   "\"%s\"", relationName, parentRelationName),
						errdetail("Citus does not support distributing partitions "
								  "if their parent is not distributed table."),
						errhint("Distribute the partitioned table \"%s\" instead.",
								parentRelationName)));
	}

	/*
	 * These checks are mostly for partitioned tables not partitions because we prevent
	 * distributing partitions directly in the above check. However, partitions can still
	 * reach this point because, we call CreateDistributedTable for partitions if their
	 * parent table is distributed.
	 */
	if (PartitionedTableNoLock(relationId))
	{
		/*
		 * Distributing partitioned tables is only supported for hash-distribution
		 * or single-shard tables.
		 */
		bool isSingleShardTable =
			distributionMethod == DISTRIBUTE_BY_NONE &&
			replicationModel == REPLICATION_MODEL_STREAMING &&
			colocationId != INVALID_COLOCATION_ID;
		if (distributionMethod != DISTRIBUTE_BY_HASH && !isSingleShardTable)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables in only supported "
								   "for hash-distributed tables")));
		}

		/* we don't support distributing tables with multi-level partitioning */
		if (PartitionTableNoLock(relationId))
		{
			char *parentRelationName = get_rel_name(parentRelationId);

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing multi-level partitioned tables "
								   "is not supported"),
							errdetail("Relation \"%s\" is partitioned table itself and "
									  "it is also partition of relation \"%s\".",
									  relationName, parentRelationName)));
		}
	}

	ErrorIfUnsupportedConstraint(relation, distributionMethod, replicationModel,
								 distributionColumn, colocationId);


	ErrorIfUnsupportedPolicy(relation);
	relation_close(relation, NoLock);
}


/*
 * ErrorIfTemporaryTable errors out if the given table is a temporary table.
 */
static void
ErrorIfTemporaryTable(Oid relationId)
{
	if (get_rel_persistence(relationId) == RELPERSISTENCE_TEMP)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute a temporary table")));
	}
}


/*
 * ErrorIfTableIsACatalogTable is a helper function to error out for citus
 * table creation from a catalog table.
 */
void
ErrorIfTableIsACatalogTable(Relation relation)
{
	if (relation->rd_rel->relnamespace != PG_CATALOG_NAMESPACE)
	{
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot create a citus table from a catalog table")));
}


/*
 * EnsureLocalTableEmptyIfNecessary errors out if the function should be empty
 * according to ShouldLocalTableBeEmpty but it is not.
 */
static void
EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod)
{
	if (ShouldLocalTableBeEmpty(relationId, distributionMethod))
	{
		EnsureLocalTableEmpty(relationId);
	}
}


/*
 * ShouldLocalTableBeEmpty returns true if the local table should be empty
 * before creating a citus table.
 * In some cases, it is possible and safe to send local data to shards while
 * distributing the table. In those cases, we can distribute non-empty local
 * tables. This function checks the distributionMethod and relation kind to
 * see whether we need to be ensure emptiness of local table.
 */
static bool
ShouldLocalTableBeEmpty(Oid relationId, char distributionMethod)
{
	bool shouldLocalTableBeEmpty = false;
	if (distributionMethod != DISTRIBUTE_BY_HASH &&
		distributionMethod != DISTRIBUTE_BY_NONE)
	{
		/*
		 * We only support hash distributed tables and reference tables
		 * for initial data loading
		 */
		shouldLocalTableBeEmpty = true;
	}
	else if (!RegularTable(relationId))
	{
		/*
		 * We only support tables and partitioned tables for initial
		 * data loading
		 */
		shouldLocalTableBeEmpty = true;
	}

	return shouldLocalTableBeEmpty;
}


/*
 * EnsureLocalTableEmpty errors out if the local table is not empty.
 */
static void
EnsureLocalTableEmpty(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool localTableEmpty = TableEmpty(relationId);

	if (!localTableEmpty)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot distribute relation \"%s\"", relationName),
						errdetail("Relation \"%s\" contains data.", relationName),
						errhint("Empty your table before distributing it.")));
	}
}


/*
 * EnsureDistributableTable ensures the given table type is appropriate to
 * be distributed. Table type should be regular or citus local table.
 */
static void
EnsureDistributableTable(Oid relationId)
{
	bool isLocalTable = IsCitusTableType(relationId, CITUS_LOCAL_TABLE);
	bool isRegularTable = !IsCitusTableType(relationId, ANY_CITUS_TABLE_TYPE);

	if (!isLocalTable && !isRegularTable)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}
}


/*
 * EnsureTableNotDistributed errors out if the table is distributed.
 */
void
EnsureTableNotDistributed(Oid relationId)
{
	char *relationName = get_rel_name(relationId);

	bool isCitusTable = IsCitusTable(relationId);

	if (isCitusTable)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}
}


/*
 * EnsureRelationHasNoTriggers errors out if the given table has triggers on
 * it. See also GetExplicitTriggerIdList function's comment for the triggers this
 * function errors out.
 */
static void
EnsureRelationHasNoTriggers(Oid relationId)
{
	List *explicitTriggerIds = GetExplicitTriggerIdList(relationId);

	if (list_length(explicitTriggerIds) > 0)
	{
		char *relationName = get_rel_name(relationId);

		Assert(relationName != NULL);
		ereport(ERROR, (errmsg("cannot distribute relation \"%s\" because it "
							   "has triggers", relationName),
						errhint("Consider dropping all the triggers on \"%s\" "
								"and retry.", relationName)));
	}
}


/*
 * LookupDistributionMethod maps the oids of citus.distribution_type enum
 * values to pg_dist_partition.partmethod values.
 *
 * The passed in oid has to belong to a value of citus.distribution_type.
 */
char
LookupDistributionMethod(Oid distributionMethodOid)
{
	char distributionMethod = 0;

	HeapTuple enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(
											  distributionMethodOid));
	if (!HeapTupleIsValid(enumTuple))
	{
		ereport(ERROR, (errmsg("invalid internal value for enum: %u",
							   distributionMethodOid)));
	}

	Form_pg_enum enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	const char *enumLabel = NameStr(enumForm->enumlabel);

	if (strncmp(enumLabel, "append", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_APPEND;
	}
	else if (strncmp(enumLabel, "hash", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_HASH;
	}
	else if (strncmp(enumLabel, "range", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_RANGE;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid label for enum: %s", enumLabel)));
	}

	ReleaseSysCache(enumTuple);

	return distributionMethod;
}


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
static Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	Oid operatorFamilyId = get_opclass_family(operatorClassId);
	Oid operatorClassInputType = get_opclass_input_type(operatorClassId);
	Oid supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
											   operatorClassInputType,
											   supportFunctionNumber);

	return supportFunctionOid;
}


/*
 * TableEmpty function checks whether given table contains any row and
 * returns false if there is any data.
 */
bool
TableEmpty(Oid tableId)
{
	Oid schemaId = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(tableId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);

	StringInfo selectTrueQueryString = makeStringInfo();

	bool readOnly = true;

	int spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	appendStringInfo(selectTrueQueryString, SELECT_TRUE_QUERY, tableQualifiedName);

	int spiQueryResult = SPI_execute(selectTrueQueryString->data, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   selectTrueQueryString->data)));
	}

	/* we expect that SELECT TRUE query will return single value in a single row OR empty set */
	Assert(SPI_processed == 1 || SPI_processed == 0);

	bool localTableEmpty = !SPI_processed;

	SPI_finish();

	return localTableEmpty;
}


/*
 * CanUseExclusiveConnections checks if we can open parallel connections
 * while creating shards. We simply error out if we need to execute
 * sequentially but there is data in the table, since we cannot copy the
 * data to shards sequentially.
 */
static bool
CanUseExclusiveConnections(Oid relationId, bool localTableEmpty)
{
	bool hasForeignKeyToReferenceTable = HasForeignKeyToReferenceTable(relationId);
	bool shouldRunSequential = MultiShardConnectionType == SEQUENTIAL_CONNECTION ||
							   hasForeignKeyToReferenceTable;

	if (shouldRunSequential && ParallelQueryExecutedInTransaction())
	{
		/*
		 * We decided to use sequential execution. It's either because relation
		 * has a pre-existing foreign key to a reference table or because we
		 * decided to use sequential execution due to a query executed in the
		 * current xact beforehand.
		 * We have specific error messages for either cases.
		 */

		char *relationName = get_rel_name(relationId);

		if (hasForeignKeyToReferenceTable)
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg("cannot distribute relation \"%s\" in this "
								   "transaction because it has a foreign key to "
								   "a reference table", relationName),
							errdetail("If a hash distributed table has a foreign key "
									  "to a reference table, it has to be created "
									  "in sequential mode before any parallel commands "
									  "have been executed in the same transaction"),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
		{
			ereport(ERROR, (errmsg("cannot distribute \"%s\" in sequential mode because "
								   "a parallel query was executed in this transaction",
								   relationName),
							errhint("If you have manually set "
									"citus.multi_shard_modify_mode to 'sequential', "
									"try with 'parallel' option. ")));
		}
	}
	else if (shouldRunSequential)
	{
		return false;
	}
	else if (!localTableEmpty || IsMultiStatementTransaction())
	{
		return true;
	}

	return false;
}


/*
 * CreateTruncateTrigger creates a truncate trigger on table identified by relationId
 * and assigns citus_truncate_trigger() as handler.
 */
void
CreateTruncateTrigger(Oid relationId)
{
	StringInfo triggerName = makeStringInfo();
	bool internal = true;

	appendStringInfo(triggerName, "truncate_trigger");

	CreateTrigStmt *trigger = makeNode(CreateTrigStmt);
	trigger->trigname = triggerName->data;
	trigger->relation = NULL;
	trigger->funcname = SystemFuncName(CITUS_TRUNCATE_TRIGGER_NAME);
	trigger->args = NIL;
	trigger->row = false;
	trigger->timing = TRIGGER_TYPE_AFTER;
	trigger->events = TRIGGER_TYPE_TRUNCATE;
	trigger->columns = NIL;
	trigger->whenClause = NULL;
	trigger->isconstraint = false;

	CreateTrigger(trigger, NULL, relationId, InvalidOid, InvalidOid, InvalidOid,
				  InvalidOid, InvalidOid, NULL,
				  internal, false);
}


/*
 * RegularTable function returns true if given table's relation kind is RELKIND_RELATION
 * or RELKIND_PARTITIONED_TABLE otherwise it returns false.
 */
bool
RegularTable(Oid relationId)
{
	char relationKind = get_rel_relkind(relationId);

	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
	{
		return true;
	}

	return false;
}


/*
 * CopyLocalDataIntoShards copies data from the local table, which is hidden
 * after converting it to a distributed table, into the shards of the distributed
 * table. For partitioned tables, this functions returns without copying the data
 * because we call this function for both partitioned tables and its partitions.
 * Returning early saves us from copying data to workers twice.
 *
 * This function uses CitusCopyDestReceiver to invoke the distributed COPY logic.
 * We cannot use a regular COPY here since that cannot read from a table. Instead
 * we read from the table and pass each tuple to the CitusCopyDestReceiver which
 * opens a connection and starts a COPY for each shard placement that will have
 * data.
 *
 * We could call the planner and executor here and send the output to the
 * DestReceiver, but we are in a tricky spot here since Citus is already
 * intercepting queries on this table in the planner and executor hooks and we
 * want to read from the local table. To keep it simple, we perform a heap scan
 * directly on the table.
 *
 * Any writes on the table that are started during this operation will be handled
 * as distributed queries once the current transaction commits. SELECTs will
 * continue to read from the local table until the current transaction commits,
 * after which new SELECTs will be handled as distributed queries.
 *
 * After copying local data into the distributed table, the local data remains
 * in place and should be truncated at a later time.
 */
static void
CopyLocalDataIntoShards(Oid distributedRelationId)
{
	/* take an ExclusiveLock to block all operations except SELECT */
	Relation distributedRelation = table_open(distributedRelationId, ExclusiveLock);

	/*
	 * Skip copying from partitioned tables, we will copy the data from
	 * partition to partition's shards.
	 */
	if (PartitionedTable(distributedRelationId))
	{
		table_close(distributedRelation, NoLock);

		return;
	}

	/*
	 * All writes have finished, make sure that we can see them by using the
	 * latest snapshot. We use GetLatestSnapshot instead of
	 * GetTransactionSnapshot since the latter would not reveal all writes
	 * in serializable or repeatable read mode. Note that subsequent reads
	 * from the distributed table would reveal those writes, temporarily
	 * violating the isolation level. However, this seems preferable over
	 * dropping the writes entirely.
	 */
	PushActiveSnapshot(GetLatestSnapshot());

	/* get the table columns */
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);
	TupleTableSlot *slot = table_slot_create(distributedRelation, NULL);
	List *columnNameList = TupleDescColumnNameList(tupleDescriptor);

	int partitionColumnIndex = INVALID_PARTITION_COLUMN_INDEX;

	/* determine the partition column in the tuple descriptor */
	Var *partitionColumn = PartitionColumn(distributedRelationId, 0);
	if (partitionColumn != NULL)
	{
		partitionColumnIndex = partitionColumn->varattno - 1;
	}

	/* initialise per-tuple memory context */
	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;
	const bool nonPublishableData = false;
	DestReceiver *copyDest =
		(DestReceiver *) CreateCitusCopyDestReceiver(distributedRelationId,
													 columnNameList,
													 partitionColumnIndex,
													 estate, NULL, nonPublishableData);

	/* initialise state for writing to shards, we'll open connections on demand */
	copyDest->rStartup(copyDest, 0, tupleDescriptor);

	DoCopyFromLocalTableIntoShards(distributedRelation, copyDest, slot, estate);

	/* finish writing into the shards */
	copyDest->rShutdown(copyDest);
	copyDest->rDestroy(copyDest);

	/* free memory and close the relation */
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	table_close(distributedRelation, NoLock);

	PopActiveSnapshot();
}


/*
 * DoCopyFromLocalTableIntoShards performs a copy operation
 * from local tables into shards.
 */
static void
DoCopyFromLocalTableIntoShards(Relation distributedRelation,
							   DestReceiver *copyDest,
							   TupleTableSlot *slot,
							   EState *estate)
{
	/* begin reading from local table */
	TableScanDesc scan = table_beginscan(distributedRelation, GetActiveSnapshot(), 0,
										 NULL);

	MemoryContext oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	uint64 rowsCopied = 0;
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		/* send tuple it to a shard */
		copyDest->receiveSlot(slot, copyDest);

		/* clear tuple memory */
		ResetPerTupleExprContext(estate);

		/* make sure we roll back on cancellation */
		CHECK_FOR_INTERRUPTS();

		if (rowsCopied == 0)
		{
			ereport(NOTICE, (errmsg("Copying data from local table...")));
		}

		rowsCopied++;

		if (rowsCopied % LOG_PER_TUPLE_AMOUNT == 0)
		{
			ereport(DEBUG1, (errmsg("Copied " UINT64_FORMAT " rows", rowsCopied)));
		}
	}

	if (rowsCopied % LOG_PER_TUPLE_AMOUNT != 0)
	{
		ereport(DEBUG1, (errmsg("Copied " UINT64_FORMAT " rows", rowsCopied)));
	}

	if (rowsCopied > 0)
	{
		char *qualifiedRelationName =
			generate_qualified_relation_name(RelationGetRelid(distributedRelation));
		ereport(NOTICE, (errmsg("copying the data has completed"),
						 errdetail("The local data in the table is no longer visible, "
								   "but is still on disk."),
						 errhint("To remove the local data, run: SELECT "
								 "truncate_local_data_after_distributing_table($$%s$$)",
								 qualifiedRelationName)));
	}

	MemoryContextSwitchTo(oldContext);

	/* finish reading from the local table */
	table_endscan(scan);
}


/*
 * TupleDescColumnNameList returns a list of column names for the given tuple
 * descriptor as plain strings.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	List *columnNameList = NIL;

	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
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

	return columnNameList;
}


#if (PG_VERSION_NUM >= PG_VERSION_15)

/*
 * is_valid_numeric_typmod checks if the typmod value is valid
 *
 * Because of the offset, valid numeric typmods are at least VARHDRSZ
 *
 * Copied from PG. See numeric.c for understanding how this works.
 */
static bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}


/*
 * numeric_typmod_scale extracts the scale from a numeric typmod.
 *
 * Copied from PG. See numeric.c for understanding how this works.
 *
 */
static int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}


/*
 * DistributionColumnUsesNumericColumnNegativeScale returns whether a given relation uses
 * numeric data type with negative scale on distribution column
 */
static bool
DistributionColumnUsesNumericColumnNegativeScale(TupleDesc relationDesc,
												 Var *distributionColumn)
{
	Form_pg_attribute attributeForm = TupleDescAttr(relationDesc,
													distributionColumn->varattno - 1);

	if (attributeForm->atttypid == NUMERICOID &&
		is_valid_numeric_typmod(attributeForm->atttypmod) &&
		numeric_typmod_scale(attributeForm->atttypmod) < 0)
	{
		return true;
	}

	return false;
}


#endif

/*
 * DistributionColumnUsesGeneratedStoredColumn returns whether a given relation uses
 * GENERATED ALWAYS AS (...) STORED on distribution column
 */
static bool
DistributionColumnUsesGeneratedStoredColumn(TupleDesc relationDesc,
											Var *distributionColumn)
{
	Form_pg_attribute attributeForm = TupleDescAttr(relationDesc,
													distributionColumn->varattno - 1);

	if (attributeForm->attgenerated == ATTRIBUTE_GENERATED_STORED)
	{
		return true;
	}

	return false;
}


/*
 * ErrorIfForeignTable errors out if the relation with given relationOid
 * is a foreign table.
 */
static void
ErrorIfForeignTable(Oid relationOid)
{
	if (IsForeignTable(relationOid))
	{
		char *relname = get_rel_name(relationOid);
		char *qualifiedRelname = generate_qualified_relation_name(relationOid);
		ereport(ERROR, (errmsg("foreign tables cannot be distributed"),
						(errhint("Can add foreign table \"%s\" to metadata by running: "
								 "SELECT citus_add_local_table_to_metadata($$%s$$);",
								 relname, qualifiedRelname))));
	}
}
