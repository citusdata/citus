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
#include "distributed/shared_library_init.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"
#include "distributed/worker_transaction.h"
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

/*
 * once every LOG_PER_TUPLE_AMOUNT, the copy will be logged.
 */
#define LOG_PER_TUPLE_AMOUNT 1000000

/* local function forward declarations */
static char DecideReplicationModel(char distributionMethod, char *colocateWithTableName,
								   bool viaDeprecatedAPI);
static void CreateHashDistributedTableShards(Oid relationId, int shardCount,
											 Oid colocatedTableId, bool localTableEmpty);
static uint32 ColocationIdForNewTable(Oid relationId, Var *distributionColumn,
									  char distributionMethod, char replicationModel,
									  int shardCount, bool shardCountIsStrict,
									  char *colocateWithTableName,
									  bool viaDeprecatedAPI);
static void EnsureRelationCanBeDistributed(Oid relationId, Var *distributionColumn,
										   char distributionMethod, uint32 colocationId,
										   char replicationModel, bool viaDeprecatedAPI);
static void EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
										  Oid distributionColumnType,
										  Oid sourceRelationId);
static void EnsureLocalTableEmpty(Oid relationId);
static void EnsureRelationHasNoTriggers(Oid relationId);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static void EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
											 bool viaDeprecatedAPI);
static bool ShouldLocalTableBeEmpty(Oid relationId, char distributionMethod, bool
									viaDeprecatedAPI);
static void EnsureCitusTableCanBeCreated(Oid relationOid);
static void EnsureDistributedSequencesHaveOneType(Oid relationId,
												  List *dependentSequenceList,
												  List *attnumList);
static List * GetFKeyCreationCommandsRelationInvolvedWithTableType(Oid relationId,
																   int tableTypeFlag);
static Oid DropFKeysAndUndistributeTable(Oid relationId);
static void DropFKeysRelationInvolvedWithTableType(Oid relationId, int tableTypeFlag);
static void CopyLocalDataIntoShards(Oid relationId);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
static bool DistributionColumnUsesGeneratedStoredColumn(TupleDesc relationDesc,
														Var *distributionColumn);
static bool CanUseExclusiveConnections(Oid relationId, bool localTableEmpty);
static void DoCopyFromLocalTableIntoShards(Relation distributedRelation,
										   DestReceiver *copyDest,
										   TupleTableSlot *slot,
										   EState *estate);
static void ErrorIfTemporaryTable(Oid relationId);
static void ErrorIfForeignTable(Oid relationOid);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(create_reference_table);


/*
 * master_create_distributed_table accepts a table, distribution column and
 * method and performs the corresponding catalog changes.
 *
 * Note that this UDF is deprecated and cannot create colocated tables, so we
 * always use INVALID_COLOCATION_ID.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	EnsureCitusTableCanBeCreated(relationId);

	char *colocateWithTableName = NULL;
	bool viaDeprecatedAPI = true;

	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Assert(distributionColumnName != NULL);

	char distributionMethod = LookupDistributionMethod(distributionMethodOid);

	CreateDistributedTable(relationId, distributionColumnName, distributionMethod,
						   ShardCount, false, colocateWithTableName, viaDeprecatedAPI);

	PG_RETURN_VOID();
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

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
	{
		PG_RETURN_VOID();
	}
	bool viaDeprecatedAPI = false;

	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);
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

	EnsureCitusTableCanBeCreated(relationId);

	/* enable create_distributed_table on an empty node */
	InsertCoordinatorIfClusterEmpty();

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	Relation relation = try_relation_open(relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("could not create distributed table: "
							   "relation does not exist")));
	}

	relation_close(relation, NoLock);

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
						   shardCount, shardCountIsStrict, colocateWithTableName,
						   viaDeprecatedAPI);

	PG_RETURN_VOID();
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

	char *colocateWithTableName = NULL;
	char *distributionColumnName = NULL;

	bool viaDeprecatedAPI = false;

	EnsureCitusTableCanBeCreated(relationId);

	/* enable create_reference_table on an empty node */
	InsertCoordinatorIfClusterEmpty();

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	Relation relation = try_relation_open(relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("could not create reference table: "
							   "relation does not exist")));
	}

	relation_close(relation, NoLock);

	List *workerNodeList = ActivePrimaryNodeList(ShareLock);
	int workerCount = list_length(workerNodeList);

	/* if there are no workers, error out */
	if (workerCount == 0)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot create reference table \"%s\"", relationName),
						errdetail("There are no active worker nodes.")));
	}

	CreateDistributedTable(relationId, distributionColumnName, DISTRIBUTE_BY_NONE,
						   ShardCount, false, colocateWithTableName, viaDeprecatedAPI);
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
 * CreateDistributedTable creates distributed table in the given configuration.
 * This functions contains all necessary logic to create distributed tables. It
 * performs necessary checks to ensure distributing the table is safe. If it is
 * safe to distribute the table, this function creates distributed table metadata,
 * creates shards and copies local data to shards. This function also handles
 * partitioned tables by distributing its partitions as well.
 *
 * viaDeprecatedAPI boolean flag is not optimal way to implement this function,
 * but it helps reducing code duplication a lot. We hope to remove that flag one
 * day, once we deprecate master_create_distribute_table completely.
 */
void
CreateDistributedTable(Oid relationId, char *distributionColumnName,
					   char distributionMethod, int shardCount,
					   bool shardCountIsStrict, char *colocateWithTableName,
					   bool viaDeprecatedAPI)
{
	/*
	 * EnsureTableNotDistributed errors out when relation is a citus table but
	 * we don't want to ask user to first undistribute their citus local tables
	 * when creating reference or distributed tables from them.
	 * For this reason, here we undistribute citus local tables beforehand.
	 * But since UndistributeTable does not support undistributing relations
	 * involved in foreign key relationships, we first drop foreign keys that
	 * given relation is involved, then we undistribute the relation and finally
	 * we re-create dropped foreign keys at the end of this function.
	 */
	List *originalForeignKeyRecreationCommands = NIL;
	if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		/* store foreign key creation commands that relation is involved */
		originalForeignKeyRecreationCommands =
			GetFKeyCreationCommandsRelationInvolvedWithTableType(relationId,
																 INCLUDE_ALL_TABLE_TYPES);
		relationId = DropFKeysAndUndistributeTable(relationId);
	}

	/*
	 * To support foreign keys between reference tables and local tables,
	 * we drop & re-define foreign keys at the end of this function so
	 * that ALTER TABLE hook does the necessary job, which means converting
	 * local tables to citus local tables to properly support such foreign
	 * keys.
	 *
	 * This function does not expect to create Citus local table, so we blindly
	 * create reference table when the method is DISTRIBUTE_BY_NONE.
	 */
	else if (distributionMethod == DISTRIBUTE_BY_NONE &&
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

	char replicationModel = DecideReplicationModel(distributionMethod,
												   colocateWithTableName,
												   viaDeprecatedAPI);

	Var *distributionColumn = BuildDistributionKeyFromColumnName(relationId,
																 distributionColumnName,
																 ExclusiveLock);

	/*
	 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
	 * our caller already acquired lock on relationId.
	 */
	uint32 colocationId = ColocationIdForNewTable(relationId, distributionColumn,
												  distributionMethod, replicationModel,
												  shardCount, shardCountIsStrict,
												  colocateWithTableName,
												  viaDeprecatedAPI);

	EnsureRelationCanBeDistributed(relationId, distributionColumn, distributionMethod,
								   colocationId, replicationModel, viaDeprecatedAPI);

	/*
	 * Make sure that existing reference tables have been replicated to all the nodes
	 * such that we can create foreign keys and joins work immediately after creation.
	 */
	EnsureReferenceTablesExistOnAllNodes();

	/* we need to calculate these variables before creating distributed metadata */
	bool localTableEmpty = TableEmpty(relationId);
	Oid colocatedTableId = ColocatedTableId(colocationId);

	/* setting to false since this flag is only valid for citus local tables */
	bool autoConverted = false;

	/* create an entry for distributed table in pg_dist_partition */
	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumn,
							  colocationId, replicationModel, autoConverted);

	/* foreign tables do not support TRUNCATE trigger */
	if (RegularTable(relationId))
	{
		CreateTruncateTrigger(relationId);
	}

	/*
	 * If we are using master_create_distributed_table, we don't need to continue,
	 * because deprecated API does not supports the following features.
	 */
	if (viaDeprecatedAPI)
	{
		Assert(colocateWithTableName == NULL);
		return;
	}

	/* create shards for hash distributed and reference tables */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		CreateHashDistributedTableShards(relationId, shardCount, colocatedTableId,
										 localTableEmpty);
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		/*
		 * This function does not expect to create Citus local table, so we blindly
		 * create reference table when the method is DISTRIBUTE_BY_NONE.
		 */
		CreateReferenceTableShard(relationId);
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

		foreach_oid(partitionRelationId, partitionList)
		{
			CreateDistributedTable(partitionRelationId, distributionColumnName,
								   distributionMethod, shardCount, false,
								   parentRelationName, viaDeprecatedAPI);
		}
	}

	/* copy over data for hash distributed and reference tables */
	if (distributionMethod == DISTRIBUTE_BY_HASH ||
		distributionMethod == DISTRIBUTE_BY_NONE)
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
		List *attnumList = NIL;
		List *dependentSequenceList = NIL;
		GetDependentSequencesWithRelation(citusTableId, &attnumList,
										  &dependentSequenceList, 0);
		AttrNumber currentAttnum = InvalidAttrNumber;
		Oid currentSeqOid = InvalidOid;
		forboth_int_oid(currentAttnum, attnumList, currentSeqOid,
						dependentSequenceList)
		{
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
	List *attnumList = NIL;
	List *dependentSequenceList = NIL;

	GetDependentSequencesWithRelation(relationId, &attnumList, &dependentSequenceList, 0);
	EnsureDistributedSequencesHaveOneType(relationId, dependentSequenceList, attnumList);
}


/*
 * EnsureDistributedSequencesHaveOneType first ensures that the type of the column
 * in which the sequence is used as default is supported for each sequence in input
 * dependentSequenceList, and then alters the sequence type if not the same with the column type.
 */
static void
EnsureDistributedSequencesHaveOneType(Oid relationId, List *dependentSequenceList,
									  List *attnumList)
{
	AttrNumber attnum = InvalidAttrNumber;
	Oid sequenceOid = InvalidOid;
	forboth_int_oid(attnum, attnumList, sequenceOid, dependentSequenceList)
	{
		/*
		 * We should make sure that the type of the column that uses
		 * that sequence is supported
		 */
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
 * GetFKeyCreationCommandsRelationInvolvedWithTableType returns a list of DDL
 * commands to recreate the foreign keys that relation with relationId is involved
 * with given table type.
 */
static List *
GetFKeyCreationCommandsRelationInvolvedWithTableType(Oid relationId, int tableTypeFlag)
{
	int referencingFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
							   tableTypeFlag;
	List *referencingFKeyCreationCommands =
		GetForeignConstraintCommandsInternal(relationId, referencingFKeysFlag);

	/* already captured self referencing foreign keys, so use EXCLUDE_SELF_REFERENCES */
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  EXCLUDE_SELF_REFERENCES |
							  tableTypeFlag;
	List *referencedFKeyCreationCommands =
		GetForeignConstraintCommandsInternal(relationId, referencedFKeysFlag);
	return list_concat(referencingFKeyCreationCommands, referencedFKeyCreationCommands);
}


/*
 * DropFKeysAndUndistributeTable drops all foreign keys that relation with
 * relationId is involved then undistributes it.
 * Note that as UndistributeTable changes relationId of relation, this
 * function also returns new relationId of relation.
 * Also note that callers are responsible for storing & recreating foreign
 * keys to be dropped if needed.
 */
static Oid
DropFKeysAndUndistributeTable(Oid relationId)
{
	DropFKeysRelationInvolvedWithTableType(relationId, INCLUDE_ALL_TABLE_TYPES);

	/* store them before calling UndistributeTable as it changes relationId */
	char *relationName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);

	/* suppress notices messages not to be too verbose */
	TableConversionParameters params = {
		.relationId = relationId,
		.cascadeViaForeignKeys = false,
		.suppressNoticeMessages = true
	};
	UndistributeTable(&params);

	Oid newRelationId = get_relname_relid(relationName, schemaId);

	/*
	 * We don't expect this to happen but to be on the safe side let's error
	 * out here.
	 */
	EnsureRelationExists(newRelationId);

	return newRelationId;
}


/*
 * DropFKeysRelationInvolvedWithTableType drops foreign keys that relation
 * with relationId is involved with given table type.
 */
static void
DropFKeysRelationInvolvedWithTableType(Oid relationId, int tableTypeFlag)
{
	int referencingFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
							   tableTypeFlag;
	DropRelationForeignKeys(relationId, referencingFKeysFlag);

	/* already captured self referencing foreign keys, so use EXCLUDE_SELF_REFERENCES */
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  EXCLUDE_SELF_REFERENCES |
							  tableTypeFlag;
	DropRelationForeignKeys(relationId, referencedFKeysFlag);
}


/*
 * DecideReplicationModel function decides which replication model should be
 * used depending on given distribution configuration.
 */
static char
DecideReplicationModel(char distributionMethod, char *colocateWithTableName, bool
					   viaDeprecatedAPI)
{
	if (viaDeprecatedAPI)
	{
		return REPLICATION_MODEL_COORDINATOR;
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		return REPLICATION_MODEL_2PC;
	}
	else if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0 &&
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
 * ColocationIdForNewTable returns a colocation id for hash-distributed table
 * according to given configuration. If there is no such configuration, it
 * creates one and returns colocation id of newly the created colocation group.
 * For append and range distributed tables, this function errors out if
 * colocateWithTableName parameter is not NULL, otherwise directly returns
 * INVALID_COLOCATION_ID.
 *
 * This function assumes its caller take necessary lock on relationId to
 * prevent possible changes on it.
 */
static uint32
ColocationIdForNewTable(Oid relationId, Var *distributionColumn,
						char distributionMethod, char replicationModel,
						int shardCount, bool shardCountIsStrict,
						char *colocateWithTableName, bool viaDeprecatedAPI)
{
	uint32 colocationId = INVALID_COLOCATION_ID;

	if (viaDeprecatedAPI)
	{
		return colocationId;
	}
	else if (distributionMethod == DISTRIBUTE_BY_APPEND ||
			 distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot distribute relation"),
							errdetail("Currently, colocate_with option is only supported "
									  "for hash distributed tables.")));
		}

		return colocationId;
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
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
		Assert(distributionMethod == DISTRIBUTE_BY_HASH);

		Relation pgDistColocation = table_open(DistColocationRelationId(), ExclusiveLock);

		Oid distributionColumnType = distributionColumn->vartype;
		Oid distributionColumnCollation = get_typcollation(distributionColumnType);
		bool createdColocationGroup = false;

		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
		{
			/* check for default colocation group */
			colocationId = ColocationId(shardCount, ShardReplicationFactor,
										distributionColumnType,
										distributionColumnCollation);

			/*
			 * if the shardCount is strict then we check if the shard count
			 * of the colocated table is actually shardCount
			 */
			if (shardCountIsStrict && colocationId != INVALID_COLOCATION_ID)
			{
				Oid colocatedTableId = ColocatedTableId(colocationId);

				if (colocatedTableId != InvalidOid)
				{
					CitusTableCacheEntry *cacheEntry =
						GetCitusTableCacheEntry(colocatedTableId);
					int colocatedTableShardCount = cacheEntry->shardIntervalArrayLength;

					if (colocatedTableShardCount != shardCount)
					{
						colocationId = INVALID_COLOCATION_ID;
					}
				}
			}

			if (colocationId == INVALID_COLOCATION_ID)
			{
				colocationId = CreateColocationGroup(shardCount, ShardReplicationFactor,
													 distributionColumnType,
													 distributionColumnCollation);
				createdColocationGroup = true;
			}
		}
		else if (IsColocateWithNone(colocateWithTableName))
		{
			colocationId = GetNextColocationId();

			createdColocationGroup = true;
		}
		else
		{
			text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
			Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText, false);

			EnsureTableCanBeColocatedWith(relationId, replicationModel,
										  distributionColumnType, sourceRelationId);

			colocationId = TableColocationId(sourceRelationId);
		}

		/*
		 * If we created a new colocation group then we need to keep the lock to
		 * prevent a concurrent create_distributed_table call from creating another
		 * colocation group with the same parameters. If we're using an existing
		 * colocation group then other transactions will use the same one.
		 */
		if (createdColocationGroup)
		{
			/* keep the exclusive lock */
			table_close(pgDistColocation, NoLock);
		}
		else
		{
			/* release the exclusive lock */
			table_close(pgDistColocation, ExclusiveLock);
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
							   char replicationModel, bool viaDeprecatedAPI)
{
	Oid parentRelationId = InvalidOid;

	EnsureTableNotDistributed(relationId);
	EnsureLocalTableEmptyIfNecessary(relationId, distributionMethod, viaDeprecatedAPI);

	/* user really wants triggers? */
	if (!EnableUnsafeTriggers)
	{
		EnsureRelationHasNoTriggers(relationId);
	}


	/* we assume callers took necessary locks */
	Relation relation = relation_open(relationId, NoLock);
	TupleDesc relationDesc = RelationGetDescr(relation);
	char *relationName = RelationGetRelationName(relation);

	ErrorIfTableIsACatalogTable(relation);

	/* verify target relation does not use identity columns */
	if (RelationUsesIdentityColumns(relationDesc))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not use GENERATED "
								  "... AS IDENTITY.")));
	}

	/* verify target relation is not distributed by a generated columns */
	if (distributionMethod != DISTRIBUTE_BY_NONE &&
		DistributionColumnUsesGeneratedStoredColumn(relationDesc, distributionColumn))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distribution column must not use GENERATED ALWAYS "
								  "AS (...) STORED.")));
	}

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

	if (PartitionTable(relationId))
	{
		parentRelationId = PartitionParentOid(relationId);
	}

	/* partitions cannot be distributed if their parent is not distributed */
	if (PartitionTable(relationId) && !IsCitusTable(parentRelationId))
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
	if (PartitionedTable(relationId))
	{
		/* we cannot distribute partitioned tables with master_create_distributed_table */
		if (viaDeprecatedAPI)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables in only supported "
								   "with create_distributed_table UDF")));
		}

		/* distributing partitioned tables in only supported for hash-distribution */
		if (distributionMethod != DISTRIBUTE_BY_HASH)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables in only supported "
								   "for hash-distributed tables")));
		}

		/* we don't support distributing tables with multi-level partitioning */
		if (PartitionTable(relationId))
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
 * EnsureTableCanBeColocatedWith checks whether a given replication model and
 * distribution column type is suitable to distribute a table to be colocated
 * with given source table.
 *
 * We only pass relationId to provide meaningful error messages.
 */
static void
EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
							  Oid distributionColumnType, Oid sourceRelationId)
{
	CitusTableCacheEntry *sourceTableEntry = GetCitusTableCacheEntry(sourceRelationId);
	char sourceReplicationModel = sourceTableEntry->replicationModel;
	Var *sourceDistributionColumn = DistPartitionKeyOrError(sourceRelationId);

	if (!IsCitusTableTypeCacheEntry(sourceTableEntry, HASH_DISTRIBUTED))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation"),
						errdetail("Currently, colocate_with option is only supported "
								  "for hash distributed tables.")));
	}

	if (sourceReplicationModel != replicationModel)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Replication models don't match for %s and %s.",
								  sourceRelationName, relationName)));
	}

	Oid sourceDistributionColumnType = sourceDistributionColumn->vartype;
	if (sourceDistributionColumnType != distributionColumnType)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Distribution column types don't match for "
								  "%s and %s.", sourceRelationName,
								  relationName)));
	}
}


/*
 * EnsureLocalTableEmptyIfNecessary errors out if the function should be empty
 * according to ShouldLocalTableBeEmpty but it is not.
 */
static void
EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
								 bool viaDeprecatedAPI)
{
	if (ShouldLocalTableBeEmpty(relationId, distributionMethod, viaDeprecatedAPI))
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
ShouldLocalTableBeEmpty(Oid relationId, char distributionMethod,
						bool viaDeprecatedAPI)
{
	bool shouldLocalTableBeEmpty = false;
	if (viaDeprecatedAPI)
	{
		/* we don't support copying local data via deprecated API */
		shouldLocalTableBeEmpty = true;
	}
	else if (distributionMethod != DISTRIBUTE_BY_HASH &&
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
		ereport(ERROR, (errmsg("cannot distribute relation \"%s\" because it has "
							   "triggers and \"citus.enable_unsafe_triggers\" is "
							   "set to \"false\"", relationName),
						errhint("Consider setting \"citus.enable_unsafe_triggers\" "
								"to \"true\", or drop all the triggers on \"%s\" "
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
	TupleTableSlot *slot = CreateTableSlotForRel(distributedRelation);
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

	DestReceiver *copyDest =
		(DestReceiver *) CreateCitusCopyDestReceiver(distributedRelationId,
													 columnNameList,
													 partitionColumnIndex,
													 estate, NULL);

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


/*
 * RelationUsesIdentityColumns returns whether a given relation uses
 * GENERATED ... AS IDENTITY
 */
bool
RelationUsesIdentityColumns(TupleDesc relationDesc)
{
	for (int attributeIndex = 0; attributeIndex < relationDesc->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(relationDesc, attributeIndex);

		if (attributeForm->attidentity != '\0')
		{
			return true;
		}
	}

	return false;
}


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
