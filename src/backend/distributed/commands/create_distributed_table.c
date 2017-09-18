/*-------------------------------------------------------------------------
 *
 * create_distributed_relation.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

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
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/distribution_column.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_utility.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* Replication model to use when creating distributed tables */
int ReplicationModel = REPLICATION_MODEL_COORDINATOR;


/* local function forward declarations */
static char AppropriateReplicationModel(char distributionMethod, bool viaDeprecatedAPI);
static void CreateHashDistributedTableShards(Oid relationId, Oid colocatedTableId,
											 bool localTableEmpty);
static uint32 ColocationIdForNewTable(Oid relationId, Var *distributionColumn,
									  char distributionMethod, char replicationModel,
									  char *colocateWithTableName, bool viaDeprecatedAPI);
static void EnsureRelationCanBeDistributed(Oid relationId, Var *distributionColumn,
										   char distributionMethod, uint32 colocationId,
										   char replicationModel, bool viaDeprecatedAPI);
static void EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
										  Oid distributionColumnType,
										  Oid sourceRelationId);
static void EnsureSchemaExistsOnAllNodes(Oid relationId);
static void EnsureLocalTableEmpty(Oid relationId);
static void EnsureTableNotDistributed(Oid relationId);
static char LookupDistributionMethod(Oid distributionMethodOid);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static void EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
											 bool viaDepracatedAPI);
static bool LocalTableEmpty(Oid tableId);
static void CopyLocalDataIntoShards(Oid relationId);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
static bool RelationUsesIdentityColumns(TupleDesc relationDesc);

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
	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	char *distributionColumnName = NULL;
	Var *distributionColumn = NULL;
	char distributionMethod = 0;
	char *colocateWithTableName = NULL;
	bool viaDeprecatedAPI = true;

	Relation relation = NULL;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	/*
	 * We should do this check here since the codes in the following lines rely
	 * on this relation to have a supported relation kind. More extensive checks
	 * will be performed in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	distributionColumnName = text_to_cstring(distributionColumnText);
	distributionColumn = BuildDistributionKeyFromColumnName(relation,
															distributionColumnName);
	distributionMethod = LookupDistributionMethod(distributionMethodOid);

	CreateDistributedTable(relationId, distributionColumn, distributionMethod,
						   colocateWithTableName, viaDeprecatedAPI);

	relation_close(relation, NoLock);

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
	Oid relationId = InvalidOid;
	text *distributionColumnText = NULL;
	Oid distributionMethodOid = InvalidOid;
	text *colocateWithTableNameText = NULL;

	Relation relation = NULL;
	char *distributionColumnName = NULL;
	Var *distributionColumn = NULL;
	char distributionMethod = 0;

	char *colocateWithTableName = NULL;

	bool viaDeprecatedAPI = false;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	relationId = PG_GETARG_OID(0);
	distributionColumnText = PG_GETARG_TEXT_P(1);
	distributionMethodOid = PG_GETARG_OID(2);
	colocateWithTableNameText = PG_GETARG_TEXT_P(3);

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	/*
	 * We should do this check here since the codes in the following lines rely
	 * on this relation to have a supported relation kind. More extensive checks
	 * will be performed in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	distributionColumnName = text_to_cstring(distributionColumnText);
	distributionColumn = BuildDistributionKeyFromColumnName(relation,
															distributionColumnName);
	distributionMethod = LookupDistributionMethod(distributionMethodOid);

	colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	CreateDistributedTable(relationId, distributionColumn, distributionMethod,
						   colocateWithTableName, viaDeprecatedAPI);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * CreateReferenceTable creates a distributed table with the given relationId. The
 * created table has one shard and replication factor is set to the active worker
 * count. In fact, the above is the definition of a reference table in Citus.
 */
Datum
create_reference_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation relation = NULL;
	char *colocateWithTableName = NULL;
	List *workerNodeList = NIL;
	int workerCount = 0;
	Var *distributionColumn = NULL;

	bool viaDeprecatedAPI = false;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	/*
	 * Ensure schema exists on each worker node. We can not run this function
	 * transactionally, since we may create shards over separate sessions and
	 * shard creation depends on the schema being present and visible from all
	 * sessions.
	 */
	EnsureSchemaExistsOnAllNodes(relationId);

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	/*
	 * We should do this check here since the codes in the following lines rely
	 * on this relation to have a supported relation kind. More extensive checks
	 * will be performed in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	workerNodeList = ActivePrimaryNodeList();
	workerCount = list_length(workerNodeList);

	/* if there are no workers, error out */
	if (workerCount == 0)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot create reference table \"%s\"", relationName),
						errdetail("There are no active worker nodes.")));
	}

	CreateDistributedTable(relationId, distributionColumn, DISTRIBUTE_BY_NONE,
						   colocateWithTableName, viaDeprecatedAPI);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * CreateDistributedTable creates distributed table in the given configuration.
 * This functions contains all necessary logic to create distributed tables. It
 * perform necessary checks to ensure distributing the table is safe. If it is
 * safe to distribute the table, this function creates distributed table metadata,
 * creates shards and copies local data to shards. This function also handles
 * partitioned tables by distributing its partitions as well.
 *
 * viaDeprecatedAPI boolean flag is not optimal way to implement this function,
 * but it helps reducing code duplication a lot. We hope to remove that flag one
 * day, once we deprecate master_create_distribute_table completely.
 */
void
CreateDistributedTable(Oid relationId, Var *distributionColumn, char distributionMethod,
					   char *colocateWithTableName, bool viaDeprecatedAPI)
{
	char replicationModel = REPLICATION_MODEL_INVALID;
	uint32 colocationId = INVALID_COLOCATION_ID;
	Oid colocatedTableId = InvalidOid;
	bool localTableEmpty = false;

	Relation colocatedRelation = NULL;

	replicationModel = AppropriateReplicationModel(distributionMethod, viaDeprecatedAPI);

	/*
	 * ColocationIdForNewTable assumes caller acquires lock on relationId. In our case,
	 * our caller already acquired lock on relationId.
	 */
	colocationId = ColocationIdForNewTable(relationId, distributionColumn,
										   distributionMethod, replicationModel,
										   colocateWithTableName, viaDeprecatedAPI);

	EnsureRelationCanBeDistributed(relationId, distributionColumn, distributionMethod,
								   colocationId, replicationModel, viaDeprecatedAPI);

	/* we need to calculate these variables before creating distributed metadata */
	localTableEmpty = LocalTableEmpty(relationId);
	colocatedTableId = ColocatedTableId(colocationId);

	/* create an entry for distributed table in pg_dist_partition */
	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumn,
							  colocationId, replicationModel);

	/* foreign tables does not support TRUNCATE trigger */
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
		/*
		 * We exit early but there is no need to close colocatedRelation. Because
		 * if viaDeprecatedAPI is true, we never open colocatedRelation in the first
		 * place.
		 */
		Assert(colocatedRelation == NULL);

		return;
	}

	/* create shards for hash distributed and reference tables */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		CreateHashDistributedTableShards(relationId, colocatedTableId, localTableEmpty);
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		CreateReferenceTableShard(relationId);
	}

	/* if this table is partitioned table, distribute its partitions too */
	if (PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);
		ListCell *partitionCell = NULL;

		foreach(partitionCell, partitionList)
		{
			Oid partitionRelationId = lfirst_oid(partitionCell);
			CreateDistributedTable(partitionRelationId, distributionColumn,
								   distributionMethod, colocateWithTableName,
								   viaDeprecatedAPI);
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

	if (colocatedRelation != NULL)
	{
		relation_close(colocatedRelation, NoLock);
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		CreateTableMetadataOnWorkers(relationId);
	}
}


/*
 * AppropriateReplicationModel function decides which replication model should be
 * used depending on given distribution configuration and global ReplicationModel
 * variable. If ReplicationModel conflicts with distribution configuration, this
 * function errors out.
 */
static char
AppropriateReplicationModel(char distributionMethod, bool viaDeprecatedAPI)
{
	if (viaDeprecatedAPI)
	{
		if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
		{
			ereport(NOTICE, (errmsg("using statement-based replication"),
							 errdetail("The current replication_model setting is "
									   "'streaming', which is not supported by "
									   "master_create_distributed_table."),
							 errhint("Use create_distributed_table to use the streaming "
									 "replication model.")));
		}

		return REPLICATION_MODEL_COORDINATOR;
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		return REPLICATION_MODEL_2PC;
	}
	else if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		return ReplicationModel;
	}
	else
	{
		if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
		{
			ereport(NOTICE, (errmsg("using statement-based replication"),
							 errdetail("Streaming replication is supported only for "
									   "hash-distributed tables.")));
		}

		return REPLICATION_MODEL_COORDINATOR;
	}

	/* we should not reach to this point */
	return REPLICATION_MODEL_INVALID;
}


/*
 * CreateHashDistributedTableShards creates shards of given hash distributed table.
 */
static void
CreateHashDistributedTableShards(Oid relationId, Oid colocatedTableId,
								 bool localTableEmpty)
{
	bool useExclusiveConnection = false;

	/*
	 * Ensure schema exists on each worker node. We can not run this function
	 * transactionally, since we may create shards over separate sessions and
	 * shard creation depends on the schema being present and visible from all
	 * sessions.
	 */
	EnsureSchemaExistsOnAllNodes(relationId);

	if (RegularTable(relationId))
	{
		useExclusiveConnection = IsTransactionBlock() || !localTableEmpty;
	}

	if (colocatedTableId != InvalidOid)
	{
		CreateColocatedShards(relationId, colocatedTableId, useExclusiveConnection);
	}
	else
	{
		/*
		 * This path is only reached by create_distributed_table for the distributed
		 * tables which will not be part of an existing colocation group. Therefore,
		 * we can directly use ShardCount and ShardReplicationFactor global variables
		 * here.
		 */
		CreateShardsWithRoundRobinPolicy(relationId, ShardCount, ShardReplicationFactor,
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
		Relation pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

		Oid distributionColumnType = distributionColumn->vartype;
		bool createdColocationGroup = false;

		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
		{
			/* check for default colocation group */
			colocationId = ColocationId(ShardCount, ShardReplicationFactor,
										distributionColumnType);

			if (colocationId == INVALID_COLOCATION_ID)
			{
				colocationId = CreateColocationGroup(ShardCount, ShardReplicationFactor,
													 distributionColumnType);
				createdColocationGroup = true;
			}
		}
		else if (pg_strncasecmp(colocateWithTableName, "none", NAMEDATALEN) == 0)
		{
			colocationId = GetNextColocationId();

			createdColocationGroup = true;
		}
		else
		{
			text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
			Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText);

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
			heap_close(pgDistColocation, NoLock);
		}
		else
		{
			/* release the exclusive lock */
			heap_close(pgDistColocation, ExclusiveLock);
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
 * This function assumes, callers have already acquried necessary locks to ensure
 * there will not be any change in the given relation.
 */
static void
EnsureRelationCanBeDistributed(Oid relationId, Var *distributionColumn,
							   char distributionMethod, uint32 colocationId,
							   char replicationModel, bool viaDeprecatedAPI)
{
	Relation relation = NULL;
	TupleDesc relationDesc = NULL;
	char *relationName = NULL;
	Oid parentRelationId = InvalidOid;

	EnsureTableOwner(relationId);
	EnsureTableNotDistributed(relationId);
	EnsureLocalTableEmptyIfNecessary(relationId, distributionMethod, viaDeprecatedAPI);
	EnsureReplicationSettings(InvalidOid, replicationModel);

	/* we assume callers took necessary locks */
	relation = relation_open(relationId, NoLock);
	relationDesc = RelationGetDescr(relation);
	relationName = RelationGetRelationName(relation);

	/* verify target relation does not use WITH (OIDS) PostgreSQL feature */
	if (relationDesc->tdhasoid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not specify the WITH "
								  "(OIDS) option in their definitions.")));
	}

	/* verify target relation does not use identity columns */
	if (RelationUsesIdentityColumns(relationDesc))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not use GENERATED "
								  "... AS IDENTITY.")));
	}

	/* check for support function needed by specified partition method */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(distributionColumn,
														   HASH_AM_OID, HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(distributionColumn->vartype)),
							errdatatype(distributionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
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
	if (PartitionTable(relationId) && !IsDistributedTable(parentRelationId))
	{
		char *relationName = get_rel_name(relationId);
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

		/* we currently don't support  partitioned tables for replication factor > 1 */
		if (ShardReplicationFactor > 1)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables with replication "
								   "factor greater than 1 is not supported")));
		}

		/* we currently don't support MX tables to be distributed partitioned table */
		if (replicationModel == REPLICATION_MODEL_STREAMING)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables which uses "
								   "streaming replication is not supported")));
		}

		/* we don't support distributing tables with multi-level partitioning */
		if (PartitionTable(relationId))
		{
			char *relationName = get_rel_name(relationId);
			Oid parentRelationId = PartitionParentOid(relationId);
			char *parentRelationName = get_rel_name(parentRelationId);

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing multi-level partitioned tables "
								   "is not supported"),
							errdetail("Relation \"%s\" is partitioned table itself and "
									  "it is also partition of relation \"%s\".",
									  relationName, parentRelationName)));
		}
	}

	ErrorIfUnsupportedConstraint(relation, distributionMethod, distributionColumn,
								 colocationId);

	relation_close(relation, NoLock);
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
	DistTableCacheEntry *sourceTableEntry = DistributedTableCacheEntry(sourceRelationId);
	char sourceDistributionMethod = sourceTableEntry->partitionMethod;
	char sourceReplicationModel = sourceTableEntry->replicationModel;
	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	Oid sourceDistributionColumnType = InvalidOid;

	if (sourceDistributionMethod != DISTRIBUTE_BY_HASH)
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

	sourceDistributionColumnType = sourceDistributionColumn->vartype;
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
 * EnsureSchemaExistsOnAllNodes connects to all nodes with citus extension user
 * and creates the schema of the given relationId. The function errors out if the
 * command cannot be executed in any of the worker nodes.
 */
static void
EnsureSchemaExistsOnAllNodes(Oid relationId)
{
	List *workerNodeList = ActivePrimaryNodeList();
	ListCell *workerNodeCell = NULL;
	StringInfo applySchemaCreationDDL = makeStringInfo();

	Oid schemaId = get_rel_namespace(relationId);
	const char *createSchemaDDL = CreateSchemaDDLCommand(schemaId);
	uint64 connectionFlag = FORCE_NEW_CONNECTION;

	if (createSchemaDDL == NULL)
	{
		return;
	}

	appendStringInfo(applySchemaCreationDDL, "%s", createSchemaDDL);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort, NULL,
										  NULL);

		ExecuteCriticalRemoteCommand(connection, applySchemaCreationDDL->data);
	}
}


/*
 * EnsureLocalTableEmptyIfNecessary only checks for emptiness if only an empty
 * relation can be distributed in given configuration.
 *
 * In some cases, it is possible and safe to send local data to shards while
 * distributing the table. In those cases, we can distribute non-empty local
 * tables. This function checks the distributionMethod and relation kind to
 * see whether we need to be ensure emptiness of local table. If we need to
 * be sure, this function calls EnsureLocalTableEmpty function to ensure
 * that local table does not contain any data.
 */
static void
EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
								 bool viaDepracatedAPI)
{
	if (viaDepracatedAPI)
	{
		EnsureLocalTableEmpty(relationId);
	}
	else if (distributionMethod != DISTRIBUTE_BY_HASH &&
			 distributionMethod != DISTRIBUTE_BY_NONE)
	{
		EnsureLocalTableEmpty(relationId);
	}
	else if (!RegularTable(relationId))
	{
		EnsureLocalTableEmpty(relationId);
	}
}


/*
 * EnsureLocalTableEmpty errors out if the local table is not empty.
 */
static void
EnsureLocalTableEmpty(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool localTableEmpty = LocalTableEmpty(relationId);

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
static void
EnsureTableNotDistributed(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool isDistributedTable = false;

	isDistributedTable = IsDistributedTable(relationId);

	if (isDistributedTable)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}
}


/*
 * EnsureReplicationSettings checks whether the current replication factor
 * setting is compatible with the replication model. This function errors
 * out if caller tries to use streaming replication with more than one
 * replication factor.
 */
void
EnsureReplicationSettings(Oid relationId, char replicationModel)
{
	char *msgSuffix = "the streaming replication model";
	char *extraHint = " or setting \"citus.replication_model\" to \"statement\"";

	if (relationId != InvalidOid)
	{
		msgSuffix = "tables which use the streaming replication model";
		extraHint = "";
	}

	if (replicationModel == REPLICATION_MODEL_STREAMING && ShardReplicationFactor != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication factors above one are incompatible with %s",
							   msgSuffix),
						errhint("Try again after reducing \"citus.shard_replication_"
								"factor\" to one%s.", extraHint)));
	}
}


/*
 * LookupDistributionMethod maps the oids of citus.distribution_type enum
 * values to pg_dist_partition.partmethod values.
 *
 * The passed in oid has to belong to a value of citus.distribution_type.
 */
static char
LookupDistributionMethod(Oid distributionMethodOid)
{
	HeapTuple enumTuple = NULL;
	Form_pg_enum enumForm = NULL;
	char distributionMethod = 0;
	const char *enumLabel = NULL;

	enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(distributionMethodOid));
	if (!HeapTupleIsValid(enumTuple))
	{
		ereport(ERROR, (errmsg("invalid internal value for enum: %u",
							   distributionMethodOid)));
	}

	enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	enumLabel = NameStr(enumForm->enumlabel);

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
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
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

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}


/*
 * LocalTableEmpty function checks whether given local table contains any row and
 * returns false if there is any data. This function is only for local tables and
 * should not be called for distributed tables.
 */
static bool
LocalTableEmpty(Oid tableId)
{
	Oid schemaId = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(tableId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);

	int spiConnectionResult = 0;
	int spiQueryResult = 0;
	StringInfo selectExistQueryString = makeStringInfo();

	HeapTuple tuple = NULL;
	Datum hasDataDatum = 0;
	bool localTableEmpty = false;
	bool columnNull = false;
	bool readOnly = true;

	int rowId = 0;
	int attributeId = 1;

	AssertArg(!IsDistributedTable(tableId));

	spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	appendStringInfo(selectExistQueryString, SELECT_EXIST_QUERY, tableQualifiedName);

	spiQueryResult = SPI_execute(selectExistQueryString->data, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   selectExistQueryString->data)));
	}

	/* we expect that SELECT EXISTS query will return single value in a single row */
	Assert(SPI_processed == 1);

	tuple = SPI_tuptable->vals[rowId];
	hasDataDatum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, attributeId, &columnNull);
	localTableEmpty = !DatumGetBool(hasDataDatum);

	SPI_finish();

	return localTableEmpty;
}


/*
 * CreateTruncateTrigger creates a truncate trigger on table identified by relationId
 * and assigns citus_truncate_trigger() as handler.
 */
void
CreateTruncateTrigger(Oid relationId)
{
	CreateTrigStmt *trigger = NULL;
	StringInfo triggerName = makeStringInfo();
	bool internal = true;

	appendStringInfo(triggerName, "truncate_trigger");

	trigger = makeNode(CreateTrigStmt);
	trigger->trigname = triggerName->data;
	trigger->relation = NULL;
	trigger->funcname = SystemFuncName("citus_truncate_trigger");
	trigger->args = NIL;
	trigger->row = false;
	trigger->timing = TRIGGER_TYPE_BEFORE;
	trigger->events = TRIGGER_TYPE_TRUNCATE;
	trigger->columns = NIL;
	trigger->whenClause = NULL;
	trigger->isconstraint = false;

	CreateTrigger(trigger, NULL, relationId, InvalidOid, InvalidOid, InvalidOid,
				  internal);
}


/*
 * RegularTable function returns true if given table's relation kind is RELKIND_RELATION
 * (or RELKIND_PARTITIONED_TABLE for PG >= 10), otherwise it returns false.
 */
bool
RegularTable(Oid relationId)
{
	char relationKind = get_rel_relkind(relationId);

#if (PG_VERSION_NUM >= 100000)
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
#else
	if (relationKind == RELKIND_RELATION)
#endif
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
	DestReceiver *copyDest = NULL;
	List *columnNameList = NIL;
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	Var *partitionColumn = NULL;
	int partitionColumnIndex = INVALID_PARTITION_COLUMN_INDEX;
	bool stopOnFailure = true;

	EState *estate = NULL;
	HeapScanDesc scan = NULL;
	HeapTuple tuple = NULL;
	ExprContext *econtext = NULL;
	MemoryContext oldContext = NULL;
	TupleTableSlot *slot = NULL;
	uint64 rowsCopied = 0;

	/* take an ExclusiveLock to block all operations except SELECT */
	distributedRelation = heap_open(distributedRelationId, ExclusiveLock);

	/*
	 * Skip copying from partitioned tables, we will copy the data from
	 * partition to partition's shards.
	 */
	if (PartitionedTable(distributedRelationId))
	{
		heap_close(distributedRelation, NoLock);

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
	tupleDescriptor = RelationGetDescr(distributedRelation);
	slot = MakeSingleTupleTableSlot(tupleDescriptor);
	columnNameList = TupleDescColumnNameList(tupleDescriptor);

	/* determine the partition column in the tuple descriptor */
	partitionColumn = PartitionColumn(distributedRelationId, 0);
	if (partitionColumn != NULL)
	{
		partitionColumnIndex = partitionColumn->varattno - 1;
	}

	/* initialise per-tuple memory context */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;

	copyDest =
		(DestReceiver *) CreateCitusCopyDestReceiver(distributedRelationId,
													 columnNameList,
													 partitionColumnIndex,
													 estate, stopOnFailure);

	/* initialise state for writing to shards, we'll open connections on demand */
	copyDest->rStartup(copyDest, 0, tupleDescriptor);

	/* begin reading from local table */
	scan = heap_beginscan(distributedRelation, GetActiveSnapshot(), 0, NULL);

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		/* materialize tuple and send it to a shard */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
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

		if (rowsCopied % 1000000 == 0)
		{
			ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
		}
	}

	if (rowsCopied % 1000000 != 0)
	{
		ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
	}

	MemoryContextSwitchTo(oldContext);

	/* finish reading from the local table */
	heap_endscan(scan);

	/* finish writing into the shards */
	copyDest->rShutdown(copyDest);

	/* free memory and close the relation */
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	heap_close(distributedRelation, NoLock);

	PopActiveSnapshot();
}


/*
 * TupleDescColumnNameList returns a list of column names for the given tuple
 * descriptor as plain strings.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	List *columnNameList = NIL;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	return columnNameList;
}


/*
 * RelationUsesIdentityColumns returns whether a given relation uses the SQL
 * GENERATED ... AS IDENTITY features supported as of PostgreSQL 10.
 */
static bool
RelationUsesIdentityColumns(TupleDesc relationDesc)
{
#if (PG_VERSION_NUM >= 100000)
	int attributeIndex = 0;

	for (attributeIndex = 0; attributeIndex < relationDesc->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = relationDesc->attrs[attributeIndex];

		if (attributeForm->attidentity != '\0')
		{
			return true;
		}
	}
#endif

	return false;
}
