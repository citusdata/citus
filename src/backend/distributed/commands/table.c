/*-------------------------------------------------------------------------
 *
 * table.c
 *    Commands for creating and altering distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/distribution_column.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/version_compat.h"
#include "distributed/worker_shard_visibility.h"


/* controlled via GUC, should be accessed via GetEnableLocalReferenceForeignKeys() */
bool EnableLocalReferenceForeignKeys = true;

/*
 * GUC that controls whether to allow unique/exclude constraints without
 * distribution column.
 */
bool AllowUnsafeConstraints = false;

/* Local functions forward declarations for unsupported command checks */
static void PostprocessCreateTableStmtForeignKeys(CreateStmt *createStatement);
static void PostprocessCreateTableStmtPartitionOf(CreateStmt *createStatement,
												  const char *queryString);
static void PreprocessAttachPartitionToCitusTable(Oid parentRelationId,
												  Oid partitionRelationId);
static void PreprocessAttachCitusPartitionToCitusTable(Oid parentCitusRelationId,
													   Oid partitionRelationId);
static void DistributePartitionUsingParent(Oid parentRelationId,
										   Oid partitionRelationId);
static void ErrorIfMultiLevelPartitioning(Oid parentRelationId, Oid partitionRelationId);
static void ErrorIfAttachCitusTableToPgLocalTable(Oid parentRelationId,
												  Oid partitionRelationId);
static bool DeparserSupportsAlterTableAddColumn(AlterTableStmt *alterTableStatement,
												AlterTableCmd *addColumnSubCommand);
static bool ATDefinesFKeyBetweenPostgresAndCitusLocalOrRef(
	AlterTableStmt *alterTableStatement);
static bool ShouldMarkConnectedRelationsNotAutoConverted(Oid leftRelationId,
														 Oid rightRelationId);
static bool RelationIdListContainsCitusTableType(List *relationIdList,
												 CitusTableType citusTableType);
static bool RelationIdListContainsPostgresTable(List *relationIdList);
static void ConvertPostgresLocalTablesToCitusLocalTables(
	AlterTableStmt *alterTableStatement);
static bool RangeVarListHasLocalRelationConvertedByUser(List *relationRangeVarList,
														AlterTableStmt *
														alterTableStatement);
static int CompareRangeVarsByOid(const void *leftElement, const void *rightElement);
static List * GetAlterTableAddFKeyRightRelationIdList(
	AlterTableStmt *alterTableStatement);
static List * GetAlterTableAddFKeyRightRelationRangeVarList(
	AlterTableStmt *alterTableStatement);
static List * GetAlterTableAddFKeyConstraintList(AlterTableStmt *alterTableStatement);
static List * GetAlterTableCommandFKeyConstraintList(AlterTableCmd *command);
static List * GetRangeVarListFromFKeyConstraintList(List *fKeyConstraintList);
static List * GetRelationIdListFromRangeVarList(List *rangeVarList, LOCKMODE lockmode,
												bool missingOk);
static bool AlterTableCommandTypeIsTrigger(AlterTableType alterTableType);
static bool AlterTableDropsForeignKey(AlterTableStmt *alterTableStatement);
static void ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement);
static bool AlterInvolvesPartitionColumn(AlterTableStmt *alterTableStatement,
										 AlterTableCmd *command);
static bool AlterColumnInvolvesIdentityColumn(AlterTableStmt *alterTableStatement,
											  AlterTableCmd *command);
static void ErrorIfUnsupportedAlterAddConstraintStmt(AlterTableStmt *alterTableStatement);
static List * CreateRightShardListForInterShardDDLTask(Oid rightRelationId,
													   Oid leftRelationId,
													   List *leftShardList);
static void SetInterShardDDLTaskPlacementList(Task *task,
											  ShardInterval *leftShardInterval,
											  ShardInterval *rightShardInterval);
static void SetInterShardDDLTaskRelationShardList(Task *task,
												  ShardInterval *leftShardInterval,
												  ShardInterval *rightShardInterval);
static Oid get_attrdef_oid(Oid relationId, AttrNumber attnum);

static char * GetAddColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId,
												char *colname, TypeName *typeName,
												bool ifNotExists);
static void ErrorIfAlterTableDropTableNameFromPostgresFdw(List *optionList, Oid
														  relationId);


/*
 * We need to run some of the commands sequentially if there is a foreign constraint
 * from/to reference table.
 */
static bool SetupExecutionModeForAlterTable(Oid relationId, AlterTableCmd *command);

/*
 * PreprocessDropTableStmt processes DROP TABLE commands for partitioned tables.
 * If we are trying to DROP partitioned tables, we first need to go to MX nodes
 * and DETACH partitions from their parents. Otherwise, we process DROP command
 * multiple times in MX workers. For shards, we send DROP commands with IF EXISTS
 * parameter which solves problem of processing same command multiple times.
 * However, for distributed table itself, we directly remove related table from
 * Postgres catalogs via performDeletion function, thus we need to be cautious
 * about not processing same DROP command twice.
 */
List *
PreprocessDropTableStmt(Node *node, const char *queryString,
						ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropTableStatement = castNode(DropStmt, node);

	Assert(dropTableStatement->removeType == OBJECT_TABLE);

	List *tableNameList = NULL;
	foreach_ptr(tableNameList, dropTableStatement->objects)
	{
		RangeVar *tableRangeVar = makeRangeVarFromNameList(tableNameList);
		bool missingOK = true;

		Oid relationId = RangeVarGetRelid(tableRangeVar, AccessShareLock, missingOK);

		ErrorIfIllegallyChangingKnownShard(relationId);

		/* we're not interested in non-valid, non-distributed relations */
		if (relationId == InvalidOid || !IsCitusTable(relationId))
		{
			continue;
		}

		/*
		 * While changing the tables that are part of a colocation group we need to
		 * prevent concurrent mutations to the placements of the shard groups.
		 */
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		if (cacheEntry->colocationId != INVALID_COLOCATION_ID)
		{
			LockColocationId(cacheEntry->colocationId, ShareLock);
		}

		/* invalidate foreign key cache if the table involved in any foreign key */
		if ((TableReferenced(relationId) || TableReferencing(relationId)))
		{
			MarkInvalidateForeignKeyGraph();
		}

		/* we're only interested in partitioned and mx tables */
		if (!ShouldSyncTableMetadata(relationId) || !PartitionedTable(relationId))
		{
			continue;
		}

		EnsureCoordinator();

		List *partitionList = PartitionList(relationId);
		if (list_length(partitionList) == 0)
		{
			continue;
		}

		SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

		Oid partitionRelationId = InvalidOid;
		foreach_oid(partitionRelationId, partitionList)
		{
			char *detachPartitionCommand =
				GenerateDetachPartitionCommand(partitionRelationId);

			SendCommandToWorkersWithMetadata(detachPartitionCommand);
		}

		SendCommandToWorkersWithMetadata(ENABLE_DDL_PROPAGATION);
	}

	return NIL;
}


/*
 * PostprocessCreateTableStmt takes CreateStmt object as a parameter and
 * processes foreign keys on relation via PostprocessCreateTableStmtForeignKeys
 * function.
 *
 * This function also processes CREATE TABLE ... PARTITION OF statements via
 * PostprocessCreateTableStmtPartitionOf function.
 *
 * Also CREATE TABLE ... INHERITS ... commands are filtered here. If the inherited
 * table is a distributed table, this function errors out, as we currently don't
 * support local tables inheriting a distributed table.
 */
void
PostprocessCreateTableStmt(CreateStmt *createStatement, const char *queryString)
{
	PostprocessCreateTableStmtForeignKeys(createStatement);

	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(createStatement->relation, NoLock, missingOk);
	Oid schemaId = get_rel_namespace(relationId);
	if (createStatement->ofTypename && IsTenantSchema(schemaId))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create tables in a distributed schema using "
						"CREATE TABLE OF syntax")));
	}

	if (createStatement->inhRelations != NIL)
	{
		if (createStatement->partbound != NULL)
		{
			/* process CREATE TABLE ... PARTITION OF command */
			PostprocessCreateTableStmtPartitionOf(createStatement, queryString);
		}
		else
		{
			/* process CREATE TABLE ... INHERITS ... */

			if (IsTenantSchema(schemaId))
			{
				ereport(ERROR, (errmsg("tables in a distributed schema cannot inherit "
									   "or be inherited")));
			}

			RangeVar *parentRelation = NULL;
			foreach_ptr(parentRelation, createStatement->inhRelations)
			{
				Oid parentRelationId = RangeVarGetRelid(parentRelation, NoLock,
														missingOk);
				Assert(parentRelationId != InvalidOid);

				/*
				 * Throw a better error message if the user tries to inherit a
				 * tenant table or if the user tries to inherit from a tenant
				 * table.
				 */
				if (IsTenantSchema(get_rel_namespace(parentRelationId)))
				{
					ereport(ERROR, (errmsg("tables in a distributed schema cannot "
										   "inherit or be inherited")));
				}
				else if (IsCitusTable(parentRelationId))
				{
					/* here we error out if inheriting a distributed table */
					ereport(ERROR, (errmsg("non-distributed tables cannot inherit "
										   "distributed tables")));
				}
			}
		}
	}
}


/*
 * PostprocessCreateTableStmtForeignKeys drops ands re-defines foreign keys
 * defined by given CREATE TABLE command if command defined any foreign to
 * reference or citus local tables.
 */
static void
PostprocessCreateTableStmtForeignKeys(CreateStmt *createStatement)
{
	if (!ShouldEnableLocalReferenceForeignKeys())
	{
		/*
		 * Either the user disabled foreign keys from/to local/reference tables
		 * or the coordinator is not in the metadata */
		return;
	}

	/*
	 * Relation must exist and it is already locked as standard process utility
	 * is already executed.
	 */
	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(createStatement->relation, NoLock, missingOk);

	if (ShouldCreateTenantSchemaTable(relationId))
	{
		/*
		 * Avoid unnecessarily adding the table into metadata if we will
		 * distribute it as a tenant table later.
		 */
		return;
	}

	/*
	 * As we are just creating the table, we cannot have foreign keys that our
	 * relation is referenced. So we use INCLUDE_REFERENCING_CONSTRAINTS here.
	 * Reason behind using other two flags is explained below.
	 */
	int nonDistTableFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
								INCLUDE_CITUS_LOCAL_TABLES |
								INCLUDE_REFERENCE_TABLES;
	List *nonDistTableForeignKeyIdList =
		GetForeignKeyOids(relationId, nonDistTableFKeysFlag);
	bool hasForeignKeyToNonDistTable = list_length(nonDistTableForeignKeyIdList) != 0;
	if (hasForeignKeyToNonDistTable)
	{
		/*
		 * To support foreign keys from postgres tables to reference or citus
		 * local tables, we drop and re-define foreign keys so that our ALTER
		 * TABLE hook does the necessary job.
		 */
		List *relationFKeyCreationCommands =
			GetForeignConstraintCommandsInternal(relationId, nonDistTableFKeysFlag);
		DropRelationForeignKeys(relationId, nonDistTableFKeysFlag);

		bool skip_validation = true;
		ExecuteForeignKeyCreateCommandList(relationFKeyCreationCommands,
										   skip_validation);
	}
}


/*
 * ShouldEnableLocalReferenceForeignKeys is a wrapper around getting the GUC
 * EnableLocalReferenceForeignKeys. If the coordinator is not added
 * to the metadata, the function returns false. Else, the function returns
 * the value set by the user
 *
 */
bool
ShouldEnableLocalReferenceForeignKeys(void)
{
	if (!EnableLocalReferenceForeignKeys)
	{
		return false;
	}

	return CoordinatorAddedAsWorkerNode();
}


/*
 * PostprocessCreateTableStmtPartitionOf processes CREATE TABLE ... PARTITION OF
 * statements and it checks if user creates the table as a partition of a distributed
 * table. In that case, it distributes partition as well. Since the table itself is a
 * partition, CreateDistributedTable will attach it to its parent table automatically
 * after distributing it.
 */
static void
PostprocessCreateTableStmtPartitionOf(CreateStmt *createStatement, const
									  char *queryString)
{
	RangeVar *parentRelation = linitial(createStatement->inhRelations);
	bool missingOk = false;
	Oid parentRelationId = RangeVarGetRelid(parentRelation, NoLock, missingOk);

	/* a partition can only inherit from single parent table */
	Assert(list_length(createStatement->inhRelations) == 1);

	Assert(parentRelationId != InvalidOid);

	Oid relationId = RangeVarGetRelid(createStatement->relation, NoLock, missingOk);

	/*
	 * In case of an IF NOT EXISTS statement, Postgres lets it pass through the
	 * standardProcess_Utility, and gets into this Post-process hook by
	 * ignoring the statement if the table already exists. Thus, we need to make
	 * sure Citus behaves like plain PG in case the relation already exists.
	 */
	if (createStatement->if_not_exists)
	{
		if (IsCitusTable(relationId))
		{
			/*
			 * Ignore if the relation is already distributed.
			 */
			return;
		}
		else if (!PartitionTable(relationId) ||
				 PartitionParentOid(relationId) != parentRelationId)
		{
			/*
			 * Ignore if the relation is not a partition, or if that
			 * partition's parent is not the current parent from parentRelationId
			 */
			return;
		}
	}

	if (IsTenantSchema(get_rel_namespace(parentRelationId)) ||
		IsTenantSchema(get_rel_namespace(relationId)))
	{
		ErrorIfIllegalPartitioningInTenantSchema(parentRelationId, relationId);
	}

	/*
	 * If a partition is being created and if its parent is a distributed
	 * table, we will distribute this table as well.
	 */
	if (IsCitusTable(parentRelationId))
	{
		/*
		 * We can create Citus local tables right away, without switching to
		 * sequential mode, because they are going to have only one shard.
		 */
		if (IsCitusTableType(parentRelationId, CITUS_LOCAL_TABLE))
		{
			CreateCitusLocalTablePartitionOf(createStatement, relationId,
											 parentRelationId);
			return;
		}

		DistributePartitionUsingParent(parentRelationId, relationId);
	}
}


/*
 * PreprocessAlterTableStmtAttachPartition takes AlterTableStmt object as
 * parameter but it only processes into ALTER TABLE ... ATTACH PARTITION
 * commands and distributes the partition if necessary. There are four cases
 * to consider;
 *
 * Parent is not distributed, partition is not distributed: We do not need to
 * do anything in this case.
 *
 * Parent is not distributed, partition is distributed: This can happen if
 * user first distributes a table and tries to attach it to a non-distributed
 * table. Non-distributed tables cannot have distributed partitions, thus we
 * simply error out in this case.
 *
 * Parent is distributed, partition is not distributed: We should distribute
 * the table and attach it to its parent in workers. CreateDistributedTable
 * perform both of these operations. Thus, we will not propagate ALTER TABLE
 * ... ATTACH PARTITION command to workers.
 *
 * Parent is distributed, partition is distributed: Partition is already
 * distributed, we only need to attach it to its parent in workers. Attaching
 * operation will be performed via propagating this ALTER TABLE ... ATTACH
 * PARTITION command to workers.
 *
 * This function does nothing if the provided CreateStmt is not an ALTER TABLE ...
 * ATTACH PARTITION OF command.
 */
List *
PreprocessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement,
										const char *queryString)
{
	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *alterTableCommand = NULL;
	foreach_ptr(alterTableCommand, commandList)
	{
		if (alterTableCommand->subtype == AT_AttachPartition)
		{
			/*
			 * We acquire the lock on the parent and child as we are in the pre-process
			 * and want to ensure we acquire the locks in the same order with Postgres
			 */
			LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
			Oid parentRelationId = AlterTableLookupRelation(alterTableStatement,
															lockmode);
			PartitionCmd *partitionCommand = (PartitionCmd *) alterTableCommand->def;
			bool partitionMissingOk = true;
			Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name, lockmode,
													   partitionMissingOk);
			if (!OidIsValid(partitionRelationId))
			{
				/*
				 * We can stop propagation here instead of logging error. Pg will complain
				 * in standard_utility, so we are safe to stop here. We pass missing_ok
				 * as true to not diverge from pg output.
				 */
				return NIL;
			}

			if (IsTenantSchema(get_rel_namespace(parentRelationId)) ||
				IsTenantSchema(get_rel_namespace(partitionRelationId)))
			{
				ErrorIfIllegalPartitioningInTenantSchema(parentRelationId,
														 partitionRelationId);
			}

			if (!IsCitusTable(parentRelationId))
			{
				/*
				 * If the parent is a regular Postgres table, but the partition is a
				 * Citus table, we error out.
				 */
				ErrorIfAttachCitusTableToPgLocalTable(parentRelationId,
													  partitionRelationId);

				/*
				 * If both the parent and the child table are Postgres tables,
				 * we can just skip preprocessing this command.
				 */
				continue;
			}

			/* Citus doesn't support multi-level partitioned tables */
			ErrorIfMultiLevelPartitioning(parentRelationId, partitionRelationId);

			/* attaching to a Citus table */
			PreprocessAttachPartitionToCitusTable(parentRelationId, partitionRelationId);
		}
	}

	return NIL;
}


/*
 * PreprocessAttachPartitionToCitusTable takes a parent relation, which is a Citus table,
 * and a partition to be attached to it.
 * If the partition table is a regular Postgres table:
 * - Converts the partition to Citus Local Table, if the parent is a Citus Local Table.
 * - Distributes the partition, if the parent is a distributed table.
 * If not, calls PreprocessAttachCitusPartitionToCitusTable to attach given partition to
 * the parent relation.
 */
static void
PreprocessAttachPartitionToCitusTable(Oid parentRelationId, Oid partitionRelationId)
{
	Assert(IsCitusTable(parentRelationId));

	/* reference tables cannot be partitioned */
	Assert(!IsCitusTableType(parentRelationId, REFERENCE_TABLE));

	/* if parent of this table is distributed, distribute this table too */
	if (!IsCitusTable(partitionRelationId))
	{
		if (IsCitusTableType(parentRelationId, CITUS_LOCAL_TABLE))
		{
			/*
			 * We pass the cascade option as false, since Citus Local Table partitions
			 * cannot have non-inherited foreign keys.
			 */
			bool cascadeViaForeignKeys = false;
			CitusTableCacheEntry *entry = GetCitusTableCacheEntry(parentRelationId);
			bool autoConverted = entry->autoConverted;
			CreateCitusLocalTable(partitionRelationId, cascadeViaForeignKeys,
								  autoConverted);
		}
		else if (IsCitusTableType(parentRelationId, DISTRIBUTED_TABLE))
		{
			DistributePartitionUsingParent(parentRelationId, partitionRelationId);
		}
	}
	else
	{
		/* both the parent and child are Citus tables */
		PreprocessAttachCitusPartitionToCitusTable(parentRelationId, partitionRelationId);
	}
}


/*
 * PreprocessAttachCitusPartitionToCitusTable takes a parent relation, and a partition
 * to be attached to it. Both of them are Citus tables.
 * Errors out if the partition is a reference table.
 * Errors out if the partition is distributed and the parent is a Citus Local Table.
 * Distributes the partition, if it's a Citus Local Table, and the parent is distributed.
 */
static void
PreprocessAttachCitusPartitionToCitusTable(Oid parentCitusRelationId, Oid
										   partitionRelationId)
{
	if (IsCitusTableType(partitionRelationId, REFERENCE_TABLE))
	{
		ereport(ERROR, (errmsg("partitioned reference tables are not supported")));
	}
	else if (IsCitusTableType(partitionRelationId, DISTRIBUTED_TABLE) &&
			 IsCitusTableType(parentCitusRelationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("non-distributed partitioned tables cannot have "
							   "distributed partitions")));
	}
	else if (IsCitusTableType(partitionRelationId, CITUS_LOCAL_TABLE) &&
			 IsCitusTableType(parentCitusRelationId, DISTRIBUTED_TABLE))
	{
		/* if the parent is a distributed table, distribute the partition too */
		DistributePartitionUsingParent(parentCitusRelationId, partitionRelationId);
	}
	else if (IsCitusTableType(partitionRelationId, CITUS_LOCAL_TABLE) &&
			 IsCitusTableType(parentCitusRelationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * We should ensure that the partition relation has no foreign keys,
		 * as Citus Local Table partitions can only have inherited foreign keys.
		 */
		if (TableHasExternalForeignKeys(partitionRelationId))
		{
			ereport(ERROR, (errmsg("partition local tables added to citus metadata "
								   "cannot have non-inherited foreign keys")));
		}
	}

	/*
	 * We don't need to add other cases here, like distributed - distributed and
	 * citus_local - citus_local, as PreprocessAlterTableStmt and standard process
	 * utility would do the work to attach partitions to shell and shard relations.
	 */
}


/*
 * DistributePartitionUsingParent takes a parent and a partition relation and
 * distributes the partition, using the same distribution column as the parent, if the
 * parent has a distribution column. It creates a *hash* distributed table by default, as
 * partitioned tables can only be distributed by hash, unless it's null key distributed.
 *
 * If the parent has no distribution key, we distribute the partition with null key too.
 */
static void
DistributePartitionUsingParent(Oid parentCitusRelationId, Oid partitionRelationId)
{
	char *parentRelationName = generate_qualified_relation_name(parentCitusRelationId);

	/*
	 * We can create tenant tables and single shard tables right away, without
	 * switching to sequential mode, because they are going to have only one shard.
	 */
	if (ShouldCreateTenantSchemaTable(partitionRelationId))
	{
		CreateTenantSchemaTable(partitionRelationId);
		return;
	}
	else if (!HasDistributionKey(parentCitusRelationId))
	{
		/*
		 * If the parent is null key distributed, we should distribute the partition
		 * with null distribution key as well.
		 */
		ColocationParam colocationParam = {
			.colocationParamType = COLOCATE_WITH_TABLE_LIKE_OPT,
			.colocateWithTableName = parentRelationName,
		};
		CreateSingleShardTable(partitionRelationId, colocationParam);
		return;
	}

	Var *distributionColumn = DistPartitionKeyOrError(parentCitusRelationId);
	char *distributionColumnName = ColumnToColumnName(parentCitusRelationId,
													  (Node *) distributionColumn);

	char distributionMethod = DISTRIBUTE_BY_HASH;

	SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong(
		parentCitusRelationId, partitionRelationId);

	CreateDistributedTable(partitionRelationId, distributionColumnName,
						   distributionMethod, ShardCount, false,
						   parentRelationName);
}


/*
 * ErrorIfMultiLevelPartitioning takes a parent, and a partition relation to be attached
 * and errors out if the partition is also a partitioned table, which means we are
 * trying to build a multi-level partitioned table.
 */
static void
ErrorIfMultiLevelPartitioning(Oid parentRelationId, Oid partitionRelationId)
{
	if (PartitionedTable(partitionRelationId))
	{
		char *relationName = get_rel_name(partitionRelationId);
		char *parentRelationName = get_rel_name(parentRelationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Citus doesn't support multi-level "
							   "partitioned tables"),
						errdetail("Relation \"%s\" is partitioned table "
								  "itself and it is also partition of "
								  "relation \"%s\".",
								  relationName, parentRelationName)));
	}
}


/*
 * ErrorIfAttachCitusTableToPgLocalTable takes a parent, and a partition relation
 * to be attached. Errors out if the partition is a Citus table, and the parent is a
 * regular Postgres table.
 */
static void
ErrorIfAttachCitusTableToPgLocalTable(Oid parentRelationId, Oid partitionRelationId)
{
	if (!IsCitusTable(parentRelationId) &&
		IsCitusTable(partitionRelationId))
	{
		char *parentRelationName = get_rel_name(parentRelationId);

		ereport(ERROR, (errmsg("non-citus partitioned tables cannot have "
							   "citus table partitions"),
						errhint("Distribute the partitioned table \"%s\" "
								"instead, or add it to metadata", parentRelationName)));
	}
}


/*
 * PostprocessAlterTableSchemaStmt is executed after the change has been applied
 * locally, we can now use the new dependencies of the table to ensure all its
 * dependencies exist on the workers before we apply the commands remotely.
 */
List *
PostprocessAlterTableSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	/*
	 * We will let Postgres deal with missing_ok
	 */
	List *tableAddresses = GetObjectAddressListFromParseTree((Node *) stmt, true, true);

	/*  the code-path only supports a single object */
	Assert(list_length(tableAddresses) == 1);

	/* We have already asserted that we have exactly 1 address in the addresses. */
	ObjectAddress *tableAddress = linitial(tableAddresses);

	/*
	 * Check whether we are dealing with a sequence or view here and route queries
	 * accordingly to the right processor function.
	 */
	char relKind = get_rel_relkind(tableAddress->objectId);
	if (relKind == RELKIND_SEQUENCE)
	{
		stmt->objectType = OBJECT_SEQUENCE;
		return PostprocessAlterSequenceSchemaStmt((Node *) stmt, queryString);
	}
	else if (relKind == RELKIND_VIEW)
	{
		stmt->objectType = OBJECT_VIEW;
		return PostprocessAlterViewSchemaStmt((Node *) stmt, queryString);
	}

	if (!ShouldPropagate() || !IsCitusTable(tableAddress->objectId))
	{
		return NIL;
	}

	EnsureAllObjectDependenciesExistOnAllNodes(tableAddresses);

	return NIL;
}


/*
 * ChooseForeignKeyConstraintNameAddition returns the string of column names to be used when generating a foreign
 * key constraint name. This function is copied from postgres codebase.
 */
static char *
ChooseForeignKeyConstraintNameAddition(List *columnNames)
{
	char buf[NAMEDATALEN * 2];
	int buflen = 0;

	buf[0] = '\0';

	String *columnNameString = NULL;

	foreach_ptr(columnNameString, columnNames)
	{
		const char *name = strVal(columnNameString);

		if (buflen > 0)
		{
			buf[buflen++] = '_';                                                                        /* insert _ between names */
		}

		/*
		 *  At this point we have buflen <= NAMEDATALEN.  name should be less
		 *  than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
		{
			break;
		}
	}
	return pstrdup(buf);
}


/*
 * GenerateConstraintName creates and returns a default name for the constraints Citus supports
 * for default naming. See ConstTypeCitusCanDefaultName function for the supported constraint types.
 */
static char *
GenerateConstraintName(const char *tableName, Oid namespaceId, Constraint *constraint)
{
	char *conname = NULL;

	switch (constraint->contype)
	{
		case CONSTR_PRIMARY:
		{
			conname = ChooseIndexName(tableName, namespaceId,
									  NULL, NULL, true, true);
			break;
		}

		case CONSTR_UNIQUE:
		{
			ListCell *lc;
			List *indexParams = NIL;

			foreach(lc, constraint->keys)
			{
				IndexElem *iparam = makeNode(IndexElem);
				iparam->name = pstrdup(strVal(lfirst(lc)));
				indexParams = lappend(indexParams, iparam);
			}

			conname = ChooseIndexName(tableName, namespaceId,
									  ChooseIndexColumnNames(indexParams),
									  NULL, false, true);
			break;
		}

		case CONSTR_EXCLUSION:
		{
			ListCell *lc;
			List *indexParams = NIL;
			List *excludeOpNames = NIL;

			foreach(lc, constraint->exclusions)
			{
				List *pair = (List *) lfirst(lc);

				Assert(list_length(pair) == 2);
				IndexElem *elem = linitial_node(IndexElem, pair);
				List *opname = lsecond_node(List, pair);

				indexParams = lappend(indexParams, elem);
				excludeOpNames = lappend(excludeOpNames, opname);
			}

			conname = ChooseIndexName(tableName, namespaceId,
									  ChooseIndexColumnNames(indexParams),
									  excludeOpNames,
									  false, true);
			break;
		}

		case CONSTR_CHECK:
		{
			conname = ChooseConstraintName(tableName, NULL, "check", namespaceId, NIL);

			break;
		}

		case CONSTR_FOREIGN:
		{
			conname = ChooseConstraintName(tableName,
										   ChooseForeignKeyConstraintNameAddition(
											   constraint->fk_attrs),
										   "fkey",
										   namespaceId,
										   NIL);
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg(
								"unsupported constraint type for generating a constraint name: %d",
								constraint->contype)));
			break;
		}
	}

	return conname;
}


/*
 * EnsureSequentialModeForAlterTableOperation makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginning.
 */
static void
EnsureSequentialModeForAlterTableOperation(void)
{
	const char *objTypeString = "ALTER TABLE ... ADD FOREIGN KEY";

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot run %s command because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction", objTypeString),
						errdetail("When running command on/for a distributed %s, Citus "
								  "needs to perform all operations over a single "
								  "connection per node to ensure consistency.",
								  objTypeString),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "A command for a distributed %s is run. To make sure subsequent "
						 "commands see the %s correctly we need to make sure to "
						 "use only one connection for all future commands",
						 objTypeString, objTypeString)));

	SetLocalMultiShardModifyModeToSequential();
}


/*
 * SwitchToSequentialAndLocalExecutionIfConstraintNameTooLong generates the longest index constraint name
 * among the shards of the partitions, and if exceeds the limit switches to sequential and
 * local execution to prevent self-deadlocks.
 */
static void
SwitchToSequentialAndLocalExecutionIfConstraintNameTooLong(Oid relationId,
														   Constraint *constraint)
{
	if (!PartitionedTable(relationId))
	{
		/* Citus already handles long names for regular tables */
		return;
	}

	if (ShardIntervalCount(relationId) == 0)
	{
		/*
		 * Relation has no shards, so we cannot run into "long shard index
		 * name" issue.
		 */
		return;
	}

	Oid longestNamePartitionId = PartitionWithLongestNameRelationId(relationId);

	if (!OidIsValid(longestNamePartitionId))
	{
		/* no partitions have been created yet */
		return;
	}

	char *longestPartitionShardName = get_rel_name(longestNamePartitionId);
	ShardInterval *shardInterval = LoadShardIntervalWithLongestShardName(
		longestNamePartitionId);

	AppendShardIdToName(&longestPartitionShardName, shardInterval->shardId);


	Relation rel = RelationIdGetRelation(longestNamePartitionId);
	Oid namespaceOid = RelationGetNamespace(rel);
	RelationClose(rel);

	char *longestConname = GenerateConstraintName(longestPartitionShardName,
												  namespaceOid, constraint);

	if (longestConname && strnlen(longestConname, NAMEDATALEN) >= NAMEDATALEN - 1)
	{
		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg(
								"The constraint name (%s) on a shard is too long and could lead "
								"to deadlocks when executed in a transaction "
								"block after a parallel query", longestConname),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			elog(DEBUG1, "the constraint name on the shards of the partition "
						 "is too long, switching to sequential and local execution "
						 "mode to prevent self deadlocks: %s", longestConname);

			SetLocalMultiShardModifyModeToSequential();
			SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);
		}
	}
}


/*
 * PreprocessAlterTableAddConstraint creates a new constraint name for {PRIMARY KEY, UNIQUE, EXCLUDE, CHECK, FOREIGN KEY}
 * and changes the original alterTableCommand run by the standard utility hook to use the new constraint name.
 * Then it converts the ALTER TABLE ... ADD {PRIMARY KEY, UNIQUE, EXCLUDE, CHECK, FOREIGN KEY} ... command
 * into ALTER TABLE ... ADD CONSTRAINT <constraint name> {PRIMARY KEY, UNIQUE, EXCLUDE, CHECK, FOREIGN KEY} format and returns the DDLJob
 * to run this command in the workers.
 */
static List *
PreprocessAlterTableAddConstraint(AlterTableStmt *alterTableStatement, Oid
								  relationId,
								  Constraint *constraint)
{
	PrepareAlterTableStmtForConstraint(alterTableStatement, relationId, constraint);

	char *ddlCommand = DeparseTreeNode((Node *) alterTableStatement);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;


	if (constraint->contype == CONSTR_FOREIGN)
	{
		Oid rightRelationId = RangeVarGetRelid(constraint->pktable, NoLock,
											   false);

		/*
		 * If one of the relations involved in the FOREIGN KEY constraint is not a distributed table, citus errors out eventually.
		 * PreprocessAlterTableStmt function returns an empty tasklist in those cases.
		 * leftRelation is checked in PreprocessAlterTableStmt before
		 * calling PreprocessAlterTableAddConstraint. However, we need to handle the rightRelation since PreprocessAlterTableAddConstraint
		 * returns early.
		 */
		bool referencedIsLocalTable = !IsCitusTable(rightRelationId);
		if (referencedIsLocalTable)
		{
			ddlJob->taskList = NIL;
		}
		else
		{
			ddlJob->taskList = InterShardDDLTaskList(relationId, rightRelationId,
													 ddlCommand);
		}
	}
	else
	{
		ddlJob->taskList = DDLTaskList(relationId, ddlCommand);
	}

	return list_make1(ddlJob);
}


/*
 * PrepareAlterTableStmtForConstraint assigns a name to the constraint if it
 * does not have one and switches to sequential and local execution if the
 * constraint name is too long.
 */
void
PrepareAlterTableStmtForConstraint(AlterTableStmt *alterTableStatement,
								   Oid relationId,
								   Constraint *constraint)
{
	if (constraint->conname == NULL && constraint->indexname == NULL)
	{
		Relation rel = RelationIdGetRelation(relationId);

		/*
		 * Change the alterTableCommand so that the standard utility
		 * hook runs it with the name we created.
		 */

		constraint->conname = GenerateConstraintName(RelationGetRelationName(rel),
													 RelationGetNamespace(rel),
													 constraint);

		RelationClose(rel);
	}

	SwitchToSequentialAndLocalExecutionIfConstraintNameTooLong(relationId, constraint);

	if (constraint->contype == CONSTR_FOREIGN)
	{
		Oid rightRelationId = RangeVarGetRelid(constraint->pktable, NoLock,
											   false);

		if (IsCitusTableType(rightRelationId, REFERENCE_TABLE))
		{
			EnsureSequentialModeForAlterTableOperation();
		}
	}
}


/*
 * PreprocessAlterTableStmt determines whether a given ALTER TABLE statement
 * involves a distributed table. If so (and if the statement does not use
 * unsupported options), it modifies the input statement to ensure proper
 * execution against the master node table and creates a DDLJob to encapsulate
 * information needed during the worker node portion of DDL execution before
 * returning that DDLJob in a List. If no distributed table is involved, this
 * function returns NIL.
 */
List *
PreprocessAlterTableStmt(Node *node, const char *alterTableCommand,
						 ProcessUtilityContext processUtilityContext)
{
	AlterTableStmt *alterTableStatement = castNode(AlterTableStmt, node);

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return NIL;
	}

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);

	if (!OidIsValid(leftRelationId))
	{
		return NIL;
	}

	/*
	 * check whether we are dealing with a sequence or view here
	 */
	char relKind = get_rel_relkind(leftRelationId);
	if (relKind == RELKIND_SEQUENCE)
	{
		AlterTableStmt *stmtCopy = copyObject(alterTableStatement);
		stmtCopy->objtype = OBJECT_SEQUENCE;
#if (PG_VERSION_NUM >= PG_VERSION_15)

		/*
		 * it must be ALTER TABLE .. OWNER TO ..
		 * or ALTER TABLE .. SET LOGGED/UNLOGGED command
		 * since these are the only ALTER commands of a sequence that
		 * pass through an AlterTableStmt
		 */
		return PreprocessSequenceAlterTableStmt((Node *) stmtCopy, alterTableCommand,
												processUtilityContext);
#else

		/*
		 * it must be ALTER TABLE .. OWNER TO .. command
		 * since this is the only ALTER command of a sequence that
		 * passes through an AlterTableStmt
		 */
		return PreprocessAlterSequenceOwnerStmt((Node *) stmtCopy, alterTableCommand,
												processUtilityContext);
#endif
	}
	else if (relKind == RELKIND_VIEW)
	{
		/*
		 * it must be ALTER TABLE .. OWNER TO .. command
		 * since this is the only ALTER command of a view that
		 * passes through an AlterTableStmt
		 */
		AlterTableStmt *stmtCopy = copyObject(alterTableStatement);
		stmtCopy->objtype = OBJECT_VIEW;
		return PreprocessAlterViewStmt((Node *) stmtCopy, alterTableCommand,
									   processUtilityContext);
	}

	/*
	 * AlterTableStmt applies also to INDEX relations, and we have support for
	 * SET/SET storage parameters in Citus, so we might have to check for
	 * another relation here.
	 *
	 * ALTER INDEX ATTACH PARTITION also applies to INDEX relation, so we might
	 * check another relation for that option as well.
	 */
	char leftRelationKind = get_rel_relkind(leftRelationId);
	if (leftRelationKind == RELKIND_INDEX ||
		leftRelationKind == RELKIND_PARTITIONED_INDEX)
	{
		bool missingOk = false;
		leftRelationId = IndexGetRelation(leftRelationId, missingOk);
	}

	if (ShouldEnableLocalReferenceForeignKeys() &&
		processUtilityContext != PROCESS_UTILITY_SUBCOMMAND &&
		ATDefinesFKeyBetweenPostgresAndCitusLocalOrRef(alterTableStatement))
	{
		/*
		 * We don't process subcommands generated by postgres.
		 * This is mainly because postgres issues ALTER TABLE commands
		 * for some set of objects that are defined via CREATE TABLE commands.
		 * However, citus already has a separate logic for CREATE TABLE
		 * commands.
		 *
		 * To support foreign keys from/to postgres local tables to/from reference
		 * or citus local tables, we convert given postgres local table -and the
		 * other postgres tables that it is connected via a fkey graph- to a citus
		 * local table.
		 *
		 * Note that we don't convert postgres tables to citus local tables if
		 * coordinator is not added to metadata as CreateCitusLocalTable requires
		 * this. In this case, we assume user is about to create reference or
		 * distributed table from local table and we don't want to break user
		 * experience by asking to add coordinator to metadata.
		 */
		ConvertPostgresLocalTablesToCitusLocalTables(alterTableStatement);

		/*
		 * CreateCitusLocalTable converts relation to a shard relation and creates
		 * shell table from scratch.
		 * For this reason we should re-enter to PreprocessAlterTableStmt to operate
		 * on shell table relation id.
		 */
		return PreprocessAlterTableStmt(node, alterTableCommand, processUtilityContext);
	}

	if (AlterTableDropsForeignKey(alterTableStatement))
	{
		/*
		 * The foreign key graph keeps track of the foreign keys including local tables.
		 * So, even if a foreign key on a local table is dropped, we should invalidate
		 * the graph so that the next commands can see the graph up-to-date.
		 * We are aware that utility hook would still invalidate foreign key graph
		 * even when command fails, but currently we are ok with that.
		 */
		MarkInvalidateForeignKeyGraph();
	}

	bool referencingIsLocalTable = !IsCitusTable(leftRelationId);
	if (referencingIsLocalTable)
	{
		return NIL;
	}

	/*
	 * The PostgreSQL parser dispatches several commands into the node type
	 * AlterTableStmt, from ALTER INDEX to ALTER SEQUENCE or ALTER VIEW. Here
	 * we have a special implementation for ALTER INDEX, and a specific error
	 * message in case of unsupported sub-command.
	 */
	if (leftRelationKind == RELKIND_INDEX ||
		leftRelationKind == RELKIND_PARTITIONED_INDEX)
	{
		ErrorIfUnsupportedAlterIndexStmt(alterTableStatement);
	}
	else
	{
		/* this function also accepts more than just RELKIND_RELATION... */
		ErrorIfUnsupportedAlterTableStmt(alterTableStatement);
	}

	EnsureCoordinator();

	/* these will be set in below loop according to subcommands */
	Oid rightRelationId = InvalidOid;
	bool executeSequentially = false;

	/*
	 * We check if there is:
	 *  - an ADD/DROP FOREIGN CONSTRAINT command in sub commands
	 *    list. If there is we assign referenced relation id to rightRelationId and
	 *    we also set skip_validation to true to prevent PostgreSQL to verify validity
	 *    of the foreign constraint in master. Validity will be checked in workers
	 *    anyway.
	 *  - an ADD COLUMN .. that is the only subcommand in the list OR
	 *  - an ADD COLUMN .. DEFAULT nextval('..') OR
	 *    an ADD COLUMN .. SERIAL pseudo-type OR
	 *    an ALTER COLUMN .. SET DEFAULT nextval('..'). If there is we set
	 *    deparseAT variable to true which means we will deparse the statement
	 *    before we propagate the command to shards. For shards, all the defaults
	 *    coming from a user-defined sequence will be replaced by
	 *    NOT NULL constraint.
	 */
	List *commandList = alterTableStatement->cmds;

	/*
	 * if deparsing is needed, we will use a different version of the original
	 * alterTableStmt
	 */
	bool deparseAT = false;
	bool propagateCommandToWorkers = true;

	/*
	 * Sometimes we want to run a different DDL Command string in MX workers
	 * For example, in cases where worker_nextval should be used instead
	 * of nextval() in column defaults with type int and smallint
	 */
	bool useInitialDDLCommandString = true;

	AlterTableStmt *newStmt = copyObject(alterTableStatement);

	AlterTableCmd *newCmd = makeNode(AlterTableCmd);

	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		/*
		 * if deparsing is needed, we will use a different version of the original
		 * AlterTableCmd
		 */
		newCmd = copyObject(command);

		if (alterTableType == AT_AddConstraint)
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				/*
				 * We only support ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY, if it is
				 * only subcommand of ALTER TABLE. It was already checked in
				 * ErrorIfUnsupportedAlterTableStmt.
				 */
				Assert(list_length(commandList) == 1);

				rightRelationId = RangeVarGetRelid(constraint->pktable, lockmode,
												   alterTableStatement->missing_ok);

				if (processUtilityContext != PROCESS_UTILITY_SUBCOMMAND &&
					ShouldMarkConnectedRelationsNotAutoConverted(leftRelationId,
																 rightRelationId))
				{
					List *relationList = list_make2_oid(leftRelationId, rightRelationId);
					UpdateAutoConvertedForConnectedRelations(relationList, false);
				}

				/*
				 * Foreign constraint validations will be done in workers. If we do not
				 * set this flag, PostgreSQL tries to do additional checking when we drop
				 * to standard_ProcessUtility. standard_ProcessUtility tries to open new
				 * connections to workers to verify foreign constraints while original
				 * transaction is in process, which causes deadlock.
				 */
				constraint->skip_validation = true;

				if (constraint->conname == NULL)
				{
					return PreprocessAlterTableAddConstraint(alterTableStatement,
															 leftRelationId,
															 constraint);
				}
			}
			/*
			 * When constraint->indexname is not NULL we are handling an
			 * ADD {PRIMARY KEY, UNIQUE} USING INDEX command. In this case
			 * we do not have to create a name and change the command.
			 * The existing index name will be used by the postgres.
			 */
			else if (constraint->conname == NULL && constraint->indexname == NULL)
			{
				if (ConstrTypeCitusCanDefaultName(constraint->contype))
				{
					/*
					 * Create a constraint name. Convert ALTER TABLE ... ADD PRIMARY ... command into
					 * ALTER TABLE ... ADD CONSTRAINT <conname> PRIMARY KEY ... form and create the ddl jobs
					 * for running this form of the command on the workers.
					 */
					return PreprocessAlterTableAddConstraint(alterTableStatement,
															 leftRelationId,
															 constraint);
				}
			}
		}
		else if (alterTableType == AT_DropConstraint)
		{
			char *constraintName = command->name;
			if (ConstraintIsAForeignKey(constraintName, leftRelationId))
			{
				/*
				 * We only support ALTER TABLE DROP CONSTRAINT ... FOREIGN KEY, if it is
				 * only subcommand of ALTER TABLE. It was already checked in
				 * ErrorIfUnsupportedAlterTableStmt.
				 */
				Assert(list_length(commandList) == 1);

				bool missingOk = false;
				Oid foreignKeyId = get_relation_constraint_oid(leftRelationId,
															   constraintName, missingOk);
				rightRelationId = GetReferencedTableId(foreignKeyId);
			}

			/*
			 * We support deparsing for DROP CONSTRAINT, but currently deparsing is only
			 * possible if all subcommands are supported.
			 */
			if (list_length(commandList) == 1 &&
				alterTableStatement->objtype == OBJECT_TABLE)
			{
				deparseAT = true;
			}
		}
		else if (alterTableType == AT_AddColumn)
		{
			ColumnDef *columnDefinition = (ColumnDef *) command->def;
			List *columnConstraints = columnDefinition->constraints;

			Constraint *constraint = NULL;
			foreach_ptr(constraint, columnConstraints)
			{
				if (constraint->contype == CONSTR_FOREIGN)
				{
					rightRelationId = RangeVarGetRelid(constraint->pktable, lockmode,
													   alterTableStatement->missing_ok);

					/*
					 * Foreign constraint validations will be done in workers. If we do not
					 * set this flag, PostgreSQL tries to do additional checking when we drop
					 * to standard_ProcessUtility. standard_ProcessUtility tries to open new
					 * connections to workers to verify foreign constraints while original
					 * transaction is in process, which causes deadlock.
					 */
					constraint->skip_validation = true;
					break;
				}
			}

			if (DeparserSupportsAlterTableAddColumn(alterTableStatement, command))
			{
				deparseAT = true;

				constraint = NULL;
				foreach_ptr(constraint, columnConstraints)
				{
					if (ConstrTypeCitusCanDefaultName(constraint->contype))
					{
						PrepareAlterTableStmtForConstraint(alterTableStatement,
														   leftRelationId,
														   constraint);
					}
				}

				/*
				 * Copy the constraints to the new subcommand because now we
				 * might have assigned names to some of them.
				 */
				ColumnDef *newColumnDef = (ColumnDef *) newCmd->def;
				newColumnDef->constraints = copyObject(columnConstraints);
			}

			/*
			 * We check for ADD COLUMN .. DEFAULT expr
			 * if expr contains nextval('user_defined_seq')
			 * we should deparse the statement
			 */
			constraint = NULL;
			int constraintIdx = 0;
			foreach_ptr(constraint, columnConstraints)
			{
				if (constraint->contype == CONSTR_DEFAULT)
				{
					if (constraint->raw_expr != NULL)
					{
						ParseState *pstate = make_parsestate(NULL);
						Node *expr = transformExpr(pstate, constraint->raw_expr,
												   EXPR_KIND_COLUMN_DEFAULT);

						if (contain_nextval_expression_walker(expr, NULL))
						{
							deparseAT = true;
							useInitialDDLCommandString = false;

							/* drop the default expression from new subcomand */
							ColumnDef *newColumnDef = (ColumnDef *) newCmd->def;
							newColumnDef->constraints =
								list_delete_nth_cell(newColumnDef->constraints,
													 constraintIdx);
						}
					}

					/* there can only be one DEFAULT constraint that can be used per column */
					break;
				}

				constraintIdx++;
			}


			/*
			 * We check for ADD COLUMN .. SERIAL pseudo-type
			 * if that's the case, we should deparse the statement
			 * The structure of this check is copied from transformColumnDefinition.
			 */
			if (columnDefinition->typeName && list_length(
					columnDefinition->typeName->names) == 1 &&
				!columnDefinition->typeName->pct_type)
			{
				char *typeName = strVal(linitial(columnDefinition->typeName->names));

				if (strcmp(typeName, "smallserial") == 0 ||
					strcmp(typeName, "serial2") == 0 ||
					strcmp(typeName, "serial") == 0 ||
					strcmp(typeName, "serial4") == 0 ||
					strcmp(typeName, "bigserial") == 0 ||
					strcmp(typeName, "serial8") == 0)
				{
					deparseAT = true;

					ColumnDef *newColDef = copyObject(columnDefinition);
					newColDef->is_not_null = false;

					if (strcmp(typeName, "smallserial") == 0 ||
						strcmp(typeName, "serial2") == 0)
					{
						newColDef->typeName->names = NIL;
						newColDef->typeName->typeOid = INT2OID;
					}
					else if (strcmp(typeName, "serial") == 0 ||
							 strcmp(typeName, "serial4") == 0)
					{
						newColDef->typeName->names = NIL;
						newColDef->typeName->typeOid = INT4OID;
					}
					else if (strcmp(typeName, "bigserial") == 0 ||
							 strcmp(typeName, "serial8") == 0)
					{
						newColDef->typeName->names = NIL;
						newColDef->typeName->typeOid = INT8OID;
					}
					newCmd->def = (Node *) newColDef;
				}
			}
		}
		/*
		 * We check for ALTER COLUMN .. SET/DROP DEFAULT
		 * we should not propagate anything to shards
		 */
		else if (alterTableType == AT_ColumnDefault)
		{
			ParseState *pstate = make_parsestate(NULL);
			Node *expr = transformExpr(pstate, command->def,
									   EXPR_KIND_COLUMN_DEFAULT);

			if (contain_nextval_expression_walker(expr, NULL))
			{
				propagateCommandToWorkers = false;
				useInitialDDLCommandString = false;
			}
		}
		else if (alterTableType == AT_AttachPartition)
		{
			PartitionCmd *partitionCommand = (PartitionCmd *) command->def;
			Oid attachedRelationId = RangeVarGetRelid(partitionCommand->name, NoLock,
													  false);
			char attachedRelationKind = get_rel_relkind(attachedRelationId);

			/*
			 * We support ALTER INDEX ATTACH PARTITION and ALTER TABLE ATTACH PARTITION
			 * if it is only subcommand of ALTER TABLE command. Since the attached relation
			 * type is index for ALTER INDEX ATTACH PARTITION, we need to use the relation
			 * id this index is created for.
			 *
			 * Both were already checked in ErrorIfUnsupportedAlterIndexStmt and
			 * ErrorIfUnsupportedAlterTableStmt.
			 */
			if (attachedRelationKind == RELKIND_INDEX)
			{
				bool missingOk = false;
				rightRelationId = IndexGetRelation(attachedRelationId, missingOk);

				/*
				 * Since left relation is checked above to make sure it is Citus table,
				 * partition of that must be Citus table as well.
				 */
				Assert(IsCitusTable(rightRelationId));
			}
			else if (attachedRelationKind == RELKIND_RELATION ||
					 attachedRelationKind == RELKIND_FOREIGN_TABLE)
			{
				Assert(list_length(commandList) <= 1);

				/*
				 * Do not generate tasks if relation is distributed and the partition
				 * is not distributed. Because, we'll manually convert the partition into
				 * distributed table and co-locate with its parent.
				 */
				if (!IsCitusTable(attachedRelationId))
				{
					return NIL;
				}

				rightRelationId = attachedRelationId;
			}
		}
		else if (alterTableType == AT_DetachPartition)
		{
			PartitionCmd *partitionCommand = (PartitionCmd *) command->def;

			/*
			 * We only support ALTER TABLE DETACH PARTITION, if it is only subcommand of
			 * ALTER TABLE. It was already checked in ErrorIfUnsupportedAlterTableStmt.
			 */
			Assert(list_length(commandList) <= 1);

			rightRelationId = RangeVarGetRelid(partitionCommand->name, NoLock, false);
		}
		else if (AlterTableCommandTypeIsTrigger(alterTableType))
		{
			char *triggerName = command->name;
			return CitusCreateTriggerCommandDDLJob(leftRelationId, triggerName,
												   alterTableCommand);
		}

		/*
		 * We check and set the execution mode only if we fall into either of first two
		 * conditional blocks, otherwise we already continue the loop
		 */
		executeSequentially |= SetupExecutionModeForAlterTable(leftRelationId,
															   command);
	}

	if (executeSequentially)
	{
		SetLocalMultiShardModifyModeToSequential();
	}

	/* fill them here as it is possible to use them in some conditional blocks below */
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, leftRelationId);

	if (deparseAT)
	{
		newStmt->cmds = list_make1(newCmd);
		alterTableCommand = DeparseTreeNode((Node *) newStmt);
	}

	ddlJob->metadataSyncCommand = useInitialDDLCommandString ? alterTableCommand : NULL;

	if (OidIsValid(rightRelationId))
	{
		bool referencedIsLocalTable = !IsCitusTable(rightRelationId);
		if (referencedIsLocalTable || !propagateCommandToWorkers)
		{
			ddlJob->taskList = NIL;
		}
		else
		{
			/* if foreign key or attaching partition index related, use specialized task list function ... */
			ddlJob->taskList = InterShardDDLTaskList(leftRelationId, rightRelationId,
													 alterTableCommand);
		}
	}
	else
	{
		/* ... otherwise use standard DDL task list function */
		ddlJob->taskList = DDLTaskList(leftRelationId, alterTableCommand);
		if (!propagateCommandToWorkers)
		{
			ddlJob->taskList = NIL;
		}
	}

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * DeparserSupportsAlterTableAddColumn returns true if it's safe to deparse
 * the given ALTER TABLE statement that is known to contain given ADD COLUMN
 * subcommand.
 */
static bool
DeparserSupportsAlterTableAddColumn(AlterTableStmt *alterTableStatement,
									AlterTableCmd *addColumnSubCommand)
{
	/*
	 * We support deparsing for ADD COLUMN only of it's the only
	 * subcommand.
	 */
	if (list_length(alterTableStatement->cmds) == 1 &&
		alterTableStatement->objtype == OBJECT_TABLE)
	{
		ColumnDef *columnDefinition = (ColumnDef *) addColumnSubCommand->def;
		Constraint *constraint = NULL;
		foreach_ptr(constraint, columnDefinition->constraints)
		{
			if (constraint->contype == CONSTR_CHECK)
			{
				/*
				 * Given that we're in the preprocess, any reference to the
				 * column that we're adding would break the deparser. This
				 * can only be the case with CHECK constraints. For this
				 * reason, we skip deparsing the command and fall back to
				 * legacy behavior that we follow for ADD COLUMN subcommands.
				 *
				 * For other constraint types, we prepare the constraint to
				 * make sure that we can deparse it.
				 */
				return false;
			}
		}

		return true;
	}

	return false;
}


/*
 * ATDefinesFKeyBetweenPostgresAndCitusLocalOrRef returns true if given
 * alter table command defines foreign key between a postgres table and a
 * reference or citus local table.
 */
static bool
ATDefinesFKeyBetweenPostgresAndCitusLocalOrRef(AlterTableStmt *alterTableStatement)
{
	List *foreignKeyConstraintList =
		GetAlterTableAddFKeyConstraintList(alterTableStatement);
	if (list_length(foreignKeyConstraintList) == 0)
	{
		/* we are not defining any foreign keys */
		return false;
	}

	List *rightRelationIdList =
		GetAlterTableAddFKeyRightRelationIdList(alterTableStatement);

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!IsCitusTable(leftRelationId))
	{
		return RelationIdListContainsCitusTableType(rightRelationIdList,
													CITUS_LOCAL_TABLE) ||
			   RelationIdListContainsCitusTableType(rightRelationIdList,
													REFERENCE_TABLE);
	}
	else if (IsCitusTableType(leftRelationId, CITUS_LOCAL_TABLE) ||
			 IsCitusTableType(leftRelationId, REFERENCE_TABLE))
	{
		return RelationIdListContainsPostgresTable(rightRelationIdList);
	}

	return false;
}


/*
 * ShouldMarkConnectedRelationsNotAutoConverted takes two relations.
 * If both of them are Citus Local Tables, and one of them is auto-converted while the
 * other one is not; then it returns true. False otherwise.
 */
static bool
ShouldMarkConnectedRelationsNotAutoConverted(Oid leftRelationId, Oid rightRelationId)
{
	if (!IsCitusTableType(leftRelationId, CITUS_LOCAL_TABLE))
	{
		return false;
	}

	if (!IsCitusTableType(rightRelationId, CITUS_LOCAL_TABLE))
	{
		return false;
	}

	CitusTableCacheEntry *entryLeft = GetCitusTableCacheEntry(leftRelationId);
	CitusTableCacheEntry *entryRight = GetCitusTableCacheEntry(rightRelationId);

	return entryLeft->autoConverted != entryRight->autoConverted;
}


/*
 * RelationIdListContainsCitusTableType returns true if given relationIdList
 * contains a citus table with given type.
 */
static bool
RelationIdListContainsCitusTableType(List *relationIdList, CitusTableType citusTableType)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (IsCitusTableType(relationId, citusTableType))
		{
			return true;
		}
	}

	return false;
}


/*
 * RelationIdListContainsPostgresTable returns true if given relationIdList
 * contains a postgres table.
 */
static bool
RelationIdListContainsPostgresTable(List *relationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (OidIsValid(relationId) && !IsCitusTable(relationId))
		{
			return true;
		}
	}

	return false;
}


/*
 * ConvertPostgresLocalTablesToCitusLocalTables converts each postgres table
 * involved in foreign keys to be defined by given alter table command and the
 * other tables connected to them via a foreign key graph to citus local tables.
 */
static void
ConvertPostgresLocalTablesToCitusLocalTables(AlterTableStmt *alterTableStatement)
{
	List *rightRelationRangeVarList =
		GetAlterTableAddFKeyRightRelationRangeVarList(alterTableStatement);
	RangeVar *leftRelationRangeVar = alterTableStatement->relation;
	List *relationRangeVarList = lappend(rightRelationRangeVarList, leftRelationRangeVar);

	/*
	 * To prevent deadlocks, sort the list before converting each postgres local
	 * table to a citus local table.
	 */
	relationRangeVarList = SortList(relationRangeVarList, CompareRangeVarsByOid);
	bool containsAnyUserConvertedLocalRelation =
		RangeVarListHasLocalRelationConvertedByUser(relationRangeVarList,
													alterTableStatement);

	/*
	 * Here we should operate on RangeVar objects since relations oid's would
	 * change in below loop due to CreateCitusLocalTable.
	 */
	RangeVar *relationRangeVar;
	foreach_ptr(relationRangeVar, relationRangeVarList)
	{
		List *commandList = alterTableStatement->cmds;
		LOCKMODE lockMode = AlterTableGetLockLevel(commandList);
		bool missingOk = alterTableStatement->missing_ok;
		Oid relationId = RangeVarGetRelid(relationRangeVar, lockMode, missingOk);
		if (!OidIsValid(relationId))
		{
			/*
			 * As we are in preprocess, missingOk might be true and relation
			 * might not exist.
			 */
			continue;
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			CitusTableCacheEntry *entry = GetCitusTableCacheEntry(relationId);
			if (!entry->autoConverted)
			{
				/*
				 * This citus local table is already added to the metadata
				 * by the user, so no further operation needed.
				 */
				continue;
			}
			else if (!containsAnyUserConvertedLocalRelation)
			{
				/*
				 * We are safe to skip this relation because none of the citus local
				 * tables involved are manually added to the metadata by the user.
				 * This implies that all the Citus local tables involved are marked
				 * as autoConverted = true and there is no chance to update
				 * autoConverted = false.
				 */
				continue;
			}
		}
		else if (IsCitusTable(relationId))
		{
			/* we can directly skip for table types other than citus local tables */
			continue;
		}

		/*
		 * The only reason behind using a try/catch block here is giving a proper
		 * error message. For example, when creating a citus local table we might
		 * give an error telling that partitioned tables are not supported for
		 * citus local table creation. But as a user it wouldn't make much sense
		 * to see such an error. So here we extend error message to tell that we
		 * actually ended up with this error when trying to define the foreign key.
		 *
		 * Also, as CopyErrorData() requires (CurrentMemoryContext != ErrorContext),
		 * so we store CurrentMemoryContext here.
		 */
		MemoryContext savedMemoryContext = CurrentMemoryContext;
		PG_TRY();
		{
			bool cascade = true;

			/*
			 * Without this check, we would be erroring out in CreateCitusLocalTable
			 * for this case anyway. The purpose of this check&error is to provide
			 * a more meaningful message for the user.
			 */
			if (PartitionTable(relationId))
			{
				ereport(ERROR, (errmsg("cannot build foreign key between"
									   " reference table and a partition"),
								errhint("Try using parent table: %s",
										get_rel_name(PartitionParentOid(relationId)))));
			}
			else
			{
				/*
				 * There might be two scenarios:
				 *
				 *   a) A user created foreign key from a reference table
				 *      to Postgres local table(s) or Citus local table(s)
				 *      where all of the citus local tables involved are auto
				 *      converted. In that case, we mark the new table as auto
				 *      converted as well.
				 *
				 *   b) A user created foreign key from a reference table
				 *      to Postgres local table(s) or Citus local table(s)
				 *      where at least one of the citus local tables
				 *      involved is not auto converted. In that case, we mark
				 *      this new Citus local table as autoConverted = false
				 *      as well. Because our logic is to keep all the connected
				 *      Citus local tables to have the same autoConverted value.
				 */
				bool autoConverted = containsAnyUserConvertedLocalRelation ? false : true;
				CreateCitusLocalTable(relationId, cascade, autoConverted);
			}
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(savedMemoryContext);

			ErrorData *errorData = CopyErrorData();
			FlushErrorState();

			if (errorData->elevel != ERROR)
			{
				PG_RE_THROW();
			}

			/* override error detail */
			errorData->detail = "When adding a foreign key from a local table to "
								"a reference table, Citus applies a conversion to "
								"all the local tables in the foreign key graph";
			ThrowErrorData(errorData);
		}
		PG_END_TRY();
	}
}


/*
 * RangeVarListHasLocalRelationConvertedByUser takes a list of relations and returns true
 * if any of these relations is marked as auto-converted = false. Returns true otherwise.
 * This function also takes the current alterTableStatement command, to obtain the
 * necessary locks.
 */
static bool
RangeVarListHasLocalRelationConvertedByUser(List *relationRangeVarList,
											AlterTableStmt *alterTableStatement)
{
	RangeVar *relationRangeVar;
	foreach_ptr(relationRangeVar, relationRangeVarList)
	{
		/*
		 * Here we iterate the relation list, and if at least one of the relations
		 * is marked as not-auto-converted, we should mark all of them as
		 * not-auto-converted. In that case, we return true here.
		 */
		List *commandList = alterTableStatement->cmds;
		LOCKMODE lockMode = AlterTableGetLockLevel(commandList);
		bool missingOk = alterTableStatement->missing_ok;
		Oid relationId = RangeVarGetRelid(relationRangeVar, lockMode, missingOk);
		if (OidIsValid(relationId) && IsCitusTable(relationId) &&
			IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			CitusTableCacheEntry *entry = GetCitusTableCacheEntry(relationId);
			if (!entry->autoConverted)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * CompareRangeVarsByOid is a comparison function to sort RangeVar object list.
 */
static int
CompareRangeVarsByOid(const void *leftElement, const void *rightElement)
{
	RangeVar *leftRangeVar = *((RangeVar **) leftElement);
	RangeVar *rightRangeVar = *((RangeVar **) rightElement);

	/*
	 * Any way we will check their existence, so it's okay to map non-existing
	 * relations to InvalidOid when sorting.
	 */
	bool missingOk = true;

	/*
	 * As this is an object comparator function, there is no way to understand
	 * proper lock mode. So assume caller already locked relations.
	 */
	LOCKMODE lockMode = NoLock;

	Oid leftRelationId = RangeVarGetRelid(leftRangeVar, lockMode, missingOk);
	Oid rightRelationId = RangeVarGetRelid(rightRangeVar, lockMode, missingOk);
	return CompareOids(&leftRelationId, &rightRelationId);
}


/*
 * GetAlterTableAddFKeyRightRelationIdList returns a list of oid's for right
 * relations involved in foreign keys to be defined by given ALTER TABLE command.
 */
static List *
GetAlterTableAddFKeyRightRelationIdList(AlterTableStmt *alterTableStatement)
{
	List *rightRelationRangeVarList =
		GetAlterTableAddFKeyRightRelationRangeVarList(alterTableStatement);
	List *commandList = alterTableStatement->cmds;
	LOCKMODE lockMode = AlterTableGetLockLevel(commandList);
	bool missingOk = alterTableStatement->missing_ok;
	List *rightRelationIdList =
		GetRelationIdListFromRangeVarList(rightRelationRangeVarList, lockMode, missingOk);
	return rightRelationIdList;
}


/*
 * GetAlterTableAddFKeyRightRelationRangeVarList returns a list of RangeVar
 * objects for right relations involved in foreign keys to be defined by
 * given ALTER TABLE command.
 */
static List *
GetAlterTableAddFKeyRightRelationRangeVarList(AlterTableStmt *alterTableStatement)
{
	List *fKeyConstraintList = GetAlterTableAddFKeyConstraintList(alterTableStatement);
	List *rightRelationRangeVarList =
		GetRangeVarListFromFKeyConstraintList(fKeyConstraintList);
	return rightRelationRangeVarList;
}


/*
 * GetAlterTableAddFKeyConstraintList returns a list of Constraint objects for
 * foreign keys that given ALTER TABLE to be defined by given ALTER TABLE command.
 */
static List *
GetAlterTableAddFKeyConstraintList(AlterTableStmt *alterTableStatement)
{
	List *foreignKeyConstraintList = NIL;

	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		List *commandForeignKeyConstraintList =
			GetAlterTableCommandFKeyConstraintList(command);
		foreignKeyConstraintList = list_concat(foreignKeyConstraintList,
											   commandForeignKeyConstraintList);
	}

	return foreignKeyConstraintList;
}


/*
 * GetAlterTableCommandFKeyConstraintList returns a list of Constraint objects
 * for the foreign keys that given ALTER TABLE subcommand defines. Note that
 * this is only possible if it is an:
 *  - ADD CONSTRAINT subcommand (explicitly defines) or,
 *  - ADD COLUMN subcommand (implicitly defines by adding a new column that
 *    references to another table.
 */
static List *
GetAlterTableCommandFKeyConstraintList(AlterTableCmd *command)
{
	List *fkeyConstraintList = NIL;

	AlterTableType alterTableType = command->subtype;
	if (alterTableType == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		if (constraint->contype == CONSTR_FOREIGN)
		{
			fkeyConstraintList = lappend(fkeyConstraintList, constraint);
		}
	}
	else if (alterTableType == AT_AddColumn)
	{
		ColumnDef *columnDefinition = (ColumnDef *) command->def;
		List *columnConstraints = columnDefinition->constraints;

		Constraint *constraint = NULL;
		foreach_ptr(constraint, columnConstraints)
		{
			if (constraint->contype == CONSTR_FOREIGN)
			{
				fkeyConstraintList = lappend(fkeyConstraintList, constraint);
			}
		}
	}

	return fkeyConstraintList;
}


/*
 * GetRangeVarListFromFKeyConstraintList returns a list of RangeVar objects for
 * right relations in fKeyConstraintList.
 */
static List *
GetRangeVarListFromFKeyConstraintList(List *fKeyConstraintList)
{
	List *rightRelationRangeVarList = NIL;

	Constraint *fKeyConstraint = NULL;
	foreach_ptr(fKeyConstraint, fKeyConstraintList)
	{
		RangeVar *rightRelationRangeVar = fKeyConstraint->pktable;
		rightRelationRangeVarList = lappend(rightRelationRangeVarList,
											rightRelationRangeVar);
	}

	return rightRelationRangeVarList;
}


/*
 * GetRelationIdListFromRangeVarList returns relation id list for relations
 * identified by RangeVar objects in given list.
 */
static List *
GetRelationIdListFromRangeVarList(List *rangeVarList, LOCKMODE lockMode, bool missingOk)
{
	List *relationIdList = NIL;

	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, rangeVarList)
	{
		Oid rightRelationId = RangeVarGetRelid(rangeVar, lockMode, missingOk);
		relationIdList = lappend_oid(relationIdList, rightRelationId);
	}

	return relationIdList;
}


/*
 * AlterTableCommandTypeIsTrigger returns true if given alter table command type
 * is identifies an ALTER TABLE .. TRIGGER .. command.
 */
static bool
AlterTableCommandTypeIsTrigger(AlterTableType alterTableType)
{
	switch (alterTableType)
	{
		case AT_EnableTrig:
		case AT_EnableAlwaysTrig:
		case AT_EnableReplicaTrig:
		case AT_EnableTrigUser:
		case AT_DisableTrig:
		case AT_DisableTrigUser:
		case AT_EnableTrigAll:
		case AT_DisableTrigAll:
		{
			return true;
		}

		default:
			return false;
	}
}


/*
 * ConstrTypeUsesIndex returns true if the given constraint type uses an index
 */
bool
ConstrTypeUsesIndex(ConstrType constrType)
{
	return constrType == CONSTR_PRIMARY ||
		   constrType == CONSTR_UNIQUE ||
		   constrType == CONSTR_EXCLUSION;
}


/*
 * ConstrTypeSupportsDefaultNaming returns true if we can generate a default name for the given constraint type
 */
bool
ConstrTypeCitusCanDefaultName(ConstrType constrType)
{
	return constrType == CONSTR_PRIMARY ||
		   constrType == CONSTR_UNIQUE ||
		   constrType == CONSTR_EXCLUSION ||
		   constrType == CONSTR_CHECK ||
		   constrType == CONSTR_FOREIGN;
}


/*
 * AlterTableDropsForeignKey returns true if the given AlterTableStmt drops
 * a foreign key. False otherwise.
 */
static bool
AlterTableDropsForeignKey(AlterTableStmt *alterTableStatement)
{
	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

	AlterTableCmd *command = NULL;
	foreach_ptr(command, alterTableStatement->cmds)
	{
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_DropColumn)
		{
			char *columnName = command->name;
			if (ColumnAppearsInForeignKey(columnName, relationId))
			{
				/* dropping a column in the either side of the fkey will drop the fkey */
				return true;
			}
		}

		/*
		 * In order to drop the foreign key, other than DROP COLUMN, the command must be
		 * DROP CONSTRAINT command.
		 */
		if (alterTableType != AT_DropConstraint)
		{
			continue;
		}

		char *constraintName = command->name;
		if (ConstraintIsAForeignKey(constraintName, relationId))
		{
			return true;
		}
		else if (ConstraintIsAUniquenessConstraint(constraintName, relationId))
		{
			/*
			 * If the uniqueness constraint of the column that the foreign key depends on
			 * is getting dropped, then the foreign key will also be dropped.
			 */
			bool missingOk = false;
			Oid uniquenessConstraintId =
				get_relation_constraint_oid(relationId, constraintName, missingOk);
			Oid indexId = get_constraint_index(uniquenessConstraintId);
			if (AnyForeignKeyDependsOnIndex(indexId))
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * AnyForeignKeyDependsOnIndex scans pg_depend and returns true if given index
 * is valid and any foreign key depends on it.
 */
bool
AnyForeignKeyDependsOnIndex(Oid indexId)
{
	Oid dependentObjectClassId = RelationRelationId;
	Oid dependentObjectId = indexId;
	List *dependencyTupleList =
		GetPgDependTuplesForDependingObjects(dependentObjectClassId, dependentObjectId);

	HeapTuple dependencyTuple = NULL;
	foreach_ptr(dependencyTuple, dependencyTupleList)
	{
		Form_pg_depend dependencyForm = (Form_pg_depend) GETSTRUCT(dependencyTuple);
		Oid dependingClassId = dependencyForm->classid;
		if (dependingClassId != ConstraintRelationId)
		{
			continue;
		}

		Oid dependingObjectId = dependencyForm->objid;
		if (ConstraintWithIdIsOfType(dependingObjectId, CONSTRAINT_FOREIGN))
		{
			return true;
		}
	}

	return false;
}


/*
 * PreprocessAlterTableStmt issues a warning.
 * ALTER TABLE ALL IN TABLESPACE statements have their node type as
 * AlterTableMoveAllStmt. At the moment we do not support this functionality in
 * the distributed environment. We warn out here.
 */
List *
PreprocessAlterTableMoveAllStmt(Node *node, const char *queryString,
								ProcessUtilityContext processUtilityContext)
{
	if (EnableUnsupportedFeatureMessages)
	{
		ereport(WARNING, (errmsg("not propagating ALTER TABLE ALL IN TABLESPACE "
								 "commands to worker nodes"),
						  errhint("Connect to worker nodes directly to manually "
								  "move all tables.")));
	}

	return NIL;
}


/*
 * PreprocessAlterTableSchemaStmt is executed before the statement is applied
 * to the local postgres instance.
 *
 * In this stage we can prepare the commands that will alter the schemas of the
 * shards.
 */
List *
PreprocessAlterTableSchemaStmt(Node *node, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	if (stmt->relation == NULL)
	{
		return NIL;
	}

	List *addresses = GetObjectAddressListFromParseTree((Node *) stmt,
														stmt->missing_ok, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	/* We have already asserted that we have exactly 1 address in the addresses. */
	ObjectAddress *address = linitial(addresses);
	Oid relationId = address->objectId;

	/*
	 * Check whether we are dealing with a sequence or view here and route queries
	 * accordingly to the right processor function. We need to check both objects here
	 * since PG supports targeting sequences and views with ALTER TABLE commands.
	 */
	char relKind = get_rel_relkind(relationId);
	if (relKind == RELKIND_SEQUENCE)
	{
		AlterObjectSchemaStmt *stmtCopy = copyObject(stmt);
		stmtCopy->objectType = OBJECT_SEQUENCE;
		return PreprocessAlterSequenceSchemaStmt((Node *) stmtCopy, queryString,
												 processUtilityContext);
	}
	else if (relKind == RELKIND_VIEW)
	{
		AlterObjectSchemaStmt *stmtCopy = copyObject(stmt);
		stmtCopy->objectType = OBJECT_VIEW;
		return PreprocessAlterViewSchemaStmt((Node *) stmtCopy, queryString,
											 processUtilityContext);
	}

	/* first check whether a distributed relation is affected */
	if (!OidIsValid(relationId) || !IsCitusTable(relationId))
	{
		return NIL;
	}

	Oid oldSchemaId = get_rel_namespace(relationId);
	Oid newSchemaId = get_namespace_oid(stmt->newschema, stmt->missing_ok);
	if (!OidIsValid(oldSchemaId) || !OidIsValid(newSchemaId))
	{
		return NIL;
	}

	/*  Do nothing if new schema is the same as old schema */
	if (newSchemaId == oldSchemaId)
	{
		return NIL;
	}

	/* Undistribute table if its old schema is a tenant schema */
	if (IsTenantSchema(oldSchemaId) && IsCoordinator())
	{
		EnsureUndistributeTenantTableSafe(relationId,
										  TenantOperationNames[TENANT_SET_SCHEMA]);

		char *oldSchemaName = get_namespace_name(oldSchemaId);
		char *tableName = stmt->relation->relname;
		ereport(NOTICE, (errmsg("undistributing table %s in distributed schema %s "
								"before altering its schema", tableName, oldSchemaName)));

		/* Undistribute tenant table by suppressing weird notices */
		TableConversionParameters params = {
			.relationId = relationId,
			.cascadeViaForeignKeys = false,
			.bypassTenantCheck = true,
			.suppressNoticeMessages = true,
		};
		UndistributeTable(&params);

		/* relation id changes after undistribute_table */
		relationId = get_relname_relid(tableName, oldSchemaId);

		/*
		 * After undistribution, the table could be Citus table or Postgres table.
		 * If it is Postgres table, do not propagate the `ALTER TABLE SET SCHEMA`
		 * command to workers.
		 */
		if (!IsCitusTable(relationId))
		{
			return NIL;
		}
	}

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	QualifyTreeNode((Node *) stmt);
	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->metadataSyncCommand = DeparseTreeNode((Node *) stmt);
	ddlJob->taskList = DDLTaskList(relationId, ddlJob->metadataSyncCommand);
	return list_make1(ddlJob);
}


/*
 * SkipForeignKeyValidationIfConstraintIsFkey checks and processes the alter table
 * statement to be worked on the distributed table. Currently, it only processes
 * ALTER TABLE ... ADD FOREIGN KEY command to skip the validation step.
 */
void
SkipForeignKeyValidationIfConstraintIsFkey(AlterTableStmt *alterTableStatement,
										   bool processLocalRelation)
{
	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return;
	}

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return;
	}

	if (!IsCitusTable(leftRelationId) && !processLocalRelation)
	{
		return;
	}

	/*
	 * We check if there is a ADD FOREIGN CONSTRAINT command in sub commands
	 * list. We set skip_validation to true to prevent PostgreSQL to verify
	 * validity of the foreign constraint. Validity will be checked on the
	 * shards anyway.
	 */
	AlterTableCmd *command = NULL;
	foreach_ptr(command, alterTableStatement->cmds)
	{
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			/* skip only if the constraint is a foreign key */
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				/* foreign constraint validations will be done in shards. */
				constraint->skip_validation = true;
			}
		}
	}
}


/*
 * IsAlterTableRenameStmt returns whether the passed-in RenameStmt is one of
 * the following forms:
 *
 *   - ALTER TABLE RENAME
 *   - ALTER TABLE RENAME COLUMN
 *   - ALTER TABLE RENAME CONSTRAINT
 */
bool
IsAlterTableRenameStmt(RenameStmt *renameStmt)
{
	bool isAlterTableRenameStmt = false;

	if (renameStmt->renameType == OBJECT_TABLE ||
		renameStmt->renameType == OBJECT_FOREIGN_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_COLUMN &&
			 (renameStmt->relationType == OBJECT_TABLE ||
			  renameStmt->relationType == OBJECT_FOREIGN_TABLE))
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_TABCONSTRAINT)
	{
		isAlterTableRenameStmt = true;
	}

	return isAlterTableRenameStmt;
}


/*
 * ErrorIfAlterDropsPartitionColumn checks if any subcommands of the given alter table
 * command is a DROP COLUMN command which drops the partition column of a distributed
 * table. If there is such a subcommand, this function errors out.
 */
void
ErrorIfAlterDropsPartitionColumn(AlterTableStmt *alterTableStatement)
{
	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return;
	}

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return;
	}

	bool isCitusRelation = IsCitusTable(leftRelationId);
	if (!isCitusRelation)
	{
		return;
	}

	/* then check if any of subcommands drop partition column.*/
	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;
		if (alterTableType == AT_DropColumn)
		{
			if (AlterInvolvesPartitionColumn(alterTableStatement, command))
			{
				ereport(ERROR, (errmsg("cannot execute ALTER TABLE command "
									   "dropping partition column")));
			}
		}
	}
}


/*
 * PostprocessAlterTableStmt runs after the ALTER TABLE command has already run
 * on the master, so we are checking constraints over the table with constraints
 * already defined (to make the constraint check process same for ALTER TABLE and
 * CREATE TABLE). If constraints do not fulfill the rules we defined, they will be
 * removed and the table will return back to the state before the ALTER TABLE command.
 */
void
PostprocessAlterTableStmt(AlterTableStmt *alterTableStatement)
{
	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

	if (relationId != InvalidOid)
	{
		/*
		 * check whether we are dealing with a sequence here
		 * if yes, it must be ALTER TABLE .. OWNER TO .. command
		 * since this is the only ALTER command of a sequence that
		 * passes through an AlterTableStmt
		 */
		char relKind = get_rel_relkind(relationId);
		if (relKind == RELKIND_SEQUENCE)
		{
			alterTableStatement->objtype = OBJECT_SEQUENCE;
			PostprocessAlterSequenceOwnerStmt((Node *) alterTableStatement, NULL);
			return;
		}
		else if (relKind == RELKIND_VIEW)
		{
			alterTableStatement->objtype = OBJECT_VIEW;
			PostprocessAlterViewStmt((Node *) alterTableStatement, NULL);
			return;
		}

		/*
		 * Before ensuring each dependency exist, update dependent sequences
		 * types if necessary.
		 */
		EnsureRelationHasCompatibleSequenceTypes(relationId);

		/* changing a relation could introduce new dependencies */
		ObjectAddress *tableAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*tableAddress, RelationRelationId, relationId);
		EnsureAllObjectDependenciesExistOnAllNodes(list_make1(tableAddress));
	}

	/* for the new sequences coming with this ALTER TABLE statement */
	bool needMetadataSyncForNewSequences = false;

	char *alterTableDefaultNextvalCmd = NULL;

	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			Assert(list_length(commandList) == 1);

			ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);

			if (!OidIsValid(relationId))
			{
				continue;
			}

			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				InvalidateForeignKeyGraph();
			}
		}
		else if (alterTableType == AT_AddColumn)
		{
			ColumnDef *columnDefinition = (ColumnDef *) command->def;
			List *columnConstraints = columnDefinition->constraints;
			if (columnConstraints)
			{
				ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);
			}

			if (!OidIsValid(relationId))
			{
				continue;
			}

			Constraint *constraint = NULL;
			foreach_ptr(constraint, columnConstraints)
			{
				if (constraint->conname == NULL &&
					(constraint->contype == CONSTR_PRIMARY ||
					 constraint->contype == CONSTR_UNIQUE ||
					 constraint->contype == CONSTR_FOREIGN ||
					 constraint->contype == CONSTR_CHECK))
				{
					ErrorUnsupportedAlterTableAddColumn(relationId, command,
														constraint);
				}
			}

			/*
			 * We check for ADD COLUMN .. DEFAULT expr
			 * if expr contains nextval('user_defined_seq')
			 * we should make sure that the type of the column that uses
			 * that sequence is supported
			 */
			constraint = NULL;
			foreach_ptr(constraint, columnConstraints)
			{
				if (constraint->contype == CONSTR_DEFAULT)
				{
					if (constraint->raw_expr != NULL)
					{
						ParseState *pstate = make_parsestate(NULL);
						Node *expr = transformExpr(pstate, constraint->raw_expr,
												   EXPR_KIND_COLUMN_DEFAULT);

						/*
						 * We should make sure that the type of the column that uses
						 * that sequence is supported
						 */
						if (contain_nextval_expression_walker(expr, NULL))
						{
							AttrNumber attnum = get_attnum(relationId,
														   columnDefinition->colname);
							Oid seqOid = GetSequenceOid(relationId, attnum);
							if (seqOid != InvalidOid)
							{
								if (ShouldSyncTableMetadata(relationId))
								{
									needMetadataSyncForNewSequences = true;
									alterTableDefaultNextvalCmd =
										GetAddColumnWithNextvalDefaultCmd(seqOid,
																		  relationId,
																		  columnDefinition
																		  ->colname,
																		  columnDefinition
																		  ->typeName,
																		  command->
																		  missing_ok);
								}
							}
						}
					}
				}
			}
		}
		/*
		 * We check for ALTER COLUMN .. SET DEFAULT nextval('user_defined_seq')
		 * we should make sure that the type of the column that uses
		 * that sequence is supported
		 */
		else if (alterTableType == AT_ColumnDefault)
		{
			ParseState *pstate = make_parsestate(NULL);
			Node *expr = transformExpr(pstate, command->def,
									   EXPR_KIND_COLUMN_DEFAULT);

			if (contain_nextval_expression_walker(expr, NULL))
			{
				AttrNumber attnum = get_attnum(relationId, command->name);
				Oid seqOid = GetSequenceOid(relationId, attnum);
				if (seqOid != InvalidOid)
				{
					if (ShouldSyncTableMetadata(relationId))
					{
						needMetadataSyncForNewSequences = true;
						bool missingTableOk = false;
						alterTableDefaultNextvalCmd = GetAlterColumnWithNextvalDefaultCmd(
							seqOid, relationId, command->name, missingTableOk);
					}
				}
			}
		}
	}

	if (needMetadataSyncForNewSequences)
	{
		/* prevent recursive propagation */
		SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

		/*
		 * It's easy to retrieve the sequence id to create the proper commands
		 * in postprocess, after the dependency between the sequence and the table
		 * has been created. We already return ddlJobs in PreprocessAlterTableStmt,
		 * hence we can't return ddlJobs in PostprocessAlterTableStmt.
		 * That's why we execute the following here instead of
		 * in ExecuteDistributedDDLJob
		 */
		SendCommandToWorkersWithMetadata(alterTableDefaultNextvalCmd);

		SendCommandToWorkersWithMetadata(ENABLE_DDL_PROPAGATION);
	}
}


/*
 * FixAlterTableStmtIndexNames runs after the ALTER TABLE command
 * has already run on the coordinator, and also after the distributed DDL
 * Jobs have been executed on the workers.
 *
 * We might have wrong index names generated on indexes of shards of partitions,
 * see https://github.com/citusdata/citus/pull/5397 for the details. So we
 * perform the relevant checks and index renaming here.
 */
void
FixAlterTableStmtIndexNames(AlterTableStmt *alterTableStatement)
{
	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!(OidIsValid(relationId) && IsCitusTable(relationId) &&
		  PartitionedTable(relationId)))
	{
		/* we are only interested in partitioned Citus tables */
		return;
	}

	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		/*
		 * If this a partitioned table, and the constraint type uses an index
		 * UNIQUE, PRIMARY KEY, EXCLUDE constraint,
		 * we have wrong index names generated on indexes of shards of
		 * partitions of this table, so we should fix them
		 */
		Constraint *constraint = (Constraint *) command->def;
		if (alterTableType == AT_AddConstraint &&
			ConstrTypeUsesIndex(constraint->contype))
		{
			bool missingOk = false;
			const char *constraintName = constraint->conname;
			Oid constraintId =
				get_relation_constraint_oid(relationId, constraintName, missingOk);

			/* fix only the relevant index */
			Oid parentIndexOid = get_constraint_index(constraintId);

			FixPartitionShardIndexNames(relationId, parentIndexOid);
		}
		/*
		 * If this is an ALTER TABLE .. ATTACH PARTITION command
		 * we have wrong index names generated on indexes of shards of
		 * the current partition being attached, so we should fix them
		 */
		else if (alterTableType == AT_AttachPartition)
		{
			PartitionCmd *partitionCommand = (PartitionCmd *) command->def;
			bool partitionMissingOk = false;
			Oid partitionRelationId =
				RangeVarGetRelid(partitionCommand->name, lockmode,
								 partitionMissingOk);
			Oid parentIndexOid = InvalidOid;     /* fix all the indexes */

			FixPartitionShardIndexNames(partitionRelationId, parentIndexOid);
		}
	}
}


/*
 * GetSequenceOid returns the oid of the sequence used as default value
 * of the attribute with given attnum of the given table relationId
 * If there is no sequence used it returns InvalidOid.
 */
Oid
GetSequenceOid(Oid relationId, AttrNumber attnum)
{
	/* get attrdefoid from the given relationId and attnum */
	Oid attrdefOid = get_attrdef_oid(relationId, attnum);

	/* retrieve the sequence id of the sequence found in nextval('seq') */
	List *sequencesFromAttrDef = GetSequencesFromAttrDef(attrdefOid);

	if (list_length(sequencesFromAttrDef) == 0)
	{
		/*
		 * We need this check because sometimes there are cases where the
		 * dependency between the table and the sequence is not formed
		 * One example is when the default is defined by
		 * DEFAULT nextval('seq_name'::text) (not by DEFAULT nextval('seq_name'))
		 * In these cases, sequencesFromAttrDef with be empty.
		 */
		return InvalidOid;
	}

	if (list_length(sequencesFromAttrDef) > 1)
	{
		/* to simplify and eliminate cases like "DEFAULT nextval('..') - nextval('..')" */
		ereport(ERROR, (errmsg(
							"More than one sequence in a column default"
							" is not supported for distribution "
							"or for adding local tables to metadata")));
	}

	return lfirst_oid(list_head(sequencesFromAttrDef));
}


/*
 * get_attrdef_oid gets the oid of the attrdef that has dependency with
 * the given relationId (refobjid) and attnum (refobjsubid).
 * If there is no such attrdef it returns InvalidOid.
 * NOTE: we are iterating pg_depend here since this function is used together
 * with other functions that iterate pg_depend. Normally, a look at pg_attrdef
 * would make more sense.
 */
static Oid
get_attrdef_oid(Oid relationId, AttrNumber attnum)
{
	Oid resultAttrdefOid = InvalidOid;

	ScanKeyData key[3];

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	ScanKeyInit(&key[2],
				Anum_pg_depend_refobjsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(attnum));

	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, attnum ? 3 : 2, key);

	HeapTuple tup;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		if (deprec->classid == AttrDefaultRelationId)
		{
			resultAttrdefOid = deprec->objid;
		}
	}

	systable_endscan(scan);
	table_close(depRel, AccessShareLock);
	return resultAttrdefOid;
}


/*
 * GetAlterColumnWithNextvalDefaultCmd returns a string representing:
 * ALTER TABLE ALTER COLUMN .. SET DEFAULT nextval()
 * If sequence type is not bigint, we use worker_nextval() instead of nextval().
 */
char *
GetAlterColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId, char *colname, bool
									missingTableOk)
{
	char *qualifiedSequenceName = generate_qualified_relation_name(sequenceOid);
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	char *nextvalFunctionName = "nextval";
	bool useWorkerNextval = (pg_get_sequencedef(sequenceOid)->seqtypid != INT8OID);
	if (useWorkerNextval)
	{
		/*
		 * We use worker_nextval for int and smallint types.
		 * Check issue #5126 and PR #5254 for details.
		 * https://github.com/citusdata/citus/issues/5126
		 */
		nextvalFunctionName = "worker_nextval";
	}

	StringInfoData str = { 0 };
	initStringInfo(&str);

	appendStringInfo(&str, "ALTER TABLE ");

	if (missingTableOk)
	{
		appendStringInfo(&str, "IF EXISTS ");
	}

	appendStringInfo(&str, "%s ALTER COLUMN %s "
						   "SET DEFAULT %s(%s::regclass)",
					 qualifiedRelationName,
					 colname,
					 quote_qualified_identifier("pg_catalog", nextvalFunctionName),
					 quote_literal_cstr(qualifiedSequenceName));

	return str.data;
}


/*
 * GetAddColumnWithNextvalDefaultCmd returns a string representing:
 * ALTER TABLE ADD COLUMN .. DEFAULT nextval()
 * If sequence type is not bigint, we use worker_nextval() instead of nextval().
 */
static char *
GetAddColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId, char *colname,
								  TypeName *typeName, bool ifNotExists)
{
	char *qualifiedSequenceName = generate_qualified_relation_name(sequenceOid);
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	char *nextvalFunctionName = "nextval";
	bool useWorkerNextval = (pg_get_sequencedef(sequenceOid)->seqtypid != INT8OID);
	if (useWorkerNextval)
	{
		/*
		 * We use worker_nextval for int and smallint types.
		 * Check issue #5126 and PR #5254 for details.
		 * https://github.com/citusdata/citus/issues/5126
		 */
		nextvalFunctionName = "worker_nextval";
	}

	int32 typmod = 0;
	Oid typeOid = InvalidOid;
	bits16 formatFlags = FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY;
	typenameTypeIdAndMod(NULL, typeName, &typeOid, &typmod);

	StringInfoData str = { 0 };
	initStringInfo(&str);
	appendStringInfo(&str,
					 "ALTER TABLE %s ADD COLUMN %s %s %s "
					 "DEFAULT %s(%s::regclass)", qualifiedRelationName,
					 ifNotExists ? "IF NOT EXISTS" : "", colname,
					 format_type_extended(typeOid, typmod, formatFlags),
					 quote_qualified_identifier("pg_catalog", nextvalFunctionName),
					 quote_literal_cstr(qualifiedSequenceName));

	return str.data;
}


void
ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
									Constraint *constraint)
{
	ColumnDef *columnDefinition = (ColumnDef *) command->def;
	char *colName = columnDefinition->colname;
	char *errMsg =
		"cannot execute ADD COLUMN command with PRIMARY KEY, UNIQUE, FOREIGN and CHECK constraints";
	StringInfo errHint = makeStringInfo();
	appendStringInfo(errHint, "You can issue each command separately such as ");
	appendStringInfo(errHint,
					 "ALTER TABLE %s ADD COLUMN %s data_type; ALTER TABLE %s ADD CONSTRAINT constraint_name ",
					 get_rel_name(relationId),
					 colName, get_rel_name(relationId));

	if (constraint->contype == CONSTR_UNIQUE)
	{
		appendStringInfo(errHint, "UNIQUE (%s)", colName);
	}
	else if (constraint->contype == CONSTR_PRIMARY)
	{
		appendStringInfo(errHint, "PRIMARY KEY (%s)", colName);
	}
	else if (constraint->contype == CONSTR_CHECK)
	{
		appendStringInfo(errHint, "CHECK (check_expression)");
	}
	else if (constraint->contype == CONSTR_FOREIGN)
	{
		RangeVar *referencedTable = constraint->pktable;
		Oid referencedRelationId = RangeVarGetRelid(referencedTable, NoLock, false);

		appendStringInfo(errHint, "FOREIGN KEY (%s) REFERENCES %s", colName,
						 get_rel_name(referencedRelationId));

		if (list_length(constraint->pk_attrs) > 0)
		{
			char *referencedColumn = strVal(lfirst(list_head(constraint->pk_attrs)));
			appendStringInfo(errHint, "(%s)", referencedColumn);
		}

		if (constraint->fk_del_action == FKCONSTR_ACTION_SETNULL)
		{
			appendStringInfo(errHint, " %s", "ON DELETE SET NULL");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_CASCADE)
		{
			appendStringInfo(errHint, " %s", "ON DELETE CASCADE");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_SETDEFAULT)
		{
			appendStringInfo(errHint, " %s", "ON DELETE SET DEFAULT");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_RESTRICT)
		{
			appendStringInfo(errHint, " %s", "ON DELETE RESTRICT");
		}

		if (constraint->fk_upd_action == FKCONSTR_ACTION_SETNULL)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE SET NULL");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_CASCADE)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE CASCADE");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_SETDEFAULT)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE SET DEFAULT");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_RESTRICT)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE RESTRICT");
		}
	}

	appendStringInfo(errHint, "%s", ";");

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("%s", errMsg),
					errhint("%s", errHint->data),
					errdetail("Adding a column with a constraint in "
							  "one command is not supported because "
							  "all constraints in Citus must have "
							  "explicit names")));
}


/*
 * ErrorIfUnsupportedConstraint runs checks related to unique index / exclude
 * constraints.
 *
 * The function skips the uniqeness checks for reference tables (i.e., distribution
 * method is 'none').
 *
 * Forbid UNIQUE, PRIMARY KEY, or EXCLUDE constraints on append partitioned
 * tables, since currently there is no way of enforcing uniqueness for
 * overlapping shards.
 *
 * Similarly, do not allow such constraints if they do not include partition
 * column. This check is important for two reasons:
 * i. First, currently Citus does not enforce uniqueness constraint on multiple
 * shards.
 * ii. Second, INSERT INTO .. ON CONFLICT (i.e., UPSERT) queries can be executed
 * with no further check for constraints.
 */
void
ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
							 char referencingReplicationModel,
							 Var *distributionColumn, uint32 colocationId)
{
	/*
	 * We first perform check for foreign constraints. It is important to do this
	 * check before next check, because other types of constraints are allowed on
	 * reference tables and we return early for those constraints thanks to next
	 * check. Therefore, for reference tables, we first check for foreign constraints
	 * and if they are OK, we do not error out for other types of constraints.
	 */
	ErrorIfUnsupportedForeignConstraintExists(relation, distributionMethod,
											  referencingReplicationModel,
											  distributionColumn,
											  colocationId);

	/*
	 * Citus supports any kind of uniqueness constraints for reference tables
	 * given that they only consist of a single shard and we can simply rely on
	 * Postgres.
	 */
	if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		return;
	}

	if (distributionColumn == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("distribution column of distributed table is NULL")));
	}

	char *relationName = RelationGetRelationName(relation);
	List *indexOidList = RelationGetIndexList(relation);

	Oid indexOid = InvalidOid;
	foreach_oid(indexOid, indexOidList)
	{
		Relation indexDesc = index_open(indexOid, RowExclusiveLock);
		bool hasDistributionColumn = false;

		/* extract index key information from the index's pg_index info */
		IndexInfo *indexInfo = BuildIndexInfo(indexDesc);

		/* only check unique indexes and exclusion constraints. */
		if (indexInfo->ii_Unique == false && indexInfo->ii_ExclusionOps == NULL)
		{
			index_close(indexDesc, NoLock);
			continue;
		}

		/*
		 * Citus cannot enforce uniqueness/exclusion constraints with overlapping shards.
		 * Thus, emit a warning for unique indexes and exclusion constraints on
		 * append partitioned tables.
		 */
		if (distributionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							  errmsg("table \"%s\" has a UNIQUE or EXCLUDE constraint",
									 relationName),
							  errdetail("UNIQUE constraints, EXCLUDE constraints, "
										"and PRIMARY KEYs on "
										"append-partitioned tables cannot be enforced."),
							  errhint("Consider using hash partitioning.")));
		}

		if (AllowUnsafeConstraints)
		{
			/*
			 * The user explicitly wants to allow the constraint without
			 * distribution column.
			 */
			index_close(indexDesc, NoLock);
			continue;
		}

		int attributeCount = indexInfo->ii_NumIndexAttrs;
		AttrNumber *attributeNumberArray = indexInfo->ii_IndexAttrNumbers;

		for (int attributeIndex = 0; attributeIndex < attributeCount; attributeIndex++)
		{
			AttrNumber attributeNumber = attributeNumberArray[attributeIndex];

			if (distributionColumn->varattno != attributeNumber)
			{
				continue;
			}

			bool uniqueConstraint = indexInfo->ii_Unique;
			bool exclusionConstraintWithEquality = (indexInfo->ii_ExclusionOps != NULL &&
													OperatorImplementsEquality(
														indexInfo->ii_ExclusionOps[
															attributeIndex]));

			if (uniqueConstraint || exclusionConstraintWithEquality)
			{
				hasDistributionColumn = true;
				break;
			}
		}

		if (!hasDistributionColumn)
		{
			ereport(ERROR, (
						errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create constraint on \"%s\"",
							   relationName),
						errdetail("Distributed relations cannot have UNIQUE, "
								  "EXCLUDE, or PRIMARY KEY constraints that do not "
								  "include the partition column (with an equality "
								  "operator if EXCLUDE).")));
		}

		index_close(indexDesc, NoLock);
	}
}


/*
 * ErrorIfAlterTableDropTableNameFromPostgresFdw errors if given alter foreign table
 * option list drops 'table_name' from a postgresfdw foreign table which is
 * inside metadata.
 */
static void
ErrorIfAlterTableDropTableNameFromPostgresFdw(List *optionList, Oid relationId)
{
	char relationKind PG_USED_FOR_ASSERTS_ONLY =
		get_rel_relkind(relationId);
	Assert(relationKind == RELKIND_FOREIGN_TABLE);

	ForeignTable *foreignTable = GetForeignTable(relationId);
	Oid serverId = foreignTable->serverid;
	if (!ServerUsesPostgresFdw(serverId))
	{
		return;
	}

	if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE) &&
		ForeignTableDropsTableNameOption(optionList))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "alter foreign table alter options (drop table_name) command "
					 "is not allowed for Citus tables"),
				 errdetail(
					 "Table_name option can not be dropped from a foreign table "
					 "which is inside metadata."),
				 errhint(
					 "Try to undistribute foreign table before dropping table_name option.")));
	}
}


/*
 * ErrorIfUnsupportedAlterTableStmt checks if the corresponding alter table
 * statement is supported for distributed tables and errors out if it is not.
 * Currently, only the following commands are supported.
 *
 * ALTER TABLE ADD|DROP COLUMN
 * ALTER TABLE ALTER COLUMN SET DATA TYPE
 * ALTER TABLE SET|DROP NOT NULL
 * ALTER TABLE SET|DROP DEFAULT
 * ALTER TABLE ADD|DROP CONSTRAINT
 * ALTER TABLE REPLICA IDENTITY
 * ALTER TABLE SET ()
 * ALTER TABLE ENABLE|DISABLE|NO FORCE|FORCE ROW LEVEL SECURITY
 * ALTER TABLE RESET ()
 * ALTER TABLE ENABLE/DISABLE TRIGGER (if enable_unsafe_triggers is not set, we only support triggers for citus local tables)
 */
static void
ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement)
{
	List *commandList = alterTableStatement->cmds;

	LOCKMODE lockmode = AlterTableGetLockLevel(commandList);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

	/* error out if any of the subcommands are unsupported */
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		switch (alterTableType)
		{
			case AT_AddColumn:
			{
				if (IsA(command->def, ColumnDef))
				{
					ColumnDef *column = (ColumnDef *) command->def;

					/*
					 * Check for SERIAL pseudo-types. The structure of this
					 * check is copied from transformColumnDefinition.
					 */
					if (column->typeName && list_length(column->typeName->names) == 1 &&
						!column->typeName->pct_type)
					{
						char *typeName = strVal(linitial(column->typeName->names));

						if (strcmp(typeName, "smallserial") == 0 ||
							strcmp(typeName, "serial2") == 0 ||
							strcmp(typeName, "serial") == 0 ||
							strcmp(typeName, "serial4") == 0 ||
							strcmp(typeName, "bigserial") == 0 ||
							strcmp(typeName, "serial8") == 0)
						{
							/*
							 * We currently don't support adding a serial column for an MX table
							 * TODO: record the dependency in the workers
							 */
							if (ShouldSyncTableMetadata(relationId))
							{
								ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg(
													"cannot execute ADD COLUMN commands involving serial"
													" pseudotypes when metadata is synchronized to workers")));
							}

							/*
							 * we only allow adding a serial column if it is the only subcommand
							 * and it has no constraints
							 */
							if (commandList->length > 1 || column->constraints)
							{
								ereport(ERROR, (errcode(
													ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg(
													"cannot execute ADD COLUMN commands involving "
													"serial pseudotypes with other subcommands/constraints"),
												errhint(
													"You can issue each subcommand separately")));
							}

							/*
							 * Currently we don't support backfilling the new column with default values
							 * if the table is not empty
							 */
							if (!TableEmpty(relationId))
							{
								ereport(ERROR, (errcode(
													ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg(
													"Cannot add a column involving serial pseudotypes "
													"because the table is not empty"),
												errhint(
													"You can first call ALTER TABLE .. ADD COLUMN .. smallint/int/bigint\n"
													"Then set the default by ALTER TABLE .. ALTER COLUMN .. SET DEFAULT nextval('..')")));
							}
						}
					}


					Constraint *columnConstraint = NULL;
					foreach_ptr(columnConstraint, column->constraints)
					{
						if (columnConstraint->contype == CONSTR_IDENTITY)
						{
							/*
							 * We currently don't support adding an identity column for an MX table
							 */
							if (ShouldSyncTableMetadata(relationId))
							{
								ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg(
													"cannot execute ADD COLUMN commands involving identity"
													" columns when metadata is synchronized to workers")));
							}

							/*
							 * Currently we don't support backfilling the new identity column with default values
							 * if the table is not empty
							 */
							if (!TableEmpty(relationId))
							{
								ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
												errmsg(
													"Cannot add an identity column because the table is not empty")));
							}
						}
					}


					List *columnConstraints = column->constraints;

					Constraint *constraint = NULL;
					foreach_ptr(constraint, columnConstraints)
					{
						if (constraint->contype == CONSTR_DEFAULT)
						{
							if (constraint->raw_expr != NULL)
							{
								ParseState *pstate = make_parsestate(NULL);
								Node *expr = transformExpr(pstate, constraint->raw_expr,
														   EXPR_KIND_COLUMN_DEFAULT);

								if (contain_nextval_expression_walker(expr, NULL))
								{
									/*
									 * we only allow adding a column with non_const default
									 * if its the only subcommand and has no other constraints
									 */
									if (commandList->length > 1 ||
										columnConstraints->length > 1)
									{
										ereport(ERROR, (errcode(
															ERRCODE_FEATURE_NOT_SUPPORTED),
														errmsg(
															"cannot execute ADD COLUMN .. DEFAULT nextval('..')"
															" command with other subcommands/constraints"),
														errhint(
															"You can issue each subcommand separately")));
									}

									/*
									 * Currently we don't support backfilling the new column with default values
									 * if the table is not empty
									 */
									if (!TableEmpty(relationId))
									{
										ereport(ERROR, (errcode(
															ERRCODE_FEATURE_NOT_SUPPORTED),
														errmsg(
															"cannot add a column involving DEFAULT nextval('..') "
															"because the table is not empty"),
														errhint(
															"You can first call ALTER TABLE .. ADD COLUMN .. smallint/int/bigint\n"
															"Then set the default by ALTER TABLE .. ALTER COLUMN .. SET DEFAULT nextval('..')")));
									}
								}
							}
						}
					}
				}

				break;
			}

			case AT_ColumnDefault:
			{
				if (AlterInvolvesPartitionColumn(alterTableStatement, command))
				{
					ereport(ERROR, (errmsg("cannot execute ALTER TABLE command "
										   "involving partition column")));
				}

				ParseState *pstate = make_parsestate(NULL);
				Node *expr = transformExpr(pstate, command->def,
										   EXPR_KIND_COLUMN_DEFAULT);

				if (contain_nextval_expression_walker(expr, NULL))
				{
					/*
					 * we only allow altering a column's default to non_const expr
					 * if its the only subcommand
					 */
					if (commandList->length > 1)
					{
						ereport(ERROR, (errcode(
											ERRCODE_FEATURE_NOT_SUPPORTED),
										errmsg(
											"cannot execute ALTER COLUMN COLUMN .. SET DEFAULT "
											"nextval('..') command with other subcommands"),
										errhint(
											"You can issue each subcommand separately")));
					}
				}

				break;
			}

			case AT_AlterColumnType:
			{
				if (AlterInvolvesPartitionColumn(alterTableStatement, command))
				{
					ereport(ERROR, (errmsg("cannot execute ALTER TABLE command "
										   "involving partition column")));
				}

				/*
				 * We check for ALTER COLUMN TYPE ...
				 * if the column is an identity column,
				 * changing the type of the column
				 * should not be allowed for now
				 */
				if (AlterColumnInvolvesIdentityColumn(alterTableStatement, command))
				{
					ereport(ERROR, (errmsg("cannot execute ALTER COLUMN command "
										   "involving identity column")));
				}

				/*
				 * We check for ALTER COLUMN TYPE ...
				 * if the column has default coming from a user-defined sequence
				 * changing the type of the column
				 * should not be allowed for now
				 */
				AttrNumber attnum = get_attnum(relationId, command->name);
				List *seqInfoList = NIL;
				GetDependentSequencesWithRelation(relationId, &seqInfoList, attnum,
												  DEPENDENCY_AUTO);
				if (seqInfoList != NIL)
				{
					ereport(ERROR, (errmsg("cannot execute ALTER COLUMN TYPE .. command "
										   "because the column involves a default coming "
										   "from a sequence")));
				}
				break;
			}

			case AT_DropColumn:
			case AT_DropNotNull:
			{
				if (AlterInvolvesPartitionColumn(alterTableStatement, command))
				{
					ereport(ERROR, (errmsg("cannot execute ALTER TABLE command "
										   "involving partition column")));
				}
				break;
			}

			case AT_AddConstraint:
			{
				/* we only allow constraints if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot execute ADD CONSTRAINT command with "
										   "other subcommands"),
									errhint("You can issue each subcommand separately")));
				}

				break;
			}

			case AT_AttachPartition:
			{
				PartitionCmd *partitionCommand = (PartitionCmd *) command->def;
				bool missingOK = false;
				Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name,
														   lockmode, missingOK);

				/* we only allow partitioning commands if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot execute ATTACH PARTITION command "
										   "with other subcommands"),
									errhint("You can issue each subcommand "
											"separately.")));
				}

				if (IsCitusTableType(partitionRelationId, CITUS_LOCAL_TABLE) ||
					IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
				{
					/*
					 * Citus Local Tables cannot be colocated with other tables.
					 * If either of two relations is not a Citus Local Table, then we
					 * don't need to check colocation since CreateCitusLocalTable would
					 * anyway throw an error.
					 */
					break;
				}

				if (IsCitusTable(partitionRelationId) &&
					!TablesColocated(relationId, partitionRelationId))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("distributed tables cannot have "
										   "non-colocated distributed tables as a "
										   "partition ")));
				}

				break;
			}

			case AT_DetachPartitionFinalize:
			{
				ereport(ERROR, (errmsg("ALTER TABLE .. DETACH PARTITION .. FINALIZE "
									   "commands are currently unsupported.")));
				break;
			}

			case AT_DetachPartition:
			{
				/* we only allow partitioning commands if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot execute DETACH PARTITION command "
										   "with other subcommands"),
									errhint("You can issue each subcommand "
											"separately.")));
				}

				PartitionCmd *partitionCommand = (PartitionCmd *) command->def;

				if (partitionCommand->concurrent)
				{
					ereport(ERROR, (errmsg("ALTER TABLE .. DETACH PARTITION .. "
										   "CONCURRENTLY commands are currently "
										   "unsupported.")));
				}

				break;
			}

			case AT_EnableTrig:
			case AT_EnableAlwaysTrig:
			case AT_EnableReplicaTrig:
			case AT_EnableTrigUser:
			case AT_DisableTrig:
			case AT_DisableTrigUser:
			case AT_EnableTrigAll:
			case AT_DisableTrigAll:
			{
				/*
				 * Postgres already does not allow executing ALTER TABLE TRIGGER
				 * commands with other subcommands, but let's be on the safe side.
				 */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("cannot execute ENABLE/DISABLE TRIGGER "
										   "command with other subcommands"),
									errhint("You can issue each subcommand separately")));
				}

				ErrorOutForTriggerIfNotSupported(relationId);

				break;
			}

#if PG_VERSION_NUM >= PG_VERSION_15
			case AT_SetAccessMethod:
#endif
			case AT_SetNotNull:
			case AT_ReplicaIdentity:
			case AT_ChangeOwner:
			case AT_EnableRowSecurity:
			case AT_DisableRowSecurity:
			case AT_ForceRowSecurity:
			case AT_NoForceRowSecurity:
			case AT_ValidateConstraint:
			case AT_DropConstraint: /* we do the check for invalidation in AlterTableDropsForeignKey */
			case AT_SetCompression:
			{
				/*
				 * We will not perform any special check for:
				 * ALTER TABLE .. SET ACCESS METHOD ..
				 * ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
				 * ALTER TABLE .. REPLICA IDENTITY ..
				 * ALTER TABLE .. VALIDATE CONSTRAINT ..
				 * ALTER TABLE .. ALTER COLUMN .. SET COMPRESSION ..
				 */
				break;
			}

			case AT_SetRelOptions:  /* SET (...) */
			case AT_ResetRelOptions:    /* RESET (...) */
			case AT_ReplaceRelOptions:  /* replace entire option list */
			case AT_SetLogged:
			case AT_SetUnLogged:
			{
				/* this command is supported by Citus */
				break;
			}

			case AT_GenericOptions:
			{
				if (IsForeignTable(relationId))
				{
					List *optionList = (List *) command->def;
					ErrorIfAlterTableDropTableNameFromPostgresFdw(optionList, relationId);
					break;
				}
			}

			/* fallthrough */

			default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("alter table command is currently unsupported"),
						 errdetail("Only ADD|DROP COLUMN, SET|DROP NOT NULL, "
								   "SET|DROP DEFAULT, ADD|DROP|VALIDATE CONSTRAINT, "
								   "SET (), RESET (), "
								   "ENABLE|DISABLE|NO FORCE|FORCE ROW LEVEL SECURITY, "
								   "ATTACH|DETACH PARTITION and TYPE subcommands "
								   "are supported.")));
			}
		}
	}
}


/*
 * SetupExecutionModeForAlterTable is the function that is responsible
 * for two things for practical purpose for not doing the same checks
 * twice:
 *     (a) For any command, decide and return whether we should
 *         run the command in sequential mode
 *     (b) For commands in a transaction block, set the transaction local
 *         multi-shard modify mode to sequential when necessary
 *
 * The commands that operate on the same reference table shard in parallel
 * is in the interest of (a), where the return value indicates the executor
 * to run the command sequentially to prevent self-deadlocks.
 *
 * The commands that both operate on the same reference table shard in parallel
 * and cascades to run any parallel operation is in the interest of (b). By
 * setting the multi-shard mode, we ensure that the cascading parallel commands
 * are executed sequentially to prevent self-deadlocks.
 *
 * One final note on the function is that if the function decides to execute
 * the command in sequential mode, and a parallel command has already been
 * executed in the same transaction, the function errors out. See the comment
 * in the function for the rationale.
 */
static bool
SetupExecutionModeForAlterTable(Oid relationId, AlterTableCmd *command)
{
	bool executeSequentially = false;
	AlterTableType alterTableType = command->subtype;
	if (alterTableType == AT_DropConstraint)
	{
		char *constraintName = command->name;
		if (ConstraintIsAForeignKeyToReferenceTable(constraintName, relationId))
		{
			executeSequentially = true;
		}
	}
	else if (alterTableType == AT_AddColumn)
	{
		ColumnDef *columnDefinition = (ColumnDef *) command->def;
		List *columnConstraints = columnDefinition->constraints;

		Constraint *constraint = NULL;
		foreach_ptr(constraint, columnConstraints)
		{
			if (constraint->contype == CONSTR_FOREIGN)
			{
				Oid rightRelationId = RangeVarGetRelid(constraint->pktable, NoLock,
													   false);
				if (IsCitusTableType(rightRelationId, REFERENCE_TABLE))
				{
					executeSequentially = true;
				}
			}
		}
	}
	else if (alterTableType == AT_DropColumn || alterTableType == AT_AlterColumnType)
	{
		char *affectedColumnName = command->name;

		if (ColumnAppearsInForeignKeyToReferenceTable(affectedColumnName,
													  relationId))
		{
			if (alterTableType == AT_AlterColumnType)
			{
				SetLocalMultiShardModifyModeToSequential();
			}

			executeSequentially = true;
		}
	}
	else if (alterTableType == AT_AddConstraint)
	{
		/*
		 * We need to execute the ddls working with reference tables on the
		 * right side sequentially, because parallel ddl operations
		 * relating to one and only shard of a reference table on a worker
		 * may cause self-deadlocks.
		 */
		Constraint *constraint = (Constraint *) command->def;
		if (constraint->contype == CONSTR_FOREIGN)
		{
			Oid rightRelationId = RangeVarGetRelid(constraint->pktable, NoLock,
												   false);
			if (IsCitusTableType(rightRelationId, REFERENCE_TABLE))
			{
				executeSequentially = true;
			}
		}
	}
	else if (alterTableType == AT_DetachPartition || alterTableType == AT_AttachPartition)
	{
		/* check if there are foreign constraints to reference tables */
		if (HasForeignKeyToReferenceTable(relationId))
		{
			executeSequentially = true;
		}
	}

	/*
	 * If there has already been a parallel query executed, the sequential mode
	 * would still use the already opened parallel connections to the workers for
	 * the distributed tables, thus contradicting our purpose of using
	 * sequential mode.
	 */
	if (executeSequentially &&
		HasDistributionKey(relationId) &&
		ParallelQueryExecutedInTransaction())
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errmsg("cannot modify table \"%s\" because there "
							   "was a parallel operation on a distributed table "
							   "in the transaction", relationName),
						errdetail("When there is a foreign key to a reference "
								  "table, Citus needs to perform all operations "
								  "over a single connection per node to ensure "
								  "consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	return executeSequentially;
}


/*
 * InterShardDDLTaskList builds a list of tasks to execute a inter shard DDL command on a
 * shards of given list of distributed table. At the moment this function is used to run
 * foreign key, partitioning and attaching partition index command on worker node.
 *
 * leftRelationId is the relation id of actual distributed table which given command is
 * applied. rightRelationId is the relation id of either index or distributed table which
 * given command refers to.
 */
List *
InterShardDDLTaskList(Oid leftRelationId, Oid rightRelationId,
					  const char *commandString)
{
	List *leftShardList = LoadShardIntervalList(leftRelationId);
	List *rightShardList = CreateRightShardListForInterShardDDLTask(rightRelationId,
																	leftRelationId,
																	leftShardList);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(leftShardList, ShareLock);

	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	Oid leftSchemaId = get_rel_namespace(leftRelationId);
	char *leftSchemaName = get_namespace_name(leftSchemaId);
	char *escapedLeftSchemaName = quote_literal_cstr(leftSchemaName);

	Oid rightSchemaId = get_rel_namespace(rightRelationId);
	char *rightSchemaName = get_namespace_name(rightSchemaId);
	char *escapedRightSchemaName = quote_literal_cstr(rightSchemaName);

	char *escapedCommandString = quote_literal_cstr(commandString);

	List *taskList = NIL;

	ShardInterval *leftShardInterval = NULL;
	ShardInterval *rightShardInterval = NULL;
	forboth_ptr(leftShardInterval, leftShardList, rightShardInterval, rightShardList)
	{
		uint64 leftShardId = leftShardInterval->shardId;
		uint64 rightShardId = rightShardInterval->shardId;

		StringInfo applyCommand = makeStringInfo();
		appendStringInfo(applyCommand, WORKER_APPLY_INTER_SHARD_DDL_COMMAND,
						 leftShardId, escapedLeftSchemaName, rightShardId,
						 escapedRightSchemaName, escapedCommandString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, applyCommand->data);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = leftShardId;
		SetInterShardDDLTaskPlacementList(task, leftShardInterval, rightShardInterval);
		SetInterShardDDLTaskRelationShardList(task, leftShardInterval,
											  rightShardInterval);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * CreateRightShardListForInterShardDDLTask is a helper function that creates
 * shard list for the right relation for InterShardDDLTaskList.
 */
static List *
CreateRightShardListForInterShardDDLTask(Oid rightRelationId, Oid leftRelationId,
										 List *leftShardList)
{
	List *rightShardList = LoadShardIntervalList(rightRelationId);


	if (!IsCitusTableType(leftRelationId, CITUS_LOCAL_TABLE) &&
		IsCitusTableType(rightRelationId, REFERENCE_TABLE))
	{
		/*
		 * If the right relation is a reference table and left relation is not
		 * a citus local table, we need to make sure that the tasks are created
		 * in a way that the right shard stays the same since we only have one
		 * placement per worker.
		 * If left relation is a citus local table, then we don't need to populate
		 * reference table shards as we will execute ADD/DROP constraint command
		 * only for coordinator placement of reference table.
		 */
		ShardInterval *rightShard = (ShardInterval *) linitial(rightShardList);
		int leftShardCount = list_length(leftShardList);
		rightShardList = GenerateListFromElement(rightShard, leftShardCount);
	}

	return rightShardList;
}


/*
 * SetInterShardDDLTaskPlacementList sets taskPlacementList field of given
 * inter-shard DDL task according to passed shard interval arguments.
 */
static void
SetInterShardDDLTaskPlacementList(Task *task, ShardInterval *leftShardInterval,
								  ShardInterval *rightShardInterval)
{
	uint64 leftShardId = leftShardInterval->shardId;
	List *leftShardPlacementList = ActiveShardPlacementList(leftShardId);

	uint64 rightShardId = rightShardInterval->shardId;
	List *rightShardPlacementList = ActiveShardPlacementList(rightShardId);

	List *intersectedPlacementList = NIL;

	ShardPlacement *leftShardPlacement = NULL;
	foreach_ptr(leftShardPlacement, leftShardPlacementList)
	{
		ShardPlacement *rightShardPlacement = NULL;
		foreach_ptr(rightShardPlacement, rightShardPlacementList)
		{
			if (leftShardPlacement->nodeId == rightShardPlacement->nodeId)
			{
				intersectedPlacementList = lappend(intersectedPlacementList,
												   leftShardPlacement);
			}
		}
	}

	task->taskPlacementList = intersectedPlacementList;
}


/*
 * SetInterShardDDLTaskRelationShardList sets relationShardList field of given
 * inter-shard DDL task according to passed shard interval arguments.
 */
static void
SetInterShardDDLTaskRelationShardList(Task *task, ShardInterval *leftShardInterval,
									  ShardInterval *rightShardInterval)
{
	RelationShard *leftRelationShard = CitusMakeNode(RelationShard);
	leftRelationShard->relationId = leftShardInterval->relationId;
	leftRelationShard->shardId = leftShardInterval->shardId;

	RelationShard *rightRelationShard = CitusMakeNode(RelationShard);
	rightRelationShard->relationId = rightShardInterval->relationId;
	rightRelationShard->shardId = rightShardInterval->shardId;

	task->relationShardList = list_make2(leftRelationShard, rightRelationShard);
}


/*
 * AlterColumnInvolvesIdentityColumn checks if the given alter column command
 * involves relation's identity column.
 */
static bool
AlterColumnInvolvesIdentityColumn(AlterTableStmt *alterTableStatement,
								  AlterTableCmd *command)
{
	bool involvesIdentityColumn = false;
	char *alterColumnName = command->name;

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(relationId))
	{
		return false;
	}

	HeapTuple tuple = SearchSysCacheAttName(relationId, alterColumnName);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_attribute targetAttr = (Form_pg_attribute) GETSTRUCT(tuple);

		if (targetAttr->attidentity)
		{
			involvesIdentityColumn = true;
		}

		ReleaseSysCache(tuple);
	}

	return involvesIdentityColumn;
}


/*
 * AlterInvolvesPartitionColumn checks if the given alter table command
 * involves relation's partition column.
 */
static bool
AlterInvolvesPartitionColumn(AlterTableStmt *alterTableStatement,
							 AlterTableCmd *command)
{
	bool involvesPartitionColumn = false;
	char *alterColumnName = command->name;

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(relationId))
	{
		return false;
	}

	Var *partitionColumn = DistPartitionKey(relationId);

	HeapTuple tuple = SearchSysCacheAttName(relationId, alterColumnName);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_attribute targetAttr = (Form_pg_attribute) GETSTRUCT(tuple);

		/* reference tables do not have partition column, so allow them */
		if (partitionColumn != NULL &&
			targetAttr->attnum == partitionColumn->varattno)
		{
			involvesPartitionColumn = true;
		}

		ReleaseSysCache(tuple);
	}

	return involvesPartitionColumn;
}


/*
 * ErrorIfUnsupportedAlterAddConstraintStmt runs the constraint checks on distributed
 * table using the same logic with create_distributed_table.
 */
static void
ErrorIfUnsupportedAlterAddConstraintStmt(AlterTableStmt *alterTableStatement)
{
	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	char distributionMethod = PartitionMethod(relationId);
	char referencingReplicationModel = TableReplicationModel(relationId);
	Var *distributionColumn = DistPartitionKey(relationId);
	uint32 colocationId = TableColocationId(relationId);
	Relation relation = relation_open(relationId, ExclusiveLock);

	ErrorIfUnsupportedConstraint(relation, distributionMethod,
								 referencingReplicationModel,
								 distributionColumn, colocationId);
	relation_close(relation, NoLock);
}


/*
 * AlterTableSchemaStmtObjectAddress returns the ObjectAddress of the table that
 * is the object of the AlterObjectSchemaStmt.
 *
 * This could be called both before or after it has been applied locally. It will
 * look in the old schema first, if the table cannot be found in that schema it
 * will look in the new schema. Errors if missing_ok is false and the table cannot
 * be found in either of the schemas.
 */
List *
AlterTableSchemaStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	const char *tableName = stmt->relation->relname;
	Oid tableOid = InvalidOid;
	if (stmt->relation->schemaname)
	{
		const char *schemaName = stmt->relation->schemaname;
		Oid schemaOid = get_namespace_oid(schemaName, missing_ok);
		tableOid = get_relname_relid(tableName, schemaOid);
	}
	else
	{
		tableOid = RelnameGetRelid(stmt->relation->relname);
	}

	if (tableOid == InvalidOid)
	{
		const char *newSchemaName = stmt->newschema;
		Oid newSchemaOid = get_namespace_oid(newSchemaName, true);
		tableOid = get_relname_relid(tableName, newSchemaOid);

		if (!missing_ok && tableOid == InvalidOid)
		{
			const char *quotedTableName =
				quote_qualified_identifier(stmt->relation->schemaname, tableName);

			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
							errmsg("relation \"%s\" does not exist",
								   quotedTableName)));
		}
	}

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, RelationRelationId, tableOid);

	return list_make1(address);
}


/*
 * MakeNameListFromRangeVar makes a namelist from a RangeVar. Its behaviour
 * should be the exact opposite of postgres' makeRangeVarFromNameList.
 */
List *
MakeNameListFromRangeVar(const RangeVar *rel)
{
	if (rel->catalogname != NULL)
	{
		Assert(rel->schemaname != NULL);
		Assert(rel->relname != NULL);
		return list_make3(makeString(rel->catalogname),
						  makeString(rel->schemaname),
						  makeString(rel->relname));
	}
	else if (rel->schemaname != NULL)
	{
		Assert(rel->relname != NULL);
		return list_make2(makeString(rel->schemaname),
						  makeString(rel->relname));
	}
	else
	{
		Assert(rel->relname != NULL);
		return list_make1(makeString(rel->relname));
	}
}


/*
 * ErrorIfTableHasIdentityColumn errors out if the given table has identity column
 */
void
ErrorIfTableHasIdentityColumn(Oid relationId)
{
	Relation relation = relation_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attidentity)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"cannot complete operation on a table with identity column")));
		}
	}

	relation_close(relation, NoLock);
}


/*
 * ConvertNewTableIfNecessary converts the given table to a tenant schema
 * table or a Citus managed table if necessary.
 *
 * Input node is expected to be a CreateStmt or a CreateTableAsStmt.
 */
void
ConvertNewTableIfNecessary(Node *createStmt)
{
	/*
	 * Need to increment command counter so that next command
	 * can see the new table.
	 */
	CommandCounterIncrement();

	if (IsA(createStmt, CreateTableAsStmt))
	{
		CreateTableAsStmt *createTableAsStmt = (CreateTableAsStmt *) createStmt;

		bool missingOk = false;
		Oid createdRelationId = RangeVarGetRelid(createTableAsStmt->into->rel,
												 NoLock, missingOk);

		if (ShouldCreateTenantSchemaTable(createdRelationId))
		{
			/* not try to convert the table if it already exists and IF NOT EXISTS syntax is used */
			if (createTableAsStmt->if_not_exists && IsCitusTable(createdRelationId))
			{
				return;
			}

			/*
			 * We allow mat views in a distributed schema but do not make them a tenant
			 * table. We should skip converting them.
			 */
			if (get_rel_relkind(createdRelationId) == RELKIND_MATVIEW)
			{
				return;
			}

			CreateTenantSchemaTable(createdRelationId);
		}

		/*
		 * We simply ignore the tables created by using that syntax when using
		 * Citus managed tables.
		 */
		return;
	}

	CreateStmt *baseCreateTableStmt = (CreateStmt *) createStmt;

	bool missingOk = false;
	Oid createdRelationId = RangeVarGetRelid(baseCreateTableStmt->relation,
											 NoLock, missingOk);

	/* not try to convert the table if it already exists and IF NOT EXISTS syntax is used */
	if (baseCreateTableStmt->if_not_exists && IsCitusTable(createdRelationId))
	{
		return;
	}

	/*
	 * Check ShouldCreateTenantSchemaTable() before ShouldAddNewTableToMetadata()
	 * because we don't want to unnecessarily add the table into metadata
	 * (as a Citus managed table) before distributing it as a tenant table.
	 */
	if (ShouldCreateTenantSchemaTable(createdRelationId))
	{
		/*
		 * We skip creating tenant schema table if the table is a partition
		 * table because in that case PostprocessCreateTableStmt() should've
		 * already created a tenant schema table from the partition table.
		 */
		if (!PartitionTable(createdRelationId))
		{
			CreateTenantSchemaTable(createdRelationId);
		}
	}
	else if (ShouldAddNewTableToMetadata(createdRelationId))
	{
		/*
		 * Here we set autoConverted to false, since the user explicitly
		 * wants these tables to be added to metadata, by setting the
		 * GUC use_citus_managed_tables to true.
		 */
		bool autoConverted = false;
		bool cascade = true;
		CreateCitusLocalTable(createdRelationId, cascade, autoConverted);
	}
}


/*
 * ConvertToTenantTableIfNecessary converts given relation to a tenant table if its
 * schema changed to a distributed schema.
 */
void
ConvertToTenantTableIfNecessary(AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	if (!IsCoordinator())
	{
		return;
	}

	/*
	 * We will let Postgres deal with missing_ok
	 */
	List *tableAddresses = GetObjectAddressListFromParseTree((Node *) stmt, true, true);

	/*  the code-path only supports a single object */
	Assert(list_length(tableAddresses) == 1);

	/* We have already asserted that we have exactly 1 address in the addresses. */
	ObjectAddress *tableAddress = linitial(tableAddresses);
	char relKind = get_rel_relkind(tableAddress->objectId);
	if (relKind == RELKIND_SEQUENCE || relKind == RELKIND_VIEW)
	{
		return;
	}

	Oid relationId = tableAddress->objectId;
	Oid schemaId = get_namespace_oid(stmt->newschema, stmt->missing_ok);
	if (!OidIsValid(schemaId))
	{
		return;
	}

	/*
	 * Make table a tenant table when its schema actually changed. When its schema
	 * is not changed as in `ALTER TABLE <tbl> SET SCHEMA <same_schema>`, we detect
	 * that by seeing the table is still a single shard table. (i.e. not undistributed
	 * at `preprocess` step)
	 */
	if (!IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED) &&
		IsTenantSchema(schemaId))
	{
		EnsureTenantTable(relationId, "ALTER TABLE SET SCHEMA");

		char *schemaName = get_namespace_name(schemaId);
		char *tableName = stmt->relation->relname;
		ereport(NOTICE, (errmsg("Moving %s into distributed schema %s",
								tableName, schemaName)));

		CreateTenantSchemaTable(relationId);
	}
}
