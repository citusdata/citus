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
#include "distributed/pg_version_constants.h"
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
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "distributed/worker_shard_visibility.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* controlled via GUC, should be accessed via GetEnableLocalReferenceForeignKeys() */
bool EnableLocalReferenceForeignKeys = true;


/* Local functions forward declarations for unsupported command checks */
static void PostprocessCreateTableStmtForeignKeys(CreateStmt *createStatement);
static void PostprocessCreateTableStmtPartitionOf(CreateStmt *createStatement,
												  const char *queryString);
static bool AlterTableDefinesFKeyBetweenPostgresAndNonDistTable(
	AlterTableStmt *alterTableStatement);
static bool RelationIdListContainsCitusTableType(List *relationIdList,
												 CitusTableType citusTableType);
static bool RelationIdListContainsPostgresTable(List *relationIdList);
static void ConvertPostgresLocalTablesToCitusLocalTables(
	AlterTableStmt *alterTableStatement);
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
static void ErrorIfCitusLocalTablePartitionCommand(AlterTableCmd *alterTableCmd,
												   Oid parentRelationId);
static Oid GetPartitionCommandChildRelationId(AlterTableCmd *alterTableCmd,
											  bool missingOk);
static List * InterShardDDLTaskList(Oid leftRelationId, Oid rightRelationId,
									const char *commandString);
static bool AlterInvolvesPartitionColumn(AlterTableStmt *alterTableStatement,
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
static char * GetAlterColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId,
												  char *colname);
static char * GetAddColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId,
												char *colname, TypeName *typeName);


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

		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			/* prevent concurrent EnsureReferenceTablesExistOnAllNodes */
			int colocationId = CreateReferenceTableColocationId();
			LockColocationId(colocationId, ExclusiveLock);
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
			RangeVar *parentRelation = NULL;
			foreach_ptr(parentRelation, createStatement->inhRelations)
			{
				bool missingOk = false;
				Oid parentRelationId = RangeVarGetRelid(parentRelation, NoLock,
														missingOk);
				Assert(parentRelationId != InvalidOid);

				if (IsCitusTable(parentRelationId))
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

	/*
	 * If a partition is being created and if its parent is a distributed
	 * table, we will distribute this table as well.
	 */
	if (IsCitusTable(parentRelationId))
	{
		Var *parentDistributionColumn = DistPartitionKeyOrError(parentRelationId);
		char parentDistributionMethod = DISTRIBUTE_BY_HASH;
		char *parentRelationName = generate_qualified_relation_name(parentRelationId);
		bool viaDeprecatedAPI = false;

		SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong(parentRelationId,
																  relationId);

		CreateDistributedTable(relationId, parentDistributionColumn,
							   parentDistributionMethod, ShardCount, false,
							   parentRelationName, viaDeprecatedAPI);
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
			Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
			PartitionCmd *partitionCommand = (PartitionCmd *) alterTableCommand->def;
			bool partitionMissingOk = false;
			Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name, lockmode,
													   partitionMissingOk);

			/*
			 * If user first distributes the table then tries to attach it to non
			 * distributed table, we error out.
			 */
			if (!IsCitusTable(relationId) &&
				IsCitusTable(partitionRelationId))
			{
				char *parentRelationName = get_rel_name(relationId);

				ereport(ERROR, (errmsg("non-distributed tables cannot have "
									   "distributed partitions"),
								errhint("Distribute the partitioned table \"%s\" "
										"instead", parentRelationName)));
			}

			/* if parent of this table is distributed, distribute this table too */
			if (IsCitusTable(relationId) &&
				!IsCitusTable(partitionRelationId))
			{
				Var *distributionColumn = DistPartitionKeyOrError(relationId);
				char *distributionColumnName = ColumnToColumnName(relationId,
																  nodeToString(
																	  distributionColumn));
				distributionColumn = FindColumnWithNameOnTargetRelation(relationId,
																		distributionColumnName,
																		partitionRelationId);

				char distributionMethod = DISTRIBUTE_BY_HASH;
				char *parentRelationName = generate_qualified_relation_name(relationId);
				bool viaDeprecatedAPI = false;

				SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong(
					relationId, partitionRelationId);

				CreateDistributedTable(partitionRelationId, distributionColumn,
									   distributionMethod, ShardCount, false,
									   parentRelationName, viaDeprecatedAPI);
			}
		}
	}

	return NIL;
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
	Assert(stmt->objectType == OBJECT_TABLE);

	/*
	 * We will let Postgres deal with missing_ok
	 */
	ObjectAddress tableAddress = GetObjectAddressFromParseTree((Node *) stmt, true);

	/* check whether we are dealing with a sequence here */
	if (get_rel_relkind(tableAddress.objectId) == RELKIND_SEQUENCE)
	{
		stmt->objectType = OBJECT_SEQUENCE;
		return PostprocessAlterSequenceSchemaStmt((Node *) stmt, queryString);
	}

	if (!ShouldPropagate() || !IsCitusTable(tableAddress.objectId))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&tableAddress);

	return NIL;
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
	 * check whether we are dealing with a sequence here
	 * if yes, it must be ALTER TABLE .. OWNER TO .. command
	 * since this is the only ALTER command of a sequence that
	 * passes through an AlterTableStmt
	 */
	if (get_rel_relkind(leftRelationId) == RELKIND_SEQUENCE)
	{
		AlterTableStmt *stmtCopy = copyObject(alterTableStatement);
		AlterTableStmtObjType_compat(stmtCopy) = OBJECT_SEQUENCE;
		return PreprocessAlterSequenceOwnerStmt((Node *) stmtCopy, alterTableCommand,
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
		AlterTableDefinesFKeyBetweenPostgresAndNonDistTable(alterTableStatement))
	{
		/*
		 * We don't process subcommands generated by postgres.
		 * This is mainly because postgres started to issue ALTER TABLE commands
		 * for some set of objects that are defined via CREATE TABLE commands as
		 * of pg13. However, citus already has a separate logic for CREATE TABLE
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

				/*
				 * Foreign constraint validations will be done in workers. If we do not
				 * set this flag, PostgreSQL tries to do additional checking when we drop
				 * to standard_ProcessUtility. standard_ProcessUtility tries to open new
				 * connections to workers to verify foreign constraints while original
				 * transaction is in process, which causes deadlock.
				 */
				constraint->skip_validation = true;
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
		}
		else if (alterTableType == AT_AddColumn)
		{
			/*
			 * TODO: This code path is nothing beneficial since we do not
			 * support ALTER TABLE %s ADD COLUMN %s [constraint] for foreign keys.
			 * However, the code is kept in case we fix the constraint
			 * creation without a name and allow foreign key creation with the mentioned
			 * command.
			 */
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

			/*
			 * We check for ADD COLUMN .. DEFAULT expr
			 * if expr contains nextval('user_defined_seq')
			 * we should deparse the statement
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

						if (contain_nextval_expression_walker(expr, NULL))
						{
							deparseAT = true;
							useInitialDDLCommandString = false;

							/* the new column definition will have no constraint */
							ColumnDef *newColDef = copyObject(columnDefinition);
							newColDef->constraints = NULL;

							newCmd->def = (Node *) newColDef;
						}
					}
				}
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
			else if (attachedRelationKind == RELKIND_RELATION)
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
			/*
			 * We already error'ed out for ENABLE/DISABLE trigger commands for
			 * other citus table types in ErrorIfUnsupportedAlterTableStmt.
			 */
			Assert(IsCitusTableType(leftRelationId, CITUS_LOCAL_TABLE));

			char *triggerName = command->name;
			return CitusLocalTableTriggerCommandDDLJob(leftRelationId, triggerName,
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
	ddlJob->targetRelationId = leftRelationId;
	ddlJob->concurrentIndexCmd = false;

	const char *sqlForTaskList = alterTableCommand;
	if (deparseAT)
	{
		newStmt->cmds = list_make1(newCmd);
		sqlForTaskList = DeparseTreeNode((Node *) newStmt);
	}

	ddlJob->commandString = useInitialDDLCommandString ? alterTableCommand : NULL;

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
													 sqlForTaskList);
		}
	}
	else
	{
		/* ... otherwise use standard DDL task list function */
		ddlJob->taskList = DDLTaskList(leftRelationId, sqlForTaskList);
		if (!propagateCommandToWorkers)
		{
			ddlJob->taskList = NIL;
		}
	}

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * AlterTableDefinesFKeyBetweenPostgresAndNonDistTable returns true if given
 * alter table command defines foreign key between a postgres table and a
 * reference or citus local table.
 */
static bool
AlterTableDefinesFKeyBetweenPostgresAndNonDistTable(AlterTableStmt *alterTableStatement)
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
													CITUS_TABLE_WITH_NO_DIST_KEY);
	}
	else if (IsCitusTableType(leftRelationId, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		return RelationIdListContainsPostgresTable(rightRelationIdList);
	}

	return false;
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
		else if (IsCitusTable(relationId))
		{
			/*
			 * relationRangeVarList has also reference and citus local tables
			 * involved in this ADD FOREIGN KEY command. Moreover, even if
			 * relationId was belonging to a postgres  local table initially,
			 * we might had already converted it to a citus local table by cascading.
			 */
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
			CreateCitusLocalTable(relationId, cascade);
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
	ereport(WARNING, (errmsg("not propagating ALTER TABLE ALL IN TABLESPACE "
							 "commands to worker nodes"),
					  errhint("Connect to worker nodes directly to manually "
							  "move all tables.")));

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
	Assert(stmt->objectType == OBJECT_TABLE);

	if (stmt->relation == NULL)
	{
		return NIL;
	}
	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt,
														  stmt->missing_ok);
	Oid relationId = address.objectId;

	/* check whether we are dealing with a sequence here */
	if (get_rel_relkind(relationId) == RELKIND_SEQUENCE)
	{
		AlterObjectSchemaStmt *stmtCopy = copyObject(stmt);
		stmtCopy->objectType = OBJECT_SEQUENCE;
		return PreprocessAlterSequenceSchemaStmt((Node *) stmtCopy, queryString,
												 processUtilityContext);
	}

	/* first check whether a distributed relation is affected */
	if (!OidIsValid(relationId) || !IsCitusTable(relationId))
	{
		return NIL;
	}
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	QualifyTreeNode((Node *) stmt);
	ddlJob->targetRelationId = relationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = DeparseTreeNode((Node *) stmt);
	ddlJob->taskList = DDLTaskList(relationId, ddlJob->commandString);
	return list_make1(ddlJob);
}


/*
 * SkipForeignKeyValidationIfConstraintIsFkey checks and processes the alter table
 * statement to be worked on the distributed table. Currently, it only processes
 * ALTER TABLE ... ADD FOREIGN KEY command to skip the validation step.
 */
Node *
SkipForeignKeyValidationIfConstraintIsFkey(AlterTableStmt *alterTableStatement,
										   bool processLocalRelation)
{
	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return (Node *) alterTableStatement;
	}

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return (Node *) alterTableStatement;
	}

	if (!IsCitusTable(leftRelationId) && !processLocalRelation)
	{
		return (Node *) alterTableStatement;
	}

	/*
	 * We check if there is a ADD FOREIGN CONSTRAINT command in sub commands list.
	 * If there is we assign referenced releation id to rightRelationId and we also
	 * set skip_validation to true to prevent PostgreSQL to verify validity of the
	 * foreign constraint in master. Validity will be checked in workers anyway.
	 */
	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				/* foreign constraint validations will be done in shards. */
				constraint->skip_validation = true;
			}
		}
	}

	return (Node *) alterTableStatement;
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
 * ErrorIfDropPartitionColumn checks if any subcommands of the given alter table
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
		if (get_rel_relkind(relationId) == RELKIND_SEQUENCE)
		{
			AlterTableStmtObjType_compat(alterTableStatement) = OBJECT_SEQUENCE;
			PostprocessAlterSequenceOwnerStmt((Node *) alterTableStatement, NULL);
			return;
		}

		/* changing a relation could introduce new dependencies */
		ObjectAddress tableAddress = { 0 };
		ObjectAddressSet(tableAddress, RelationRelationId, relationId);
		EnsureDependenciesExistOnAllNodes(&tableAddress);
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
								EnsureDistributedSequencesHaveOneType(relationId,
																	  list_make1_oid(
																		  seqOid),
																	  list_make1_int(
																		  attnum));

								if (ShouldSyncTableMetadata(relationId) &&
									ClusterHasKnownMetadataWorkers())
								{
									needMetadataSyncForNewSequences = true;
									MarkSequenceDistributedAndPropagateDependencies(
										seqOid);
									alterTableDefaultNextvalCmd =
										GetAddColumnWithNextvalDefaultCmd(seqOid,
																		  relationId,
																		  columnDefinition
																		  ->colname,
																		  columnDefinition
																		  ->typeName);
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
					EnsureDistributedSequencesHaveOneType(relationId,
														  list_make1_oid(seqOid),
														  list_make1_int(attnum));

					if (ShouldSyncTableMetadata(relationId) &&
						ClusterHasKnownMetadataWorkers())
					{
						needMetadataSyncForNewSequences = true;
						MarkSequenceDistributedAndPropagateDependencies(seqOid);
						alterTableDefaultNextvalCmd = GetAlterColumnWithNextvalDefaultCmd(
							seqOid, relationId, command->name);
					}
				}
			}
		}
	}

	if (needMetadataSyncForNewSequences)
	{
		List *sequenceCommandList = NIL;

		/* commands to create sequences */
		List *sequenceDDLCommands = SequenceDDLCommandsForTable(relationId);
		sequenceCommandList = list_concat(sequenceCommandList, sequenceDDLCommands);

		/* prevent recursive propagation */
		SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

		/* send the commands one by one */
		const char *sequenceCommand = NULL;
		foreach_ptr(sequenceCommand, sequenceCommandList)
		{
			SendCommandToWorkersWithMetadata(sequenceCommand);
		}

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
static char *
GetAlterColumnWithNextvalDefaultCmd(Oid sequenceOid, Oid relationId, char *colname)
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
	appendStringInfo(&str, "ALTER TABLE %s ALTER COLUMN %s "
						   "SET DEFAULT %s(%s::regclass)",
					 qualifiedRelationName, colname,
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
								  TypeName *typeName)
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
					 "ALTER TABLE %s ADD COLUMN %s %s "
					 "DEFAULT %s(%s::regclass)", qualifiedRelationName, colname,
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
		char *referencedColumn = strVal(lfirst(list_head(constraint->pk_attrs)));
		Oid referencedRelationId = RangeVarGetRelid(referencedTable, NoLock, false);

		appendStringInfo(errHint, "FOREIGN KEY (%s) REFERENCES %s(%s)", colName,
						 get_rel_name(referencedRelationId), referencedColumn);

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
 * ALTER TABLE RESET ()
 * ALTER TABLE ENABLE/DISABLE TRIGGER (only for citus local tables)
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
							if (ShouldSyncTableMetadata(relationId) &&
								ClusterHasKnownMetadataWorkers())
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
				 * if the column has default coming from a user-defined sequence
				 * changing the type of the column should not be allowed for now
				 */
				AttrNumber attnum = get_attnum(relationId, command->name);
				List *attnumList = NIL;
				List *dependentSequenceList = NIL;
				GetDependentSequencesWithRelation(relationId, &attnumList,
												  &dependentSequenceList, attnum);
				if (dependentSequenceList != NIL)
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
				Constraint *constraint = (Constraint *) command->def;

				/* we only allow constraints if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot execute ADD CONSTRAINT command with "
										   "other subcommands"),
									errhint("You can issue each subcommand separately")));
				}

				/*
				 * We will use constraint name in each placement by extending it at
				 * workers. Therefore we require it to be exist.
				 */
				if (constraint->conname == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create constraint without a name on a "
										   "distributed table")));
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

				ErrorIfCitusLocalTablePartitionCommand(command, relationId);

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

#if PG_VERSION_NUM >= PG_VERSION_14
			case AT_DetachPartitionFinalize:
			{
				ereport(ERROR, (errmsg("ALTER TABLE .. DETACH PARTITION .. FINALIZE "
									   "commands are currently unsupported.")));
				break;
			}

#endif
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
				#if PG_VERSION_NUM >= PG_VERSION_14
				PartitionCmd *partitionCommand = (PartitionCmd *) command->def;

				if (partitionCommand->concurrent)
				{
					ereport(ERROR, (errmsg("ALTER TABLE .. DETACH PARTITION .. "
										   "CONCURRENTLY commands are currently "
										   "unsupported.")));
				}
				#endif
				ErrorIfCitusLocalTablePartitionCommand(command, relationId);

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

				ErrorOutForTriggerIfNotCitusLocalTable(relationId);

				break;
			}

			case AT_SetNotNull:
			case AT_ReplicaIdentity:
			case AT_ValidateConstraint:
			case AT_DropConstraint: /* we do the check for invalidation in AlterTableDropsForeignKey */
#if PG_VERSION_NUM >= PG_VERSION_14
			case AT_SetCompression:
#endif
				{
					/*
					 * We will not perform any special check for:
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

			default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("alter table command is currently unsupported"),
						 errdetail("Only ADD|DROP COLUMN, SET|DROP NOT NULL, "
								   "SET|DROP DEFAULT, ADD|DROP|VALIDATE CONSTRAINT, "
								   "SET (), RESET (), "
								   "ATTACH|DETACH PARTITION and TYPE subcommands "
								   "are supported.")));
			}
		}
	}
}


/*
 * ErrorIfCitusLocalTablePartitionCommand errors out if given alter table subcommand is
 * an ALTER TABLE ATTACH / DETACH PARTITION command run for a citus local table.
 */
static void
ErrorIfCitusLocalTablePartitionCommand(AlterTableCmd *alterTableCmd, Oid parentRelationId)
{
	AlterTableType alterTableType = alterTableCmd->subtype;
	if (alterTableType != AT_AttachPartition && alterTableType != AT_DetachPartition)
	{
		return;
	}

	bool missingOK = false;
	Oid childRelationId = GetPartitionCommandChildRelationId(alterTableCmd, missingOK);
	if (!IsCitusTableType(parentRelationId, CITUS_LOCAL_TABLE) &&
		!IsCitusTableType(childRelationId, CITUS_LOCAL_TABLE))
	{
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot execute ATTACH/DETACH PARTITION command as "
						   "local tables added to metadata cannot be involved in "
						   "partition relationships with other tables")));
}


/*
 * GetPartitionCommandChildRelationId returns child relationId for given
 * ALTER TABLE ATTACH / DETACH PARTITION subcommand.
 */
static Oid
GetPartitionCommandChildRelationId(AlterTableCmd *alterTableCmd, bool missingOk)
{
	AlterTableType alterTableType PG_USED_FOR_ASSERTS_ONLY = alterTableCmd->subtype;
	Assert(alterTableType == AT_AttachPartition || alterTableType == AT_DetachPartition);

	PartitionCmd *partitionCommand = (PartitionCmd *) alterTableCmd->def;
	RangeVar *childRelationRangeVar = partitionCommand->name;
	Oid childRelationId = RangeVarGetRelid(childRelationRangeVar, AccessExclusiveLock,
										   missingOk);
	return childRelationId;
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
		/*
		 * TODO: This code path will never be executed since we do not
		 * support foreign constraint creation via
		 * ALTER TABLE %s ADD COLUMN %s [constraint]. However, the code
		 * is kept in case we fix the constraint creation without a name
		 * and allow foreign key creation with the mentioned command.
		 */
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
		!IsCitusTableType(relationId, CITUS_TABLE_WITH_NO_DIST_KEY) &&
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
static List *
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

	ListCell *leftShardCell = NULL;
	ListCell *rightShardCell = NULL;
	forboth(leftShardCell, leftShardList, rightShardCell, rightShardList)
	{
		ShardInterval *leftShardInterval = (ShardInterval *) lfirst(leftShardCell);
		ShardInterval *rightShardInterval = (ShardInterval *) lfirst(rightShardCell);

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
	Oid leftRelationId = leftShardInterval->relationId;
	Oid rightRelationId = rightShardInterval->relationId;
	if (IsCitusTableType(leftRelationId, REFERENCE_TABLE) &&
		IsCitusTableType(rightRelationId, CITUS_LOCAL_TABLE))
	{
		/*
		 * If we are defining/dropping a foreign key from a reference table
		 * to a citus local table, then we will execute ADD/DROP constraint
		 * command only for coordinator placement of reference table.
		 */
		uint64 leftShardId = leftShardInterval->shardId;
		task->taskPlacementList = ActiveShardPlacementListOnGroup(leftShardId,
																  COORDINATOR_GROUP_ID);
	}
	else
	{
		uint64 leftShardId = leftShardInterval->shardId;
		task->taskPlacementList = ActiveShardPlacementList(leftShardId);
	}
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
 * ErrorIfUnsopprtedAlterAddConstraintStmt runs the constraint checks on distributed
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
ObjectAddress
AlterTableSchemaStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TABLE);

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

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, RelationRelationId, tableOid);

	return address;
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
