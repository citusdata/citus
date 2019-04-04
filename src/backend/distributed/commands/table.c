/*-------------------------------------------------------------------------
 *
 * table.c
 *    Commands for creating and altering distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* Local functions forward declarations for unsupported command checks */
static void ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement);
static List * InterShardDDLTaskList(Oid leftRelationId, Oid rightRelationId,
									const char *commandString);
static bool AlterInvolvesPartitionColumn(AlterTableStmt *alterTableStatement,
										 AlterTableCmd *command);
static void ErrorIfUnsupportedAlterAddConstraintStmt(AlterTableStmt *alterTableStatement);

/*
 * We need to run some of the commands sequentially if there is a foreign constraint
 * from/to reference table.
 */
static bool SetupExecutionModeForAlterTable(Oid relationId, AlterTableCmd *command);

/*
 * ProcessDropTableStmt processes DROP TABLE commands for partitioned tables.
 * If we are trying to DROP partitioned tables, we first need to go to MX nodes
 * and DETACH partitions from their parents. Otherwise, we process DROP command
 * multiple times in MX workers. For shards, we send DROP commands with IF EXISTS
 * parameter which solves problem of processing same command multiple times.
 * However, for distributed table itself, we directly remove related table from
 * Postgres catalogs via performDeletion function, thus we need to be cautious
 * about not processing same DROP command twice.
 */
void
ProcessDropTableStmt(DropStmt *dropTableStatement)
{
	ListCell *dropTableCell = NULL;

	Assert(dropTableStatement->removeType == OBJECT_TABLE);

	foreach(dropTableCell, dropTableStatement->objects)
	{
		List *tableNameList = (List *) lfirst(dropTableCell);
		RangeVar *tableRangeVar = makeRangeVarFromNameList(tableNameList);
		bool missingOK = true;
		List *partitionList = NIL;
		ListCell *partitionCell = NULL;

		Oid relationId = RangeVarGetRelid(tableRangeVar, AccessShareLock, missingOK);

		/* we're not interested in non-valid, non-distributed relations */
		if (relationId == InvalidOid || !IsDistributedTable(relationId))
		{
			continue;
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

		partitionList = PartitionList(relationId);
		if (list_length(partitionList) == 0)
		{
			continue;
		}

		SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);

		foreach(partitionCell, partitionList)
		{
			Oid partitionRelationId = lfirst_oid(partitionCell);
			char *detachPartitionCommand =
				GenerateDetachPartitionCommand(partitionRelationId);

			SendCommandToWorkers(WORKERS_WITH_METADATA, detachPartitionCommand);
		}
	}
}


/*
 * ProcessCreateTableStmtPartitionOf takes CreateStmt object as a parameter but
 * it only processes CREATE TABLE ... PARTITION OF statements and it checks if
 * user creates the table as a partition of a distributed table. In that case,
 * it distributes partition as well. Since the table itself is a partition,
 * CreateDistributedTable will attach it to its parent table automatically after
 * distributing it.
 *
 * This function does nothing if PostgreSQL's version is less then 10 and given
 * CreateStmt is not a CREATE TABLE ... PARTITION OF command.
 */
void
ProcessCreateTableStmtPartitionOf(CreateStmt *createStatement)
{
#if (PG_VERSION_NUM >= 100000)
	if (createStatement->inhRelations != NIL && createStatement->partbound != NULL)
	{
		RangeVar *parentRelation = linitial(createStatement->inhRelations);
		bool parentMissingOk = false;
		Oid parentRelationId = RangeVarGetRelid(parentRelation, NoLock,
												parentMissingOk);

		/* a partition can only inherit from single parent table */
		Assert(list_length(createStatement->inhRelations) == 1);

		Assert(parentRelationId != InvalidOid);

		/*
		 * If a partition is being created and if its parent is a distributed
		 * table, we will distribute this table as well.
		 */
		if (IsDistributedTable(parentRelationId))
		{
			bool missingOk = false;
			Oid relationId = RangeVarGetRelid(createStatement->relation, NoLock,
											  missingOk);
			Var *parentDistributionColumn = DistPartitionKey(parentRelationId);
			char parentDistributionMethod = DISTRIBUTE_BY_HASH;
			char *parentRelationName = generate_qualified_relation_name(parentRelationId);
			bool viaDeprecatedAPI = false;

			CreateDistributedTable(relationId, parentDistributionColumn,
								   parentDistributionMethod, parentRelationName,
								   viaDeprecatedAPI);
		}
	}
#endif
}


/*
 * ProcessAlterTableStmtAttachPartition takes AlterTableStmt object as parameter
 * but it only processes into ALTER TABLE ... ATTACH PARTITION commands and
 * distributes the partition if necessary. There are four cases to consider;
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
 * This function does nothing if PostgreSQL's version is less then 10 and given
 * CreateStmt is not a ALTER TABLE ... ATTACH PARTITION OF command.
 */
void
ProcessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement)
{
#if (PG_VERSION_NUM >= 100000)
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *alterTableCommand = (AlterTableCmd *) lfirst(commandCell);

		if (alterTableCommand->subtype == AT_AttachPartition)
		{
			Oid relationId = AlterTableLookupRelation(alterTableStatement, NoLock);
			PartitionCmd *partitionCommand = (PartitionCmd *) alterTableCommand->def;
			bool partitionMissingOk = false;
			Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name, NoLock,
													   partitionMissingOk);

			/*
			 * If user first distributes the table then tries to attach it to non
			 * distributed table, we error out.
			 */
			if (!IsDistributedTable(relationId) &&
				IsDistributedTable(partitionRelationId))
			{
				char *parentRelationName = get_rel_name(partitionRelationId);

				ereport(ERROR, (errmsg("non-distributed tables cannot have "
									   "distributed partitions"),
								errhint("Distribute the partitioned table \"%s\" "
										"instead", parentRelationName)));
			}

			/* if parent of this table is distributed, distribute this table too */
			if (IsDistributedTable(relationId) &&
				!IsDistributedTable(partitionRelationId))
			{
				Var *distributionColumn = DistPartitionKey(relationId);
				char distributionMethod = DISTRIBUTE_BY_HASH;
				char *parentRelationName = generate_qualified_relation_name(relationId);
				bool viaDeprecatedAPI = false;

				CreateDistributedTable(partitionRelationId, distributionColumn,
									   distributionMethod, parentRelationName,
									   viaDeprecatedAPI);
			}
		}
	}
#endif
}


/*
 * PlanAlterTableStmt determines whether a given ALTER TABLE statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PlanAlterTableStmt(AlterTableStmt *alterTableStatement, const char *alterTableCommand)
{
	List *ddlJobs = NIL;
	DDLJob *ddlJob = NULL;
	LOCKMODE lockmode = 0;
	Oid leftRelationId = InvalidOid;
	Oid rightRelationId = InvalidOid;
	char leftRelationKind;
	bool isDistributedRelation = false;
	List *commandList = NIL;
	ListCell *commandCell = NULL;
	bool executeSequentially = false;

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return NIL;
	}

	lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return NIL;
	}

	/*
	 * AlterTableStmt applies also to INDEX relations, and we have support for
	 * SET/SET storage parameters in Citus, so we might have to check for
	 * another relation here.
	 */
	leftRelationKind = get_rel_relkind(leftRelationId);
	if (leftRelationKind == RELKIND_INDEX)
	{
		leftRelationId = IndexGetRelation(leftRelationId, false);
	}

	isDistributedRelation = IsDistributedTable(leftRelationId);
	if (!isDistributedRelation)
	{
		return NIL;
	}

	/*
	 * The PostgreSQL parser dispatches several commands into the node type
	 * AlterTableStmt, from ALTER INDEX to ALTER SEQUENCE or ALTER VIEW. Here
	 * we have a special implementation for ALTER INDEX, and a specific error
	 * message in case of unsupported sub-command.
	 */
	if (leftRelationKind == RELKIND_INDEX)
	{
		ErrorIfUnsupportedAlterIndexStmt(alterTableStatement);
	}
	else
	{
		/* this function also accepts more than just RELKIND_RELATION... */
		ErrorIfUnsupportedAlterTableStmt(alterTableStatement);
	}

	/*
	 * We check if there is a ADD/DROP FOREIGN CONSTRAINT command in sub commands list.
	 * If there is we assign referenced relation id to rightRelationId and we also
	 * set skip_validation to true to prevent PostgreSQL to verify validity of the
	 * foreign constraint in master. Validity will be checked in workers anyway.
	 */
	commandList = alterTableStatement->cmds;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = command->subtype;

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
				Assert(list_length(commandList) <= 1);

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

			ListCell *columnConstraint = NULL;
			foreach(columnConstraint, columnConstraints)
			{
				Constraint *constraint = (Constraint *) lfirst(columnConstraint);
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
		}
#if (PG_VERSION_NUM >= 100000)
		else if (alterTableType == AT_AttachPartition)
		{
			PartitionCmd *partitionCommand = (PartitionCmd *) command->def;

			/*
			 * We only support ALTER TABLE ATTACH PARTITION, if it is only subcommand of
			 * ALTER TABLE. It was already checked in ErrorIfUnsupportedAlterTableStmt.
			 */
			Assert(list_length(commandList) <= 1);

			rightRelationId = RangeVarGetRelid(partitionCommand->name, NoLock, false);

			/*
			 * Do not generate tasks if relation is distributed and the partition
			 * is not distributed. Because, we'll manually convert the partition into
			 * distributed table and co-locate with its parent.
			 */
			if (!IsDistributedTable(rightRelationId))
			{
				return NIL;
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
#endif
		executeSequentially |= SetupExecutionModeForAlterTable(leftRelationId,
															   command);
	}

	ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = leftRelationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = alterTableCommand;
	ddlJob->executeSequentially = executeSequentially;

	if (rightRelationId)
	{
		if (!IsDistributedTable(rightRelationId))
		{
			ddlJob->taskList = NIL;
		}
		else
		{
			/* if foreign key related, use specialized task list function ... */
			ddlJob->taskList = InterShardDDLTaskList(leftRelationId, rightRelationId,
													 alterTableCommand);
		}
	}
	else
	{
		/* ... otherwise use standard DDL task list function */
		ddlJob->taskList = DDLTaskList(leftRelationId, alterTableCommand);
	}

	ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * WorkerProcessAlterTableStmt checks and processes the alter table statement to be
 * worked on the distributed table of the worker node. Currently, it only processes
 * ALTER TABLE ... ADD FOREIGN KEY command to skip the validation step.
 */
Node *
WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
							const char *alterTableCommand)
{
	LOCKMODE lockmode = 0;
	Oid leftRelationId = InvalidOid;
	bool isDistributedRelation = false;
	List *commandList = NIL;
	ListCell *commandCell = NULL;

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return (Node *) alterTableStatement;
	}

	lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return (Node *) alterTableStatement;
	}

	isDistributedRelation = IsDistributedTable(leftRelationId);
	if (!isDistributedRelation)
	{
		return (Node *) alterTableStatement;
	}

	/*
	 * We check if there is a ADD FOREIGN CONSTRAINT command in sub commands list.
	 * If there is we assign referenced releation id to rightRelationId and we also
	 * set skip_validation to true to prevent PostgreSQL to verify validity of the
	 * foreign constraint in master. Validity will be checked in workers anyway.
	 */
	commandList = alterTableStatement->cmds;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
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

	if (renameStmt->renameType == OBJECT_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_COLUMN &&
			 renameStmt->relationType == OBJECT_TABLE)
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
	LOCKMODE lockmode = 0;
	Oid leftRelationId = InvalidOid;
	bool isDistributedRelation = false;
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return;
	}

	lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return;
	}

	isDistributedRelation = IsDistributedTable(leftRelationId);
	if (!isDistributedRelation)
	{
		return;
	}

	/* then check if any of subcommands drop partition column.*/
	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
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
 * PostProcessAlterTableStmt runs after the ALTER TABLE command has already run on the
 * master, so we are checking constraints over the table with constraints already defined
 * (to make the constraint check process same for ALTER TABLE and CREATE TABLE). If
 * constraints do not fulfill the rules we defined, they will be removed and the table
 * will return back to the state before the ALTER TABLE command.
 */
void
PostProcessAlterTableStmt(AlterTableStmt *alterTableStatement)
{
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			LOCKMODE lockmode = NoLock;
			Oid relationId = InvalidOid;
			Constraint *constraint = NULL;

			Assert(list_length(commandList) == 1);

			ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);

			lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
			relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

			if (!OidIsValid(relationId))
			{
				continue;
			}

			constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				InvalidateForeignKeyGraph();
			}
		}
		else if (alterTableType == AT_AddColumn)
		{
			List *columnConstraints = NIL;
			ListCell *columnConstraint = NULL;
			Oid relationId = InvalidOid;
			LOCKMODE lockmode = NoLock;

			ColumnDef *columnDefinition = (ColumnDef *) command->def;
			columnConstraints = columnDefinition->constraints;
			if (columnConstraints)
			{
				ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);
			}

			lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
			relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
			if (!OidIsValid(relationId))
			{
				continue;
			}

			foreach(columnConstraint, columnConstraints)
			{
				Constraint *constraint = (Constraint *) lfirst(columnConstraint);

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
		}
	}
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


/*ErrorIfUnsupportedConstraint
 *  run checks related to unique index / exclude
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
							 Var *distributionColumn, uint32 colocationId)
{
	char *relationName = NULL;
	List *indexOidList = NULL;
	ListCell *indexOidCell = NULL;

	/*
	 * We first perform check for foreign constraints. It is important to do this check
	 * before next check, because other types of constraints are allowed on reference
	 * tables and we return early for those constraints thanks to next check. Therefore,
	 * for reference tables, we first check for foreing constraints and if they are OK,
	 * we do not error out for other types of constraints.
	 */
	ErrorIfUnsupportedForeignConstraint(relation, distributionMethod, distributionColumn,
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

	relationName = RelationGetRelationName(relation);
	indexOidList = RelationGetIndexList(relation);

	foreach(indexOidCell, indexOidList)
	{
		Oid indexOid = lfirst_oid(indexOidCell);
		Relation indexDesc = index_open(indexOid, RowExclusiveLock);
		IndexInfo *indexInfo = NULL;
		AttrNumber *attributeNumberArray = NULL;
		bool hasDistributionColumn = false;
		int attributeCount = 0;
		int attributeIndex = 0;

		/* extract index key information from the index's pg_index info */
		indexInfo = BuildIndexInfo(indexDesc);

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

		attributeCount = indexInfo->ii_NumIndexAttrs;
		attributeNumberArray = IndexInfoAttributeNumberArray(indexInfo);

		for (attributeIndex = 0; attributeIndex < attributeCount; attributeIndex++)
		{
			AttrNumber attributeNumber = attributeNumberArray[attributeIndex];
			bool uniqueConstraint = false;
			bool exclusionConstraintWithEquality = false;

			if (distributionColumn->varattno != attributeNumber)
			{
				continue;
			}

			uniqueConstraint = indexInfo->ii_Unique;
			exclusionConstraintWithEquality = (indexInfo->ii_ExclusionOps != NULL &&
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
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
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
 */
static void
ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement)
{
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	/* error out if any of the subcommands are unsupported */
	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
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
							ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											errmsg("cannot execute ADD COLUMN commands "
												   "involving serial pseudotypes")));
						}
					}
				}

				break;
			}

			case AT_DropColumn:
			case AT_ColumnDefault:
			case AT_AlterColumnType:
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

#if (PG_VERSION_NUM >= 100000)
			case AT_AttachPartition:
			{
				Oid relationId = AlterTableLookupRelation(alterTableStatement,
														  NoLock);
				PartitionCmd *partitionCommand = (PartitionCmd *) command->def;
				bool missingOK = false;
				Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name,
														   NoLock, missingOK);

				/* we only allow partitioning commands if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot execute ATTACH PARTITION command "
										   "with other subcommands"),
									errhint("You can issue each subcommand "
											"separately.")));
				}

				if (IsDistributedTable(partitionRelationId) &&
					!TablesColocated(relationId, partitionRelationId))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("distributed tables cannot have "
										   "non-colocated distributed tables as a "
										   "partition ")));
				}

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

				break;
			}

#endif
			case AT_DropConstraint:
			{
				LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
				Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

				if (!OidIsValid(relationId))
				{
					return;
				}

				if (ConstraintIsAForeignKey(command->name, relationId))
				{
					MarkInvalidateForeignKeyGraph();
				}

				break;
			}

			case AT_SetNotNull:
			case AT_EnableTrigAll:
			case AT_DisableTrigAll:
			case AT_ReplicaIdentity:
			case AT_ValidateConstraint:
			{
				/*
				 * We will not perform any special check for ALTER TABLE DROP CONSTRAINT
				 * , ALTER TABLE .. ALTER COLUMN .. SET NOT NULL and ALTER TABLE ENABLE/
				 * DISABLE TRIGGER ALL, ALTER TABLE .. REPLICA IDENTITY .., ALTER TABLE
				 * .. VALIDATE CONSTRAINT ..
				 */
				break;
			}

			case AT_SetRelOptions:  /* SET (...) */
			case AT_ResetRelOptions:    /* RESET (...) */
			case AT_ReplaceRelOptions:  /* replace entire option list */
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
 * SetupExecutionModeForAlterTable is the function that is responsible
 * for two things for practial purpose for not doing the same checks
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

		ListCell *columnConstraint = NULL;
		foreach(columnConstraint, columnConstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(columnConstraint);
			if (constraint->contype == CONSTR_FOREIGN)
			{
				Oid rightRelationId = RangeVarGetRelid(constraint->pktable, NoLock,
													   false);
				if (IsDistributedTable(rightRelationId) &&
					PartitionMethod(rightRelationId) == DISTRIBUTE_BY_NONE)
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
			if (IsTransactionBlock() && alterTableType == AT_AlterColumnType)
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
			if (IsDistributedTable(rightRelationId) &&
				PartitionMethod(rightRelationId) == DISTRIBUTE_BY_NONE)
			{
				executeSequentially = true;
			}
		}
	}
	#if (PG_VERSION_NUM >= 100000)
	else if (alterTableType == AT_DetachPartition || alterTableType == AT_AttachPartition)
	{
		/* check if there are foreign constraints to reference tables */
		if (HasForeignKeyToReferenceTable(relationId))
		{
			executeSequentially = true;
		}
	}
	#endif

	/*
	 * If there has already been a parallel query executed, the sequential mode
	 * would still use the already opened parallel connections to the workers for
	 * the distributed tables, thus contradicting our purpose of using
	 * sequential mode.
	 */
	if (executeSequentially && IsDistributedTable(relationId) &&
		PartitionMethod(relationId) != DISTRIBUTE_BY_NONE &&
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
 * foreign key and partitioning command on worker node.
 *
 * leftRelationId is the relation id of actual distributed table which given command is
 * applied. rightRelationId is the relation id of distributed table which given command
 * refers to.
 */
static List *
InterShardDDLTaskList(Oid leftRelationId, Oid rightRelationId,
					  const char *commandString)
{
	List *taskList = NIL;

	List *leftShardList = LoadShardIntervalList(leftRelationId);
	ListCell *leftShardCell = NULL;
	Oid leftSchemaId = get_rel_namespace(leftRelationId);
	char *leftSchemaName = get_namespace_name(leftSchemaId);
	char *escapedLeftSchemaName = quote_literal_cstr(leftSchemaName);

	char rightPartitionMethod = PartitionMethod(rightRelationId);
	List *rightShardList = LoadShardIntervalList(rightRelationId);
	ListCell *rightShardCell = NULL;
	Oid rightSchemaId = get_rel_namespace(rightRelationId);
	char *rightSchemaName = get_namespace_name(rightSchemaId);
	char *escapedRightSchemaName = quote_literal_cstr(rightSchemaName);

	char *escapedCommandString = quote_literal_cstr(commandString);
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/*
	 * If the rightPartitionMethod is a reference table, we need to make sure
	 * that the tasks are created in a way that the right shard stays the same
	 * since we only have one placement per worker. This hack is first implemented
	 * for foreign constraint support from distributed tables to reference tables.
	 */
	if (rightPartitionMethod == DISTRIBUTE_BY_NONE)
	{
		ShardInterval *rightShardInterval = NULL;
		int rightShardCount = list_length(rightShardList);
		int leftShardCount = list_length(leftShardList);
		int shardCounter = 0;

		Assert(rightShardCount == 1);

		rightShardInterval = (ShardInterval *) linitial(rightShardList);
		for (shardCounter = rightShardCount; shardCounter < leftShardCount;
			 shardCounter++)
		{
			rightShardList = lappend(rightShardList, rightShardInterval);
		}
	}

	/* lock metadata before getting placement lists */
	LockShardListMetadata(leftShardList, ShareLock);

	forboth(leftShardCell, leftShardList, rightShardCell, rightShardList)
	{
		ShardInterval *leftShardInterval = (ShardInterval *) lfirst(leftShardCell);
		uint64 leftShardId = leftShardInterval->shardId;
		StringInfo applyCommand = makeStringInfo();
		Task *task = NULL;
		RelationShard *leftRelationShard = CitusMakeNode(RelationShard);
		RelationShard *rightRelationShard = CitusMakeNode(RelationShard);

		ShardInterval *rightShardInterval = (ShardInterval *) lfirst(rightShardCell);
		uint64 rightShardId = rightShardInterval->shardId;

		leftRelationShard->relationId = leftRelationId;
		leftRelationShard->shardId = leftShardId;

		rightRelationShard->relationId = rightRelationId;
		rightRelationShard->shardId = rightShardId;

		appendStringInfo(applyCommand, WORKER_APPLY_INTER_SHARD_DDL_COMMAND,
						 leftShardId, escapedLeftSchemaName, rightShardId,
						 escapedRightSchemaName, escapedCommandString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = applyCommand->data;
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = leftShardId;
		task->taskPlacementList = FinalizedShardPlacementList(leftShardId);
		task->relationShardList = list_make2(leftRelationShard, rightRelationShard);

		taskList = lappend(taskList, task);
	}

	return taskList;
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
	Var *partitionColumn = NULL;
	HeapTuple tuple = NULL;
	char *alterColumnName = command->name;

	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(relationId))
	{
		return false;
	}

	partitionColumn = DistPartitionKey(relationId);

	tuple = SearchSysCacheAttName(relationId, alterColumnName);
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
	Var *distributionColumn = DistPartitionKey(relationId);
	uint32 colocationId = TableColocationId(relationId);
	Relation relation = relation_open(relationId, ExclusiveLock);

	ErrorIfUnsupportedConstraint(relation, distributionMethod, distributionColumn,
								 colocationId);
	relation_close(relation, NoLock);
}
