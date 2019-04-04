/*-------------------------------------------------------------------------
 * utility_hook.c
 *	  Citus utility hook and related functionality.
 *
 * The utility hook is called by PostgreSQL when processing any command
 * that is not SELECT, UPDATE, DELETE, INSERT, in place of the regular
 * ProcessUtility function. We use this primarily to implement (or in
 * some cases prevent) DDL commands and COPY on distributed tables.
 *
 * For DDL commands that affect distributed tables, we check whether
 * they are valid (and implemented) for the distributed table and then
 * propagate the command to all shards and, in case of MX, to distributed
 * tables on other nodes. We still call the original ProcessUtility
 * function to apply catalog changes on the coordinator.
 *
 * For COPY into a distributed table, we provide an alternative
 * implementation in ProcessCopyStmt that sends rows to shards based
 * on their distribution column value instead of writing it to the local
 * table on the coordinator. For COPY from a distributed table, we
 * replace the table with a SELECT * FROM table and pass it back to
 * ProcessUtility, which will plan the query via the distributed planner
 * hook.
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h" /* IWYU pragma: keep */
#include "distributed/maintenanced.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transmit.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


bool EnableDDLPropagation = true; /* ddl propagation is enabled */
static bool shouldInvalidateForeignKeyGraph = false;
static int activeAlterTables = 0;


/* Local functions forward declarations for helper functions */
static void ExecuteDistributedDDLJob(DDLJob *ddlJob);
static char * SetSearchPathToCurrentSearchPathCommand(void);
static char * CurrentSearchPath(void);
static void PostProcessUtility(Node *parsetree);


/*
 * multi_ProcessUtility9x is the 9.x-compatible wrapper for Citus' main utility
 * hook. It simply adapts the old-style hook to call into the new-style (10+)
 * hook, which is what now houses all actual logic.
 */
void
multi_ProcessUtility9x(Node *parsetree,
					   const char *queryString,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   DestReceiver *dest,
					   char *completionTag)
{
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = parsetree;

	multi_ProcessUtility(plannedStmt, queryString, context, params, NULL, dest,
						 completionTag);
}


/*
 * CitusProcessUtility is a version-aware wrapper of ProcessUtility to account
 * for argument differences between the 9.x and 10+ PostgreSQL versions.
 */
void
CitusProcessUtility(Node *node, const char *queryString, ProcessUtilityContext context,
					ParamListInfo params, DestReceiver *dest, char *completionTag)
{
#if (PG_VERSION_NUM >= 100000)
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = node;

	ProcessUtility(plannedStmt, queryString, context, params, NULL, dest,
				   completionTag);
#else
	ProcessUtility(node, queryString, context, params, dest, completionTag);
#endif
}


/*
 * multi_ProcessUtility is the main entry hook for implementing Citus-specific
 * utility behavior. Its primary responsibilities are intercepting COPY and DDL
 * commands and augmenting the coordinator's command with corresponding tasks
 * to be run on worker nodes, after suitably ensuring said commands' options
 * are fully supported by Citus. Much of the DDL behavior is toggled by Citus'
 * enable_ddl_propagation GUC. In addition to DDL and COPY, utilities such as
 * TRUNCATE and VACUUM are also supported.
 */
void
multi_ProcessUtility(PlannedStmt *pstmt,
					 const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 struct QueryEnvironment *queryEnv,
					 DestReceiver *dest,
					 char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	List *ddlJobs = NIL;
	bool checkExtensionVersion = false;

	if (IsA(parsetree, TransactionStmt))
	{
		/*
		 * Transaction statements (e.g. ABORT, COMMIT) can be run in aborted
		 * transactions in which case a lot of checks cannot be done safely in
		 * that state. Since we never need to intercept transaction statements,
		 * skip our checks and immediately fall into standard_ProcessUtility.
		 */
#if (PG_VERSION_NUM >= 100000)
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
#else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
#endif

		return;
	}

	checkExtensionVersion = IsCitusExtensionStmt(parsetree);
	if (EnableVersionChecks && checkExtensionVersion)
	{
		ErrorIfUnstableCreateOrAlterExtensionStmt(parsetree);
	}


	if (!CitusHasBeenLoaded())
	{
		/*
		 * Ensure that utility commands do not behave any differently until CREATE
		 * EXTENSION is invoked.
		 */
#if (PG_VERSION_NUM >= 100000)
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
#else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
#endif

		return;
	}

#if (PG_VERSION_NUM >= 100000)
	if (IsA(parsetree, CreateSubscriptionStmt))
	{
		CreateSubscriptionStmt *createSubStmt = (CreateSubscriptionStmt *) parsetree;

		parsetree = ProcessCreateSubscriptionStmt(createSubStmt);
	}
#endif

#if (PG_VERSION_NUM >= 110000)
	if (IsA(parsetree, CallStmt))
	{
		/*
		 * Stored procedures are a bit strange in the sense that some statements
		 * are not in a transaction block, but can be rolled back. We need to
		 * make sure we send all statements in a transaction block. The
		 * StoredProcedureLevel variable signals this to the router executor
		 * and indicates how deep in the call stack we are in case of nested
		 * stored procedures.
		 */
		StoredProcedureLevel += 1;

		PG_TRY();
		{
			standard_ProcessUtility(pstmt, queryString, context,
									params, queryEnv, dest, completionTag);

			StoredProcedureLevel -= 1;
		}
		PG_CATCH();
		{
			StoredProcedureLevel -= 1;
			PG_RE_THROW();
		}
		PG_END_TRY();

		return;
	}
#endif

	/*
	 * TRANSMIT used to be separate command, but to avoid patching the grammar
	 * it's no overlaid onto COPY, but with FORMAT = 'transmit' instead of the
	 * normal FORMAT options.
	 */
	if (IsTransmitStmt(parsetree))
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;
		char *userName = TransmitStatementUser(copyStatement);
		bool missingOK = false;
		StringInfo transmitPath = makeStringInfo();

		VerifyTransmitStmt(copyStatement);

		/* ->relation->relname is the target file in our overloaded COPY */
		appendStringInfoString(transmitPath, copyStatement->relation->relname);

		if (userName != NULL)
		{
			Oid userId = get_role_oid(userName, missingOK);
			appendStringInfo(transmitPath, ".%d", userId);
		}

		if (copyStatement->is_from)
		{
			RedirectCopyDataToRegularFile(transmitPath->data);
		}
		else
		{
			SendRegularFile(transmitPath->data);
		}

		/* Don't execute the faux copy statement */
		return;
	}

	if (IsA(parsetree, CopyStmt))
	{
		MemoryContext planContext = GetMemoryChunkContext(parsetree);
		MemoryContext previousContext;

		parsetree = copyObject(parsetree);
		parsetree = ProcessCopyStmt((CopyStmt *) parsetree, completionTag, queryString);

		previousContext = MemoryContextSwitchTo(planContext);
		parsetree = copyObject(parsetree);
		MemoryContextSwitchTo(previousContext);

		if (parsetree == NULL)
		{
			return;
		}
	}

	/* we're mostly in DDL (and VACUUM/TRUNCATE) territory at this point... */

	if (IsA(parsetree, CreateSeqStmt))
	{
		ErrorIfUnsupportedSeqStmt((CreateSeqStmt *) parsetree);
	}

	if (IsA(parsetree, AlterSeqStmt))
	{
		ErrorIfDistributedAlterSeqOwnedBy((AlterSeqStmt *) parsetree);
	}

	if (IsA(parsetree, TruncateStmt))
	{
		ProcessTruncateStatement((TruncateStmt *) parsetree);
	}

	/* only generate worker DDLJobs if propagation is enabled */
	if (EnableDDLPropagation)
	{
		if (IsA(parsetree, IndexStmt))
		{
			MemoryContext oldContext = MemoryContextSwitchTo(GetMemoryChunkContext(
																 parsetree));

			/* copy parse tree since we might scribble on it to fix the schema name */
			parsetree = copyObject(parsetree);

			MemoryContextSwitchTo(oldContext);

			ddlJobs = PlanIndexStmt((IndexStmt *) parsetree, queryString);
		}

		if (IsA(parsetree, DropStmt))
		{
			DropStmt *dropStatement = (DropStmt *) parsetree;
			if (dropStatement->removeType == OBJECT_INDEX)
			{
				ddlJobs = PlanDropIndexStmt(dropStatement, queryString);
			}

			if (dropStatement->removeType == OBJECT_TABLE)
			{
				ProcessDropTableStmt(dropStatement);
			}

			if (dropStatement->removeType == OBJECT_SCHEMA)
			{
				ProcessDropSchemaStmt(dropStatement);
			}

			if (dropStatement->removeType == OBJECT_POLICY)
			{
				ddlJobs = PlanDropPolicyStmt(dropStatement, queryString);
			}
		}

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE ||
				alterTableStmt->relkind == OBJECT_INDEX)
			{
				ddlJobs = PlanAlterTableStmt(alterTableStmt, queryString);
			}
		}

		/*
		 * ALTER TABLE ... RENAME statements have their node type as RenameStmt and
		 * not AlterTableStmt. So, we intercept RenameStmt to tackle these commands.
		 */
		if (IsA(parsetree, RenameStmt))
		{
			ddlJobs = PlanRenameStmt((RenameStmt *) parsetree, queryString);
		}

		/* handle distributed CLUSTER statements */
		if (IsA(parsetree, ClusterStmt))
		{
			ddlJobs = PlanClusterStmt((ClusterStmt *) parsetree, queryString);
		}

		/*
		 * ALTER ... SET SCHEMA statements have their node type as AlterObjectSchemaStmt.
		 * So, we intercept AlterObjectSchemaStmt to tackle these commands.
		 */
		if (IsA(parsetree, AlterObjectSchemaStmt))
		{
			AlterObjectSchemaStmt *setSchemaStmt = (AlterObjectSchemaStmt *) parsetree;
			ddlJobs = PlanAlterObjectSchemaStmt(setSchemaStmt, queryString);
		}

		if (IsA(parsetree, GrantStmt))
		{
			ddlJobs = PlanGrantStmt((GrantStmt *) parsetree);
		}

		if (IsA(parsetree, CreatePolicyStmt))
		{
			ddlJobs = PlanCreatePolicyStmt((CreatePolicyStmt *) parsetree);
		}

		if (IsA(parsetree, AlterPolicyStmt))
		{
			ddlJobs = PlanAlterPolicyStmt((AlterPolicyStmt *) parsetree);
		}

		/*
		 * ALTER TABLE ALL IN TABLESPACE statements have their node type as
		 * AlterTableMoveAllStmt. At the moment we do not support this functionality in
		 * the distributed environment. We warn out here.
		 */
		if (IsA(parsetree, AlterTableMoveAllStmt))
		{
			ereport(WARNING, (errmsg("not propagating ALTER TABLE ALL IN TABLESPACE "
									 "commands to worker nodes"),
							  errhint("Connect to worker nodes directly to manually "
									  "move all tables.")));
		}
	}
	else
	{
		/*
		 * citus.enable_ddl_propagation is disabled, which means that PostgreSQL
		 * should handle the DDL command on a distributed table directly, without
		 * Citus intervening. The only exception is partition column drop, in
		 * which case we error out. Advanced Citus users use this to implement their
		 * own DDL propagation. We also use it to avoid re-propagating DDL commands
		 * when changing MX tables on workers. Below, we also make sure that DDL
		 * commands don't run queries that might get intercepted by Citus and error
		 * out, specifically we skip validation in foreign keys.
		 */

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE)
			{
				ErrorIfAlterDropsPartitionColumn(alterTableStmt);

				/*
				 * When issuing an ALTER TABLE ... ADD FOREIGN KEY command, the
				 * the validation step should be skipped on the distributed table.
				 * Therefore, we check whether the given ALTER TABLE statement is a
				 * FOREIGN KEY constraint and if so disable the validation step.
				 * Note that validation is done on the shard level when DDL
				 * propagation is enabled. Unlike the preceeding Plan* calls, the
				 * following eagerly executes some tasks on workers.
				 */
				parsetree = WorkerProcessAlterTableStmt(alterTableStmt, queryString);
			}
		}
	}

	/* inform the user about potential caveats */
	if (IsA(parsetree, CreatedbStmt))
	{
		ereport(NOTICE, (errmsg("Citus partially supports CREATE DATABASE for "
								"distributed databases"),
						 errdetail("Citus does not propagate CREATE DATABASE "
								   "command to workers"),
						 errhint("You can manually create a database and its "
								 "extensions on workers.")));
	}
	else if (IsA(parsetree, CreateRoleStmt))
	{
		ereport(NOTICE, (errmsg("not propagating CREATE ROLE/USER commands to worker"
								" nodes"),
						 errhint("Connect to worker nodes directly to manually create all"
								 " necessary users and roles.")));
	}

	/*
	 * Make sure that on DROP DATABASE we terminate the background daemon
	 * associated with it.
	 */
	if (IsA(parsetree, DropdbStmt))
	{
		const bool missingOK = true;
		DropdbStmt *dropDbStatement = (DropdbStmt *) parsetree;
		char *dbname = dropDbStatement->dbname;
		Oid databaseOid = get_database_oid(dbname, missingOK);

		if (OidIsValid(databaseOid))
		{
			StopMaintenanceDaemon(databaseOid);
		}
	}

	pstmt->utilityStmt = parsetree;

	PG_TRY();
	{
		if (IsA(parsetree, AlterTableStmt))
		{
			activeAlterTables++;
		}

#if (PG_VERSION_NUM >= 100000)
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
#else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
#endif

		if (IsA(parsetree, AlterTableStmt))
		{
			activeAlterTables--;
		}
	}
	PG_CATCH();
	{
		if (IsA(parsetree, AlterTableStmt))
		{
			activeAlterTables--;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * We only process CREATE TABLE ... PARTITION OF commands in the function below
	 * to handle the case when user creates a table as a partition of distributed table.
	 */
	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *createStatement = (CreateStmt *) parsetree;

		ProcessCreateTableStmtPartitionOf(createStatement);
	}

	/*
	 * We only process ALTER TABLE ... ATTACH PARTITION commands in the function below
	 * and distribute the partition if necessary.
	 */
	if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *alterTableStatement = (AlterTableStmt *) parsetree;

		ProcessAlterTableStmtAttachPartition(alterTableStatement);
	}

	/* don't run post-process code for local commands */
	if (ddlJobs != NIL)
	{
		PostProcessUtility(parsetree);
	}

	/*
	 * Re-forming the foreign key graph relies on the command being executed
	 * on the local table first. However, in order to decide whether the
	 * command leads to an invalidation, we need to check before the command
	 * is being executed since we read pg_constraint table. Thus, we maintain a
	 * local flag and do the invalidation after multi_ProcessUtility,
	 * before ExecuteDistributedDDLJob().
	 */
	InvalidateForeignKeyGraphForDDL();

	/* after local command has completed, finish by executing worker DDLJobs, if any */
	if (ddlJobs != NIL)
	{
		ListCell *ddlJobCell = NULL;

		if (IsA(parsetree, AlterTableStmt))
		{
			PostProcessAlterTableStmt(castNode(AlterTableStmt, parsetree));
		}

		foreach(ddlJobCell, ddlJobs)
		{
			DDLJob *ddlJob = (DDLJob *) lfirst(ddlJobCell);

			ExecuteDistributedDDLJob(ddlJob);
		}
	}

	/* TODO: fold VACUUM's processing into the above block */
	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacuumStmt = (VacuumStmt *) parsetree;

		ProcessVacuumStmt(vacuumStmt, queryString);
	}

	/*
	 * Ensure value is valid, we can't do some checks during CREATE
	 * EXTENSION. This is important to register some invalidation callbacks.
	 */
	CitusHasBeenLoaded();
}


/*
 * ExecuteDistributedDDLJob simply executes a provided DDLJob in a distributed trans-
 * action, including metadata sync if needed. If the multi shard commit protocol is
 * in its default value of '1pc', then a notice message indicating that '2pc' might be
 * used for extra safety. In the commit protocol, a BEGIN is sent after connection to
 * each shard placement and COMMIT/ROLLBACK is handled by
 * CoordinatedTransactionCallback function.
 *
 * The function errors out if the node is not the coordinator or if the DDL is on
 * a partitioned table which has replication factor > 1.
 *
 */
static void
ExecuteDistributedDDLJob(DDLJob *ddlJob)
{
	bool shouldSyncMetadata = ShouldSyncTableMetadata(ddlJob->targetRelationId);

	EnsureCoordinator();
	EnsurePartitionTableNotReplicated(ddlJob->targetRelationId);

	if (!ddlJob->concurrentIndexCmd)
	{
		if (shouldSyncMetadata)
		{
			char *setSearchPathCommand = SetSearchPathToCurrentSearchPathCommand();

			SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);

			/*
			 * Given that we're relaying the query to the worker nodes directly,
			 * we should set the search path exactly the same when necessary.
			 */
			if (setSearchPathCommand != NULL)
			{
				SendCommandToWorkers(WORKERS_WITH_METADATA, setSearchPathCommand);
			}

			SendCommandToWorkers(WORKERS_WITH_METADATA, (char *) ddlJob->commandString);
		}

		if (MultiShardConnectionType == SEQUENTIAL_CONNECTION ||
			ddlJob->executeSequentially)
		{
			ExecuteModifyTasksSequentiallyWithoutResults(ddlJob->taskList, CMD_UTILITY);
		}
		else
		{
			ExecuteModifyTasksWithoutResults(ddlJob->taskList);
		}
	}
	else
	{
		/* save old commit protocol to restore at xact end */
		Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
		SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
		MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

		PG_TRY();
		{
			ExecuteModifyTasksSequentiallyWithoutResults(ddlJob->taskList, CMD_UTILITY);

			if (shouldSyncMetadata)
			{
				List *commandList = list_make1(DISABLE_DDL_PROPAGATION);
				char *setSearchPathCommand = SetSearchPathToCurrentSearchPathCommand();

				/*
				 * Given that we're relaying the query to the worker nodes directly,
				 * we should set the search path exactly the same when necessary.
				 */
				if (setSearchPathCommand != NULL)
				{
					commandList = lappend(commandList, setSearchPathCommand);
				}

				commandList = lappend(commandList, (char *) ddlJob->commandString);

				SendBareCommandListToWorkers(WORKERS_WITH_METADATA, commandList);
			}
		}
		PG_CATCH();
		{
			ereport(ERROR,
					(errmsg("CONCURRENTLY-enabled index command failed"),
					 errdetail("CONCURRENTLY-enabled index commands can fail partially, "
							   "leaving behind an INVALID index."),
					 errhint("Use DROP INDEX CONCURRENTLY IF EXISTS to remove the "
							 "invalid index, then retry the original command.")));
		}
		PG_END_TRY();
	}
}


/*
 * SetSearchPathToCurrentSearchPathCommand generates a command which can
 * set the search path to the exact same search path that the issueing node
 * has.
 *
 * If the current search path is null (or doesn't have any valid schemas),
 * the function returns NULL.
 */
static char *
SetSearchPathToCurrentSearchPathCommand(void)
{
	StringInfo setCommand = NULL;
	char *currentSearchPath = CurrentSearchPath();

	if (currentSearchPath == NULL)
	{
		return NULL;
	}

	setCommand = makeStringInfo();
	appendStringInfo(setCommand, "SET search_path TO %s;", currentSearchPath);

	return setCommand->data;
}


/*
 * CurrentSearchPath is a C interface for calling current_schemas(bool) that
 * PostgreSQL exports.
 *
 * CurrentSchemas returns all the schemas in the seach_path that are seperated
 * with comma (,) sign. The returned string can be used to set the search_path.
 *
 * The function omits implicit schemas.
 *
 * The function returns NULL if there are no valid schemas in the search_path,
 * mimicing current_schemas(false) function.
 */
static char *
CurrentSearchPath(void)
{
	StringInfo currentSearchPath = makeStringInfo();
	List *searchPathList = fetch_search_path(false);
	ListCell *searchPathCell;
	bool schemaAdded = false;

	foreach(searchPathCell, searchPathList)
	{
		char *schemaName = get_namespace_name(lfirst_oid(searchPathCell));

		/* watch out for deleted namespace */
		if (schemaName)
		{
			if (schemaAdded)
			{
				appendStringInfoString(currentSearchPath, ",");
				schemaAdded = false;
			}

			appendStringInfoString(currentSearchPath, quote_identifier(schemaName));
			schemaAdded = true;
		}
	}

	/* fetch_search_path() returns a palloc'd list that we should free now */
	list_free(searchPathList);

	return (currentSearchPath->len > 0 ? currentSearchPath->data : NULL);
}


/*
 * PostProcessUtility performs additional tasks after a utility's local portion
 * has been completed.
 */
static void
PostProcessUtility(Node *parsetree)
{
	if (IsA(parsetree, IndexStmt))
	{
		PostProcessIndexStmt(castNode(IndexStmt, parsetree));
	}
}


/*
 * MarkInvalidateForeignKeyGraph marks whether the foreign key graph should be
 * invalidated due to a DDL.
 */
void
MarkInvalidateForeignKeyGraph()
{
	shouldInvalidateForeignKeyGraph = true;
}


/*
 * InvalidateForeignKeyGraphForDDL simply keeps track of whether
 * the foreign key graph should be invalidated due to a DDL.
 */
void
InvalidateForeignKeyGraphForDDL(void)
{
	if (shouldInvalidateForeignKeyGraph)
	{
		InvalidateForeignKeyGraph();

		shouldInvalidateForeignKeyGraph = false;
	}
}


/*
 * DDLTaskList builds a list of tasks to execute a DDL command on a
 * given list of shards.
 */
List *
DDLTaskList(Oid relationId, const char *commandString)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	ListCell *shardIntervalCell = NULL;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	char *escapedCommandString = quote_literal_cstr(commandString);
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		StringInfo applyCommand = makeStringInfo();
		Task *task = NULL;

		/*
		 * If rightRelationId is not InvalidOid, instead of worker_apply_shard_ddl_command
		 * we use worker_apply_inter_shard_ddl_command.
		 */
		appendStringInfo(applyCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
						 escapedSchemaName, escapedCommandString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = applyCommand->data;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * AlterTableInProgress returns true if we're processing an ALTER TABLE command
 * right now.
 */
bool
AlterTableInProgress(void)
{
	return activeAlterTables > 0;
}
