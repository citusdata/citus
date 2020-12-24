/*-------------------------------------------------------------------------
 * utility_hook.c
 *	  Citus utility hook and related functionality.
 *
 * The utility hook is called by PostgreSQL when processing any command
 * that is not SELECT, UPDATE, DELETE, INSERT, in place of the regular
 * PostprocessUtility function. We use this primarily to implement (or in
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
 * PostprocessUtility, which will plan the query via the distributed planner
 * hook.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "distributed/pg_version_constants.h"

#include "postgres.h"
#include "miscadmin.h"

#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "distributed/adaptive_executor.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h" /* IWYU pragma: keep */
#include "distributed/deparser.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/maintenanced.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/resource_lock.h"
#include "distributed/transmit.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

bool EnableDDLPropagation = true; /* ddl propagation is enabled */
PropSetCmdBehavior PropagateSetCommands = PROPSETCMD_NONE; /* SET prop off */
static bool shouldInvalidateForeignKeyGraph = false;
static int activeAlterTables = 0;
static int activeDropSchemaOrDBs = 0;


/* Local functions forward declarations for helper functions */
static char * SetSearchPathToCurrentSearchPathCommand(void);
static char * CurrentSearchPath(void);
static void IncrementUtilityHookCountersIfNecessary(Node *parsetree);
static void PostStandardProcessUtility(Node *parsetree);
static void DecrementUtilityHookCountersIfNecessary(Node *parsetree);
static bool IsDropSchemaOrDB(Node *parsetree);


/*
 * CitusProcessUtility is a convenience method to create a PlannedStmt out of pieces of a
 * utility statement before invoking ProcessUtility.
 */
void
CitusProcessUtility(Node *node, const char *queryString, ProcessUtilityContext context,
					ParamListInfo params, DestReceiver *dest,
					QueryCompletionCompat *completionTag)
{
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = node;

	ProcessUtility(plannedStmt, queryString, context, params, NULL, dest,
				   completionTag);
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
					 QueryCompletionCompat *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	List *ddlJobs = NIL;

	if (IsA(parsetree, TransactionStmt) ||
		IsA(parsetree, LockStmt) ||
		IsA(parsetree, ListenStmt) ||
		IsA(parsetree, NotifyStmt) ||
		IsA(parsetree, ExecuteStmt) ||
		IsA(parsetree, PrepareStmt) ||
		IsA(parsetree, DiscardStmt) ||
		IsA(parsetree, DeallocateStmt))
	{
		/*
		 * Skip additional checks for common commands that do not have any
		 * Citus-specific logic.
		 *
		 * Transaction statements (e.g. ABORT, COMMIT) can be run in aborted
		 * transactions in which case a lot of checks cannot be done safely in
		 * that state. Since we never need to intercept transaction statements,
		 * skip our checks and immediately fall into standard_ProcessUtility.
		 */
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);

		return;
	}

	bool isCreateAlterExtensionUpdateCitusStmt = IsCreateAlterExtensionUpdateCitusStmt(
		parsetree);
	if (EnableVersionChecks && isCreateAlterExtensionUpdateCitusStmt)
	{
		ErrorIfUnstableCreateOrAlterExtensionStmt(parsetree);
	}

	if (!CitusHasBeenLoaded())
	{
		/*
		 * Ensure that utility commands do not behave any differently until CREATE
		 * EXTENSION is invoked.
		 */
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);

		return;
	}

	if (IsA(parsetree, ExplainStmt) &&
		IsA(((ExplainStmt *) parsetree)->query, Query))
	{
		ExplainStmt *explainStmt = (ExplainStmt *) parsetree;

		if (IsTransactionBlock())
		{
			bool analyze = false;

			DefElem *option = NULL;
			foreach_ptr(option, explainStmt->options)
			{
				if (strcmp(option->defname, "analyze") == 0)
				{
					analyze = defGetBoolean(option);
				}

				/* don't "break", as explain.c will use the last value */
			}

			if (analyze)
			{
				/*
				 * Since we cannot execute EXPLAIN ANALYZE locally, we
				 * cannot continue.
				 */
				ErrorIfTransactionAccessedPlacementsLocally();
			}
		}

		/*
		 * EXPLAIN ANALYZE is tricky with local execution, and there is not
		 * much difference between the local and distributed execution in terms
		 * of the actual EXPLAIN output.
		 *
		 * TODO: It might be nice to have a way to show that the query is locally
		 * executed. Shall we add a INFO output?
		 */
		DisableLocalExecution();
	}

	if (IsA(parsetree, CreateSubscriptionStmt))
	{
		CreateSubscriptionStmt *createSubStmt = (CreateSubscriptionStmt *) parsetree;

		parsetree = ProcessCreateSubscriptionStmt(createSubStmt);
	}

	if (IsA(parsetree, CallStmt))
	{
		CallStmt *callStmt = (CallStmt *) parsetree;

		/*
		 * If the procedure is distributed and we are using MX then we have the
		 * possibility of calling it on the worker. If the data is located on
		 * the worker this can avoid making many network round trips.
		 */
		if (context == PROCESS_UTILITY_TOPLEVEL &&
			CallDistributedProcedureRemotely(callStmt, dest))
		{
			return;
		}

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

	if (IsA(parsetree, DoStmt))
	{
		/*
		 * All statements in a DO block are executed in a single transaciton,
		 * so we need to keep track of whether we are inside a DO block.
		 */
		DoBlockLevel += 1;

		PG_TRY();
		{
			standard_ProcessUtility(pstmt, queryString, context,
									params, queryEnv, dest, completionTag);

			DoBlockLevel -= 1;
		}
		PG_CATCH();
		{
			DoBlockLevel -= 1;
			PG_RE_THROW();
		}
		PG_END_TRY();

		return;
	}

	/* process SET LOCAL stmts of allowed GUCs in multi-stmt xacts */
	if (IsA(parsetree, VariableSetStmt))
	{
		VariableSetStmt *setStmt = (VariableSetStmt *) parsetree;

		/* at present, we only implement the NONE and LOCAL behaviors */
		AssertState(PropagateSetCommands == PROPSETCMD_NONE ||
					PropagateSetCommands == PROPSETCMD_LOCAL);

		if (IsMultiStatementTransaction() && ShouldPropagateSetCommand(setStmt))
		{
			PostprocessVariableSetStmt(setStmt, queryString);
		}
	}

	/*
	 * TRANSMIT used to be separate command, but to avoid patching the grammar
	 * it's now overlaid onto COPY, but with FORMAT = 'transmit' instead of the
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

		parsetree = copyObject(parsetree);
		parsetree = ProcessCopyStmt((CopyStmt *) parsetree, completionTag, queryString);

		if (parsetree == NULL)
		{
			return;
		}

		MemoryContext previousContext = MemoryContextSwitchTo(planContext);
		parsetree = copyObject(parsetree);
		MemoryContextSwitchTo(previousContext);

		/*
		 * we need to set the parsetree here already as we copy and replace the original
		 * parsetree during ddl propagation. In reality we need to refactor the code above
		 * to not juggle copy the parsetree and leak it to a potential cache above the
		 * utility hook.
		 */
		pstmt->utilityStmt = parsetree;
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
		PreprocessTruncateStatement((TruncateStmt *) parsetree);
	}

	/* only generate worker DDLJobs if propagation is enabled */
	const DistributeObjectOps *ops = NULL;
	if (EnableDDLPropagation)
	{
		/* copy planned statement since we might scribble on it or its utilityStmt */
		pstmt = copyObject(pstmt);
		parsetree = pstmt->utilityStmt;
		ops = GetDistributeObjectOps(parsetree);

		if (ops && ops->preprocess)
		{
			ddlJobs = ops->preprocess(parsetree, queryString);
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
			if (alterTableStmt->relkind == OBJECT_TABLE ||
				alterTableStmt->relkind == OBJECT_FOREIGN_TABLE)
			{
				ErrorIfAlterDropsPartitionColumn(alterTableStmt);

				/*
				 * When issuing an ALTER TABLE ... ADD FOREIGN KEY command, the
				 * the validation step should be skipped on the distributed table.
				 * Therefore, we check whether the given ALTER TABLE statement is a
				 * FOREIGN KEY constraint and if so disable the validation step.
				 * Note validation is done on the shard level when DDL propagation
				 * is enabled. The following eagerly executes some tasks on workers.
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

	if (IsDropCitusExtensionStmt(parsetree))
	{
		StopMaintenanceDaemon(MyDatabaseId);
	}

	pstmt->utilityStmt = parsetree;

	PG_TRY();
	{
		IncrementUtilityHookCountersIfNecessary(parsetree);

		/*
		 * Check if we are running ALTER EXTENSION citus UPDATE (TO "<version>") command and
		 * the available version is different than the current version of Citus. In this case,
		 * ALTER EXTENSION citus UPDATE command can actually update Citus to a new version.
		 */
		bool isAlterExtensionUpdateCitusStmt = isCreateAlterExtensionUpdateCitusStmt &&
											   IsA(parsetree, AlterExtensionStmt);

		bool citusCanBeUpdatedToAvailableVersion = false;

		if (isAlterExtensionUpdateCitusStmt)
		{
			citusCanBeUpdatedToAvailableVersion = !InstalledAndAvailableVersionsSame();
		}

		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);

		/*
		 * if we are running ALTER EXTENSION citus UPDATE (to "<version>") command, we may need
		 * to mark existing objects as distributed depending on the "version" parameter if
		 * specified in "ALTER EXTENSION citus UPDATE" command
		 */
		if (isAlterExtensionUpdateCitusStmt && citusCanBeUpdatedToAvailableVersion)
		{
			PostprocessAlterExtensionCitusUpdateStmt(parsetree);
		}

		/*
		 * Postgres added the following CommandCounterIncrement as a patch in:
		 *  - 10.7 -> 10.8
		 *  - 11.2 -> 11.3
		 * The patch was a response to bug #15631.
		 *
		 * CommandCounterIncrement is used to make changes to the catalog visible for post
		 * processing of create commands (eg. create type). It is safe to call
		 * CommandCounterIncrement twice, as the call is a no-op if the command id is not
		 * used yet.
		 *
		 * Once versions older than above are not deemed important anymore this patch can
		 * be remove from citus.
		 */
		CommandCounterIncrement();

		PostStandardProcessUtility(parsetree);
	}
	PG_CATCH();
	{
		PostStandardProcessUtility(parsetree);

		/*
		 * We call MarkInvalidateForeignKeyGraph in preprocess without knowing
		 * that if command would fail or not. However, we don't want to invalidate
		 * foreign key graph for failed DDL commands, so here we don't call
		 * InvalidateForeignKeyGraphForDDL.
		 * On the other hand, we should still reset shouldInvalidateForeignKeyGraph
		 * flag as next DDL command still relies on this flag to invalidate foreign
		 * key graph.
		 */
		shouldInvalidateForeignKeyGraph = false;

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Post process for ddl statements
	 */

	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *createStatement = (CreateStmt *) parsetree;

		PostprocessCreateTableStmt(createStatement, queryString);
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

	if (EnableDDLPropagation)
	{
		if (ops && ops->postprocess)
		{
			List *processJobs = ops->postprocess(parsetree, queryString);

			if (processJobs)
			{
				Assert(ddlJobs == NIL); /* jobs should not have been set before */
				ddlJobs = processJobs;
			}
		}

		if (IsA(parsetree, RenameStmt) && ((RenameStmt *) parsetree)->renameType ==
			OBJECT_ROLE && EnableAlterRolePropagation)
		{
			ereport(NOTICE, (errmsg("not propagating ALTER ROLE ... RENAME TO commands "
									"to worker nodes"),
							 errhint("Connect to worker nodes directly to manually "
									 "rename the role")));
		}
	}

	/*
	 * We only process ALTER TABLE ... ATTACH PARTITION commands in the function below
	 * and distribute the partition if necessary.
	 */
	if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *alterTableStatement = (AlterTableStmt *) parsetree;

		PostprocessAlterTableStmtAttachPartition(alterTableStatement, queryString);
	}

	/* after local command has completed, finish by executing worker DDLJobs, if any */
	if (ddlJobs != NIL)
	{
		if (IsA(parsetree, AlterTableStmt))
		{
			PostprocessAlterTableStmt(castNode(AlterTableStmt, parsetree));
		}

		DDLJob *ddlJob = NULL;
		foreach_ptr(ddlJob, ddlJobs)
		{
			ExecuteDistributedDDLJob(ddlJob);
		}

		/*
		 * For CREATE/DROP/REINDEX CONCURRENTLY we mark the index as valid
		 * after successfully completing the distributed DDL job.
		 */
		if (IsA(parsetree, IndexStmt))
		{
			IndexStmt *indexStmt = (IndexStmt *) parsetree;

			if (indexStmt->concurrent)
			{
				/* no failures during CONCURRENTLY, mark the index as valid */
				MarkIndexValid(indexStmt);
			}
		}
	}

	/* TODO: fold VACUUM's processing into the above block */
	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacuumStmt = (VacuumStmt *) parsetree;

		PostprocessVacuumStmt(vacuumStmt, queryString);
	}

	if (!IsDropCitusExtensionStmt(parsetree) && !IsA(parsetree, DropdbStmt))
	{
		/*
		 * Ensure value is valid, we can't do some checks during CREATE
		 * EXTENSION. This is important to register some invalidation callbacks.
		 */
		CitusHasBeenLoaded();
	}
}


/*
 * IsDropSchemaOrDB returns true if parsetree represents DROP SCHEMA ...or
 * a DROP DATABASE.
 */
static bool
IsDropSchemaOrDB(Node *parsetree)
{
	if (!IsA(parsetree, DropStmt))
	{
		return false;
	}

	DropStmt *dropStatement = (DropStmt *) parsetree;
	return (dropStatement->removeType == OBJECT_SCHEMA) ||
		   (dropStatement->removeType == OBJECT_DATABASE);
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
void
ExecuteDistributedDDLJob(DDLJob *ddlJob)
{
	bool shouldSyncMetadata = false;

	EnsureCoordinator();

	Oid targetRelationId = ddlJob->targetRelationId;

	if (OidIsValid(targetRelationId))
	{
		/*
		 * Only for ddlJobs that are targetting a relation (table) we want to sync
		 * its metadata and verify some properties around the table.
		 */
		shouldSyncMetadata = ShouldSyncTableMetadata(targetRelationId);
		EnsurePartitionTableNotReplicated(targetRelationId);
	}

	bool localExecutionSupported = true;

	if (!ddlJob->concurrentIndexCmd)
	{
		if (shouldSyncMetadata)
		{
			char *setSearchPathCommand = SetSearchPathToCurrentSearchPathCommand();

			SendCommandToWorkersWithMetadata(DISABLE_DDL_PROPAGATION);

			/*
			 * Given that we're relaying the query to the worker nodes directly,
			 * we should set the search path exactly the same when necessary.
			 */
			if (setSearchPathCommand != NULL)
			{
				SendCommandToWorkersWithMetadata(setSearchPathCommand);
			}

			SendCommandToWorkersWithMetadata((char *) ddlJob->commandString);
		}

		ExecuteUtilityTaskList(ddlJob->taskList, localExecutionSupported);
	}
	else
	{
		localExecutionSupported = false;

		/*
		 * Start a new transaction to make sure CONCURRENTLY commands
		 * on localhost do not block waiting for this transaction to finish.
		 */
		if (ddlJob->startNewTransaction)
		{
			CommitTransactionCommand();
			StartTransactionCommand();
		}

		/* save old commit protocol to restore at xact end */
		Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
		SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
		MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

		PG_TRY();
		{
			ExecuteUtilityTaskList(ddlJob->taskList, localExecutionSupported);

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

				SendBareCommandListToMetadataWorkers(commandList);
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
 * CreateCustomDDLTaskList creates a DDLJob which will apply a command to all placements
 * of shards of a distributed table. The command to be applied is generated by the
 * TableDDLCommand structure passed in.
 */
DDLJob *
CreateCustomDDLTaskList(Oid relationId, TableDDLCommand *command)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	uint64 jobId = INVALID_JOB_ID;
	Oid namespace = get_rel_namespace(relationId);
	char *namespaceName = get_namespace_name(namespace);
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		char *commandStr = GetShardedTableDDLCommand(command, shardId, namespaceName);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, commandStr);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = relationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = GetTableDDLCommand(command);
	ddlJob->taskList = taskList;

	return ddlJob;
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
	char *currentSearchPath = CurrentSearchPath();

	if (currentSearchPath == NULL)
	{
		return NULL;
	}

	StringInfo setCommand = makeStringInfo();
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
	bool schemaAdded = false;

	Oid searchPathOid = InvalidOid;
	foreach_oid(searchPathOid, searchPathList)
	{
		char *schemaName = get_namespace_name(searchPathOid);

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
 * IncrementUtilityHookCountersIfNecessary increments activeAlterTables and
 * activeDropSchemaOrDBs counters if utility command being processed implies
 * to do so.
 */
static void
IncrementUtilityHookCountersIfNecessary(Node *parsetree)
{
	if (IsA(parsetree, AlterTableStmt))
	{
		activeAlterTables++;
	}

	if (IsDropSchemaOrDB(parsetree))
	{
		activeDropSchemaOrDBs++;
	}
}


/*
 * PostStandardProcessUtility performs operations to alter (backend) global
 * state of citus utility hook. Those operations should be done after standard
 * process utility executes even if it errors out.
 */
static void
PostStandardProcessUtility(Node *parsetree)
{
	DecrementUtilityHookCountersIfNecessary(parsetree);
}


/*
 * DecrementUtilityHookCountersIfNecessary decrements activeAlterTables and
 * activeDropSchemaOrDBs counters if utility command being processed implies
 * to do so.
 */
static void
DecrementUtilityHookCountersIfNecessary(Node *parsetree)
{
	if (IsA(parsetree, AlterTableStmt))
	{
		activeAlterTables--;
	}

	if (IsDropSchemaOrDB(parsetree))
	{
		activeDropSchemaOrDBs--;
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
		ereport(DEBUG1, (errmsg("DDL command invalidates foreign key graph")));

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
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	char *escapedCommandString = quote_literal_cstr(commandString);
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		StringInfo applyCommand = makeStringInfo();

		/*
		 * If rightRelationId is not InvalidOid, instead of worker_apply_shard_ddl_command
		 * we use worker_apply_inter_shard_ddl_command.
		 */
		appendStringInfo(applyCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
						 escapedSchemaName, escapedCommandString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, applyCommand->data);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * NodeDDLTaskList builds a list of tasks to execute a DDL command on a
 * given target set of nodes.
 */
List *
NodeDDLTaskList(TargetWorkerSet targets, List *commands)
{
	List *workerNodes = TargetWorkerSetNodeList(targets, NoLock);

	if (list_length(workerNodes) <= 0)
	{
		/*
		 * if there are no nodes we don't have to plan any ddl tasks. Planning them would
		 * cause the executor to stop responding.
		 */
		return NIL;
	}

	Task *task = CitusMakeNode(Task);
	task->taskType = DDL_TASK;
	SetTaskQueryStringList(task, commands);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodes)
	{
		ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
		targetPlacement->nodeName = workerNode->workerName;
		targetPlacement->nodePort = workerNode->workerPort;
		targetPlacement->groupId = workerNode->groupId;

		task->taskPlacementList = lappend(task->taskPlacementList, targetPlacement);
	}

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = InvalidOid;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = NULL;
	ddlJob->taskList = list_make1(task);

	return list_make1(ddlJob);
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


/*
 * DropSchemaOrDBInProgress returns true if we're processing a DROP SCHEMA
 * or a DROP DATABASE command right now.
 */
bool
DropSchemaOrDBInProgress(void)
{
	return activeDropSchemaOrDBs > 0;
}
