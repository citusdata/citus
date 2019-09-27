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
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

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
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h" /* IWYU pragma: keep */
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
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


/* Local functions forward declarations for helper functions */
static void ExecuteDistributedDDLJob(DDLJob *ddlJob);
static char * SetSearchPathToCurrentSearchPathCommand(void);
static char * CurrentSearchPath(void);
static void PostProcessUtility(Node *parsetree);
static List * PlanRenameAttributeStmt(RenameStmt *stmt, const char *queryString);
static List * PlanAlterOwnerStmt(AlterOwnerStmt *stmt, const char *queryString);
static List * PlanAlterObjectDependsStmt(AlterObjectDependsStmt *stmt,
										 const char *queryString);

static void ExecuteNodeBaseDDLCommands(List *taskList);


/*
 * CitusProcessUtility is a convenience method to create a PlannedStmt out of pieces of a
 * utility statement before invoking ProcessUtility.
 */
void
CitusProcessUtility(Node *node, const char *queryString, ProcessUtilityContext context,
					ParamListInfo params, DestReceiver *dest, char *completionTag)
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
					 char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	List *ddlJobs = NIL;
	bool checkExtensionVersion = false;

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
			ListCell *optionCell = NULL;
			bool analyze = false;

			foreach(optionCell, explainStmt->options)
			{
				DefElem *option = (DefElem *) lfirst(optionCell);

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
				ErrorIfLocalExecutionHappened();
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

	/* process SET LOCAL stmts of whitelisted GUCs in multi-stmt xacts */
	if (IsA(parsetree, VariableSetStmt))
	{
		VariableSetStmt *setStmt = (VariableSetStmt *) parsetree;

		/* at present, we only implement the NONE and LOCAL behaviors */
		AssertState(PropagateSetCommands == PROPSETCMD_NONE ||
					PropagateSetCommands == PROPSETCMD_LOCAL);

		if (IsMultiStatementTransaction() && ShouldPropagateSetCommand(setStmt))
		{
			ProcessVariableSetStmt(setStmt, queryString);
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

		/*
		 * we need to set the parsetree here already as we copy and replace the original
		 * parsetree during ddl propagation. In reality we need to refactor the code above
		 * to not juggle copy the parsetree and leak it to a potential cache above the
		 * utillity hook.
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
		ProcessTruncateStatement((TruncateStmt *) parsetree);
	}

	/* only generate worker DDLJobs if propagation is enabled */
	if (EnableDDLPropagation)
	{
		/* copy planned statement since we might scribble on it or its utilityStmt */
		pstmt = copyObject(pstmt);
		parsetree = pstmt->utilityStmt;

		if (IsA(parsetree, IndexStmt))
		{
			ddlJobs = PlanIndexStmt((IndexStmt *) parsetree, queryString);
		}

		if (IsA(parsetree, ReindexStmt))
		{
			ddlJobs = PlanReindexStmt((ReindexStmt *) parsetree, queryString);
		}

		if (IsA(parsetree, DropStmt))
		{
			DropStmt *dropStatement = (DropStmt *) parsetree;
			switch (dropStatement->removeType)
			{
				case OBJECT_INDEX:
				{
					ddlJobs = PlanDropIndexStmt(dropStatement, queryString);
					break;
				}

				case OBJECT_TABLE:
				{
					ProcessDropTableStmt(dropStatement);
					break;
				}

				case OBJECT_SCHEMA:
				{
					ProcessDropSchemaStmt(dropStatement);
					break;
				}

				case OBJECT_POLICY:
				{
					ddlJobs = PlanDropPolicyStmt(dropStatement, queryString);
					break;
				}

				case OBJECT_TYPE:
				{
					ddlJobs = PlanDropTypeStmt(dropStatement, queryString);
					break;
				}

				case OBJECT_PROCEDURE:
				case OBJECT_AGGREGATE:
				case OBJECT_FUNCTION:
				{
					ddlJobs = PlanDropFunctionStmt(dropStatement, queryString);
				}

				default:
				{
					/* unsupported type, skipping*/
				}
			}
		}

		if (IsA(parsetree, AlterEnumStmt))
		{
			ddlJobs = PlanAlterEnumStmt(castNode(AlterEnumStmt, parsetree), queryString);
		}

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE ||
				alterTableStmt->relkind == OBJECT_FOREIGN_TABLE ||
				alterTableStmt->relkind == OBJECT_INDEX)
			{
				ddlJobs = PlanAlterTableStmt(alterTableStmt, queryString);
			}

			if (alterTableStmt->relkind == OBJECT_TYPE)
			{
				ddlJobs = PlanAlterTypeStmt(alterTableStmt, queryString);
			}
		}

		/*
		 * ALTER TABLE ... RENAME statements have their node type as RenameStmt and
		 * not AlterTableStmt. So, we intercept RenameStmt to tackle these commands.
		 */
		if (IsA(parsetree, RenameStmt))
		{
			RenameStmt *renameStmt = (RenameStmt *) parsetree;

			switch (renameStmt->renameType)
			{
				case OBJECT_TYPE:
				{
					ddlJobs = PlanRenameTypeStmt(renameStmt, queryString);
					break;
				}

				case OBJECT_ATTRIBUTE:
				{
					ddlJobs = PlanRenameAttributeStmt(renameStmt, queryString);
					break;
				}

				case OBJECT_PROCEDURE:
				case OBJECT_AGGREGATE:
				case OBJECT_FUNCTION:
				{
					ddlJobs = PlanRenameFunctionStmt(renameStmt, queryString);
					break;
				}

				default:
				{
					ddlJobs = PlanRenameStmt(renameStmt, queryString);
					break;
				}
			}
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
			ddlJobs = PlanAlterObjectSchemaStmt(
				castNode(AlterObjectSchemaStmt, parsetree), queryString);
		}

		if (IsA(parsetree, GrantStmt))
		{
			ddlJobs = PlanGrantStmt((GrantStmt *) parsetree);
		}

		if (IsA(parsetree, AlterOwnerStmt))
		{
			ddlJobs = PlanAlterOwnerStmt(castNode(AlterOwnerStmt, parsetree),
										 queryString);
		}

		if (IsA(parsetree, CreatePolicyStmt))
		{
			ddlJobs = PlanCreatePolicyStmt((CreatePolicyStmt *) parsetree);
		}

		if (IsA(parsetree, AlterPolicyStmt))
		{
			ddlJobs = PlanAlterPolicyStmt((AlterPolicyStmt *) parsetree);
		}

		if (IsA(parsetree, CompositeTypeStmt))
		{
			ddlJobs = PlanCompositeTypeStmt(castNode(CompositeTypeStmt, parsetree),
											queryString);
		}

		if (IsA(parsetree, CreateEnumStmt))
		{
			ddlJobs = PlanCreateEnumStmt(castNode(CreateEnumStmt, parsetree),
										 queryString);
		}

		if (IsA(parsetree, AlterFunctionStmt))
		{
			ddlJobs = PlanAlterFunctionStmt(castNode(AlterFunctionStmt, parsetree),
											queryString);
		}

		if (IsA(parsetree, CreateFunctionStmt))
		{
			ddlJobs = PlanCreateFunctionStmt(castNode(CreateFunctionStmt, parsetree),
											 queryString);
		}

		if (IsA(parsetree, AlterObjectDependsStmt))
		{
			ddlJobs = PlanAlterObjectDependsStmt(
				castNode(AlterObjectDependsStmt, parsetree), queryString);
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
			if (alterTableStmt->relkind == OBJECT_TABLE ||
				alterTableStmt->relkind == OBJECT_FOREIGN_TABLE)
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

		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);

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
		 * Once versions older then above are not deemed important anymore this patch can
		 * be remove from citus.
		 */
		CommandCounterIncrement();

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
	 * Post process for ddl statements
	 */
	if (EnableDDLPropagation)
	{
		if (IsA(parsetree, CompositeTypeStmt))
		{
			ProcessCompositeTypeStmt(castNode(CompositeTypeStmt, parsetree), queryString);
		}

		if (IsA(parsetree, CreateEnumStmt))
		{
			ProcessCreateEnumStmt(castNode(CreateEnumStmt, parsetree), queryString);
		}

		if (IsA(parsetree, AlterObjectSchemaStmt))
		{
			ProcessAlterObjectSchemaStmt(castNode(AlterObjectSchemaStmt, parsetree),
										 queryString);
		}

		if (IsA(parsetree, AlterEnumStmt))
		{
			ProcessAlterEnumStmt(castNode(AlterEnumStmt, parsetree), queryString);
		}

		if (IsA(parsetree, CreateFunctionStmt))
		{
			Assert(ddlJobs == NIL); /* jobs should not have been set before */
			ddlJobs = ProcessCreateFunctionStmt(castNode(CreateFunctionStmt, parsetree),
												queryString);
		}
	}

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
 * PlanRenameAttributeStmt called for RenameStmt's that are targetting an attribute eg.
 * type attributes. Based on the relation type the attribute gets renamed it dispatches to
 * a specialized implementation if present, otherwise return an empty list for its DDLJobs
 */
static List *
PlanRenameAttributeStmt(RenameStmt *stmt, const char *queryString)
{
	Assert(stmt->renameType == OBJECT_ATTRIBUTE);

	switch (stmt->relationType)
	{
		case OBJECT_TYPE:
		{
			return PlanRenameTypeAttributeStmt(stmt, queryString);
		}

		default:
		{
			/* unsupported relation for attribute rename, do nothing */
			return NIL;
		}
	}
}


/*
 * PlanAlterOwnerStmt gets called for statements that change the ownership of an object.
 * Based on the type of object the ownership gets changed for it dispatches to a
 * specialized implementation or returns an empty list of DDLJobs for objects that do not
 * have an implementation provided.
 */
static List *
PlanAlterOwnerStmt(AlterOwnerStmt *stmt, const char *queryString)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return PlanAlterTypeOwnerStmt(stmt, queryString);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return PlanAlterFunctionOwnerStmt(stmt, queryString);
		}

		default:
		{
			/* do nothing for unsupported alter owner statements */
			return NIL;
		}
	}
}


/*
 * PlanAlterObjectDependsStmt gets called during the planning phase for
 * ALTER ... DEPENDS ON EXTENSION ... statements. Based on the object type we call out to
 * a specialized implementation. If no implementation is available we do nothing, eg. we
 * allow the local node to execute.
 */
static List *
PlanAlterObjectDependsStmt(AlterObjectDependsStmt *stmt, const char *queryString)
{
	switch (stmt->objectType)
	{
		case OBJECT_PROCEDURE:
		case OBJECT_FUNCTION:
		{
			return PlanAlterFunctionDependsStmt(stmt, queryString);
		}

		default:
		{
			return NIL;
		}
	}
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
	bool shouldSyncMetadata = false;

	EnsureCoordinator();

	if (ddlJob->targetRelationId != InvalidOid)
	{
		/*
		 * Only for ddlJobs that are targetting a relation (table) we want to sync its
		 * metadata and verify some properties around the table.
		 */
		shouldSyncMetadata = ShouldSyncTableMetadata(ddlJob->targetRelationId);
		EnsurePartitionTableNotReplicated(ddlJob->targetRelationId);
	}

	if (TaskExecutorType != MULTI_EXECUTOR_ADAPTIVE &&
		ddlJob->targetRelationId == InvalidOid)
	{
		/*
		 * Some ddl jobs can only be run by the adaptive executor and not our legacy ones.
		 *
		 * These are tasks that are not pinned to any relation nor shards. We can execute
		 * these very naively with a simple for loop that sends them to the target worker.
		 */

		ExecuteNodeBaseDDLCommands(ddlJob->taskList);
	}
	else if (!ddlJob->concurrentIndexCmd)
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

		/* use adaptive executor when enabled */
		ExecuteUtilityTaskListWithoutResults(ddlJob->taskList);
	}
	else
	{
		/* save old commit protocol to restore at xact end */
		Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
		SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
		MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

		PG_TRY();
		{
			/* use adaptive executor when enabled */
			ExecuteUtilityTaskListWithoutResults(ddlJob->taskList);

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
 * ExecuteNodeBaseDDLCommands executes ddl commands naively only when we are not using the
 * adaptive executor. It gets connections to the target placements and executes the
 * commands.
 */
static void
ExecuteNodeBaseDDLCommands(List *taskList)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		ListCell *taskPlacementCell = NULL;

		/* these tasks should not be pinned to any shard */
		Assert(task->anchorShardId == INVALID_SHARD_ID);

		foreach(taskPlacementCell, task->taskPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(taskPlacementCell);
			SendCommandToWorkerAsUser(placement->nodeName, placement->nodePort, NULL,
									  task->queryString);
		}
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
 * NodeDDLTaskList builds a list of tasks to execute a DDL command on a
 * given target set of nodes.
 */
List *
NodeDDLTaskList(TargetWorkerSet targets, List *commands)
{
	List *workerNodes = TargetWorkerSetNodeList(targets, NoLock);
	char *concatenatedCommands = StringJoin(commands, ';');
	DDLJob *ddlJob = NULL;
	ListCell *workerNodeCell = NULL;
	Task *task = NULL;

	if (list_length(workerNodes) <= 0)
	{
		/*
		 * if there are no nodes we don't have to plan any ddl tasks. Planning them would
		 * cause a hang in the executor.
		 */
		return NIL;
	}

	task = CitusMakeNode(Task);
	task->taskType = DDL_TASK;
	task->queryString = concatenatedCommands;

	foreach(workerNodeCell, workerNodes)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		ShardPlacement *targetPlacement = NULL;

		targetPlacement = CitusMakeNode(ShardPlacement);
		targetPlacement->nodeName = workerNode->workerName;
		targetPlacement->nodePort = workerNode->workerPort;
		targetPlacement->groupId = workerNode->groupId;

		task->taskPlacementList = lappend(task->taskPlacementList, targetPlacement);
	}

	ddlJob = palloc0(sizeof(DDLJob));
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
