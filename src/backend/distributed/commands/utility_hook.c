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
#if PG_VERSION_NUM < 140000
#include "access/xact.h"
#endif
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
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/maintenanced.h"
#include "distributed/multi_partitioning_utils.h"
#if PG_VERSION_NUM < 140000
#include "distributed/metadata_cache.h"
#endif
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "distributed/worker_shard_visibility.h"
#include "distributed/worker_transaction.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

bool EnableDDLPropagation = true; /* ddl propagation is enabled */
int CreateObjectPropagationMode = CREATE_OBJECT_PROPAGATION_IMMEDIATE;
PropSetCmdBehavior PropagateSetCommands = PROPSETCMD_NONE; /* SET prop off */
static bool shouldInvalidateForeignKeyGraph = false;
static int activeAlterTables = 0;
static int activeDropSchemaOrDBs = 0;
static bool ConstraintDropped = false;


ProcessUtility_hook_type PrevProcessUtility = NULL;
int UtilityHookLevel = 0;


/* Local functions forward declarations for helper functions */
static void ProcessUtilityInternal(PlannedStmt *pstmt,
								   const char *queryString,
								   ProcessUtilityContext context,
								   ParamListInfo params,
								   struct QueryEnvironment *queryEnv,
								   DestReceiver *dest,
								   QueryCompletionCompat *completionTag);
#if PG_VERSION_NUM >= 140000
static void set_indexsafe_procflags(void);
#endif
static char * SetSearchPathToCurrentSearchPathCommand(void);
static char * CurrentSearchPath(void);
static void IncrementUtilityHookCountersIfNecessary(Node *parsetree);
static void PostStandardProcessUtility(Node *parsetree);
static void DecrementUtilityHookCountersIfNecessary(Node *parsetree);
static bool IsDropSchemaOrDB(Node *parsetree);
static bool ShouldCheckUndistributeCitusLocalTables(void);
static bool ShouldAddNewTableToMetadata(Node *parsetree);
static bool ServerUsesPostgresFDW(char *serverName);
static void ErrorIfOptionListHasNoTableName(List *optionList);


/*
 * ProcessUtilityParseTree is a convenience method to create a PlannedStmt out of
 * pieces of a utility statement before invoking ProcessUtility.
 */
void
ProcessUtilityParseTree(Node *node, const char *queryString, ProcessUtilityContext
						context,
						ParamListInfo params, DestReceiver *dest,
						QueryCompletionCompat *completionTag)
{
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = node;

	ProcessUtility_compat(plannedStmt, queryString, false, context, params, NULL, dest,
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
#if PG_VERSION_NUM >= PG_VERSION_14
					 bool readOnlyTree,
#endif
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 struct QueryEnvironment *queryEnv,
					 DestReceiver *dest,
					 QueryCompletionCompat *completionTag)
{
	Node *parsetree;

#if PG_VERSION_NUM >= PG_VERSION_14
	if (readOnlyTree)
	{
		pstmt = copyObject(pstmt);
	}
#endif

	parsetree = pstmt->utilityStmt;

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
		PrevProcessUtility_compat(pstmt, queryString, false, context,
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
		PrevProcessUtility_compat(pstmt, queryString, false, context,
								  params, queryEnv, dest, completionTag);

		return;
	}
	else if (IsA(parsetree, CallStmt))
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
			PrevProcessUtility_compat(pstmt, queryString, false, context,
									  params, queryEnv, dest, completionTag);

			StoredProcedureLevel -= 1;

			if (InDelegatedProcedureCall && StoredProcedureLevel == 0)
			{
				InDelegatedProcedureCall = false;
			}
		}
		PG_CATCH();
		{
			StoredProcedureLevel -= 1;

			if (InDelegatedProcedureCall && StoredProcedureLevel == 0)
			{
				InDelegatedProcedureCall = false;
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		return;
	}
	else if (IsA(parsetree, DoStmt))
	{
		/*
		 * All statements in a DO block are executed in a single transaction,
		 * so we need to keep track of whether we are inside a DO block.
		 */
		DoBlockLevel += 1;

		PG_TRY();
		{
			PrevProcessUtility_compat(pstmt, queryString, false, context,
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

	UtilityHookLevel++;

	PG_TRY();
	{
		ProcessUtilityInternal(pstmt, queryString, context, params, queryEnv, dest,
							   completionTag);

		if (UtilityHookLevel == 1)
		{
			/*
			 * When Citus local tables are disconnected from the foreign key graph, which
			 * can happen due to various kinds of drop commands, we immediately
			 * undistribute them at the end of the command.
			 */
			if (ShouldCheckUndistributeCitusLocalTables())
			{
				UndistributeDisconnectedCitusLocalTables();
			}
			ResetConstraintDropped();

			if (context == PROCESS_UTILITY_TOPLEVEL &&
				ShouldAddNewTableToMetadata(parsetree))
			{
				/*
				 * Here we need to increment command counter so that next command
				 * can see the new table.
				 */
				CommandCounterIncrement();
				CreateStmt *createTableStmt = (CreateStmt *) parsetree;
				Oid relationId = RangeVarGetRelid(createTableStmt->relation,
												  NoLock, false);

				/*
				 * Here we set autoConverted to false, since the user explicitly
				 * wants these tables to be added to metadata, by setting the
				 * GUC use_citus_managed_tables to true.
				 */
				bool autoConverted = false;
				bool cascade = true;
				CreateCitusLocalTable(relationId, cascade, autoConverted);
			}
		}

		UtilityHookLevel--;
	}
	PG_CATCH();
	{
		if (UtilityHookLevel == 1)
		{
			ResetConstraintDropped();
		}

		UtilityHookLevel--;

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ProcessUtilityInternal is a helper function for multi_ProcessUtility where majority
 * of the Citus specific utility statements are handled here. The distinction between
 * both functions is that Citus_ProcessUtility does not handle CALL and DO statements.
 * The reason for the distinction is implemented to be able to find the "top-level" DDL
 * commands (not internal/cascading ones). UtilityHookLevel variable is used to achieve
 * this goal.
 */
static void
ProcessUtilityInternal(PlannedStmt *pstmt,
					   const char *queryString,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   struct QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletionCompat *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	List *ddlJobs = NIL;

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

	/*
	 * Process SET LOCAL and SET TRANSACTION statements in multi-statement
	 * transactions.
	 */
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

	/*
	 * We only process ALTER TABLE ... ATTACH PARTITION commands in the function below
	 * and distribute the partition if necessary.
	 */
	if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *alterTableStatement = (AlterTableStmt *) parsetree;

		PreprocessAlterTableStmtAttachPartition(alterTableStatement, queryString);
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
			ddlJobs = ops->preprocess(parsetree, queryString, context);
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
			if (AlterTableStmtObjType_compat(alterTableStmt) == OBJECT_TABLE ||
				AlterTableStmtObjType_compat(alterTableStmt) == OBJECT_FOREIGN_TABLE)
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
				parsetree =
					SkipForeignKeyValidationIfConstraintIsFkey(alterTableStmt, false);
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
		bool isCreateAlterExtensionUpdateCitusStmt =
			IsCreateAlterExtensionUpdateCitusStmt(parsetree);
		bool isAlterExtensionUpdateCitusStmt = isCreateAlterExtensionUpdateCitusStmt &&
											   IsA(parsetree, AlterExtensionStmt);

		bool citusCanBeUpdatedToAvailableVersion = false;

		if (isAlterExtensionUpdateCitusStmt)
		{
			citusCanBeUpdatedToAvailableVersion = !InstalledAndAvailableVersionsSame();
		}

		PrevProcessUtility_compat(pstmt, queryString, false, context,
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

		PostStandardProcessUtility(parsetree);
	}
	PG_CATCH();
	{
		PostStandardProcessUtility(parsetree);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Post process for ddl statements
	 */
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

	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *createStatement = (CreateStmt *) parsetree;

		PostprocessCreateTableStmt(createStatement, queryString);
	}

	if (IsA(parsetree, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createForeignTableStmt =
			(CreateForeignTableStmt *) parsetree;

		CreateStmt *createTableStmt = (CreateStmt *) (&createForeignTableStmt->base);

		/*
		 * Error out with a hint if the foreign table is using postgres_fdw and
		 * the option table_name is not provided.
		 * Citus relays all the Citus local foreign table logic to the placement of the
		 * Citus local table. If table_name is NOT provided, Citus would try to talk to
		 * the foreign postgres table over the shard's table name, which would not exist
		 * on the remote server.
		 */
		if (ServerUsesPostgresFDW(createForeignTableStmt->servername))
		{
			ErrorIfOptionListHasNoTableName(createForeignTableStmt->options);
		}

		PostprocessCreateTableStmt(createTableStmt, queryString);
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

		if (IsA(parsetree, AlterTableStmt))
		{
			/*
			 * This postprocess needs to be done after the distributed ddl jobs have
			 * been executed in the workers, hence is separate from PostprocessAlterTableStmt.
			 * We might have wrong index names generated on indexes of shards of partitions,
			 * so we perform the relevant checks and index renaming here.
			 */
			FixAlterTableStmtIndexNames(castNode(AlterTableStmt, parsetree));
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

			/*
			 * We make sure schema name is not null in the PreprocessIndexStmt.
			 */
			Oid schemaId = get_namespace_oid(indexStmt->relation->schemaname, true);
			Oid relationId = get_relname_relid(indexStmt->relation->relname, schemaId);

			/*
			 * If this a partitioned table, and CREATE INDEX was not run with ONLY,
			 * we have wrong index names generated on indexes of shards of
			 * partitions of this table, so we should fix them.
			 */
			if (IsCitusTable(relationId) && PartitionedTable(relationId) &&
				indexStmt->relation->inh)
			{
				/* only fix this specific index */
				Oid indexRelationId =
					get_relname_relid(indexStmt->idxname, schemaId);

				FixPartitionShardIndexNames(relationId, indexRelationId);
			}
		}

		/*
		 * Since we must have objects on workers before distributing them,
		 * mark object distributed as the last step.
		 */
		if (ops && ops->markDistributed)
		{
			ObjectAddress address = GetObjectAddressFromParseTree(parsetree, false);
			MarkObjectDistributed(&address);
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
		CitusHasBeenLoaded(); /* lgtm[cpp/return-value-ignored] */
	}
}


/*
 * UndistributeDisconnectedCitusLocalTables undistributes citus local tables that
 * are not connected to any reference tables via their individual foreign key
 * subgraphs. Note that this function undistributes only the auto-converted tables,
 * i.e the ones that are converted by Citus by cascading through foreign keys.
 */
void
UndistributeDisconnectedCitusLocalTables(void)
{
	List *citusLocalTableIdList = CitusTableTypeIdList(CITUS_LOCAL_TABLE);
	citusLocalTableIdList = SortList(citusLocalTableIdList, CompareOids);

	Oid citusLocalTableId = InvalidOid;
	foreach_oid(citusLocalTableId, citusLocalTableIdList)
	{
		/* acquire ShareRowExclusiveLock to prevent concurrent foreign key creation */
		LOCKMODE lockMode = ShareRowExclusiveLock;
		LockRelationOid(citusLocalTableId, lockMode);

		HeapTuple heapTuple =
			SearchSysCache1(RELOID, ObjectIdGetDatum(citusLocalTableId));
		if (!HeapTupleIsValid(heapTuple))
		{
			/*
			 * UndistributeTable drops relation, skip if already undistributed
			 * via cascade.
			 */
			continue;
		}
		ReleaseSysCache(heapTuple);

		if (PartitionTable(citusLocalTableId))
		{
			/* we skip here, we'll undistribute from the parent if necessary */
			UnlockRelationOid(citusLocalTableId, lockMode);
			continue;
		}

		if (!ShouldUndistributeCitusLocalTable(citusLocalTableId))
		{
			/* still connected to a reference table, skip it */
			UnlockRelationOid(citusLocalTableId, lockMode);
			continue;
		}

		/*
		 * Citus local table is not connected to any reference tables, then
		 * undistribute it via cascade. Here, instead of first dropping foreing
		 * keys then undistributing the table, we just set cascadeViaForeignKeys
		 * to true for simplicity.
		 *
		 * We suppress notices messages not to be too verbose. On the other hand,
		 * as UndistributeTable moves data to a new table, we want to inform user
		 * as it might take some time.
		 */
		ereport(NOTICE, (errmsg("removing table %s from metadata as it is not "
								"connected to any reference tables via foreign keys",
								generate_qualified_relation_name(citusLocalTableId))));
		TableConversionParameters params = {
			.relationId = citusLocalTableId,
			.cascadeViaForeignKeys = true,
			.suppressNoticeMessages = true
		};
		UndistributeTable(&params);
	}
}


/*
 * ShouldCheckUndistributeCitusLocalTables returns true if we might need to check
 * citus local tables for undistributing automatically.
 */
static bool
ShouldCheckUndistributeCitusLocalTables(void)
{
	if (!ConstraintDropped)
	{
		/*
		 * citus_drop_trigger executes notify_constraint_dropped to set
		 * ConstraintDropped to true, which means that last command dropped
		 * a table constraint.
		 */
		return false;
	}

	if (!CitusHasBeenLoaded())
	{
		/*
		 * If we are dropping citus, we should not try to undistribute citus
		 * local tables as they will also be dropped.
		 */
		return false;
	}

	if (!InCoordinatedTransaction())
	{
		/* not interacting with any Citus objects */
		return false;
	}

	if (IsCitusInternalBackend() || IsRebalancerInternalBackend())
	{
		/* connection from the coordinator operating on a shard */
		return false;
	}

	if (!ShouldEnableLocalReferenceForeignKeys())
	{
		/*
		 * If foreign keys between reference tables and local tables are
		 * disabled, then user might be using citus_add_local_table_to_metadata for
		 * their own purposes. In that case, we should not undistribute
		 * citus local tables.
		 */
		return false;
	}

	if (!IsCoordinator())
	{
		/* we should not perform this operation in worker nodes */
		return false;
	}

	return true;
}


/*
 * ShouldAddNewTableToMetadata takes a Node* and returns true if we need to add a
 * newly created table to metadata, false otherwise.
 * This function checks whether the given Node* is a CREATE TABLE statement.
 * For partitions and temporary tables, ShouldAddNewTableToMetadata returns false.
 * For other tables created, returns true, if we are on a coordinator that is added
 * as worker, and ofcourse, if the GUC use_citus_managed_tables is set to on.
 */
static bool
ShouldAddNewTableToMetadata(Node *parsetree)
{
	CreateStmt *createTableStmt;

	if (IsA(parsetree, CreateStmt))
	{
		createTableStmt = (CreateStmt *) parsetree;
	}
	else if (IsA(parsetree, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createForeignTableStmt =
			(CreateForeignTableStmt *) parsetree;
		createTableStmt = (CreateStmt *) &(createForeignTableStmt->base);
	}
	else
	{
		/* if the command is not CREATE [FOREIGN] TABLE, we can early return false */
		return false;
	}

	if (createTableStmt->relation->relpersistence == RELPERSISTENCE_TEMP ||
		createTableStmt->partbound != NULL)
	{
		/*
		 * Shouldn't add table to metadata if it's a temp table, or a partition.
		 * Creating partitions of a table that is added to metadata is already handled.
		 */
		return false;
	}

	if (AddAllLocalTablesToMetadata && !IsBinaryUpgrade &&
		IsCoordinator() && CoordinatorAddedAsWorkerNode())
	{
		/*
		 * We have verified that the GUC is set to true, and we are not upgrading,
		 * and we are on the coordinator that is added as worker node.
		 * So return true here, to add this newly created table to metadata.
		 */
		return true;
	}

	return false;
}


/*
 * ServerUsesPostgresFDW gets a foreign server name and returns true if the FDW that
 * the server depends on is postgres_fdw. Returns false otherwise.
 */
static bool
ServerUsesPostgresFDW(char *serverName)
{
	ForeignServer *server = GetForeignServerByName(serverName, false);
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, "postgres_fdw") == 0)
	{
		return true;
	}

	return false;
}


/*
 * ErrorIfOptionListHasNoTableName gets an option list (DefElem) and errors out
 * if the list does not contain a table_name element.
 */
static void
ErrorIfOptionListHasNoTableName(List *optionList)
{
	char *table_nameString = "table_name";
	DefElem *option = NULL;
	foreach_ptr(option, optionList)
	{
		char *optionName = option->defname;
		if (strcmp(optionName, table_nameString) == 0)
		{
			return;
		}
	}

	ereport(ERROR, (errmsg(
						"table_name option must be provided when using postgres_fdw with Citus"),
					errhint("Provide the option \"table_name\" with value target table's"
							" name")));
}


/*
 * NotifyUtilityHookConstraintDropped sets ConstraintDropped to true to tell us
 * last command dropped a table constraint.
 */
void
NotifyUtilityHookConstraintDropped(void)
{
	ConstraintDropped = true;
}


/*
 * ResetConstraintDropped sets ConstraintDropped to false.
 */
void
ResetConstraintDropped(void)
{
	ConstraintDropped = false;
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

	ObjectAddress targetObjectAddress = ddlJob->targetObjectAddress;

	if (OidIsValid(targetObjectAddress.classId))
	{
		/*
		 * Only for ddlJobs that are targetting an object we want to sync
		 * its metadata.
		 */
		shouldSyncMetadata = ShouldSyncUserCommandForObject(targetObjectAddress);

		if (targetObjectAddress.classId == RelationRelationId)
		{
			EnsurePartitionTableNotReplicated(targetObjectAddress.objectId);
		}
	}

	bool localExecutionSupported = true;

	if (!TaskListCannotBeExecutedInTransaction(ddlJob->taskList))
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

			if (ddlJob->metadataSyncCommand != NULL)
			{
				SendCommandToWorkersWithMetadata((char *) ddlJob->metadataSyncCommand);
			}
		}

		ExecuteUtilityTaskList(ddlJob->taskList, localExecutionSupported);
	}
	else
	{
		localExecutionSupported = false;

		/*
		 * Start a new transaction to make sure CONCURRENTLY commands
		 * on localhost do not block waiting for this transaction to finish.
		 *
		 * In addition to doing that, we also need to tell other backends
		 * --including the ones spawned for connections opened to localhost to
		 * build indexes on shards of this relation-- that concurrent index
		 * builds can safely ignore us.
		 *
		 * Normally, DefineIndex() only does that if index doesn't have any
		 * predicates (i.e.: where clause) and no index expressions at all.
		 * However, now that we already called standard process utility,
		 * index build on the shell table is finished anyway.
		 *
		 * The reason behind doing so is that we cannot guarantee not
		 * grabbing any snapshots via adaptive executor, and the backends
		 * creating indexes on local shards (if any) might block on waiting
		 * for current xact of the current backend to finish, which would
		 * cause self deadlocks that are not detectable.
		 */
		if (ddlJob->startNewTransaction)
		{
#if PG_VERSION_NUM < 140000

			/*
			 * Older versions of postgres doesn't have PROC_IN_SAFE_IC flag
			 * so we cannot use set_indexsafe_procflags in those versions.
			 *
			 * For this reason, we do our best to ensure not grabbing any
			 * snapshots later in the executor.
			 */

			/*
			 * If cache is not populated, system catalog lookups will cause
			 * the xmin of current backend to change. Then the last phase
			 * of CREATE INDEX CONCURRENTLY, which is in a separate backend,
			 * will hang waiting for our backend and result in a deadlock.
			 *
			 * We populate the cache before starting the next transaction to
			 * avoid this. Most of the metadata has already been resolved in
			 * planning phase, we only need to lookup metadata needed for
			 * connection establishment.
			 */
			(void) CurrentDatabaseName();

			/*
			 * ConnParams (AuthInfo and PoolInfo) gets a snapshot, which
			 * will blocks the remote connections to localhost. Hence we warm up
			 * the cache here so that after we start a new transaction, the entries
			 * will already be in the hash table, hence we won't be holding any snapshots.
			 */
			WarmUpConnParamsHash();
#endif

			/*
			 * Since it is not certain whether the code-path that we followed
			 * until reaching here caused grabbing any snapshots or not, we
			 * need to pop the active snapshot if we had any, to ensure not
			 * leaking any snapshots.
			 *
			 * For example, EnsureCoordinator might return without grabbing
			 * any snapshots if we didn't receive any invalidation messages
			 * but the otherwise is also possible.
			 */
			if (ActiveSnapshotSet())
			{
				PopActiveSnapshot();
			}

			CommitTransactionCommand();
			StartTransactionCommand();

#if PG_VERSION_NUM >= 140000

			/*
			 * Tell other backends to ignore us, even if we grab any
			 * snapshots via adaptive executor.
			 */
			set_indexsafe_procflags();

			/*
			 * We should not have any CREATE INDEX commands go through the
			 * local backend as we signaled other backends that this backend
			 * is executing a "safe" index command (PROC_IN_SAFE_IC), which
			 * is NOT true, we are only faking postgres based on the reasoning
			 * given above.
			 */
			Assert(localExecutionSupported == false);
#endif
		}

		MemoryContext savedContext = CurrentMemoryContext;

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

				commandList = lappend(commandList, (char *) ddlJob->metadataSyncCommand);

				SendBareCommandListToMetadataWorkers(commandList);
			}
		}
		PG_CATCH();
		{
			/* CopyErrorData() requires (CurrentMemoryContext != ErrorContext) */
			MemoryContextSwitchTo(savedContext);
			ErrorData *edata = CopyErrorData();

			/*
			 * In concurrent index creation, if a worker index with the same name already
			 * exists, prompt to DROP the current index and retry the original command
			 */
			if (edata->sqlerrcode == ERRCODE_DUPLICATE_TABLE)
			{
				ereport(ERROR,
						(errmsg("CONCURRENTLY-enabled index command failed"),
						 errdetail(
							 "CONCURRENTLY-enabled index commands can fail partially, "
							 "leaving behind an INVALID index."),
						 errhint("Use DROP INDEX CONCURRENTLY IF EXISTS to remove the "
								 "invalid index, then retry the original command.")));
			}
			else
			{
				ereport(WARNING,
						(errmsg(
							 "CONCURRENTLY-enabled index commands can fail partially, "
							 "leaving behind an INVALID index.\n Use DROP INDEX "
							 "CONCURRENTLY IF EXISTS to remove the invalid index.")));
				PG_RE_THROW();
			}
		}
		PG_END_TRY();
	}
}


#if PG_VERSION_NUM >= 140000

/*
 * set_indexsafe_procflags sets PROC_IN_SAFE_IC flag in MyProc->statusFlags.
 *
 * The flag is reset automatically at transaction end, so it must be set
 * for each transaction.
 *
 * Copied from pg/src/backend/commands/indexcmds.c
 * Also see pg commit c98763bf51bf610b3ee7e209fc76c3ff9a6b3163.
 */
static void
set_indexsafe_procflags(void)
{
	Assert(MyProc->xid == InvalidTransactionId &&
		   MyProc->xmin == InvalidTransactionId);

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyProc->statusFlags |= PROC_IN_SAFE_IC;
	ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
	LWLockRelease(ProcArrayLock);
}


#endif


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
	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->metadataSyncCommand = GetTableDDLCommand(command);
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

	/*
	 * Re-forming the foreign key graph relies on the command being executed
	 * on the local table first. However, in order to decide whether the
	 * command leads to an invalidation, we need to check before the command
	 * is being executed since we read pg_constraint table. Thus, we maintain a
	 * local flag and do the invalidation after multi_ProcessUtility,
	 * before ExecuteDistributedDDLJob().
	 */
	InvalidateForeignKeyGraphForDDL();
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
	/* don't allow concurrent node list changes that require an exclusive lock */
	List *workerNodes = TargetWorkerSetNodeList(targets, RowShareLock);

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
	ddlJob->targetObjectAddress = InvalidObjectAddress;
	ddlJob->metadataSyncCommand = NULL;
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


/*
 * ColumnarTableSetOptionsHook propagates columnar table options to shards, if
 * necessary.
 */
void
ColumnarTableSetOptionsHook(Oid relationId, ColumnarOptions options)
{
	if (EnableDDLPropagation && IsCitusTable(relationId))
	{
		/* when a columnar table is distributed update all settings on the shards */
		Oid namespaceId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(namespaceId);
		char *relationName = get_rel_name(relationId);
		TableDDLCommand *command = ColumnarGetCustomTableOptionsDDL(schemaName,
																	relationName,
																	options);
		DDLJob *ddljob = CreateCustomDDLTaskList(relationId, command);

		ExecuteDistributedDDLJob(ddljob);
	}
}
