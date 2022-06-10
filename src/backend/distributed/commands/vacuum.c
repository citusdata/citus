/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *    Commands for vacuuming distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "postmaster/bgworker_internals.h"
#include "access/xact.h"


#define VACUUM_PARALLEL_NOTSET -2

/*
 * Subset of VacuumParams we care about
 */
typedef struct CitusVacuumParams
{
	int options;
	VacOptValue truncate;
	VacOptValue index_cleanup;

	#if PG_VERSION_NUM >= PG_VERSION_13
	int nworkers;
	#endif
} CitusVacuumParams;

/* Local functions forward declarations for processing distributed table commands */
static bool IsDistributedVacuumStmt(List *vacuumRelationIdList);
static List * VacuumTaskList(Oid relationId, CitusVacuumParams vacuumParams,
							 List *vacuumColumnList);
static char * DeparseVacuumStmtPrefix(CitusVacuumParams vacuumParams);
static char * DeparseVacuumColumnNames(List *columnNameList);
static List * VacuumColumnList(VacuumStmt *vacuumStmt, int relationIndex);
static List * ExtractVacuumTargetRels(VacuumStmt *vacuumStmt);
static void ExecuteVacuumOnDistributedTables(VacuumStmt *vacuumStmt, List *relationIdList,
											 CitusVacuumParams vacuumParams);
static void ExecuteUnqualifiedVacuumTasks(VacuumStmt *vacuumStmt,
										  CitusVacuumParams vacuumParams);
static CitusVacuumParams VacuumStmtParams(VacuumStmt *vacstmt);
static List * VacuumRelationIdList(VacuumStmt *vacuumStmt, CitusVacuumParams
								   vacuumParams);

/*
 * PostprocessVacuumStmt processes vacuum statements that may need propagation to
 * citus tables only if ddl propagation is enabled. If a VACUUM or ANALYZE command
 * references a citus table or no table, it is propagated to all involved nodes; otherwise,
 * the statements will not be propagated.
 *
 * Unlike most other Process functions within this file, this function does not
 * return a modified parse node, as it is expected that the local VACUUM or
 * ANALYZE has already been processed.
 */
List *
PostprocessVacuumStmt(Node *node, const char *vacuumCommand)
{
	VacuumStmt *vacuumStmt = castNode(VacuumStmt, node);

	CitusVacuumParams vacuumParams = VacuumStmtParams(vacuumStmt);

	if (vacuumParams.options & VACOPT_VACUUM)
	{
		/*
		 * We commit the current transaction here so that the global lock
		 * taken from the shell table for VACUUM is released, which would block execution
		 * of shard placements. We don't do this in case of "ANALYZE <table>" command because
		 * its semantics are different than VACUUM and it doesn't acquire the global lock.
		 */
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	/*
	 * when no table is specified propagate the command as it is;
	 * otherwise, only propagate when there is at least 1 citus table
	 */
	List *relationIdList = VacuumRelationIdList(vacuumStmt, vacuumParams);

	if (list_length(vacuumStmt->rels) == 0)
	{
		/* no table is specified (unqualified vacuum) */

		ExecuteUnqualifiedVacuumTasks(vacuumStmt, vacuumParams);
	}
	else if (IsDistributedVacuumStmt(relationIdList))
	{
		/* there is at least 1 citus table specified */

		ExecuteVacuumOnDistributedTables(vacuumStmt, relationIdList,
										 vacuumParams);
	}

	/* else only local tables are specified */

	return NIL;
}


/*
 * VacuumRelationIdList returns the oid of the relations in the given vacuum statement.
 */
static List *
VacuumRelationIdList(VacuumStmt *vacuumStmt, CitusVacuumParams vacuumParams)
{
	LOCKMODE lockMode = (vacuumParams.options & VACOPT_FULL) ? AccessExclusiveLock :
						ShareUpdateExclusiveLock;

	List *vacuumRelationList = ExtractVacuumTargetRels(vacuumStmt);

	List *relationIdList = NIL;

	RangeVar *vacuumRelation = NULL;
	foreach_ptr(vacuumRelation, vacuumRelationList)
	{
		Oid relationId = RangeVarGetRelid(vacuumRelation, lockMode, false);
		relationIdList = lappend_oid(relationIdList, relationId);
	}

	return relationIdList;
}


/*
 * IsDistributedVacuumStmt returns true if there is any citus table in the relation id list;
 * otherwise, it returns false.
 */
static bool
IsDistributedVacuumStmt(List *vacuumRelationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, vacuumRelationIdList)
	{
		if (OidIsValid(relationId) && IsCitusTable(relationId))
		{
			return true;
		}
	}

	return false;
}


/*
 * ExecuteVacuumOnDistributedTables executes the vacuum for the shard placements of given tables
 * if they are citus tables.
 */
static void
ExecuteVacuumOnDistributedTables(VacuumStmt *vacuumStmt, List *relationIdList,
								 CitusVacuumParams vacuumParams)
{
	int relationIndex = 0;
	int executedVacuumCount = 0;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (IsCitusTable(relationId))
		{
			List *vacuumColumnList = VacuumColumnList(vacuumStmt, relationIndex);
			List *taskList = VacuumTaskList(relationId, vacuumParams, vacuumColumnList);

			/* local execution is not implemented for VACUUM commands */
			bool localExecutionSupported = false;
			ExecuteUtilityTaskList(taskList, localExecutionSupported);
			executedVacuumCount++;
		}
		relationIndex++;
	}
}


/*
 * VacuumTaskList returns a list of tasks to be executed as part of processing
 * a VacuumStmt which targets a distributed relation.
 */
static List *
VacuumTaskList(Oid relationId, CitusVacuumParams vacuumParams, List *vacuumColumnList)
{
	/* resulting task list */
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);

	const char *vacuumStringPrefix = DeparseVacuumStmtPrefix(vacuumParams);
	const char *columnNames = DeparseVacuumColumnNames(vacuumColumnList);

	/*
	 * We obtain ShareUpdateExclusiveLock here to not conflict with INSERT's
	 * RowExclusiveLock. However if VACUUM FULL is used, we already obtain
	 * AccessExclusiveLock before reaching to that point and INSERT's will be
	 * blocked anyway. This is inline with PostgreSQL's own behaviour.
	 */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* grab shard lock before getting placement list */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardRelationName = pstrdup(relationName);

		/* build shard relation name */
		AppendShardIdToName(&shardRelationName, shardId);

		char *quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

		/* copy base vacuum string and build the shard specific command */
		StringInfo vacuumStringForShard = makeStringInfo();
		appendStringInfoString(vacuumStringForShard, vacuumStringPrefix);

		appendStringInfoString(vacuumStringForShard, quotedShardName);
		appendStringInfoString(vacuumStringForShard, columnNames);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = VACUUM_ANALYZE_TASK;
		SetTaskQueryString(task, vacuumStringForShard->data);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);
		task->cannotBeExecutedInTransction = ((vacuumParams.options) & VACOPT_VACUUM);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * DeparseVacuumStmtPrefix returns a StringInfo appropriate for use as a prefix
 * during distributed execution of a VACUUM or ANALYZE statement. Callers may
 * reuse this prefix within a loop to generate shard-specific VACUUM or ANALYZE
 * statements.
 */
static char *
DeparseVacuumStmtPrefix(CitusVacuumParams vacuumParams)
{
	int vacuumFlags = vacuumParams.options;
	StringInfo vacuumPrefix = makeStringInfo();

	/* determine actual command and block out its bits */
	if (vacuumFlags & VACOPT_VACUUM)
	{
		appendStringInfoString(vacuumPrefix, "VACUUM ");
		vacuumFlags &= ~VACOPT_VACUUM;
	}
	else
	{
		Assert((vacuumFlags & VACOPT_ANALYZE) != 0);

		appendStringInfoString(vacuumPrefix, "ANALYZE ");
		vacuumFlags &= ~VACOPT_ANALYZE;

		if (vacuumFlags & VACOPT_VERBOSE)
		{
			appendStringInfoString(vacuumPrefix, "VERBOSE ");
			vacuumFlags &= ~VACOPT_VERBOSE;
		}
	}

	/* if no flags remain, exit early */
	if (vacuumFlags == 0 &&
		vacuumParams.truncate == VACOPTVALUE_UNSPECIFIED &&
		vacuumParams.index_cleanup == VACOPTVALUE_UNSPECIFIED
#if PG_VERSION_NUM >= PG_VERSION_13
		&& vacuumParams.nworkers == VACUUM_PARALLEL_NOTSET
#endif
		)
	{
		return vacuumPrefix->data;
	}

	/* otherwise, handle options */
	appendStringInfoChar(vacuumPrefix, '(');

	if (vacuumFlags & VACOPT_ANALYZE)
	{
		appendStringInfoString(vacuumPrefix, "ANALYZE,");
	}

	if (vacuumFlags & VACOPT_DISABLE_PAGE_SKIPPING)
	{
		appendStringInfoString(vacuumPrefix, "DISABLE_PAGE_SKIPPING,");
	}

	if (vacuumFlags & VACOPT_FREEZE)
	{
		appendStringInfoString(vacuumPrefix, "FREEZE,");
	}

	if (vacuumFlags & VACOPT_FULL)
	{
		appendStringInfoString(vacuumPrefix, "FULL,");
	}

	if (vacuumFlags & VACOPT_VERBOSE)
	{
		appendStringInfoString(vacuumPrefix, "VERBOSE,");
	}

	if (vacuumFlags & VACOPT_SKIP_LOCKED)
	{
		appendStringInfoString(vacuumPrefix, "SKIP_LOCKED,");
	}
	#if PG_VERSION_NUM >= PG_VERSION_14
	if (vacuumFlags & VACOPT_PROCESS_TOAST)
	{
		appendStringInfoString(vacuumPrefix, "PROCESS_TOAST,");
	}
	#endif
	if (vacuumParams.truncate != VACOPTVALUE_UNSPECIFIED)
	{
		appendStringInfoString(vacuumPrefix,
							   vacuumParams.truncate == VACOPTVALUE_ENABLED ?
							   "TRUNCATE," : "TRUNCATE false,"
							   );
	}

	if (vacuumParams.index_cleanup != VACOPTVALUE_UNSPECIFIED)
	{
		appendStringInfoString(vacuumPrefix,
							   vacuumParams.index_cleanup == VACOPTVALUE_ENABLED ?
							   "INDEX_CLEANUP," : "INDEX_CLEANUP false,"
							   );
	}

#if PG_VERSION_NUM >= PG_VERSION_13
	if (vacuumParams.nworkers != VACUUM_PARALLEL_NOTSET)
	{
		appendStringInfo(vacuumPrefix, "PARALLEL %d,", vacuumParams.nworkers);
	}
#endif

	vacuumPrefix->data[vacuumPrefix->len - 1] = ')';

	appendStringInfoChar(vacuumPrefix, ' ');

	return vacuumPrefix->data;
}


/*
 * DeparseVacuumColumnNames joins the list of strings using commas as a
 * delimiter. The whole thing is placed in parenthesis and set off with a
 * single space in order to facilitate appending it to the end of any VACUUM
 * or ANALYZE command which uses explicit column names. If the provided list
 * is empty, this function returns an empty string to keep the calling code
 * simplest.
 */
static char *
DeparseVacuumColumnNames(List *columnNameList)
{
	StringInfo columnNames = makeStringInfo();

	if (columnNameList == NIL)
	{
		return columnNames->data;
	}

	appendStringInfoString(columnNames, " (");

	String *columnName = NULL;
	foreach_ptr(columnName, columnNameList)
	{
		appendStringInfo(columnNames, "%s,", strVal(columnName));
	}

	columnNames->data[columnNames->len - 1] = ')';

	return columnNames->data;
}


/*
 * VacuumColumnList returns list of columns from relation
 * in the vacuum statement at specified relationIndex.
 */
static List *
VacuumColumnList(VacuumStmt *vacuumStmt, int relationIndex)
{
	VacuumRelation *vacuumRelation = (VacuumRelation *) list_nth(vacuumStmt->rels,
																 relationIndex);

	return vacuumRelation->va_cols;
}


/*
 * ExtractVacuumTargetRels returns list of target
 * relations from vacuum statement.
 */
static List *
ExtractVacuumTargetRels(VacuumStmt *vacuumStmt)
{
	List *vacuumList = NIL;

	VacuumRelation *vacuumRelation = NULL;
	foreach_ptr(vacuumRelation, vacuumStmt->rels)
	{
		vacuumList = lappend(vacuumList, vacuumRelation->relation);
	}

	return vacuumList;
}


/*
 * VacuumStmtParams returns a CitusVacuumParams based on the supplied VacuumStmt.
 */

/*
 * This is mostly ExecVacuum from Postgres's commands/vacuum.c
 * Note that ExecVacuum does an actual vacuum as well and we don't want
 * that to happen in the coordinator hence we copied the rest here.
 */
static CitusVacuumParams
VacuumStmtParams(VacuumStmt *vacstmt)
{
	CitusVacuumParams params;
	bool verbose = false;
	bool skip_locked = false;
	bool analyze = false;
	bool freeze = false;
	bool full = false;
	bool disable_page_skipping = false;
	#if PG_VERSION_NUM >= PG_VERSION_14
	bool process_toast = false;
	#endif

	/* Set default value */
	params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params.truncate = VACOPTVALUE_UNSPECIFIED;
	#if PG_VERSION_NUM >= PG_VERSION_13
	params.nworkers = VACUUM_PARALLEL_NOTSET;
	#endif

	/* Parse options list */
	DefElem *opt = NULL;
	foreach_ptr(opt, vacstmt->options)
	{
		/* Parse common options for VACUUM and ANALYZE */
		if (strcmp(opt->defname, "verbose") == 0)
		{
			verbose = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "skip_locked") == 0)
		{
			skip_locked = defGetBoolean(opt);
		}
		else if (!vacstmt->is_vacuumcmd)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized ANALYZE option \"%s\"", opt->defname)));
		}

		/* Parse options available on VACUUM */
		else if (strcmp(opt->defname, "analyze") == 0)
		{
			analyze = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "freeze") == 0)
		{
			freeze = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "full") == 0)
		{
			full = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "disable_page_skipping") == 0)
		{
			disable_page_skipping = defGetBoolean(opt);
		}
		#if PG_VERSION_NUM >= PG_VERSION_14
		else if (strcmp(opt->defname, "process_toast") == 0)
		{
			process_toast = defGetBoolean(opt);
		}
		#endif
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			params.index_cleanup = defGetBoolean(opt) ? VACOPTVALUE_ENABLED :
								   VACOPTVALUE_DISABLED;
		}
		else if (strcmp(opt->defname, "truncate") == 0)
		{
			params.truncate = defGetBoolean(opt) ? VACOPTVALUE_ENABLED :
							  VACOPTVALUE_DISABLED;
		}
		#if PG_VERSION_NUM >= PG_VERSION_13
		else if (strcmp(opt->defname, "parallel") == 0)
		{
			if (opt->arg == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel option requires a value between 0 and %d",
								MAX_PARALLEL_WORKER_LIMIT)));
			}
			else
			{
				int nworkers = defGetInt32(opt);
				if (nworkers < 0 || nworkers > MAX_PARALLEL_WORKER_LIMIT)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("parallel vacuum degree must be between 0 and %d",
									MAX_PARALLEL_WORKER_LIMIT)));
				}

				params.nworkers = nworkers;
			}
		}
		#endif
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized VACUUM option \"%s\"", opt->defname)
					));
		}
	}

	params.options = (vacstmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
					 (verbose ? VACOPT_VERBOSE : 0) |
					 (skip_locked ? VACOPT_SKIP_LOCKED : 0) |
					 (analyze ? VACOPT_ANALYZE : 0) |
					 (freeze ? VACOPT_FREEZE : 0) |
					 (full ? VACOPT_FULL : 0) |
					 #if PG_VERSION_NUM >= PG_VERSION_14
					 (process_toast ? VACOPT_PROCESS_TOAST : 0) |
					 #endif
					 (disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0);
	return params;
}


/*
 * ExecuteUnqualifiedVacuumTasks executes tasks for unqualified vacuum commands
 */
static void
ExecuteUnqualifiedVacuumTasks(VacuumStmt *vacuumStmt, CitusVacuumParams vacuumParams)
{
	/* don't allow concurrent node list changes that require an exclusive lock */
	List *workerNodes = TargetWorkerSetNodeList(ALL_SHARD_NODES, RowShareLock);

	if (list_length(workerNodes) == 0)
	{
		return;
	}

	const char *vacuumStringPrefix = DeparseVacuumStmtPrefix(vacuumParams);

	StringInfo vacuumCommand = makeStringInfo();
	appendStringInfoString(vacuumCommand, vacuumStringPrefix);

	List *unqualifiedVacuumCommands = list_make3(DISABLE_DDL_PROPAGATION,
												 vacuumCommand->data,
												 ENABLE_DDL_PROPAGATION);

	Task *task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskType = VACUUM_ANALYZE_TASK;
	SetTaskQueryStringList(task, unqualifiedVacuumCommands);
	task->dependentTaskList = NULL;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	task->cannotBeExecutedInTransction = ((vacuumParams.options) & VACOPT_VACUUM);


	int32 localNodeGroupId = GetLocalGroupId();

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodes)
	{
		if (workerNode->groupId != localNodeGroupId)
		{
			ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
			targetPlacement->nodeName = workerNode->workerName;
			targetPlacement->nodePort = workerNode->workerPort;
			targetPlacement->groupId = workerNode->groupId;

			task->taskPlacementList = lappend(task->taskPlacementList,
											  targetPlacement);
		}
	}

	bool localExecution = false;
	ExecuteUtilityTaskList(list_make1(task), localExecution);
}
