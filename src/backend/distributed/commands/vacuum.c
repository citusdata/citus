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

#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "postmaster/bgworker_internals.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_version_constants.h"

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


#define VACUUM_PARALLEL_NOTSET -2

/*
 * Subset of VacuumParams we care about
 */
typedef struct CitusVacuumParams
{
	int options;
	VacOptValue truncate;
	VacOptValue index_cleanup;
	int nworkers;
	int ring_size;
} CitusVacuumParams;

/*
 * Information we track per VACUUM/ANALYZE target relation.
 */
typedef struct CitusVacuumRelation
{
	VacuumRelation *vacuumRelation;
	Oid relationId;
} CitusVacuumRelation;

/* Local functions forward declarations for processing distributed table commands */
static bool IsDistributedVacuumStmt(List *vacuumRelationList);
static List * VacuumTaskList(Oid relationId, CitusVacuumParams vacuumParams,
							 List *vacuumColumnList);
static char * DeparseVacuumStmtPrefix(CitusVacuumParams vacuumParams);
static char * DeparseVacuumColumnNames(List *columnNameList);
static void ExecuteVacuumOnDistributedTables(VacuumStmt *vacuumStmt, List *relationList,
											 CitusVacuumParams vacuumParams);
static void ExecuteUnqualifiedVacuumTasks(VacuumStmt *vacuumStmt,
										  CitusVacuumParams vacuumParams);
static CitusVacuumParams VacuumStmtParams(VacuumStmt *vacstmt);
static List * VacuumRelationList(VacuumStmt *vacuumStmt, CitusVacuumParams vacuumParams);

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
	List *vacuumRelationList = VacuumRelationList(vacuumStmt, vacuumParams);

	if (list_length(vacuumStmt->rels) == 0)
	{
		/* no table is specified (unqualified vacuum) */

		ExecuteUnqualifiedVacuumTasks(vacuumStmt, vacuumParams);
	}
	else if (IsDistributedVacuumStmt(vacuumRelationList))
	{
		/* there is at least 1 citus table specified */

		ExecuteVacuumOnDistributedTables(vacuumStmt, vacuumRelationList,
										 vacuumParams);
	}

	/* else only local tables are specified */

	return NIL;
}


/*
 * VacuumRelationList returns the list of relations in the given vacuum statement,
 * along with their resolved Oids (if they can be locked).
 */
static List *
VacuumRelationList(VacuumStmt *vacuumStmt, CitusVacuumParams vacuumParams)
{
	LOCKMODE lockMode = (vacuumParams.options & VACOPT_FULL) ? AccessExclusiveLock :
						ShareUpdateExclusiveLock;

	bool skipLocked = (vacuumParams.options & VACOPT_SKIP_LOCKED);

	List *relationList = NIL;

	VacuumRelation *vacuumRelation = NULL;
	foreach_declared_ptr(vacuumRelation, vacuumStmt->rels)
	{
		Oid relationId = InvalidOid;

		/*
		 * If skip_locked option is enabled, we are skipping that relation
		 * if the lock for it is currently not available; otherwise, we get the lock.
		 */
		if (vacuumRelation->relation)
		{
			relationId = RangeVarGetRelidExtended(vacuumRelation->relation,
												  lockMode,
												  skipLocked ? RVR_SKIP_LOCKED : 0, NULL,
												  NULL);
		}
		else if (OidIsValid(vacuumRelation->oid))
		{
			/* fall back to the Oid directly when provided */
			if (!skipLocked || ConditionalLockRelationOid(vacuumRelation->oid, lockMode))
			{
				if (!skipLocked)
				{
					LockRelationOid(vacuumRelation->oid, lockMode);
				}
				relationId = vacuumRelation->oid;
			}
		}

		if (OidIsValid(relationId))
		{
			CitusVacuumRelation *relation = palloc(sizeof(CitusVacuumRelation));
			relation->vacuumRelation = vacuumRelation;
			relation->relationId = relationId;
			relationList = lappend(relationList, relation);
		}
	}

	return relationList;
}


/*
 * IsDistributedVacuumStmt returns true if there is any citus table in the relation id list;
 * otherwise, it returns false.
 */
static bool
IsDistributedVacuumStmt(List *vacuumRelationList)
{
	CitusVacuumRelation *vacuumRelation = NULL;
	foreach_declared_ptr(vacuumRelation, vacuumRelationList)
	{
		if (OidIsValid(vacuumRelation->relationId) &&
			IsCitusTable(vacuumRelation->relationId))
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
ExecuteVacuumOnDistributedTables(VacuumStmt *vacuumStmt, List *relationList,
								 CitusVacuumParams vacuumParams)
{
	CitusVacuumRelation *vacuumRelationEntry = NULL;
	foreach_declared_ptr(vacuumRelationEntry, relationList)
	{
		Oid relationId = vacuumRelationEntry->relationId;
		VacuumRelation *vacuumRelation = vacuumRelationEntry->vacuumRelation;

		RangeVar *relation = vacuumRelation->relation;
		if (relation != NULL && !relation->inh)
		{
			/* ONLY specified, so don't recurse to shard placements */
			continue;
		}

		if (IsCitusTable(relationId))
		{
			List *vacuumColumnList = vacuumRelation->va_cols;
			List *taskList = VacuumTaskList(relationId, vacuumParams, vacuumColumnList);

			/* local execution is not implemented for VACUUM commands */
			bool localExecutionSupported = false;
			ExecuteUtilityTaskList(taskList, localExecutionSupported);
		}
	}
}


/*
 * VacuumTaskList returns a list of tasks to be executed as part of processing
 * a VacuumStmt which targets a distributed relation.
 */
static List *
VacuumTaskList(Oid relationId, CitusVacuumParams vacuumParams, List *vacuumColumnList)
{
	LOCKMODE lockMode = (vacuumParams.options & VACOPT_FULL) ? AccessExclusiveLock :
						ShareUpdateExclusiveLock;

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
	 * Also note that if skip locked option is enabled, we try to acquire the lock
	 * in nonblocking way. If lock is not available, vacuum just skip that relation.
	 */
	if (!(vacuumParams.options & VACOPT_SKIP_LOCKED))
	{
		LockRelationOid(relationId, lockMode);
	}
	else
	{
		if (!ConditionalLockRelationOid(relationId, lockMode))
		{
			return NIL;
		}
	}

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* grab shard lock before getting placement list */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardIntervalList)
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
		task->cannotBeExecutedInTransaction = ((vacuumParams.options) & VACOPT_VACUUM);

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
	if (vacuumFlags & VACOPT_PROCESS_TOAST &&
		vacuumFlags & VACOPT_PROCESS_MAIN)
	{
		/* process toast and process main are true by default */
		if (((vacuumFlags & ~VACOPT_PROCESS_TOAST) & ~VACOPT_PROCESS_MAIN) == 0 &&
			vacuumParams.ring_size == -1 &&
			vacuumParams.truncate == VACOPTVALUE_UNSPECIFIED &&
			vacuumParams.index_cleanup == VACOPTVALUE_UNSPECIFIED &&
			vacuumParams.nworkers == VACUUM_PARALLEL_NOTSET
			)
		{
			return vacuumPrefix->data;
		}
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

	if (!(vacuumFlags & VACOPT_PROCESS_TOAST))
	{
		appendStringInfoString(vacuumPrefix, "PROCESS_TOAST FALSE,");
	}

	if (!(vacuumFlags & VACOPT_PROCESS_MAIN))
	{
		appendStringInfoString(vacuumPrefix, "PROCESS_MAIN FALSE,");
	}

	if (vacuumFlags & VACOPT_SKIP_DATABASE_STATS)
	{
		appendStringInfoString(vacuumPrefix, "SKIP_DATABASE_STATS,");
	}

	if (vacuumFlags & VACOPT_ONLY_DATABASE_STATS)
	{
		appendStringInfoString(vacuumPrefix, "ONLY_DATABASE_STATS,");
	}

	if (vacuumParams.ring_size != -1)
	{
		appendStringInfo(vacuumPrefix, "BUFFER_USAGE_LIMIT %d,", vacuumParams.ring_size);
	}

	if (vacuumParams.truncate != VACOPTVALUE_UNSPECIFIED)
	{
		appendStringInfoString(vacuumPrefix,
							   vacuumParams.truncate == VACOPTVALUE_ENABLED ?
							   "TRUNCATE," : "TRUNCATE false,"
							   );
	}

	if (vacuumParams.index_cleanup != VACOPTVALUE_UNSPECIFIED)
	{
		switch (vacuumParams.index_cleanup)
		{
			case VACOPTVALUE_ENABLED:
			{
				appendStringInfoString(vacuumPrefix, "INDEX_CLEANUP true,");
				break;
			}

			case VACOPTVALUE_DISABLED:
			{
				appendStringInfoString(vacuumPrefix, "INDEX_CLEANUP false,");
				break;
			}

			case VACOPTVALUE_AUTO:
			{
				appendStringInfoString(vacuumPrefix, "INDEX_CLEANUP auto,");
				break;
			}

			default:
			{
				break;
			}
		}
	}

	if (vacuumParams.nworkers != VACUUM_PARALLEL_NOTSET)
	{
		appendStringInfo(vacuumPrefix, "PARALLEL %d,", vacuumParams.nworkers);
	}

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
	foreach_declared_ptr(columnName, columnNameList)
	{
		appendStringInfo(columnNames, "%s,", strVal(columnName));
	}

	columnNames->data[columnNames->len - 1] = ')';

	return columnNames->data;
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
	bool process_toast = true;
	bool process_main = true;
	bool skip_database_stats = false;
	bool only_database_stats = false;
	params.ring_size = -1;

	/* Set default value */
	params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params.truncate = VACOPTVALUE_UNSPECIFIED;
	params.nworkers = VACUUM_PARALLEL_NOTSET;

	/* Parse options list */
	DefElem *opt = NULL;
	foreach_declared_ptr(opt, vacstmt->options)
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
		else if (strcmp(opt->defname, "buffer_usage_limit") == 0)
		{
			char *vac_buffer_size = defGetString(opt);
			parse_int(vac_buffer_size, &params.ring_size, GUC_UNIT_KB, NULL);
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
		else if (strcmp(opt->defname, "process_main") == 0)
		{
			process_main = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "skip_database_stats") == 0)
		{
			skip_database_stats = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "only_database_stats") == 0)
		{
			only_database_stats = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "process_toast") == 0)
		{
			process_toast = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			/* Interpret no string as the default, which is 'auto' */
			if (!opt->arg)
			{
				params.index_cleanup = VACOPTVALUE_AUTO;
			}
			else
			{
				char *sval = defGetString(opt);

				/* Try matching on 'auto' string, or fall back on boolean */
				if (pg_strcasecmp(sval, "auto") == 0)
				{
					params.index_cleanup = VACOPTVALUE_AUTO;
				}
				else
				{
					params.index_cleanup = defGetBoolean(opt) ? VACOPTVALUE_ENABLED :
										   VACOPTVALUE_DISABLED;
				}
			}
		}
		else if (strcmp(opt->defname, "truncate") == 0)
		{
			params.truncate = defGetBoolean(opt) ? VACOPTVALUE_ENABLED :
							  VACOPTVALUE_DISABLED;
		}
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
					 (process_main ? VACOPT_PROCESS_MAIN : 0) |
					 (skip_database_stats ? VACOPT_SKIP_DATABASE_STATS : 0) |
					 (only_database_stats ? VACOPT_ONLY_DATABASE_STATS : 0) |
					 (process_toast ? VACOPT_PROCESS_TOAST : 0) |
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
	task->cannotBeExecutedInTransaction = ((vacuumParams.options) & VACOPT_VACUUM);


	bool hasPeerWorker = false;
	int32 localNodeGroupId = GetLocalGroupId();

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodes)
	{
		if (workerNode->groupId != localNodeGroupId)
		{
			ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
			targetPlacement->nodeName = workerNode->workerName;
			targetPlacement->nodePort = workerNode->workerPort;
			targetPlacement->groupId = workerNode->groupId;

			task->taskPlacementList = lappend(task->taskPlacementList,
											  targetPlacement);
			hasPeerWorker = true;
		}
	}

	if (hasPeerWorker)
	{
		bool localExecution = false;
		ExecuteUtilityTaskList(list_make1(task), localExecution);
	}
}
