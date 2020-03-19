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

#if PG_VERSION_NUM >= 120000
#include "commands/defrem.h"
#endif
#include "commands/vacuum.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Subset of VacuumParams we care about
 */
typedef struct CitusVacuumParams
{
	int options;
#if PG_VERSION_NUM >= 120000
	VacOptTernaryValue truncate;
	VacOptTernaryValue index_cleanup;
#endif
} CitusVacuumParams;


/* Local functions forward declarations for processing distributed table commands */
static bool IsDistributedVacuumStmt(int vacuumOptions, List *vacuumRelationIdList);
static List * VacuumTaskList(Oid relationId, CitusVacuumParams vacuumParams,
							 List *vacuumColumnList);
static char * DeparseVacuumStmtPrefix(CitusVacuumParams vacuumParams);
static char * DeparseVacuumColumnNames(List *columnNameList);
static List * VacuumColumnList(VacuumStmt *vacuumStmt, int relationIndex);
static List * ExtractVacuumTargetRels(VacuumStmt *vacuumStmt);
static CitusVacuumParams VacuumStmtParams(VacuumStmt *vacstmt);

/*
 * PostprocessVacuumStmt processes vacuum statements that may need propagation to
 * distributed tables. If a VACUUM or ANALYZE command references a distributed
 * table, it is propagated to all involved nodes; otherwise, this function will
 * immediately exit after some error checking.
 *
 * Unlike most other Process functions within this file, this function does not
 * return a modified parse node, as it is expected that the local VACUUM or
 * ANALYZE has already been processed.
 */
void
PostprocessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand)
{
	int relationIndex = 0;
	List *vacuumRelationList = ExtractVacuumTargetRels(vacuumStmt);
	List *relationIdList = NIL;
	CitusVacuumParams vacuumParams = VacuumStmtParams(vacuumStmt);
	LOCKMODE lockMode = (vacuumParams.options & VACOPT_FULL) ? AccessExclusiveLock :
						ShareUpdateExclusiveLock;
	int executedVacuumCount = 0;

	RangeVar *vacuumRelation = NULL;
	foreach_ptr(vacuumRelation, vacuumRelationList)
	{
		Oid relationId = RangeVarGetRelid(vacuumRelation, lockMode, false);
		relationIdList = lappend_oid(relationIdList, relationId);
	}

	bool distributedVacuumStmt = IsDistributedVacuumStmt(vacuumParams.options,
														 relationIdList);
	if (!distributedVacuumStmt)
	{
		return;
	}

	/* execute vacuum on distributed tables */
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (IsCitusTable(relationId))
		{
			/*
			 * VACUUM commands cannot run inside a transaction block, so we use
			 * the "bare" commit protocol without BEGIN/COMMIT. However, ANALYZE
			 * commands can run inside a transaction block. Notice that we do this
			 * once even if there are multiple distributed tables to be vacuumed.
			 */
			if (executedVacuumCount == 0 && (vacuumParams.options & VACOPT_VACUUM) != 0)
			{
				/* save old commit protocol to restore at xact end */
				Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
				SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
				MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;
			}

			List *vacuumColumnList = VacuumColumnList(vacuumStmt, relationIndex);
			List *taskList = VacuumTaskList(relationId, vacuumParams, vacuumColumnList);

			/* use adaptive executor when enabled */
			ExecuteUtilityTaskListWithoutResults(taskList);
			executedVacuumCount++;
		}
		relationIndex++;
	}
}


/*
 * IsSupportedDistributedVacuumStmt returns whether distributed execution of a
 * given VacuumStmt is supported. The provided relationId list represents
 * the list of tables targeted by the provided statement.
 *
 * Returns true if the statement requires distributed execution and returns
 * false otherwise.
 */
static bool
IsDistributedVacuumStmt(int vacuumOptions, List *vacuumRelationIdList)
{
	const char *stmtName = (vacuumOptions & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";
	bool distributeStmt = false;
	int distributedRelationCount = 0;

	/*
	 * No table in the vacuum statement means vacuuming all relations
	 * which is not supported by citus.
	 */
	int vacuumedRelationCount = list_length(vacuumRelationIdList);
	if (vacuumedRelationCount == 0)
	{
		/* WARN for unqualified VACUUM commands */
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", stmtName),
						  errhint("Provide a specific table in order to %s "
								  "distributed tables.", stmtName)));
	}

	Oid relationId = InvalidOid;
	foreach_oid(relationId, vacuumRelationIdList)
	{
		if (OidIsValid(relationId) && IsCitusTable(relationId))
		{
			distributedRelationCount++;
		}
	}

	if (distributedRelationCount == 0)
	{
		/* nothing to do here */
	}
	else if (!EnableDDLPropagation)
	{
		/* WARN if DDL propagation is not enabled */
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", stmtName),
						  errhint("Set citus.enable_ddl_propagation to true in order to "
								  "send targeted %s commands to worker nodes.",
								  stmtName)));
	}
	else
	{
		distributeStmt = true;
	}

	return distributeStmt;
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
	if (vacuumFlags == 0
#if PG_VERSION_NUM >= 120000
		&& vacuumParams.truncate == VACOPT_TERNARY_DEFAULT &&
		vacuumParams.index_cleanup == VACOPT_TERNARY_DEFAULT
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

#if PG_VERSION_NUM >= 120000
	if (vacuumFlags & VACOPT_SKIP_LOCKED)
	{
		appendStringInfoString(vacuumPrefix, "SKIP_LOCKED,");
	}

	if (vacuumParams.truncate != VACOPT_TERNARY_DEFAULT)
	{
		appendStringInfoString(vacuumPrefix,
							   vacuumParams.truncate == VACOPT_TERNARY_ENABLED ?
							   "TRUNCATE," : "TRUNCATE false,"
							   );
	}

	if (vacuumParams.index_cleanup != VACOPT_TERNARY_DEFAULT)
	{
		appendStringInfoString(vacuumPrefix,
							   vacuumParams.index_cleanup == VACOPT_TERNARY_ENABLED ?
							   "INDEX_CLEANUP," : "INDEX_CLEANUP false,"
							   );
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

	Value *columnName = NULL;
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
#if PG_VERSION_NUM >= 120000

/*
 * This is mostly ExecVacuum from Postgres's commands/vacuum.c
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

	/* Set default value */
	params.index_cleanup = VACOPT_TERNARY_DEFAULT;
	params.truncate = VACOPT_TERNARY_DEFAULT;

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
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			params.index_cleanup = defGetBoolean(opt) ? VACOPT_TERNARY_ENABLED :
								   VACOPT_TERNARY_DISABLED;
		}
		else if (strcmp(opt->defname, "truncate") == 0)
		{
			params.truncate = defGetBoolean(opt) ? VACOPT_TERNARY_ENABLED :
							  VACOPT_TERNARY_DISABLED;
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
					 (disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0);
	return params;
}


#else
static CitusVacuumParams
VacuumStmtParams(VacuumStmt *vacuumStmt)
{
	CitusVacuumParams params;
	params.options = vacuumStmt->options;
	return params;
}


#endif
