/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *    Commands for vacuuming distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* Local functions forward declarations for processing distributed table commands */
static bool IsDistributedVacuumStmt(VacuumStmt *vacuumStmt, List *vacuumRelationIdList);
static List * VacuumTaskList(Oid relationId, int vacuumOptions, List *vacuumColumnList);
static StringInfo DeparseVacuumStmtPrefix(int vacuumFlags);
static char * DeparseVacuumColumnNames(List *columnNameList);


/*
 * ProcessVacuumStmt processes vacuum statements that may need propagation to
 * distributed tables. If a VACUUM or ANALYZE command references a distributed
 * table, it is propagated to all involved nodes; otherwise, this function will
 * immediately exit after some error checking.
 *
 * Unlike most other Process functions within this file, this function does not
 * return a modified parse node, as it is expected that the local VACUUM or
 * ANALYZE has already been processed.
 */
void
ProcessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand)
{
	int relationIndex = 0;
	bool distributedVacuumStmt = false;
	List *vacuumRelationList = ExtractVacuumTargetRels(vacuumStmt);
	ListCell *vacuumRelationCell = NULL;
	List *relationIdList = NIL;
	ListCell *relationIdCell = NULL;
	LOCKMODE lockMode = (vacuumStmt->options & VACOPT_FULL) ? AccessExclusiveLock :
						ShareUpdateExclusiveLock;
	int executedVacuumCount = 0;

	foreach(vacuumRelationCell, vacuumRelationList)
	{
		RangeVar *vacuumRelation = (RangeVar *) lfirst(vacuumRelationCell);
		Oid relationId = RangeVarGetRelid(vacuumRelation, lockMode, false);
		relationIdList = lappend_oid(relationIdList, relationId);
	}

	distributedVacuumStmt = IsDistributedVacuumStmt(vacuumStmt, relationIdList);
	if (!distributedVacuumStmt)
	{
		return;
	}

	/* execute vacuum on distributed tables */
	foreach(relationIdCell, relationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		if (IsDistributedTable(relationId))
		{
			List *vacuumColumnList = NIL;
			List *taskList = NIL;
			int targetPoolSize = MaxAdaptiveExecutorPoolSize;

			/*
			 * VACUUM commands cannot run inside a transaction block, so we use
			 * the "bare" commit protocol without BEGIN/COMMIT. However, ANALYZE
			 * commands can run inside a transaction block. Notice that we do this
			 * once even if there are multiple distributed tables to be vacuumed.
			 */
			if (executedVacuumCount == 0 && (vacuumStmt->options & VACOPT_VACUUM) != 0)
			{
				/* save old commit protocol to restore at xact end */
				Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
				SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
				MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;
			}

			vacuumColumnList = VacuumColumnList(vacuumStmt, relationIndex);
			taskList = VacuumTaskList(relationId, vacuumStmt->options, vacuumColumnList);

			/* use adaptive executor when enabled */
			ExecuteUtilityTaskListWithoutResults(taskList, targetPoolSize);
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
IsDistributedVacuumStmt(VacuumStmt *vacuumStmt, List *vacuumRelationIdList)
{
	const char *stmtName = (vacuumStmt->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";
	bool distributeStmt = false;
	ListCell *relationIdCell = NULL;
	int distributedRelationCount = 0;
	int vacuumedRelationCount = 0;

	/*
	 * No table in the vacuum statement means vacuuming all relations
	 * which is not supported by citus.
	 */
	vacuumedRelationCount = list_length(vacuumRelationIdList);
	if (vacuumedRelationCount == 0)
	{
		/* WARN for unqualified VACUUM commands */
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", stmtName),
						  errhint("Provide a specific table in order to %s "
								  "distributed tables.", stmtName)));
	}

	foreach(relationIdCell, vacuumRelationIdList)
	{
		Oid relationId = lfirst_oid(relationIdCell);
		if (OidIsValid(relationId) && IsDistributedTable(relationId))
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
VacuumTaskList(Oid relationId, int vacuumOptions, List *vacuumColumnList)
{
	List *taskList = NIL;
	List *shardIntervalList = NIL;
	ListCell *shardIntervalCell = NULL;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;
	StringInfo vacuumString = DeparseVacuumStmtPrefix(vacuumOptions);
	const char *columnNames = NULL;
	const int vacuumPrefixLen = vacuumString->len;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(relationId);

	columnNames = DeparseVacuumColumnNames(vacuumColumnList);

	/*
	 * We obtain ShareUpdateExclusiveLock here to not conflict with INSERT's
	 * RowExclusiveLock. However if VACUUM FULL is used, we already obtain
	 * AccessExclusiveLock before reaching to that point and INSERT's will be
	 * blocked anyway. This is inline with PostgreSQL's own behaviour.
	 */
	LockRelationOid(relationId, ShareUpdateExclusiveLock);

	shardIntervalList = LoadShardIntervalList(relationId);

	/* grab shard lock before getting placement list */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		Task *task = NULL;

		char *shardName = pstrdup(tableName);
		AppendShardIdToName(&shardName, shardInterval->shardId);
		shardName = quote_qualified_identifier(schemaName, shardName);

		vacuumString->len = vacuumPrefixLen;
		appendStringInfoString(vacuumString, shardName);
		appendStringInfoString(vacuumString, columnNames);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = VACUUM_ANALYZE_TASK;
		task->queryString = pstrdup(vacuumString->data);
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

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
static StringInfo
DeparseVacuumStmtPrefix(int vacuumFlags)
{
	StringInfo vacuumPrefix = makeStringInfo();
	const int unsupportedFlags PG_USED_FOR_ASSERTS_ONLY = ~(
		VACOPT_ANALYZE |
		VACOPT_DISABLE_PAGE_SKIPPING |
		VACOPT_FREEZE |
		VACOPT_FULL |
		VACOPT_VERBOSE
		);

	/* determine actual command and block out its bit */
	if (vacuumFlags & VACOPT_VACUUM)
	{
		appendStringInfoString(vacuumPrefix, "VACUUM ");
		vacuumFlags &= ~VACOPT_VACUUM;
	}
	else
	{
		appendStringInfoString(vacuumPrefix, "ANALYZE ");
		vacuumFlags &= ~VACOPT_ANALYZE;

		if (vacuumFlags & VACOPT_VERBOSE)
		{
			appendStringInfoString(vacuumPrefix, "VERBOSE ");
			vacuumFlags &= ~VACOPT_VERBOSE;
		}
	}

	/* unsupported flags should have already been rejected */
	Assert((vacuumFlags & unsupportedFlags) == 0);

	/* if no flags remain, exit early */
	if (vacuumFlags == 0)
	{
		return vacuumPrefix;
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

	vacuumPrefix->data[vacuumPrefix->len - 1] = ')';

	appendStringInfoChar(vacuumPrefix, ' ');

	return vacuumPrefix;
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
	ListCell *columnNameCell = NULL;

	if (columnNameList == NIL)
	{
		return columnNames->data;
	}

	appendStringInfoString(columnNames, " (");

	foreach(columnNameCell, columnNameList)
	{
		char *columnName = strVal(lfirst(columnNameCell));

		appendStringInfo(columnNames, "%s,", columnName);
	}

	columnNames->data[columnNames->len - 1] = ')';

	return columnNames->data;
}
