/*-------------------------------------------------------------------------
 *
 * local_plan_cache.c
 *
 * Local plan cache related functions
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/local_plan_cache.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/version_compat.h"


static Query * GetLocalShardQueryForCache(Query *jobQuery, Task *task,
										  ParamListInfo paramListInfo);
static char * DeparseLocalShardQuery(Query *jobQuery, List *relationShardList,
									 Oid anchorDistributedTableId, int64 anchorShardId);
static int ExtractParameterTypesForParamListInfo(ParamListInfo originalParamListInfo,
												 Oid **parameterTypes);

/*
 * CacheLocalPlanForShardQuery replaces the relation OIDs in the job query
 * with shard relation OIDs and then plans the query and caches the result
 * in the originalDistributedPlan (which may be preserved across executions).
 */
void
CacheLocalPlanForShardQuery(Task *task, DistributedPlan *originalDistributedPlan,
							ParamListInfo paramListInfo)
{
	PlannedStmt *localPlan = GetCachedLocalPlan(task, originalDistributedPlan);
	if (localPlan != NULL)
	{
		/* we already have a local plan */
		return;
	}

	if (list_length(task->relationShardList) == 0)
	{
		/* zero shard plan, no need to cache */
		return;
	}

	/*
	 * All memory allocations should happen in the plan's context
	 * since we'll cache the local plan there.
	 */
	MemoryContext oldContext =
		MemoryContextSwitchTo(GetMemoryChunkContext(originalDistributedPlan));

	/*
	 * We prefer to use jobQuery (over task->query) because we don't want any
	 * functions/params to have been evaluated in the cached plan.
	 */
	Query *jobQuery = copyObject(originalDistributedPlan->workerJob->jobQuery);

	Query *localShardQuery = GetLocalShardQueryForCache(jobQuery, task, paramListInfo);

	LOCKMODE lockMode = GetQueryLockMode(localShardQuery);

	/* fast path queries can only have a single RTE by definition */
	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) linitial(localShardQuery->rtable);

	/*
	 * If the shard has been created in this transction, we wouldn't see the relationId
	 * for it, so do not cache.
	 */
	if (rangeTableEntry->relid == InvalidOid)
	{
		pfree(jobQuery);
		pfree(localShardQuery);
		MemoryContextSwitchTo(oldContext);
		return;
	}

	LockRelationOid(rangeTableEntry->relid, lockMode);

	LocalPlannedStatement *localPlannedStatement = CitusMakeNode(LocalPlannedStatement);
	localPlan = planner(localShardQuery, NULL, 0, NULL);
	localPlannedStatement->localPlan = localPlan;
	localPlannedStatement->shardId = task->anchorShardId;
	localPlannedStatement->localGroupId = GetLocalGroupId();

	originalDistributedPlan->workerJob->localPlannedStatements =
		lappend(originalDistributedPlan->workerJob->localPlannedStatements,
				localPlannedStatement);

	MemoryContextSwitchTo(oldContext);
}


/*
 * GetLocalShardQueryForCache is a helper function which generates
 * the local shard query based on the jobQuery. The function should
 * not be used for generic purposes, it is specialized for local cached
 * queries.
 *
 * It is not guaranteed to have consistent attribute numbers on the shards
 * and on the shell (e.g., distributed/reference tables) due to DROP COLUMN
 * commands.
 *
 * To avoid any edge cases due to such discrepancies, we first deparse the
 * jobQuery with the tables replaced to shards, and parse the query string
 * back. This is normally a very expensive operation, however we only do it
 * once per cached local plan, which is acceptable.
 */
static Query *
GetLocalShardQueryForCache(Query *jobQuery, Task *task, ParamListInfo orig_paramListInfo)
{
	char *shardQueryString =
		DeparseLocalShardQuery(jobQuery, task->relationShardList,
							   task->anchorDistributedTableId,
							   task->anchorShardId);
	ereport(DEBUG5, (errmsg("Local shard query that is going to be cached: %s",
							shardQueryString)));

	Oid *parameterTypes = NULL;
	int numberOfParameters =
		ExtractParameterTypesForParamListInfo(orig_paramListInfo, &parameterTypes);

	Query *localShardQuery =
		ParseQueryString(shardQueryString, parameterTypes, numberOfParameters);

	return localShardQuery;
}


/*
 * DeparseLocalShardQuery is a helper function to deparse given jobQuery for the shard(s)
 * identified by the relationShardList, anchorDistributedTableId and anchorShardId.
 *
 * For the details and comparison with TaskQueryString(), see the comments in the function.
 */
static char *
DeparseLocalShardQuery(Query *jobQuery, List *relationShardList, Oid
					   anchorDistributedTableId, int64 anchorShardId)
{
	StringInfo queryString = makeStringInfo();

	/*
	 * We imitate what TaskQueryString() does, but we cannot rely on that function
	 * as the parameters might have been already resolved on the QueryTree in the
	 * task. Instead, we operate on the jobQuery where are sure that the
	 * coordination evaluation has not happened.
	 *
	 * Local shard queries are only applicable for local cached query execution.
	 * In the local cached query execution mode, we can use a query structure
	 * (or query string) with unevaluated expressions as we allow function calls
	 * to be evaluated when the query on the shard is executed (e.g., do no have
	 * coordinator evaluation, instead let Postgres executor evaluate values).
	 *
	 * Additionally, we can allow them to be evaluated again because they are stable,
	 * and we do not cache plans / use unevaluated query strings for queries containing
	 * volatile functions.
	 */
	if (jobQuery->commandType == CMD_INSERT)
	{
		/*
		 * We currently do not support INSERT .. SELECT here. To support INSERT..SELECT
		 * queries, we should update the relation names to shard names in the SELECT
		 * clause (e.g., UpdateRelationToShardNames()).
		 */
		Assert(!CheckInsertSelectQuery(jobQuery));

		AddInsertAliasIfNeeded(jobQuery);

		/*
		 * For INSERT queries we cannot use pg_get_query_def. Mainly because we
		 * cannot run UpdateRelationToShardNames on an INSERT query. This is
		 * because the PG deparsing logic fails when trying to insert into a
		 * RTE_FUNCTION (which is what will happen if you call
		 * UpdateRelationToShardNames).
		 */
		deparse_shard_query(jobQuery, anchorDistributedTableId, anchorShardId,
							queryString);
	}
	else
	{
		UpdateRelationToShardNames((Node *) jobQuery, relationShardList);

		pg_get_query_def(jobQuery, queryString);
	}

	return queryString->data;
}


/*
 * ExtractParameterTypesForParamListInfo is a helper function which helps to
 * extract the parameter types of the given ParamListInfo via the second
 * parameter of the function.
 *
 * The function also returns the number of parameters. If no parameter exists,
 * the function returns 0.
 */
static int
ExtractParameterTypesForParamListInfo(ParamListInfo originalParamListInfo,
									  Oid **parameterTypes)
{
	*parameterTypes = NULL;

	int numberOfParameters = 0;
	if (originalParamListInfo != NULL)
	{
		const char **parameterValues = NULL;
		ParamListInfo paramListInfo = copyParamList(originalParamListInfo);
		ExtractParametersForLocalExecution(paramListInfo, parameterTypes,
										   &parameterValues);
		numberOfParameters = paramListInfo->numParams;
	}

	return numberOfParameters;
}


/*
 * GetCachedLocalPlan is a helper function which return the cached
 * plan in the distributedPlan for the given task if exists.
 *
 * Otherwise, the function returns NULL.
 */
PlannedStmt *
GetCachedLocalPlan(Task *task, DistributedPlan *distributedPlan)
{
	if (distributedPlan == NULL || distributedPlan->workerJob == NULL)
	{
		return NULL;
	}
	List *cachedPlanList = distributedPlan->workerJob->localPlannedStatements;
	LocalPlannedStatement *localPlannedStatement = NULL;

	int32 localGroupId = GetLocalGroupId();

	foreach_declared_ptr(localPlannedStatement, cachedPlanList)
	{
		if (localPlannedStatement->shardId == task->anchorShardId &&
			localPlannedStatement->localGroupId == localGroupId)
		{
			/* already have a cached plan, no need to continue */
			return localPlannedStatement->localPlan;
		}
	}

	return NULL;
}


/*
 * IsLocalPlanCachingSupported returns whether (part of) the task can be planned
 * and executed locally and whether caching is supported (single shard, no volatile
 * functions).
 */
bool
IsLocalPlanCachingSupported(Job *currentJob, DistributedPlan *originalDistributedPlan)
{
	if (originalDistributedPlan->numberOfTimesExecuted < 1)
	{
		/*
		 * Only cache if a plan is being reused (via a prepared statement).
		 */
		return false;
	}

	if (!currentJob->deferredPruning)
	{
		/*
		 * When not using deferred pruning we may have already replaced distributed
		 * table RTEs with citus_extradata_container RTEs to pass the shard ID to the
		 * deparser. In that case, we cannot pass the query tree directly to the
		 * planner.
		 *
		 * If desired, we can relax this check by improving the implementation of
		 * CacheLocalPlanForShardQuery to translate citus_extradata_container
		 * to a shard relation OID.
		 */
		return false;
	}

	List *taskList = currentJob->taskList;
	if (list_length(taskList) != 1)
	{
		/* we only support plan caching for single shard queries */
		return false;
	}

	Task *task = linitial(taskList);
	if (!TaskAccessesLocalNode(task))
	{
		/* not a local task */
		return false;
	}

	if (!EnableLocalExecution)
	{
		/* user requested not to use local execution */
		return false;
	}

	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_DISABLED)
	{
		/* transaction already connected to localhost */
		return false;
	}

	Query *originalJobQuery = originalDistributedPlan->workerJob->jobQuery;
	if (contain_volatile_functions((Node *) originalJobQuery))
	{
		/*
		 * We do not cache plans with volatile functions in the query.
		 *
		 * The reason we care about volatile functions is primarily that we
		 * already executed them in ExecuteCoordinatorEvaluableExpressions
		 * and since we're falling back to the original query tree here we would
		 * execute them again if we execute the plan.
		 */
		return false;
	}

	return true;
}
