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

#include "distributed/pg_version_constants.h"

#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/local_plan_cache.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/metadata_cache.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "optimizer/optimizer.h"
#else
#include "optimizer/planner.h"
#endif
#include "optimizer/clauses.h"


/*
 * CacheLocalPlanForShardQuery replaces the relation OIDs in the job query
 * with shard relation OIDs and then plans the query and caches the result
 * in the originalDistributedPlan (which may be preserved across executions).
 */
void
CacheLocalPlanForShardQuery(Task *task, DistributedPlan *originalDistributedPlan)
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
	Query *shardQuery = copyObject(originalDistributedPlan->workerJob->jobQuery);

	UpdateRelationsToLocalShardTables((Node *) shardQuery, task->relationShardList);

	LOCKMODE lockMode = GetQueryLockMode(shardQuery);

	/* fast path queries can only have a single RTE by definition */
	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) linitial(shardQuery->rtable);

	/*
	 * If the shard has been created in this transction, we wouldn't see the relationId
	 * for it, so do not cache.
	 */
	if (rangeTableEntry->relid == InvalidOid)
	{
		pfree(shardQuery);
		MemoryContextSwitchTo(oldContext);
		return;
	}

	if (IsLoggableLevel(DEBUG5))
	{
		StringInfo queryString = makeStringInfo();
		pg_get_query_def(shardQuery, queryString);

		ereport(DEBUG5, (errmsg("caching plan for query: %s",
								queryString->data)));
	}

	LockRelationOid(rangeTableEntry->relid, lockMode);

	LocalPlannedStatement *localPlannedStatement = CitusMakeNode(LocalPlannedStatement);
	localPlan = planner(shardQuery, 0, NULL);
	localPlannedStatement->localPlan = localPlan;
	localPlannedStatement->shardId = task->anchorShardId;
	localPlannedStatement->localGroupId = GetLocalGroupId();

	originalDistributedPlan->workerJob->localPlannedStatements =
		lappend(originalDistributedPlan->workerJob->localPlannedStatements,
				localPlannedStatement);

	MemoryContextSwitchTo(oldContext);
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

	foreach_ptr(localPlannedStatement, cachedPlanList)
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

	if (CurrentLocalExecutionStatus == LOCAL_EXECUTION_DISABLED)
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
		 * already executed them in ExecuteMasterEvaluableFunctionsAndParameters
		 * and since we're falling back to the original query tree here we would
		 * execute them again if we execute the plan.
		 */
		return false;
	}

	return true;
}
