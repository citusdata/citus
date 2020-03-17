/*-------------------------------------------------------------------------
 *
 * citus_custom_scan.c
 *
 * Definitions of custom scan methods for all executor types.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "commands/copy.h"
#include "distributed/backend_data.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_stats.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/planner.h"
#endif
#include "optimizer/clauses.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* functions for creating custom scan nodes */
static Node * AdaptiveExecutorCreateScan(CustomScan *scan);
static Node * TaskTrackerCreateScan(CustomScan *scan);
static Node * CoordinatorInsertSelectCreateScan(CustomScan *scan);
static Node * DelayedErrorCreateScan(CustomScan *scan);

/* functions that are common to different scans */
static void CitusBeginScan(CustomScanState *node, EState *estate, int eflags);
static void CitusBeginSelectScan(CustomScanState *node, EState *estate, int eflags);
static void CitusBeginModifyScan(CustomScanState *node, EState *estate, int eflags);
static void CitusPreExecScan(CitusScanState *scanState);
static bool ModifyJobNeedsEvaluation(Job *workerJob);
static void RegenerateTaskForFasthPathQuery(Job *workerJob);
static void RegenerateTaskListForInsert(Job *workerJob);
static void CacheLocalPlanForShardQuery(Task *task,
										DistributedPlan *originalDistributedPlan);
static bool IsLocalPlanCachingSupported(Job *workerJob,
										DistributedPlan *originalDistributedPlan);
static DistributedPlan * CopyDistributedPlanWithoutCache(
	DistributedPlan *originalDistributedPlan);
static void CitusEndScan(CustomScanState *node);
static void CitusReScan(CustomScanState *node);


/* create custom scan methods for all executors */
CustomScanMethods AdaptiveExecutorCustomScanMethods = {
	"Citus Adaptive",
	AdaptiveExecutorCreateScan
};

CustomScanMethods TaskTrackerCustomScanMethods = {
	"Citus Task-Tracker",
	TaskTrackerCreateScan
};

CustomScanMethods CoordinatorInsertSelectCustomScanMethods = {
	"Citus INSERT ... SELECT",
	CoordinatorInsertSelectCreateScan
};

CustomScanMethods DelayedErrorCustomScanMethods = {
	"Citus Delayed Error",
	DelayedErrorCreateScan
};


/*
 * Define executor methods for the different executor types.
 */
static CustomExecMethods AdaptiveExecutorCustomExecMethods = {
	.CustomName = "AdaptiveExecutorScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = CitusExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods TaskTrackerCustomExecMethods = {
	.CustomName = "TaskTrackerScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = TaskTrackerExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods CoordinatorInsertSelectCustomExecMethods = {
	.CustomName = "CoordinatorInsertSelectScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = CoordinatorInsertSelectExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CoordinatorInsertSelectExplainScan
};


/*
 * IsCitusCustomState returns if a given PlanState node is a CitusCustomState node.
 */
bool
IsCitusCustomState(PlanState *planState)
{
	if (!IsA(planState, CustomScanState))
	{
		return false;
	}

	CustomScanState *css = castNode(CustomScanState, planState);
	if (css->methods == &AdaptiveExecutorCustomExecMethods ||
		css->methods == &TaskTrackerCustomExecMethods ||
		css->methods == &CoordinatorInsertSelectCustomExecMethods)
	{
		return true;
	}

	return false;
}


/*
 * Let PostgreSQL know about Citus' custom scan nodes.
 */
void
RegisterCitusCustomScanMethods(void)
{
	RegisterCustomScanMethods(&AdaptiveExecutorCustomScanMethods);
	RegisterCustomScanMethods(&TaskTrackerCustomScanMethods);
	RegisterCustomScanMethods(&CoordinatorInsertSelectCustomScanMethods);
	RegisterCustomScanMethods(&DelayedErrorCustomScanMethods);
}


/*
 * CitusBeginScan sets the coordinator backend initiated by Citus for queries using
 * that function as the BeginCustomScan callback.
 *
 * The function also handles deferred shard pruning along with function evaluations.
 */
static void
CitusBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	MarkCitusInitiatedCoordinatorBackend();

	CitusScanState *scanState = (CitusScanState *) node;

#if PG_VERSION_NUM >= 120000

	/*
	 * Since we are using a tuplestore we cannot use the virtual tuples postgres had
	 * already setup on the CustomScan. Instead we need to reinitialize the tuples as
	 * minimal.
	 *
	 * During initialization postgres also created the projection information and the
	 * quals, but both are 'compiled' to be executed on virtual tuples. Since we replaced
	 * the tuples with minimal tuples we also compile both the projection and the quals
	 * on to these 'new' tuples.
	 */
	ExecInitResultSlot(&scanState->customScanState.ss.ps, &TTSOpsMinimalTuple);

	ExecInitScanTupleSlot(node->ss.ps.state, &node->ss, node->ss.ps.scandesc,
						  &TTSOpsMinimalTuple);
	ExecAssignScanProjectionInfoWithVarno(&node->ss, INDEX_VAR);

	node->ss.ps.qual = ExecInitQual(node->ss.ps.plan->qual, (PlanState *) node);
#endif

	DistributedPlan *distributedPlan = scanState->distributedPlan;
	if (distributedPlan->insertSelectQuery != NULL)
	{
		/*
		 * INSERT..SELECT via coordinator or re-partitioning are special because
		 * the SELECT part is planned separately.
		 */
		return;
	}
	else if (distributedPlan->modLevel == ROW_MODIFY_READONLY)
	{
		CitusBeginSelectScan(node, estate, eflags);
	}
	else
	{
		CitusBeginModifyScan(node, estate, eflags);
	}
}


/*
 * CitusPreExecScan is called right before postgres' executor starts pulling tuples.
 */
static void
CitusPreExecScan(CitusScanState *scanState)
{
	AdaptiveExecutorPreExecutorRun(scanState);
}


/*
 * CitusExecScan is called when a tuple is pulled from a custom scan.
 * On the first call, it executes the distributed query and writes the
 * results to a tuple store. The postgres executor calls this function
 * repeatedly to read tuples from the tuple store.
 */
TupleTableSlot *
CitusExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (!scanState->finishedRemoteScan)
	{
		AdaptiveExecutor(scanState);

		scanState->finishedRemoteScan = true;
	}

	return ReturnTupleFromTuplestore(scanState);
}


/*
 * CitusBeginSelectScan handles deferred pruning and plan caching for SELECTs.
 */
static void
CitusBeginSelectScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;

	if (!originalDistributedPlan->workerJob->deferredPruning)
	{
		/*
		 * For SELECT queries that have already been pruned we can proceed straight
		 * to execution, since none of the prepared statement logic applies.
		 */
		return;
	}

	/*
	 * Create a copy of the generic plan for the current execution, but make a shallow
	 * copy of the plan cache. That means we'll be able to access the plan cache via
	 * currentPlan->workerJob->localPlannedStatements, but it will be preserved across
	 * executions by the prepared statement logic.
	 */
	DistributedPlan *currentPlan =
		CopyDistributedPlanWithoutCache(originalDistributedPlan);
	scanState->distributedPlan = currentPlan;

	Job *workerJob = currentPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;
	PlanState *planState = &(scanState->customScanState.ss.ps);

	/*
	 * We only do deferred pruning for fast path queries, which have a single
	 * partition column value.
	 */
	Assert(currentPlan->fastPathRouterPlan || !EnableFastPathRouterPlanner);

	/*
	 * Evaluate parameters, because the parameters are only available on the
	 * coordinator and are required for pruning.
	 *
	 * We don't evaluate functions for read-only queries on the coordinator
	 * at the moment. Most function calls would be in a context where they
	 * should be re-evaluated for every row in case of volatile functions.
	 *
	 * TODO: evaluate stable functions
	 */
	ExecuteMasterEvaluableParameters(jobQuery, planState);

	/* job query no longer has parameters, so we should not send any */
	workerJob->parametersInJobQueryResolved = true;

	/* parameters are filled in, so we can generate a task for this execution */
	RegenerateTaskForFasthPathQuery(workerJob);

	if (IsLocalPlanCachingSupported(workerJob, originalDistributedPlan))
	{
		Task *task = linitial(workerJob->taskList);

		/*
		 * We are going to execute this task locally. If it's not already in
		 * the cache, create a local plan now and add it to the cache. During
		 * execution, we will get the plan from the cache.
		 *
		 * The plan will be cached across executions when originalDistributedPlan
		 * represents a prepared statement.
		 */
		CacheLocalPlanForShardQuery(task, originalDistributedPlan);
	}
}


/*
 * CitusBeginModifyScan prepares the scan state for a modification.
 *
 * Modifications are special because:
 * a) we evaluate function calls (e.g. nextval) here and the outcome may
 *    determine which shards are affected by this query.
 * b) we need to take metadata locks to make sure no write is left behind
 *    when finalizing a shard move.
 */
static void
CitusBeginModifyScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	PlanState *planState = &(scanState->customScanState.ss.ps);
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;

	DistributedPlan *currentPlan =
		CopyDistributedPlanWithoutCache(originalDistributedPlan);
	scanState->distributedPlan = currentPlan;

	Job *workerJob = currentPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;

	if (ModifyJobNeedsEvaluation(workerJob))
	{
		/* evaluate both functions and parameters */
		ExecuteMasterEvaluableFunctionsAndParameters(jobQuery, planState);

		/* job query no longer has parameters, so we should not send any */
		workerJob->parametersInJobQueryResolved = true;
	}

	if (workerJob->deferredPruning)
	{
		/*
		 * At this point, we're about to do the shard pruning for fast-path queries.
		 * Given that pruning is deferred always for INSERTs, we get here
		 * !EnableFastPathRouterPlanner  as well.
		 */
		Assert(currentPlan->fastPathRouterPlan || !EnableFastPathRouterPlanner);

		/*
		 * We can only now decide which shard to use, so we need to build a new task
		 * list.
		 */
		if (jobQuery->commandType == CMD_INSERT)
		{
			RegenerateTaskListForInsert(workerJob);
		}
		else
		{
			RegenerateTaskForFasthPathQuery(workerJob);
		}
	}
	else if (workerJob->requiresMasterEvaluation)
	{
		/*
		 * When there is no deferred pruning, but we did evaluate functions, then
		 * we only rebuild the query strings in the existing tasks.
		 */
		RebuildQueryStrings(workerJob);
	}

	/*
	 * Now that we know the shard ID(s) we can acquire the necessary shard metadata
	 * locks. Once we have the locks it's safe to load the placement metadata.
	 */

	/* prevent concurrent placement changes */
	AcquireMetadataLocks(workerJob->taskList);

	/* modify tasks are always assigned using first-replica policy */
	workerJob->taskList = FirstReplicaAssignTaskList(workerJob->taskList);

	/*
	 * Now that we have populated the task placements we can determine whether
	 * any of them are local to this node and cache a plan if needed.
	 */
	if (IsLocalPlanCachingSupported(workerJob, originalDistributedPlan))
	{
		Task *task = linitial(workerJob->taskList);

		/*
		 * We are going to execute this task locally. If it's not already in
		 * the cache, create a local plan now and add it to the cache. During
		 * execution, we will get the plan from the cache.
		 *
		 * WARNING: In this function we'll use the original plan with the original
		 * query tree, meaning parameters and function calls are back and we'll
		 * redo evaluation in the local (Postgres) executor. The reason we do this
		 * is that we only need to cache one generic plan per shard.
		 *
		 * The plan will be cached across executions when originalDistributedPlan
		 * represents a prepared statement.
		 */
		CacheLocalPlanForShardQuery(task, originalDistributedPlan);
	}
}


/*
 * ModifyJobNeedsEvaluation checks whether the functions and parameters in the job query
 * need to be evaluated before we can build task query strings.
 */
static bool
ModifyJobNeedsEvaluation(Job *workerJob)
{
	if (workerJob->requiresMasterEvaluation)
	{
		/* query contains functions that need to be evaluated on the coordinator */
		return true;
	}

	if (workerJob->partitionKeyValue != NULL)
	{
		/* the value of the distribution column is already known */
		return false;
	}

	/* pruning was deferred due to a parameter in the partition column */
	return workerJob->deferredPruning;
}


/*
 * CopyDistributedPlanWithoutCache is a helper function which copies the
 * distributedPlan into the current memory context.
 *
 * We must not change the distributed plan since it may be reused across multiple
 * executions of a prepared statement. Instead we create a deep copy that we only
 * use for the current execution.
 *
 * We also exclude localPlannedStatements from the copyObject call for performance
 * reasons, as they are immutable, so no need to have a deep copy.
 */
static DistributedPlan *
CopyDistributedPlanWithoutCache(DistributedPlan *originalDistributedPlan)
{
	List *localPlannedStatements =
		originalDistributedPlan->workerJob->localPlannedStatements;
	originalDistributedPlan->workerJob->localPlannedStatements = NIL;

	DistributedPlan *distributedPlan = copyObject(originalDistributedPlan);

	/* set back the immutable field */
	originalDistributedPlan->workerJob->localPlannedStatements = localPlannedStatements;
	distributedPlan->workerJob->localPlannedStatements = localPlannedStatements;

	return distributedPlan;
}


/*
 * CacheLocalPlanForShardQuery replaces the relation OIDs in the job query
 * with shard relation OIDs and then plans the query and caches the result
 * in the originalDistributedPlan (which may be preserved across executions).
 */
static void
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

	LOCKMODE lockMode =
		IsModifyCommand(shardQuery) ? RowExclusiveLock : (shardQuery->hasForUpdate ?
														  RowShareLock : AccessShareLock);

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
static bool
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

	if (TransactionConnectedToLocalGroup)
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


/*
 * RegenerateTaskListForInsert does the shard pruning for an INSERT query
 * queries and rebuilds the query strings.
 */
static void
RegenerateTaskListForInsert(Job *workerJob)
{
	Query *jobQuery = workerJob->jobQuery;
	bool parametersInJobQueryResolved = workerJob->parametersInJobQueryResolved;
	DeferredErrorMessage *planningError = NULL;

	/* need to perform shard pruning, rebuild the task list from scratch */
	List *taskList = RouterInsertTaskList(jobQuery, parametersInJobQueryResolved,
										  &planningError);
	if (planningError != NULL)
	{
		RaiseDeferredError(planningError, ERROR);
	}

	workerJob->taskList = taskList;

	if (workerJob->partitionKeyValue == NULL)
	{
		/*
		 * If we were not able to determine the partition key value in the planner,
		 * take another shot now. It may still be NULL in case of a multi-row
		 * insert.
		 */
		workerJob->partitionKeyValue = ExtractInsertPartitionKeyValue(jobQuery);
	}

	RebuildQueryStrings(workerJob);
}


/*
 * RegenerateTaskForFasthPathQuery does the shard pruning for
 * UPDATE/DELETE/SELECT fast path router queries and rebuilds the query strings.
 */
static void
RegenerateTaskForFasthPathQuery(Job *workerJob)
{
	bool isMultiShardQuery = false;
	List *shardIntervalList =
		TargetShardIntervalForFastPathQuery(workerJob->jobQuery,
											&isMultiShardQuery, NULL,
											&workerJob->partitionKeyValue);

	/*
	 * A fast-path router query can only yield multiple shards when the parameter
	 * cannot be resolved properly, which can be triggered by SQL function.
	 */
	if (isMultiShardQuery)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this "
							   "query because parameterized queries for SQL "
							   "functions referencing distributed tables are "
							   "not supported"),
						errhint("Consider using PL/pgSQL functions instead.")));
	}

	bool shardsPresent = false;
	List *relationShardList =
		RelationShardListForShardIntervalList(shardIntervalList, &shardsPresent);

	UpdateRelationToShardNames((Node *) workerJob->jobQuery, relationShardList);

	List *placementList =
		FindRouterWorkerList(shardIntervalList, shardsPresent, true);
	uint64 shardId = INVALID_SHARD_ID;

	if (shardsPresent)
	{
		shardId = GetAnchorShardId(shardIntervalList);
	}

	GenerateSingleShardRouterTaskList(workerJob,
									  relationShardList,
									  placementList, shardId);
}


/*
 * AdaptiveExecutorCreateScan creates the scan state for the adaptive executor.
 */
static Node *
AdaptiveExecutorCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_ADAPTIVE;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods = &AdaptiveExecutorCustomExecMethods;
	scanState->PreExecScan = &CitusPreExecScan;

	return (Node *) scanState;
}


/*
 * TaskTrackerCreateScan creates the scan state for task-tracker executor queries.
 */
static Node *
TaskTrackerCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_TASK_TRACKER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods = &TaskTrackerCustomExecMethods;

	return (Node *) scanState;
}


/*
 * CoordinatorInsertSelectCrateScan creates the scan state for executing
 * INSERT..SELECT into a distributed table via the coordinator.
 */
static Node *
CoordinatorInsertSelectCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods =
		&CoordinatorInsertSelectCustomExecMethods;

	return (Node *) scanState;
}


/*
 * DelayedErrorCreateScan is only called if we could not plan for the given
 * query. This is the case when a plan is not ready for execution because
 * CreateDistributedPlan() couldn't find a plan due to unresolved prepared
 * statement parameters, but didn't error out, because we expect custom plans
 * to come to our rescue. But sql (not plpgsql) functions unfortunately don't
 * go through a codepath supporting custom plans. Here, we error out with this
 * delayed error message.
 */
static Node *
DelayedErrorCreateScan(CustomScan *scan)
{
	DistributedPlan *distributedPlan = GetDistributedPlan(scan);

	/* raise the deferred error */
	RaiseDeferredError(distributedPlan->planningError, ERROR);

	return NULL;
}


/*
 * CitusEndScan is used to clean up tuple store of the given custom scan state.
 */
static void
CitusEndScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	Job *workerJob = scanState->distributedPlan->workerJob;
	uint64 queryId = scanState->distributedPlan->queryId;
	MultiExecutorType executorType = scanState->executorType;
	Const *partitionKeyConst = NULL;
	char *partitionKeyString = NULL;

	if (workerJob != NULL)
	{
		partitionKeyConst = workerJob->partitionKeyValue;
	}

	/* queryId is not set if pg_stat_statements is not installed */
	if (queryId != 0)
	{
		if (partitionKeyConst != NULL && executorType == MULTI_EXECUTOR_ADAPTIVE)
		{
			partitionKeyString = DatumToString(partitionKeyConst->constvalue,
											   partitionKeyConst->consttype);
		}

		/* queries without partition key are also recorded */
		CitusQueryStatsExecutorsEntry(queryId, executorType, partitionKeyString);
	}

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
}


/*
 * CitusReScan is not normally called, except in certain cases of
 * DECLARE .. CURSOR WITH HOLD ..
 */
static void
CitusReScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	Job *workerJob = scanState->distributedPlan->workerJob;
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	if (paramListInfo != NULL && !workerJob->parametersInJobQueryResolved)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cursors for queries on distributed tables with "
							   "parameters are currently unsupported")));
	}
}


/*
 * ScanStateGetTupleDescriptor returns the tuple descriptor for the given
 * scan state.
 */
TupleDesc
ScanStateGetTupleDescriptor(CitusScanState *scanState)
{
	return scanState->customScanState.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
}


/*
 * ScanStateGetExecutorState returns the executor state for the given scan
 * state.
 */
EState *
ScanStateGetExecutorState(CitusScanState *scanState)
{
	return scanState->customScanState.ss.ps.state;
}


/*
 * FetchCitusCustomScanIfExists traverses a given plan and returns a Citus CustomScan
 * if it has any.
 */
CustomScan *
FetchCitusCustomScanIfExists(Plan *plan)
{
	if (plan == NULL)
	{
		return NULL;
	}

	if (IsCitusCustomScan(plan))
	{
		return (CustomScan *) plan;
	}

	CustomScan *customScan = FetchCitusCustomScanIfExists(plan->lefttree);

	if (customScan == NULL)
	{
		customScan = FetchCitusCustomScanIfExists(plan->righttree);
	}

	return customScan;
}


/*
 * IsCitusPlan returns whether a Plan contains a CustomScan generated by Citus
 * by recursively walking through the plan tree.
 */
bool
IsCitusPlan(Plan *plan)
{
	if (plan == NULL)
	{
		return false;
	}

	if (IsCitusCustomScan(plan))
	{
		return true;
	}

	return IsCitusPlan(plan->lefttree) || IsCitusPlan(plan->righttree);
}


/*
 * IsCitusCustomScan returns whether Plan node is a CustomScan generated by Citus.
 */
bool
IsCitusCustomScan(Plan *plan)
{
	if (plan == NULL)
	{
		return false;
	}

	if (!IsA(plan, CustomScan))
	{
		return false;
	}

	CustomScan *customScan = (CustomScan *) plan;
	if (list_length(customScan->custom_private) == 0)
	{
		return false;
	}

	Node *privateNode = (Node *) linitial(customScan->custom_private);
	if (!CitusIsA(privateNode, DistributedPlan))
	{
		return false;
	}

	return true;
}
