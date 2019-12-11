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
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
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
static void CitusBeginScanWithCoordinatorProcessing(CustomScanState *node, EState *estate,
													int eflags);
static void HandleDeferredShardPruningForFastPathQueries(
	DistributedPlan *distributedPlan);
static void HandleDeferredShardPruningForInserts(DistributedPlan *distributedPlan);
static void CacheLocalPlanForTask(Task *task, DistributedPlan *originalDistributedPlan);
static DistributedPlan * CopyDistributedPlanWithoutCache(CitusScanState *scanState);
static void ResetExecutionParameters(EState *executorState);
static void CitusBeginScanWithoutCoordinatorProcessing(CustomScanState *node,
													   EState *estate, int eflags);
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
	ExecInitResultSlot(&scanState->customScanState.ss.ps, &TTSOpsMinimalTuple);

	ExecInitScanTupleSlot(node->ss.ps.state, &node->ss, node->ss.ps.scandesc, &TTSOpsMinimalTuple);
	ExecAssignScanProjectionInfoWithVarno(&node->ss, INDEX_VAR);
#endif

	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Job *workerJob = distributedPlan->workerJob;
	if (workerJob &&
		(workerJob->requiresMasterEvaluation || workerJob->deferredPruning))
	{
		CitusBeginScanWithCoordinatorProcessing(node, estate, eflags);

		return;
	}

	CitusBeginScanWithoutCoordinatorProcessing(node, estate, eflags);
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

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	if (node->ss.ps.ps_ProjInfo)
	{
		ProjectionInfo *projInfo = node->ss.ps.ps_ProjInfo;
		if (TupIsNull(resultSlot))
		{
			return ExecClearTuple(projInfo->pi_state.resultslot);
			/*nothing more to scan, return emtpy tuple*/
		}
		/*
		 * Form a projection tuple, store it in the result tuple slot
		 * and return it.
		 */
		node->ss.ps.ps_ExprContext->ecxt_scantuple = resultSlot;
		return ExecProject(node->ss.ps.ps_ProjInfo);
	}
	else
	{
		/*
		 * Here, we aren't projecting, so just return scan tuple.
		 */
		return resultSlot;
	}
}


/*
 * CitusBeginScanWithoutCoordinatorProcessing is intended to work on all executions
 * that do not require any coordinator processing. The function simply acquires the
 * necessary locks on the shards involved in the task list of the distributed plan
 * and does the placement assignements. This implies that the function is a no-op for
 * SELECT queries as they do not require any locking and placement assignements.
 */
static void
CitusBeginScanWithoutCoordinatorProcessing(CustomScanState *node, EState *estate, int
										   eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;

	if (distributedPlan->modLevel == ROW_MODIFY_READONLY ||
		distributedPlan->insertSelectQuery != NULL)
	{
		return;
	}

	/* we'll be modifying the distributed plan by assigning taskList, do it on a copy */
	distributedPlan = copyObject(distributedPlan);
	scanState->distributedPlan = distributedPlan;

	Job *workerJob = distributedPlan->workerJob;
	List *taskList = workerJob->taskList;

	/*
	 * These more complex jobs should have been evaluated in
	 * CitusBeginScanWithCoordinatorProcessing.
	 */
	Assert(!(workerJob->requiresMasterEvaluation || workerJob->deferredPruning));

	/* prevent concurrent placement changes */
	AcquireMetadataLocks(taskList);

	/* modify tasks are always assigned using first-replica policy */
	workerJob->taskList = FirstReplicaAssignTaskList(taskList);
}


/*
 * CitusBeginScanWithCoordinatorProcessing generates query strings at the start of the execution
 * in two cases: when the query requires master evaluation and/or deferred shard pruning.
 *
 * The function is also smart about caching plans if the plan is local to this node.
 */
static void
CitusBeginScanWithCoordinatorProcessing(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;
	DistributedPlan *distributedPlan = CopyDistributedPlanWithoutCache(scanState);
	Job *workerJob = distributedPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;

	/* we'd only get to this function with the following conditions */
	Assert(workerJob->requiresMasterEvaluation || workerJob->deferredPruning);

	PlanState *planState = &(scanState->customScanState.ss.ps);

	/* citus only evaluates functions for modification queries */
	bool modifyQueryRequiresMasterEvaluation =
		jobQuery->commandType != CMD_SELECT &&
		(workerJob->requiresMasterEvaluation || workerJob->deferredPruning);

	/*
	 * ExecuteMasterEvaluableFunctions handles both function evalation
	 * and parameter evaluation. Pruning is most likely deferred because
	 * there is a parameter on the distribution key. So, evaluate in both
	 * cases.
	 */
	if (modifyQueryRequiresMasterEvaluation)
	{
		/* evaluate functions and parameters for modification queries */
		ExecuteMasterEvaluableFunctionsAndParameters(jobQuery, planState);
	}
	else if (jobQuery->commandType == CMD_SELECT && !workerJob->deferredPruning)
	{
		/* we'll use generated strings, no need to have the parameters anymore */
		EState *executorState = planState->state;
		ResetExecutionParameters(executorState);

		/* we're done, we don't want to evaluate functions for SELECT queries */
		return;
	}
	else if (jobQuery->commandType == CMD_SELECT && workerJob->deferredPruning)
	{
		/*
		 * Evaluate parameters, because the parameters are only avaliable on the
		 * coordinator and are required for pruning.
		 *
		 * But, we don't want to evaluate functions for read-only queries on the
		 * coordinator as the volatile functions could yield different
		 * results per shard (also per row) and could have side-effects.
		 *
		 * Note that Citus already errors out for modification queries during
		 * planning when the query involve any volatile function that might
		 * diverge the shards as such functions are expected to yield different
		 * results per shard (also per row).
		 */
		ExecuteMasterEvaluableParameters(jobQuery, planState);
	}

	/*
	 * After evaluating the function/parameters, we're done unless shard pruning
	 * is also deferred.
	 */
	if (workerJob->requiresMasterEvaluation && !workerJob->deferredPruning)
	{
		RebuildQueryStrings(workerJob->jobQuery, workerJob->taskList);

		/* we'll use generated strings, no need to have the parameters anymore */
		EState *executorState = planState->state;
		ResetExecutionParameters(executorState);

		return;
	}

	/*
	 * At this point, we're about to do the shard pruning for fast-path queries.
	 * Given that pruning is deferred always for INSERTs, we get here
	 * !EnableFastPathRouterPlanner  as well.
	 */
	Assert(workerJob->deferredPruning &&
		   (distributedPlan->fastPathRouterPlan || !EnableFastPathRouterPlanner));
	if (jobQuery->commandType == CMD_INSERT)
	{
		HandleDeferredShardPruningForInserts(distributedPlan);
	}
	else
	{
		HandleDeferredShardPruningForFastPathQueries(distributedPlan);
	}

	if (jobQuery->commandType != CMD_SELECT)
	{
		/* prevent concurrent placement changes */
		AcquireMetadataLocks(workerJob->taskList);

		/* modify tasks are always assigned using first-replica policy */
		workerJob->taskList = FirstReplicaAssignTaskList(workerJob->taskList);
	}

	if (list_length(distributedPlan->workerJob->taskList) != 1)
	{
		/*
		 * We might have zero shard queries or multi-row INSERTs at this point,
		 * we only want to cache single task queries.
		 */
		return;
	}

	/*
	 * As long as the task accesses local node and the query doesn't have
	 * any volatile functions, we cache the local Postgres plan on the
	 * shard for re-use.
	 */
	Task *task = linitial(distributedPlan->workerJob->taskList);
	if (EnableLocalExecution && TaskAccessesLocalNode(task) &&
		!contain_volatile_functions(
			(Node *) originalDistributedPlan->workerJob->jobQuery))
	{
		CacheLocalPlanForTask(task, originalDistributedPlan);
	}
	else
	{
		/*
		 * If we're not going to use a cached plan, we'll use the query string that is
		 * already generated where the parameters are replaced, so we should not have
		 * the parameters anymore.
		 */
		EState *executorState = planState->state;
		ResetExecutionParameters(executorState);
	}
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
CopyDistributedPlanWithoutCache(CitusScanState *scanState)
{
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;
	List *localPlannedStatements =
		originalDistributedPlan->workerJob->localPlannedStatements;
	originalDistributedPlan->workerJob->localPlannedStatements = NIL;

	DistributedPlan *distributedPlan = copyObject(originalDistributedPlan);
	scanState->distributedPlan = distributedPlan;

	/* set back the immutable field */
	originalDistributedPlan->workerJob->localPlannedStatements = localPlannedStatements;
	distributedPlan->workerJob->localPlannedStatements = localPlannedStatements;

	return distributedPlan;
}


/*
 * ResetExecutionParameters set the parameter list to NULL. See the function
 * for details.
 */
static void
ResetExecutionParameters(EState *executorState)
{
	/*
	 * We've processed parameters in ExecuteMasterEvaluableFunctions and
	 * don't need to send their values to workers, since they will be
	 * represented as constants in the deparsed query. To avoid sending
	 * parameter values, we set the parameter list to NULL.
	 */
	executorState->es_param_list_info = NULL;
}


/*
 * CacheLocalPlanForTask caches a plan that is local to this node in the
 * originalDistributedPlan.
 *
 * The basic idea is to be able to skip planning on the shards when possible.
 */
static void
CacheLocalPlanForTask(Task *task, DistributedPlan *originalDistributedPlan)
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
	 * functions/params have been evaluated in the cached plan.
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
	ListCell *cachedLocalPlanCell = NULL;
	List *cachedPlanList = distributedPlan->workerJob->localPlannedStatements;
	foreach(cachedLocalPlanCell, cachedPlanList)
	{
		LocalPlannedStatement *localPlannedStatement = lfirst(cachedLocalPlanCell);

		if (localPlannedStatement->shardId == task->anchorShardId &&
			localPlannedStatement->localGroupId == GetLocalGroupId())
		{
			/* already have a cached plan, no need to continue */
			return localPlannedStatement->localPlan;
		}
	}

	return NULL;
}


/*
 * HandleDeferredShardPruningForInserts does the shard pruning for INSERT
 * queries and rebuilds the query strings.
 */
static void
HandleDeferredShardPruningForInserts(DistributedPlan *distributedPlan)
{
	Job *workerJob = distributedPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;
	DeferredErrorMessage *planningError = NULL;

	/* need to perform shard pruning, rebuild the task list from scratch */
	List *taskList = RouterInsertTaskList(jobQuery, &planningError);

	if (planningError != NULL)
	{
		RaiseDeferredError(planningError, ERROR);
	}

	workerJob->taskList = taskList;
	workerJob->partitionKeyValue = ExtractInsertPartitionKeyValue(jobQuery);

	RebuildQueryStrings(jobQuery, workerJob->taskList);
}


/*
 * HandleDeferredShardPruningForFastPathQueries does the shard pruning for
 * UPDATE/DELETE/SELECT fast path router queries and rebuilds the query strings.
 */
static void
HandleDeferredShardPruningForFastPathQueries(DistributedPlan *distributedPlan)
{
	Assert(distributedPlan->fastPathRouterPlan);

	Job *workerJob = distributedPlan->workerJob;

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
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	if (paramListInfo != NULL)
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
	return scanState->customScanState.ss.ps.ps_ResultTupleSlot->
		   tts_tupleDescriptor;
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
