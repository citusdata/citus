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
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_stats.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* functions for creating custom scan nodes */
static Node * AdaptiveExecutorCreateScan(CustomScan *scan);
static Node * TaskTrackerCreateScan(CustomScan *scan);
static Node * CoordinatorInsertSelectCreateScan(CustomScan *scan);
static Node * DelayedErrorCreateScan(CustomScan *scan);

/* functions that are common to different scans */
static void CitusBeginScan(CustomScanState *node, EState *estate, int eflags);
static void CitusGenerateDeferredQueryStrings(CustomScanState *node, EState *estate, int
											  eflags);
static void HandleDeferredShardPruningForFastPathQueries(
	DistributedPlan *distributedPlan);
static void HandleDeferredShardPruningForInserts(DistributedPlan *distributedPlan);

static void CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags);
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
	"Citus INSERT ... SELECT via coordinator",
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
#endif

	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Job *workerJob = distributedPlan->workerJob;
	if (workerJob &&
		(workerJob->requiresMasterEvaluation || workerJob->deferredPruning))
	{
		CitusGenerateDeferredQueryStrings(node, estate, eflags);
	}

	if (distributedPlan->modLevel == ROW_MODIFY_READONLY ||
		distributedPlan->insertSelectQuery != NULL)
	{
		return;
	}

	CitusModifyBeginScan(node, estate, eflags);
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

	return resultSlot;
}


/*
 * CitusModifyBeginScan first evaluates expressions in the query and then
 * performs shard pruning in case the partition column in an insert was
 * defined as a function call.
 *
 * The function also checks the validity of the given custom scan node and
 * gets locks on the shards involved in the task list of the distributed plan.
 */
static void
CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;

	/*
	 * If we have not already copied the plan into this context, do it now.
	 * Note that we could have copied the plan during CitusGenerateDeferredQueryStrings.
	 */
	if (GetMemoryChunkContext(distributedPlan) != CurrentMemoryContext)
	{
		distributedPlan = copyObject(distributedPlan);

		scanState->distributedPlan = distributedPlan;
	}

	Job *workerJob = distributedPlan->workerJob;
	List *taskList = workerJob->taskList;

	/* prevent concurrent placement changes */
	AcquireMetadataLocks(taskList);

	/* modify tasks are always assigned using first-replica policy */
	workerJob->taskList = FirstReplicaAssignTaskList(taskList);
}


/*
 * CitusGenerateDeferredQueryStrings generates query strings at the start of the execution
 * in two cases: when the query requires master evaluation and/or deferred shard pruning.
 */
static void
CitusGenerateDeferredQueryStrings(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;

	/*
	 * We must not change the distributed plan since it may be reused across multiple
	 * executions of a prepared statement. Instead we create a deep copy that we only
	 * use for the current execution.
	 */
	DistributedPlan *distributedPlan = copyObject(scanState->distributedPlan);
	scanState->distributedPlan = distributedPlan;

	Job *workerJob = distributedPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;

	/* we'd only get to this function with the following conditions */
	Assert(workerJob->requiresMasterEvaluation || workerJob->deferredPruning);

	PlanState *planState = &(scanState->customScanState.ss.ps);
	EState *executorState = planState->state;

	/* citus only evaluates functions for modification queries */
	bool modifyQueryRequiresMasterEvaluation =
		workerJob->requiresMasterEvaluation && jobQuery->commandType != CMD_SELECT;

	/*
	 * ExecuteMasterEvaluableFunctions handles both function evalation
	 * and parameter evaluation. Pruning is most likely deferred because
	 * there is a parameter on the distribution key. So, evaluate in both
	 * cases.
	 */
	bool shoudEvaluteFunctionsOrParams =
		modifyQueryRequiresMasterEvaluation || workerJob->deferredPruning;
	if (shoudEvaluteFunctionsOrParams)
	{
		distributedPlan = (scanState->distributedPlan);
		scanState->distributedPlan = distributedPlan;

		workerJob = distributedPlan->workerJob;
		jobQuery = workerJob->jobQuery;

		ExecuteMasterEvaluableFunctions(jobQuery, planState);

		/*
		 * We've processed parameters in ExecuteMasterEvaluableFunctions and
		 * don't need to send their values to workers, since they will be
		 * represented as constants in the deparsed query. To avoid sending
		 * parameter values, we set the parameter list to NULL.
		 */
		executorState->es_param_list_info = NULL;
	}

	/*
	 * After evaluating the function/parameters, we're done unless shard pruning
	 * is also deferred.
	 */
	if (!workerJob->deferredPruning)
	{
		RebuildQueryStrings(workerJob->jobQuery, workerJob->taskList);

		return;
	}

	/* at this point, we're about to do the shard pruning */
	Assert(workerJob->deferredPruning);
	if (jobQuery->commandType == CMD_INSERT)
	{
		HandleDeferredShardPruningForInserts(distributedPlan);
	}
	else
	{
		HandleDeferredShardPruningForFastPathQueries(distributedPlan);
	}
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
											&workerJob->partitionKeyValue,
											&isMultiShardQuery, NULL);

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
