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
 * The function also handles modification scan actions.
 */
static void
CitusBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = NULL;
	DistributedPlan *distributedPlan = NULL;

	MarkCitusInitiatedCoordinatorBackend();

	scanState = (CitusScanState *) node;

#if PG_VERSION_NUM >= 120000
	ExecInitResultSlot(&scanState->customScanState.ss.ps, &TTSOpsMinimalTuple);
#endif

	distributedPlan = scanState->distributedPlan;
	if (distributedPlan->modLevel == ROW_MODIFY_READONLY ||
		distributedPlan->insertSelectSubquery != NULL)
	{
		/* no more action required */
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
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		AdaptiveExecutor(scanState);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

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
	DistributedPlan *distributedPlan = NULL;
	Job *workerJob = NULL;
	Query *jobQuery = NULL;
	List *taskList = NIL;

	/*
	 * We must not change the distributed plan since it may be reused across multiple
	 * executions of a prepared statement. Instead we create a deep copy that we only
	 * use for the current execution.
	 */
	distributedPlan = scanState->distributedPlan = copyObject(scanState->distributedPlan);

	workerJob = distributedPlan->workerJob;
	jobQuery = workerJob->jobQuery;
	taskList = workerJob->taskList;

	if (workerJob->requiresMasterEvaluation)
	{
		PlanState *planState = &(scanState->customScanState.ss.ps);
		EState *executorState = planState->state;

		ExecuteMasterEvaluableFunctions(jobQuery, planState);

		/*
		 * We've processed parameters in ExecuteMasterEvaluableFunctions and
		 * don't need to send their values to workers, since they will be
		 * represented as constants in the deparsed query. To avoid sending
		 * parameter values, we set the parameter list to NULL.
		 */
		executorState->es_param_list_info = NULL;

		if (workerJob->deferredPruning)
		{
			DeferredErrorMessage *planningError = NULL;

			/* need to perform shard pruning, rebuild the task list from scratch */
			taskList = RouterInsertTaskList(jobQuery, &planningError);

			if (planningError != NULL)
			{
				RaiseDeferredError(planningError, ERROR);
			}

			workerJob->taskList = taskList;
			workerJob->partitionKeyValue = ExtractInsertPartitionKeyValue(jobQuery);
		}

		RebuildQueryStrings(jobQuery, taskList);
	}

	/* prevent concurrent placement changes */
	AcquireMetadataLocks(taskList);

	/*
	 * We are taking locks on partitions of partitioned tables. These locks are
	 * necessary for locking tables that appear in the SELECT part of the query.
	 */
	LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

	/* modify tasks are always assigned using first-replica policy */
	workerJob->taskList = FirstReplicaAssignTaskList(taskList);
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

	scanState->customScanState.methods = &CoordinatorInsertSelectCustomExecMethods;

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
	return scanState->customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
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
	CustomScan *customScan = NULL;

	if (plan == NULL)
	{
		return NULL;
	}

	if (IsCitusCustomScan(plan))
	{
		return (CustomScan *) plan;
	}

	customScan = FetchCitusCustomScanIfExists(plan->lefttree);

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
	CustomScan *customScan = NULL;
	Node *privateNode = NULL;

	if (plan == NULL)
	{
		return false;
	}

	if (!IsA(plan, CustomScan))
	{
		return false;
	}

	customScan = (CustomScan *) plan;
	if (list_length(customScan->custom_private) == 0)
	{
		return false;
	}

	privateNode = (Node *) linitial(customScan->custom_private);
	if (!CitusIsA(privateNode, DistributedPlan))
	{
		return false;
	}

	return true;
}
