/*-------------------------------------------------------------------------
 *
 * citus_custom_scan.c
 *
 * Definitions of custom scan methods for all executor types.
 *
 * Copyright (c) 2012-2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "commands/copy.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* functions for creating custom scan nodes */
static Node * RealTimeCreateScan(CustomScan *scan);
static Node * TaskTrackerCreateScan(CustomScan *scan);
static Node * RouterCreateScan(CustomScan *scan);
static Node * CoordinatorInsertSelectCreateScan(CustomScan *scan);
static Node * DelayedErrorCreateScan(CustomScan *scan);

/* functions that are common to different scans */
static void CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags);
static void ExecuteSubPlans(List *subPlanList, EState *estate);
static void CitusEndScan(CustomScanState *node);
static void CitusReScan(CustomScanState *node);


/* create custom scan methods for all executors */
CustomScanMethods RealTimeCustomScanMethods = {
	"Citus Real-Time",
	RealTimeCreateScan
};

CustomScanMethods TaskTrackerCustomScanMethods = {
	"Citus Task-Tracker",
	TaskTrackerCreateScan
};

CustomScanMethods RouterCustomScanMethods = {
	"Citus Router",
	RouterCreateScan
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
static CustomExecMethods RealTimeCustomExecMethods = {
	.CustomName = "RealTimeScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RealTimeExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods TaskTrackerCustomExecMethods = {
	.CustomName = "TaskTrackerScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = TaskTrackerExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterSequentialModifyCustomExecMethods = {
	.CustomName = "RouterSequentialModifyScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterSequentialModifyExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterMultiModifyCustomExecMethods = {
	.CustomName = "RouterMultiModifyScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterMultiModifyExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterSelectCustomExecMethods = {
	.CustomName = "RouterSelectScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RouterSelectExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods CoordinatorInsertSelectCustomExecMethods = {
	.CustomName = "CoordinatorInsertSelectScan",
	.BeginCustomScan = CitusSelectBeginScan,
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
	RegisterCustomScanMethods(&RealTimeCustomScanMethods);
	RegisterCustomScanMethods(&TaskTrackerCustomScanMethods);
	RegisterCustomScanMethods(&RouterCustomScanMethods);
	RegisterCustomScanMethods(&CoordinatorInsertSelectCustomScanMethods);
	RegisterCustomScanMethods(&DelayedErrorCustomScanMethods);
}


/*
 * RealTimeCreateScan creates the scan state for real-time executor queries.
 */
static Node *
RealTimeCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_REAL_TIME;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods = &RealTimeCustomExecMethods;

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
 * RouterCreateScan creates the scan state for router executor queries.
 */
static Node *
RouterCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));
	DistributedPlan *distributedPlan = NULL;
	Job *workerJob = NULL;
	List *taskList = NIL;
	bool isModificationQuery = false;

	scanState->executorType = MULTI_EXECUTOR_ROUTER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	distributedPlan = scanState->distributedPlan;
	workerJob = distributedPlan->workerJob;
	taskList = workerJob->taskList;

	isModificationQuery = IsModifyDistributedPlan(distributedPlan);

	/* check whether query has at most one shard */
	if (list_length(taskList) <= 1)
	{
		if (isModificationQuery)
		{
			scanState->customScanState.methods = &RouterSequentialModifyCustomExecMethods;
		}
		else
		{
			scanState->customScanState.methods = &RouterSelectCustomExecMethods;
		}
	}
	else
	{
		Assert(isModificationQuery);

		if (IsMultiRowInsert(workerJob->jobQuery) ||
			(IsUpdateOrDelete(distributedPlan) &&
			 MultiShardConnectionType == SEQUENTIAL_CONNECTION))
		{
			/*
			 * Multi shard update deletes while multi_shard_modify_mode equals
			 * to 'sequential' or Multi-row INSERT are executed sequentially
			 * instead of using parallel connections.
			 */
			scanState->customScanState.methods = &RouterSequentialModifyCustomExecMethods;
		}
		else
		{
			scanState->customScanState.methods = &RouterMultiModifyCustomExecMethods;
		}
	}

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
 * CitusSelectBeginScan starts a distributed SELECT command. In particur,
 * it executes an subplans that the SELECT depends on.
 */
static void
CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	List *subPlanList = distributedPlan->subPlanList;

	ExecuteSubPlans(subPlanList, estate);
}


/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by executing each plan from the top.
 */
static void
ExecuteSubPlans(List *subPlanList, EState *estate)
{
	ListCell *subPlanCell = NULL;
	int subPlanId = 0;

	foreach(subPlanCell, subPlanList)
	{
		PlannedStmt *subPlan = (PlannedStmt *) lfirst(subPlanCell);
		DestReceiver *copyDest = NULL;
		ParamListInfo params = NULL;
		StringInfo targetFileName = makeStringInfo();
		List *nodeList = ActivePrimaryNodeList();

		appendStringInfo(targetFileName, "base/pgsql_job_cache/%d_%d_%d_%d.data",
						 GetUserId(), GetLocalGroupId(), MyProcPid, subPlanId);

		copyDest = (DestReceiver *) CreateRemoteFileDestReceiver(targetFileName->data,
																 estate, nodeList);

		ExecutePlanIntoDestReceiver(subPlan, params, copyDest);

		subPlanId++;
	}
}


/*
 * CitusEndScan is used to clean up tuple store of the given custom scan state.
 */
static void
CitusEndScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
}


/*
 * CitusReScan is just a place holder for rescan callback. Currently, we don't
 * support rescan given that there is not any way to reach this code path.
 */
static void
CitusReScan(CustomScanState *node)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("rescan is unsupported"),
					errdetail("We don't expect this code path to be executed.")));
}
