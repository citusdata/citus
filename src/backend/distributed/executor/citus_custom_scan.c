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
#include "distributed/backend_data.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_stats.h"
#include "distributed/subplan_execution.h"
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

static CustomExecMethods RouterModifyCustomExecMethods = {
	.CustomName = "RouterModifyScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterModifyExecScan,
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

	List *relationRowLockList = NIL;

	scanState->executorType = MULTI_EXECUTOR_ROUTER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	distributedPlan = scanState->distributedPlan;
	workerJob = distributedPlan->workerJob;
	taskList = workerJob->taskList;
	isModificationQuery = IsModifyDistributedPlan(distributedPlan);

	if (list_length(taskList) == 1)
	{
		Task *task = (Task *) linitial(taskList);
		relationRowLockList = task->relationRowLockList;
	}

	/* if query is SELECT ... FOR UPDATE query, use modify logic */
	if (isModificationQuery || relationRowLockList != NIL)
	{
		scanState->customScanState.methods = &RouterModifyCustomExecMethods;
	}
	else
	{
		scanState->customScanState.methods = &RouterSelectCustomExecMethods;
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
 * CitusSelectBeginScan sets the coordinator backend initiated by Citus for queries using
 * that function as the BeginCustomScan callback.
 */
static void
CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	MarkCitusInitiatedCoordinatorBackend();
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
		if (executorType == MULTI_EXECUTOR_ROUTER && partitionKeyConst != NULL)
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
	EState *executorState = scanState->customScanState.ss.ps.state;
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	if (paramListInfo != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cursors for queries on distributed tables with "
							   "parameters are currently unsupported")));
	}
}
