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

#include "catalog/namespace.h"
#include "commands/copy.h"
#include "executor/tstoreReceiver.h"
#include "distributed/backend_data.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_stats.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/queryenvironment.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"


bool EnableLocalExecution = true;
LocalExecutionLevel LocalTaskExecutionLevel = LOCAL_EXECUTION_ALLOWED;


static void ExecuteLocallyExecutableTasks(CitusScanState *scanState);
static bool IsLocalExecutionAllowed(DistributedPlan *distributedPlan);
static bool ReplaceShardReferencesWalker(Node *node, Task *task);
static PlannedStmt * LocalTaskPlannedStmt(Job *workerJob, Task *task,
										  ParamListInfo boundParams);
static void ExecuteLocalTaskPlan(CitusScanState *scanState, PlannedStmt *taskPlan,
								 char *queryString);

/* functions for creating custom scan nodes */
static Node * AdaptiveExecutorCreateScan(CustomScan *scan);
static Node * RealTimeCreateScan(CustomScan *scan);
static Node * TaskTrackerCreateScan(CustomScan *scan);
static Node * RouterCreateScan(CustomScan *scan);
static Node * CoordinatorInsertSelectCreateScan(CustomScan *scan);
static Node * DelayedErrorCreateScan(CustomScan *scan);

/* functions that are common to different scans */
static void CitusBeginScan(CustomScanState *node, EState *estate, int eflags);
static void CitusEndScan(CustomScanState *node);
static void CitusReScan(CustomScanState *node);


/* create custom scan methods for all executors */
CustomScanMethods AdaptiveExecutorCustomScanMethods = {
	"Citus Adaptive",
	AdaptiveExecutorCreateScan
};

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
static CustomExecMethods AdaptiveExecutorCustomExecMethods = {
	.CustomName = "AdaptiveExecutorScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = CitusExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RealTimeCustomExecMethods = {
	.CustomName = "RealTimeScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = RealTimeExecScan,
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

static CustomExecMethods RouterModifyCustomExecMethods = {
	.CustomName = "RouterModifyScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = RouterModifyExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterSelectCustomExecMethods = {
	.CustomName = "RouterSelectScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = RouterSelectExecScan,
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
	RegisterCustomScanMethods(&RealTimeCustomScanMethods);
	RegisterCustomScanMethods(&TaskTrackerCustomScanMethods);
	RegisterCustomScanMethods(&RouterCustomScanMethods);
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
	distributedPlan = scanState->distributedPlan;
	if (distributedPlan->operation != CMD_SELECT &&
		distributedPlan->insertSelectSubquery == NULL)
	{
		/*
		 * Modifications require specialised logic. In particular, we need to
		 * evaluate functions and then reconsider shard pruning.
		 *
		 * We also need to take locks to prevent shards from being moved while a
		 * write is in progress.
		 */
		CitusModifyBeginScan(node, estate, eflags);

		/* plan is modified by CitusModifyBeginScan */
		distributedPlan = scanState->distributedPlan;
	}
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
		bool randomAccess = true;
		bool interTransactions = false;

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

		if (EnableLocalExecution && LocalTaskExecutionLevel >= LOCAL_EXECUTION_ALLOWED)
		{
			ExecuteLocallyExecutableTasks(scanState);
		}

		AdaptiveExecutor(node);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 *
 */
static void
ExecuteLocallyExecutableTasks(CitusScanState *scanState)
{
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Job *workerJob = distributedPlan->workerJob;
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	List *taskList = workerJob->taskList;
	ListCell *taskCell = NULL;
	int myGroupId = GetLocalGroupId();
	List *newTaskList = NIL;

	if (!IsLocalExecutionAllowed(distributedPlan))
	{
		if (LocalTaskExecutionLevel == LOCAL_EXECUTION_REQUIRED)
		{
			ereport(ERROR, (errmsg("the transaction accessed shards locally, but this "
								   "command does not support local execution")));
		}

		/* avoid local execution in the remainder of the transaction */
		LocalTaskExecutionLevel = LOCAL_EXECUTION_DISALLOWED;
		return;
	}

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *newTaskPlacementList = NIL;
		ListCell *taskPlacementCell = NULL;
		bool readOnly = ReadOnlyTask(task->taskType);

		foreach(taskPlacementCell, task->taskPlacementList)
		{
			ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);

			if (taskPlacement->groupId == myGroupId)
			{
				PlannedStmt *localPlan = LocalTaskPlannedStmt(workerJob, task,
															  paramListInfo);

				/* execute on local placement */
				ExecuteLocalTaskPlan(scanState, localPlan, task->queryString);
				LocalTaskExecutionLevel = LOCAL_EXECUTION_REQUIRED;

				if (readOnly)
				{
					/* only need to query one shard in case of read-only tasks */
					newTaskPlacementList = NIL;
					break;
				}
			}
			else
			{
				newTaskPlacementList = lappend(newTaskPlacementList, taskPlacement);
			}
		}

		if (list_length(newTaskPlacementList) > 0)
		{
			task->taskPlacementList = newTaskPlacementList;
			newTaskList = lappend(newTaskList, task);
		}
	}

	workerJob->taskList = newTaskList;
}


/*
 * IsLocalExecutionAllowed returns whether a given plan can be executed locally.
 */
static bool
IsLocalExecutionAllowed(DistributedPlan *distributedPlan)
{
	Job *workerJob = distributedPlan->workerJob;
	List *taskList = NIL;

	if (!EnableLocalExecution || LocalTaskExecutionLevel == LOCAL_EXECUTION_DISALLOWED)
	{
		/* already decided not to use local execution */
		return false;
	}

	if (distributedPlan->insertSelectSubquery != NULL)
	{
		/* INSERT...SELECT via the coordinator is always local */
		return false;
	}

	taskList = workerJob->taskList;
	if (LocalTaskExecutionLevel == LOCAL_EXECUTION_ALLOWED &&
		list_length(taskList) > 1)
	{
		/* avoid local execution for parallel queries */
		return false;
	}

	if (ContainsReadIntermediateResultFunction((Node *) workerJob->jobQuery))
	{
		return false;
	}

	return true;
}


/*
 * LocalTaskPlannedStmt builds a PlannedStmt for an individual task that can be
 * executed on the local node.
 */
static PlannedStmt *
LocalTaskPlannedStmt(Job *workerJob, Task *task, ParamListInfo boundParams)
{
	Query *shardQuery = copyObject(workerJob->jobQuery);
	PlannedStmt *localPlan = NULL;
	int cursorOptions = 0;

	/* add a RelationShard for the result relation, TODO: move into planner(s) */
	if (shardQuery->resultRelation != 0)
	{
		RangeTblEntry *rangeTableEntry = rt_fetch(shardQuery->resultRelation,
												  shardQuery->rtable);
		RelationShard *relationShard = CitusMakeNode(RelationShard);

		relationShard->relationId = rangeTableEntry->relid;
		relationShard->shardId = task->anchorShardId;

		task->relationShardList = lcons(relationShard, task->relationShardList);
	}

	UpdateRelationToShardNames((Node *) shardQuery, task->relationShardList);
	ReplaceShardReferencesWalker((Node *) shardQuery, task);

	CreateAndPushPlannerRestrictionContext();

	PG_TRY();
	{
		localPlan = standard_planner(shardQuery, cursorOptions, boundParams);
	}
	PG_CATCH();
	{
		/* TODO: use memory context callback? */
		PopPlannerRestrictionContext();
		PG_RE_THROW();
	}
	PG_END_TRY();

	PopPlannerRestrictionContext();

	return localPlan;
}


/*
 * ReplaceShardReferencesWalker replaces RTE_SHARDs with proper RTEs for the local
 * shards..
 */
static bool
ReplaceShardReferencesWalker(Node *node, Task *task)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;

		if (GetRangeTblKind(rangeTableEntry) == CITUS_RTE_SHARD)
		{
			char *schemaName = NULL;
			char *shardTableName = NULL;
			RangeVar *rangeVar = NULL;
			bool failOK = false;
			Oid shardRelationId = InvalidOid;

			/* job query from the router planner has a shard name */
			ExtractRangeTblExtraData(rangeTableEntry, NULL, &schemaName,
									 &shardTableName, NULL);

			rangeVar = makeRangeVar(schemaName, shardTableName, -1);
			shardRelationId = RangeVarGetRelid(rangeVar, AccessShareLock, failOK);

			/* change citus_extradata_container call into shard */
			rangeTableEntry->rtekind = RTE_RELATION;
			rangeTableEntry->relid = shardRelationId;
			rangeTableEntry->functions = NIL;
		}

		/* caller will descend into range table entry */
		return false;
	}
	else if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, ReplaceShardReferencesWalker, task,
								 QTW_EXAMINE_RTES);
	}
	else
	{
		return expression_tree_walker(node, ReplaceShardReferencesWalker, task);
	}
}


/*
 * ExecuteLocalTaskPlan
 */
static void
ExecuteLocalTaskPlan(CitusScanState *scanState, PlannedStmt *taskPlan, char *queryString)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	DestReceiver *dest = CreateDestReceiver(DestTuplestore);
	ScanDirection scanDirection = ForwardScanDirection;
	QueryEnvironment *queryEnv = create_queryEnv();
	QueryDesc *queryDesc = NULL;
	int eflags = 0;

	SetTuplestoreDestReceiverParams(dest, scanState->tuplestorestate,
									CurrentMemoryContext, false);

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(taskPlan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, paramListInfo, queryEnv, 0);

	ExecutorStart(queryDesc, eflags);
	ExecutorRun(queryDesc, scanDirection, 0L, true);

	if (queryDesc->operation != CMD_SELECT)
	{
		/* make sure we get the right completion tag */
		executorState->es_processed = queryDesc->estate->es_processed;
	}

	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);
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
		if (partitionKeyConst != NULL && (executorType == MULTI_EXECUTOR_ROUTER ||
										  executorType == MULTI_EXECUTOR_ADAPTIVE))
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
