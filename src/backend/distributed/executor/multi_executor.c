/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"


/*
 * Create separate set of scan methods for different executor types.
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

static CustomExecMethods RouterSingleModifyCustomExecMethods = {
	.CustomName = "RouterSingleModifyScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterSingleModifyExecScan,
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


/* local function forward declarations */
static void PrepareMasterJobDirectory(Job *workerJob);
static void LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob);
static Relation StubRelation(TupleDesc tupleDescriptor);


/*
 * RealTimeCreateScan creates a custom scan node which sets callback functions
 * for real-time executor.
 */
Node *
RealTimeCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_REAL_TIME;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	scanState->customScanState.methods = &RealTimeCustomExecMethods;

	return (Node *) scanState;
}


/*
 * TaskTrackerCreateScan creates a custom scan node which sets callback functions
 * for task-tracker executor.
 */
Node *
TaskTrackerCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_TASK_TRACKER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	scanState->customScanState.methods = &TaskTrackerCustomExecMethods;

	return (Node *) scanState;
}


/*
 * RouterCreateScan creates a custom scan node which sets callback functions
 * for router executor depending on the router executor type.
 */
Node *
RouterCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));
	MultiPlan *multiPlan = NULL;
	Job *workerJob = NULL;
	List *taskList = NIL;

	scanState->executorType = MULTI_EXECUTOR_ROUTER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	multiPlan = scanState->multiPlan;
	workerJob = multiPlan->workerJob;
	taskList = workerJob->taskList;

	/* check if this is a single shard query */
	if (list_length(taskList) == 1)
	{
		bool isModificationQuery = IsModifyMultiPlan(multiPlan);
		if (isModificationQuery)
		{
			scanState->customScanState.methods = &RouterSingleModifyCustomExecMethods;
		}
		else
		{
			scanState->customScanState.methods = &RouterSelectCustomExecMethods;
		}
	}
	else
	{
		scanState->customScanState.methods = &RouterMultiModifyCustomExecMethods;
	}

	return (Node *) scanState;
}


/*
 * InvalidCreateScan is only called on an invalid case which we would like to
 * error out. This is the case when a plan is not ready for execution because
 * CreateDistributedPlan() couldn't find a plan due to unresolved prepared
 * statement parameters, but didn't error out, because we expect custom plans
 * to come to our rescue. But sql (not plpgsql) functions unfortunately don't
 * go through a codepath supporting custom plans. We call VerifyMultiPlanValidity()
 * to do this check and provide a meaningfull error message.
 */
Node *
InvalidCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_INVALID_FIRST;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	Assert(IsA(scanState, CustomScanState));

	/* ensure plan is executable */
	VerifyMultiPlanValidity(scanState->multiPlan);

	return (Node *) scanState;
}


/*
 * CitusSelectBeginScan just checks if the given custom scan node is a proper
 * Citus scan node.
 */
void
CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	ValidateCitusScanState(node);
}


/*
 * ValidateCitusScanState checks if the given scan nodes contains a valid multi plan.
 */
void
ValidateCitusScanState(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	MultiPlan *multiPlan = scanState->multiPlan;

	Assert(IsA(scanState, CustomScanState));

	/* ensure plan is executable */
	VerifyMultiPlanValidity(multiPlan);
}


/*
 * RealTimeExecScan is a callback function which returns next tuple from a real-time
 * execution. In the first call, it executes distributed real-time plan and loads
 * results from temporary files into custom scan's tuple store. Then, it returns
 * tuples one by one from this tuple store.
 */
TupleTableSlot *
RealTimeExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;

		PrepareMasterJobDirectory(workerJob);
		MultiRealTimeExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReadNextTuple(scanState);

	return resultSlot;
}


/*
 * PrepareMasterJobDirectory creates a directory on the master node to keep job
 * execution results. We also register this directory for automatic cleanup on
 * portal delete.
 */
static void
PrepareMasterJobDirectory(Job *workerJob)
{
	StringInfo jobDirectoryName = MasterJobDirectoryName(workerJob->jobId);
	CreateDirectory(jobDirectoryName);

	ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
	ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);
}


/*
 * Load data collected by real-time or task-tracker executors into the tuplestore
 * of CitusScanState. For that, we first create a tuple store, and then copy the
 * files one-by-one into the tuple store.
 *
 * Note that in the long term it'd be a lot better if Multi*Execute() directly
 * filled the tuplestores, but that's a fair bit of work.
 */
static void
LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob)
{
	CustomScanState customScanState = citusScanState->customScanState;
	List *workerTaskList = workerJob->taskList;
	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	Relation stubRelation = NULL;
	ListCell *workerTaskCell = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	bool randomAccess = true;
	bool interTransactions = false;

	executorState = citusScanState->customScanState.ss.ps.state;
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	tupleDescriptor = customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	stubRelation = StubRelation(tupleDescriptor);

	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	Assert(citusScanState->tuplestorestate == NULL);
	citusScanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

	foreach(workerTaskCell, workerTaskList)
	{
		Task *workerTask = (Task *) lfirst(workerTaskCell);
		StringInfo jobDirectoryName = NULL;
		StringInfo taskFilename = NULL;
		List *copyOptions = NIL;
		CopyState copyState = NULL;

		jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
		taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);

		if (BinaryMasterCopyFormat)
		{
			DefElem *copyOption = makeDefElem("format", (Node *) makeString("binary"));
			copyOptions = lappend(copyOptions, copyOption);
		}

		copyState = BeginCopyFrom(stubRelation, taskFilename->data, false, NULL,
								  copyOptions);

		while (true)
		{
			MemoryContext oldContext = NULL;
			bool nextRowFound = false;

			ResetPerTupleExprContext(executorState);
			oldContext = MemoryContextSwitchTo(executorTupleContext);

			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);
			if (!nextRowFound)
			{
				MemoryContextSwitchTo(oldContext);
				break;
			}

			tuplestore_putvalues(citusScanState->tuplestorestate, tupleDescriptor,
								 columnValues, columnNulls);
			MemoryContextSwitchTo(oldContext);
		}

		EndCopyFrom(copyState);
	}
}


/*
 * StubRelation creates a stub Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there is no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
StubRelation(TupleDesc tupleDescriptor)
{
	Relation stubRelation = palloc0(sizeof(RelationData));
	stubRelation->rd_att = tupleDescriptor;
	stubRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	stubRelation->rd_rel->relkind = RELKIND_RELATION;

	return stubRelation;
}


/*
 * ReadNextTuple reads the next tuple from the tuple store of the given Citus
 * scan node and returns it. It returns null if all tuples are read from the
 * tuple store.
 */
TupleTableSlot *
ReadNextTuple(CitusScanState *scanState)
{
	Tuplestorestate *tupleStore = scanState->tuplestorestate;
	TupleTableSlot *resultSlot = NULL;
	ScanDirection scanDirection = NoMovementScanDirection;
	bool forwardScanDirection = true;

	if (tupleStore == NULL)
	{
		return NULL;
	}

	scanDirection = scanState->customScanState.ss.ps.state->es_direction;
	Assert(ScanDirectionIsValid(scanDirection));

	if (ScanDirectionIsBackward(scanDirection))
	{
		forwardScanDirection = false;
	}

	resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;
	tuplestore_gettupleslot(tupleStore, forwardScanDirection, false, resultSlot);

	return resultSlot;
}


/*
 * TaskTrackerExecScan is a callback function which returns next tuple from a
 * task-tracker execution. In the first call, it executes distributed task-tracker
 * plan and loads results from temporary files into custom scan's tuple store.
 * Then, it returns tuples one by one from this tuple store.
 */
TupleTableSlot *
TaskTrackerExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;

		PrepareMasterJobDirectory(workerJob);
		MultiTaskTrackerExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReadNextTuple(scanState);

	return resultSlot;
}


/*
 * CitusEndScan is used to clean up tuple store of the given custom scan state.
 */
void
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
void
CitusReScan(CustomScanState *node)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("rescan is unsupported"),
					errdetail("We don't expect this code path to be executed.")));
}
