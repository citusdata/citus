/*-------------------------------------------------------------------------
 *
 * subplan_execution.c
 *
 * Functions for execution subplans prior to distributed table execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "utils/datetime.h"

#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"

#define SECOND_TO_MILLI_SECOND 1000
#define MICRO_TO_MILLI_SECOND 0.001

int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;

/*
 * SubPlanExplainAnalyzeContext is both a memory context for storing
 * subplans’ EXPLAIN ANALYZE output and a flag indicating that execution
 * is running under EXPLAIN ANALYZE for subplans.
 */
MemoryContext SubPlanExplainAnalyzeContext = NULL;
SubPlanExplainOutputData *SubPlanExplainOutput;
extern uint8 TotalExplainOutputCapacity;
extern uint8 NumTasksOutput;

/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void
ExecuteSubPlans(DistributedPlan *distributedPlan, bool explainAnalyzeEnabled)
{
	uint64 planId = distributedPlan->planId;
	List *subPlanList = distributedPlan->subPlanList;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	/*
	 * If the root DistributedPlan has EXPLAIN ANALYZE enabled,
	 * its subplans should also have EXPLAIN ANALYZE enabled.
	 */
	if (explainAnalyzeEnabled)
	{
		SubPlanExplainAnalyzeContext = GetMemoryChunkContext(distributedPlan);
	}
	else
	{
		SubPlanExplainAnalyzeContext = NULL;
	}

	HTAB *intermediateResultsHash = MakeIntermediateResultHTAB();
	RecordSubplanExecutionsOnNodes(intermediateResultsHash, distributedPlan);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results of subplans will be stored in a directory that is
	 * derived from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	DistributedSubPlan *subPlan = NULL;
	foreach_declared_ptr(subPlan, subPlanList)
	{
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		ParamListInfo params = NULL;
		char *resultId = GenerateResultId(planId, subPlanId);
		List *remoteWorkerNodeList =
			FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

		IntermediateResultsHashEntry *entry =
			SearchIntermediateResult(intermediateResultsHash, resultId);

		SubPlanLevel++;
		EState *estate = CreateExecutorState();
		DestReceiver *copyDest =
			CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
										 entry->writeLocalFile);

		TimestampTz startTimestamp = GetCurrentTimestamp();

		uint64 nprocessed;

		PG_TRY();
		{
			nprocessed =
				ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);
		}
		PG_CATCH();
		{
			SubPlanExplainAnalyzeContext = NULL;
			SubPlanExplainOutput = NULL;
			TotalExplainOutputCapacity = 0;
			NumTasksOutput = 0;
			PG_RE_THROW();
		}
		PG_END_TRY();


		/*
		 * EXPLAIN ANALYZE instrumentations. Calculating these are very light-weight,
		 * so always populate them regardless of EXPLAIN ANALYZE or not.
		 */
		long durationSeconds = 0.0;
		int durationMicrosecs = 0;
		TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
							&durationMicrosecs);

		subPlan->durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
		subPlan->durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;

		subPlan->bytesSentPerWorker = RemoteFileDestReceiverBytesSent(copyDest);
		subPlan->ntuples = nprocessed;
		subPlan->remoteWorkerCount = list_length(remoteWorkerNodeList);
		subPlan->writeLocalFile = entry->writeLocalFile;

		SubPlanLevel--;

		/*
		 * Save the EXPLAIN ANALYZE output(s) for later extraction in ExplainSubPlans().
		 * Because the SubPlan context isn’t available during distributed execution,
		 * pass the pointer as a global variable in SubPlanExplainOutput.
		 */
		subPlan->totalExplainOutput = SubPlanExplainOutput;
		subPlan->numTasksOutput = NumTasksOutput;
		SubPlanExplainOutput = NULL;
		TotalExplainOutputCapacity = 0;
		NumTasksOutput = 0;
		FreeExecutorState(estate);
	}

	SubPlanExplainAnalyzeContext = NULL;
}
