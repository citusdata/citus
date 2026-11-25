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

#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"

int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;

/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void
ExecuteSubPlans(DistributedPlan *distributedPlan, CitusScanState *scanState)
{
	uint64 planId = distributedPlan->planId;
	List *subPlanList = distributedPlan->subPlanList;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	bool explainAnalyzeEnabled = RequestedForExplainAnalyze(scanState);
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

		EState *estate = CreateExecutorState();
		DestReceiver *copyDest =
			CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
										 entry->writeLocalFile);

		SubPlanLevel++;
		PG_TRY();
		{
			if (explainAnalyzeEnabled)
			{
				ExecuteSubPlanForExplainAnalyze(scanState, subPlan, copyDest,
												list_length(remoteWorkerNodeList),
												entry->writeLocalFile);
			}
			else
			{
				ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);
			}
		}
		PG_FINALLY();
		{
			SubPlanLevel--;
		}
		PG_END_TRY();

		FreeExecutorState(estate);
	}
}
