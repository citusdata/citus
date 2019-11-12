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

#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "executor/executor.h"


int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;


/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void
ExecuteSubPlans(DistributedPlan *distributedPlan)
{
	uint64 planId = distributedPlan->planId;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;
	HTAB *intermediateResultsHash = NULL;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	intermediateResultsHash = MakeIntermediateResultHTAB();
	RecordSubplanExecutionsOnNodes(intermediateResultsHash, distributedPlan);


	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results of subplans will be stored in a directory that is
	 * derived from the distributed transaction ID.
	 */
	BeginOrContinueCoordinatedTransaction();

	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		DestReceiver *copyDest = NULL;
		ParamListInfo params = NULL;
		EState *estate = NULL;
		bool writeLocalFile = false;
		char *resultId = GenerateResultId(planId, subPlanId);
		List *workerNodeList =
			FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

		/*
		 * Write intermediate results to local file only if there is no worker
		 * node that receives them.
		 *
		 * This could happen in two cases:
		 * (a) Subquery in the having
		 * (b) The intermediate result is not used, such as RETURNING of a
		 *     modifying CTE is not used
		 *
		 * For SELECT, Postgres/Citus is clever enough to not execute the CTE
		 * if it is not used at all, but for modifications we have to execute
		 * the queries.
		 */
		if (workerNodeList == NIL)
		{
			writeLocalFile = true;
			elog(DEBUG1, "%s is not broadcasted to any node, write to local only",
				 resultId);
		}

		SubPlanLevel++;
		estate = CreateExecutorState();
		copyDest = (DestReceiver *) CreateRemoteFileDestReceiver(resultId, estate,
																 workerNodeList,
																 writeLocalFile);

		ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		SubPlanLevel--;
		FreeExecutorState(estate);
	}
}
