/*-------------------------------------------------------------------------
 *
 * subplan_execution.c
 *
 * Functions for execution subplans prior to distributed table execution.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/intermediate_results.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_manager.h"
#include "executor/executor.h"


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
	List *nodeList = ActiveReadableNodeList();
	bool writeLocalFile = false;

	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		DestReceiver *copyDest = NULL;
		ParamListInfo params = NULL;
		EState *estate = NULL;

		char *resultId = GenerateResultId(planId, subPlanId);

		estate = CreateExecutorState();
		copyDest = (DestReceiver *) CreateRemoteFileDestReceiver(resultId, estate,
																 nodeList,
																 writeLocalFile);

		ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		FreeExecutorState(estate);
	}
}
