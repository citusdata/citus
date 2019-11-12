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

#include "distributed/citus_custom_scan.h"
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


static List * AppendAllAccessedWorkerNodes(List *workerNodeList,
										   DistributedPlan *distributedPlan);
static CustomScan * FetchCitusCustomScanIfExists(Plan *plan);
static void RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
										   DistributedPlan *distributedPlan);


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
	List *nodeList = NIL;
	HTAB *intermediateResultHash = NULL;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	intermediateResultHash = makeIntermediateResultHTAB();
	RecordSubplanExecutionsOnNodes(intermediateResultHash, distributedPlan);


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

		char *resultId = GenerateResultId(planId, subPlanId);

		IntermediateResultHashKey key;
		IntermediateResultHashEntry *entry = NULL;
		bool found = false;

		nodeList = NIL;

		/*
		 * Write intermediate results to local file only if there is no worker
		 * node that receives them
		 */
		bool writeLocalFile = false;


		strlcpy(key.intermediate_result_id, resultId, NAMEDATALEN);
		entry = hash_search(intermediateResultHash, &key, HASH_ENTER, &found);

		if (found)
		{
			ListCell *lcInner = NULL;
			foreach(lcInner, entry->nodeList)
			{
				WorkerNode *w = GetWorkerNodeByNodeId(lfirst_int(lcInner));

				nodeList = lappend(nodeList, w);
				elog(DEBUG4, "%s is send to %s:%d", key.intermediate_result_id,
					 w->workerName, w->workerPort);
			}
		}
		else
		{
			/* TODO: Think more on this. Consider HAVING. */
			nodeList = NIL;
			writeLocalFile = true;
			elog(DEBUG4, "%s is not sent to any node, write to local only",
				 key.intermediate_result_id);
		}


		SubPlanLevel++;
		estate = CreateExecutorState();
		copyDest = (DestReceiver *) CreateRemoteFileDestReceiver(resultId, estate,
																 nodeList,
																 writeLocalFile);

		ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		SubPlanLevel--;
		FreeExecutorState(estate);
	}
}


/*
 * RecordSubplanExecutionsOnNodes iterates over the usedSubPlanNodeList,
 * and for each entry, record the workerNodes that are accessed by
 * the distributed plan.
 *
 * Later, we'll use this information while we broadcast the intermediate
 * results to the worker nodes. The idea is that the intermediate result
 * should only be broadcasted to the worker nodes that are accessed by
 * the distributedPlan(s) that the subPlan is used in.
 *
 * Finally, the function recursively descends into the actual subplans
 * of the input distributedPlan as well.
 */
static void
RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
							   DistributedPlan *distributedPlan)
{
	List *usedSubPlanNodeList = distributedPlan->usedSubPlanNodeList;
	ListCell *usedSubPlanNodeCell = NULL;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;

	foreach(usedSubPlanNodeCell, usedSubPlanNodeList)
	{
		Const *resultIdConst = lfirst(usedSubPlanNodeCell);
		Datum resultIdDatum = resultIdConst->constvalue;
		char *resultId = TextDatumGetCString(resultIdDatum);

		IntermediateResultHashKey key;
		IntermediateResultHashEntry *entry = NULL;
		bool found = false;

		strlcpy(key.intermediate_result_id, resultId, NAMEDATALEN);
		entry = hash_search(intermediateResultsHash, &key, HASH_ENTER, &found);

		if (!found)
		{
			entry->nodeList = NIL;
		}

		/* TODO: hanefi: fix hardcoded worker node count */
		if (list_length(entry->nodeList) == 3)
		{
			entry->containsAllNodes = true;

			/* TODO: hanefi use a continue here */
		}
		else
		{
			entry->containsAllNodes = false;
		}

		/* TODO: hanefi skip if all nodes are already in nodeList */
		entry->nodeList = AppendAllAccessedWorkerNodes(entry->nodeList, distributedPlan);

		elog(DEBUG4, "subplan %s  is used in %lu", resultId, distributedPlan->planId);
	}

	/* descend into the subPlans */
	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		CustomScan *customScan = FetchCitusCustomScanIfExists(subPlan->plan->planTree);
		if (customScan)
		{
			DistributedPlan *distributedPlanOfSubPlan = GetDistributedPlan(customScan);
			RecordSubplanExecutionsOnNodes(intermediateResultsHash,
										   distributedPlanOfSubPlan);
		}
	}
}


/* TODO: hanefi super inefficient, multiple recursions */
static CustomScan *
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

	if (plan->lefttree != NULL && IsCitusPlan(plan->lefttree))
	{
		return FetchCitusCustomScanIfExists(plan->lefttree);
	}

	if (plan->righttree != NULL && IsCitusPlan(plan->righttree))
	{
		return FetchCitusCustomScanIfExists(plan->righttree);
	}

	return NULL;
}


/*
 * TODO: hanefi this is super slow, improve the performance
 */
static List *
AppendAllAccessedWorkerNodes(List *workerNodeList, DistributedPlan *distributedPlan)
{
	List *taskList = distributedPlan->workerJob->taskList;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = lfirst(taskCell);
		ListCell *placementCell = NULL;
		foreach(placementCell, task->taskPlacementList)
		{
			ShardPlacement *placement = lfirst(placementCell);
			workerNodeList = list_append_unique_int(workerNodeList, placement->nodeId);
		}
	}

	return workerNodeList;
}
