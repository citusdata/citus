/*-------------------------------------------------------------------------
 *
 * intermediate_result_pruning.c
 *   Functions for pruning intermediate result broadcasting.
 *
 * We only send intermediate results of subqueries and CTEs to worker nodes
 * that use them in the remainder of the distributed plan to avoid unnecessary
 * network traffic.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/citus_custom_scan.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/query_utils.h"
#include "distributed/worker_manager.h"
#include "utils/builtins.h"

/* controlled via GUC, used mostly for testing */
bool LogIntermediateResults = false;

static List * AppendAllAccessedWorkerNodes(List *workerNodeList,
										   DistributedPlan *distributedPlan,
										   int workerNodeCount);
static IntermediateResultsHashEntry * SearchIntermediateResult(HTAB
															   *intermediateResultsHash,
															   char *resultId);


/*
 * FindSubPlansUsedInPlan finds all the subplans used by the plan by traversing
 * the range table entries in the plan.
 */
List *
FindSubPlansUsedInNode(Node *node)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	List *subPlanList = NIL;

	ExtractRangeTableEntryWalker(node, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_FUNCTION)
		{
			char *resultId =
				FindIntermediateResultIdIfExists(rangeTableEntry);

			if (resultId == NULL)
			{
				continue;
			}

			/*
			 * Use a Value to be able to use list_append_unique and store
			 * the result ID in the DistributedPlan.
			 */
			Value *resultIdValue = makeString(resultId);
			subPlanList = list_append_unique(subPlanList, resultIdValue);
		}
	}

	return subPlanList;
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
void
RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
							   DistributedPlan *distributedPlan)
{
	Value *usedSubPlanIdValue = NULL;
	List *usedSubPlanNodeList = distributedPlan->usedSubPlanNodeList;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;
	int workerNodeCount = GetWorkerNodeCount();

	foreach_ptr(usedSubPlanIdValue, usedSubPlanNodeList)
	{
		char *resultId = strVal(usedSubPlanIdValue);

		IntermediateResultsHashEntry *entry = SearchIntermediateResult(
			intermediateResultsHash, resultId);

		/* no need to traverse the whole plan if all the workers are hit */
		if (list_length(entry->nodeIdList) == workerNodeCount)
		{
			elog(DEBUG4, "Subplan %s is used in all workers", resultId);

			break;
		}
		else
		{
			/*
			 * traverse the plan and add find all worker nodes
			 *
			 * If we have reference tables in the distributed plan, all the
			 * workers will be in the node list. We can improve intermediate result
			 * pruning by deciding which reference table shard will be accessed earlier
			 */
			entry->nodeIdList = AppendAllAccessedWorkerNodes(entry->nodeIdList,
															 distributedPlan,
															 workerNodeCount);

			elog(DEBUG4, "Subplan %s is used in %lu", resultId, distributedPlan->planId);
		}
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


/*
 * AppendAllAccessedWorkerNodes iterates over all the tasks in a distributed plan
 * to create the list of worker nodes that can be accessed when this plan is executed.
 *
 * If there are multiple placements of a Shard, all of them are considered and
 * all the workers with placements are appended to the list. This effectively
 * means that if there is a reference table access in the distributed plan, all
 * the workers will be in the resulting list.
 */
static List *
AppendAllAccessedWorkerNodes(List *workerNodeList, DistributedPlan *distributedPlan, int
							 workerNodeCount)
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

			/* early return if all the workers are accessed */
			if (list_length(workerNodeList) == workerNodeCount)
			{
				return workerNodeList;
			}
		}
	}

	return workerNodeList;
}


/*
 * MakeIntermediateResultHTAB is a helper method that creates a Hash Table that
 * stores information on the intermediate result.
 */
HTAB *
MakeIntermediateResultHTAB()
{
	HASHCTL info = { 0 };
	int initialNumberOfElements = 16;

	info.keysize = NAMEDATALEN;
	info.entrysize = sizeof(IntermediateResultsHashEntry);
	info.hash = string_hash;
	info.hcxt = CurrentMemoryContext;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *intermediateResultsHash = hash_create("Intermediate results hash",
												initialNumberOfElements, &info,
												hashFlags);

	return intermediateResultsHash;
}


/*
 * FindAllWorkerNodesUsingSubplan creates a list of worker nodes that
 * may need to access subplan results.
 */
List *
FindAllWorkerNodesUsingSubplan(HTAB *intermediateResultsHash,
							   char *resultId)
{
	List *workerNodeList = NIL;
	IntermediateResultsHashEntry *entry =
		SearchIntermediateResult(intermediateResultsHash, resultId);

	ListCell *nodeIdCell = NULL;
	foreach(nodeIdCell, entry->nodeIdList)
	{
		uint32 nodeId = lfirst_int(nodeIdCell);
		WorkerNode *workerNode = LookupNodeByNodeId(nodeId);

		if(workerNode == NULL)
		{
			elog(WARNING, "Failed Node Lookup with Id %d", nodeId);
			continue;
		}

		workerNodeList = lappend(workerNodeList, workerNode);

		if ((LogIntermediateResults && IsLoggableLevel(DEBUG1)) ||
			IsLoggableLevel(DEBUG4))
		{
			elog(DEBUG1, "Subplan %s will be sent to %s:%d", resultId,
				 workerNode->workerName, workerNode->workerPort);
		}
	}

	return workerNodeList;
}


/*
 * SearchIntermediateResult searches through intermediateResultsHash for a given
 * intermediate result id.
 *
 * If an entry is not found, creates a new entry with sane defaults.
 */
static IntermediateResultsHashEntry *
SearchIntermediateResult(HTAB *intermediateResultsHash, char *resultId)
{
	bool found = false;

	IntermediateResultsHashEntry *entry = hash_search(intermediateResultsHash, resultId,
													  HASH_ENTER, &found);

	/* use sane defaults */
	if (!found)
	{
		entry->nodeIdList = NIL;
	}

	return entry;
}
