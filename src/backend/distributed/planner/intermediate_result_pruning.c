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

static List * AppendAllAccessedWorkerNodes(IntermediateResultsHashEntry *entry,
										   DistributedPlan *distributedPlan,
										   int workerNodeCount);
static List * FindAllRemoteWorkerNodesUsingSubplan(IntermediateResultsHashEntry *entry);
static List * RemoveLocalNodeFromWorkerList(List *workerNodeList);
static void LogIntermediateResultMulticastSummary(IntermediateResultsHashEntry *entry,
												  List *workerNodeList);

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
		if (list_length(entry->nodeIdList) == workerNodeCount && entry->writeLocalFile)
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
			entry->nodeIdList = AppendAllAccessedWorkerNodes(entry,
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
AppendAllAccessedWorkerNodes(IntermediateResultsHashEntry *entry,
							 DistributedPlan *distributedPlan, int
							 workerNodeCount)
{
	List *workerNodeList = entry->nodeIdList;
	List *taskList = distributedPlan->workerJob->taskList;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = lfirst(taskCell);
		ListCell *placementCell = NULL;
		foreach(placementCell, task->taskPlacementList)
		{
			ShardPlacement *placement = lfirst(placementCell);

			if (placement->nodeId == LOCAL_NODE_ID)
			{
				entry->writeLocalFile = true;
				continue;
			}

			workerNodeList = list_append_unique_int(workerNodeList, placement->nodeId);

			/* early return if all the workers are accessed */
			if (list_length(workerNodeList) == workerNodeCount && entry->writeLocalFile)
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
 * may need to access subplan results. The function also sets writeToLocalFile
 * flag if the result should also need be written locally.
 */
List *
FindAllWorkerNodesUsingSubplan(HTAB *intermediateResultsHash,
							   char *resultId)
{
	IntermediateResultsHashEntry *entry =
		SearchIntermediateResult(intermediateResultsHash, resultId);

	List *remoteWorkerNodes = FindAllRemoteWorkerNodesUsingSubplan(entry);
	if (remoteWorkerNodes == NIL)
	{
		/*
		 * This could happen in two cases:
		 * (a) Subquery in the having
		 * (b) The intermediate result is not used, such as RETURNING of a
		 *     modifying CTE is not used
		 *
		 * For SELECT, Postgres/Citus is clever enough to not execute the CTE
		 * if it is not used at all, but for modifications we have to execute
		 * the queries.
		 */
		entry->writeLocalFile = true;
	}

	/*
	 * Don't include the current worker if the result will be written to local
	 * file as this would be very inefficient and potentially leading race
	 * conditions while tring to write the same file twice.
	 */
	if (entry->writeLocalFile)
	{
		remoteWorkerNodes = RemoveLocalNodeFromWorkerList(remoteWorkerNodes);
	}

	LogIntermediateResultMulticastSummary(entry, remoteWorkerNodes);

	return remoteWorkerNodes;
}


/*
 * FindAllRemoteWorkerNodesUsingSubplan goes over the nodeIdList of the
 * intermediate result entry, and returns a list of workerNodes that the
 * entry should be multi-casted to. The aim of the function is to filter
 * out nodes with LOCAL_NODE_ID.
 */
static List *
FindAllRemoteWorkerNodesUsingSubplan(IntermediateResultsHashEntry *entry)
{
	List *workerNodeList = NIL;

	ListCell *nodeIdCell = NULL;
	foreach(nodeIdCell, entry->nodeIdList)
	{
		uint32 nodeId = lfirst_int(nodeIdCell);
		WorkerNode *workerNode = LookupNodeByNodeId(nodeId);
		if (workerNode != NULL)
		{
			workerNodeList = lappend(workerNodeList, workerNode);
		}
	}

	return workerNodeList;
}


/*
 * RemoveLocalNodeFromWorkerList goes over the input workerNode list and
 * removes the worker node with the local group id, and returns a new list.
 */
static List *
RemoveLocalNodeFromWorkerList(List *workerNodeList)
{
	WorkerNode *workerNode = NULL;
	int32 localGroupId = GetLocalGroupId();

	/* we'll iterate over the list while deleting from it, so copy it */
	List *copyOfWorkerNodeList = list_copy(workerNodeList);
	foreach_ptr(workerNode, copyOfWorkerNodeList)
	{
		if (workerNode->groupId == localGroupId)
		{
			workerNodeList = list_delete_ptr(workerNodeList, workerNode);
			break;
		}
	}

	return workerNodeList;
}


/*
 * LogIntermediateResultMulticastSummary is a utility function to DEBUG output
 * the decisions given on which intermediate result should be sent to which node.
 *
 * For details, see the function comments.
 */
static void
LogIntermediateResultMulticastSummary(IntermediateResultsHashEntry *entry,
									  List *workerNodeList)
{
	char *resultId = entry->key;

	/*
	 * Log a summary of decisions made for intermediate result multicast. By default
	 * we log at level DEBUG4. When the user has set citus.log_intermediate_results
	 * we change the log level to DEBUG1. This is mostly useful in regression tests
	 * where we specifically want to debug this decisions, but not all DEBUG4 messages.
	 */
	int logLevel = DEBUG4;

	if (LogIntermediateResults)
	{
		logLevel = DEBUG1;
	}

	if (IsLoggableLevel(logLevel))
	{
		if (entry->writeLocalFile)
		{
			elog(logLevel, "Subplan %s will be written to local file", resultId);
		}

		WorkerNode *workerNode = NULL;
		foreach_ptr(workerNode, workerNodeList)
		{
			elog(logLevel, "Subplan %s will be sent to %s:%d", resultId,
				 workerNode->workerName, workerNode->workerPort);
		}
	}
}


/*
 * SearchIntermediateResult searches through intermediateResultsHash for a given
 * intermediate result id.
 *
 * If an entry is not found, creates a new entry with sane defaults.
 */
IntermediateResultsHashEntry *
SearchIntermediateResult(HTAB *intermediateResultsHash, char *resultId)
{
	bool found = false;

	IntermediateResultsHashEntry *entry = hash_search(intermediateResultsHash, resultId,
													  HASH_ENTER, &found);

	/* use sane defaults */
	if (!found)
	{
		entry->nodeIdList = NIL;
		entry->writeLocalFile = false;
	}

	return entry;
}
