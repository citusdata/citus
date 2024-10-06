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

#include "postgres.h"

#include "common/hashfn.h"
#include "utils/builtins.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/query_utils.h"
#include "distributed/worker_manager.h"

/* controlled via GUC, used mostly for testing */
bool LogIntermediateResults = false;


static List * FindSubPlansUsedInNode(Node *node, SubPlanAccessType accessType);
static void AppendAllAccessedWorkerNodes(IntermediateResultsHashEntry *entry,
										 DistributedPlan *distributedPlan,
										 int nodeCount);
static void AppendAllWorkerNodes(IntermediateResultsHashEntry *entry);
static List * FindAllRemoteWorkerNodesUsingSubplan(IntermediateResultsHashEntry *entry);
static List * RemoveLocalNodeFromWorkerList(List *workerNodeList);
static void LogIntermediateResultMulticastSummary(IntermediateResultsHashEntry *entry,
												  List *workerNodeList);


/*
 * FindSubPlanUsages finds the subplans used in the master query and the
 * job query and returns them as a combined list of UsedDistributedSubPlan
 * structs.
 *
 * The list may contain duplicates if the subplan is referenced multiple
 * times.
 */
List *
FindSubPlanUsages(DistributedPlan *plan)
{
	List *localSubPlans = NIL;
	List *remoteSubPlans = NIL;

	if (plan->combineQuery != NULL)
	{
		localSubPlans = FindSubPlansUsedInNode((Node *) plan->combineQuery,
											   SUBPLAN_ACCESS_LOCAL);
	}

	if (plan->workerJob != NULL)
	{
		/*
		 * Mark the subplans as needed on remote side. Note that this decision is
		 * revisited on execution, when the query only consists of intermediate
		 * results.
		 */
		remoteSubPlans = FindSubPlansUsedInNode((Node *) plan->workerJob->jobQuery,
												SUBPLAN_ACCESS_REMOTE);
	}

	if (plan->modifyQueryViaCoordinatorOrRepartition != NULL)
	{
		/* INSERT..SELECT plans currently do not have a workerJob */
		Assert(plan->workerJob == NULL);

		/*
		 * The SELECT in an INSERT..SELECT is not fully planned yet and we cannot
		 * perform pruning. We therefore require all subplans used in the
		 * INSERT..SELECT to be available all nodes.
		 */
		remoteSubPlans =
			FindSubPlansUsedInNode((Node *) plan->modifyQueryViaCoordinatorOrRepartition,
								   SUBPLAN_ACCESS_ANYWHERE);
	}

	/* merge the used subplans */
	return list_concat(localSubPlans, remoteSubPlans);
}


/*
 * FindSubPlansUsedInPlan finds all the subplans used by the plan by traversing
 * the input node.
 */
static List *
FindSubPlansUsedInNode(Node *node, SubPlanAccessType accessType)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	List *usedSubPlanList = NIL;

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
			UsedDistributedSubPlan *usedPlan = CitusMakeNode(UsedDistributedSubPlan);

			usedPlan->subPlanId = pstrdup(resultId);
			usedPlan->accessType = accessType;

			usedSubPlanList = lappend(usedSubPlanList, usedPlan);
		}
	}

	return usedSubPlanList;
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
	List *usedSubPlanNodeList = distributedPlan->usedSubPlanNodeList;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;
	int nodeCount = list_length(ActiveReadableNodeList());

	foreach(subPlanCell, usedSubPlanNodeList)
	{
		UsedDistributedSubPlan *usedPlan = lfirst(subPlanCell);

		char *resultId = usedPlan->subPlanId;

		IntermediateResultsHashEntry *entry = SearchIntermediateResult(
			intermediateResultsHash, resultId);

		/*
		 * There is no need to traverse the subplan if the intermediate result
		 * will be written to a local file and sent to all nodes. Note that the
		 * remaining subplans in the distributed plan should still be traversed.
		 */
		if (list_length(entry->nodeIdList) == nodeCount && entry->writeLocalFile)
		{
			elog(DEBUG4, "Subplan %s is used in all workers", resultId);
			continue;
		}

		if (usedPlan->accessType == SUBPLAN_ACCESS_LOCAL)
		{
			/* subPlan needs to be written locally as the planner decided */
			entry->writeLocalFile = true;
		}
		else if (usedPlan->accessType == SUBPLAN_ACCESS_REMOTE)
		{
			/*
			 * traverse the plan and add find all worker nodes
			 *
			 * If we have reference tables in the distributed plan, all the
			 * workers will be in the node list. We can improve intermediate result
			 * pruning by deciding which reference table shard will be accessed earlier.
			 */
			AppendAllAccessedWorkerNodes(entry, distributedPlan, nodeCount);

			elog(DEBUG4, "Subplan %s is used in %lu", resultId, distributedPlan->planId);
		}
		else if (usedPlan->accessType == SUBPLAN_ACCESS_ANYWHERE)
		{
			/* subplan is needed on all nodes */
			entry->writeLocalFile = true;
			AppendAllWorkerNodes(entry);
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
 * to updates the list of worker nodes that can be accessed when this plan is
 * executed in entry. Depending on the plan, the function may give the decision for
 * writing the results locally.
 *
 * If there are multiple placements of a Shard, all of them are considered and
 * all the workers with placements are appended to the list. This effectively
 * means that if there is a reference table access in the distributed plan, all
 * the workers will be in the resulting list.
 */
static void
AppendAllAccessedWorkerNodes(IntermediateResultsHashEntry *entry,
							 DistributedPlan *distributedPlan,
							 int nodeCount)
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

			if (placement->nodeId == LOCAL_NODE_ID)
			{
				entry->writeLocalFile = true;
				continue;
			}

			entry->nodeIdList =
				list_append_unique_int(entry->nodeIdList, placement->nodeId);

			/* early return if all the workers are accessed */
			if (list_length(entry->nodeIdList) == nodeCount &&
				entry->writeLocalFile)
			{
				return;
			}
		}
	}
}


/*
 * AppendAllWorkerNodes appends all node IDs of readable worker nodes to the
 * nodeIdList, meaning the corresponding intermediate result should be sent
 * to all readable nodes.
 */
static void
AppendAllWorkerNodes(IntermediateResultsHashEntry *entry)
{
	List *workerNodeList = ActiveReadableNodeList();

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		entry->nodeIdList =
			list_append_unique_int(entry->nodeIdList, workerNode->nodeId);
	}
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
	int32 localGroupId = GetLocalGroupId();

	ListCell *workerNodeCell = NULL;
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		if (workerNode->groupId == localGroupId)
		{
			return list_delete_cell(workerNodeList, workerNodeCell);
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
		foreach_declared_ptr(workerNode, workerNodeList)
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
