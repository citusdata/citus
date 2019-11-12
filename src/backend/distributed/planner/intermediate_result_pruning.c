/*-------------------------------------------------------------------------
 *
 * intermediate_result_pruning.c
 *   Functions for pruning intermediate result broadcasting.
 *
 * In plain words, each distributed plan pulls the necessary intermediate
 * results to the worker nodes that the plan hits. This is primarily useful
 * in three ways. First, if the distributed plan that uses intermediate
 * result(s) is a router query, then the intermediate results are only
 * broadcasted to a single node. Second, if a distributed plan consists
 * of only intermediate results, which is not uncommon, the intermediate
 * results are broadcasted to a single node only. Finally, if a distributed
 * query hits a sub-set of the shards in multiple workers, the intermediate
 * results will be broadcasted to the relevant node(s). The final item becomes
 * crucial for append/range distributed tables where typically the distributed
 * queries hit a small subset of shards/workers.
 *
 * To do this, for each query that Citus creates a distributed plan, we keep
 * track of the subPlans used in the queryTree, and save it in the distributed
 * plan. Just before Citus executes each subPlan, Citus first keeps track of
 * every worker node that the distributed plan hits, and marks every subPlan
 * should be broadcasted to these nodes. Later, for each subPlan which is a
 * distributed plan, Citus does this operation recursively since these
 * distributed plans may access to different subPlans, and those have to be
 * recorded as well.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/citus_custom_scan.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_manager.h"
#include "utils/builtins.h"

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
FindSubPlansUsedInPlan(DistributedPlan *plan)
{
	Query *jobQuery = NULL;
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	List *subPlanList = NIL;

	jobQuery = plan->workerJob == NULL ? plan->insertSelectSubquery :
			   plan->workerJob->jobQuery;

	/*
	 * Decide if there are no intermediate results.
	 *
	 * We usually store the intermediate result info in workerJob field. We currently
	 * have a single exception to this, which is SELECT INTO queries that have
	 * ON CONFLICT/RETURNING clauses. If the query is of such a query we need to consider
	 * pruning intermediate results even though we do not have a workerjob field
	 *
	 *  * We do not wish to proceed if one of the following is met:
	 * - there are no intermediate results
	 * - the query hits all the worker nodes
	 */
	if (jobQuery == NULL)
	{
		return NIL;
	}

	rangeTableList = ExtractRangeTableEntryList(jobQuery);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_FUNCTION)
		{
			Const *resultIdConst =
				FindIntermediateResultIdIfExists(rangeTableEntry);

			if (resultIdConst == NULL)
			{
				continue;
			}

			subPlanList = list_append_unique(subPlanList, resultIdConst);

			if (IsLoggableLevel(DEBUG4))
			{
				Datum resultIdDatum = resultIdConst->constvalue;
				char *resultId = TextDatumGetCString(resultIdDatum);

				elog(DEBUG4, "Result of SubPlan %s is used in Plan %lu",
					 resultId, plan->planId);
			}
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
	List *usedSubPlanNodeList = distributedPlan->usedSubPlanNodeList;
	ListCell *usedSubPlanNodeCell = NULL;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;
	int workerNodeCount = GetWorkerNodeCount();

	foreach(usedSubPlanNodeCell, usedSubPlanNodeList)
	{
		Const *resultIdConst = lfirst(usedSubPlanNodeCell);
		Datum resultIdDatum = resultIdConst->constvalue;
		char *resultId = TextDatumGetCString(resultIdDatum);

		IntermediateResultsHashEntry *entry = SearchIntermediateResult(
			intermediateResultsHash, resultId);

		/* no need to traverse the whole plan if all the workers are hit */
		if (list_length(entry->nodeIdList) == workerNodeCount)
		{
			elog(DEBUG4, "subplan %s is used in all workers", resultId);

			break;
		}
		else
		{
			entry->nodeIdList = AppendAllAccessedWorkerNodes(entry->nodeIdList,
															 distributedPlan,
															 workerNodeCount);

			elog(DEBUG4, "subplan %s is used in %lu", resultId, distributedPlan->planId);
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
 * If there are multiple placements of a Shard, all of them are considered.
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
	HTAB *intermediateResultsHash = NULL;
	uint32 hashFlags = 0;
	HASHCTL info = { 0 };
	int initialNumberOfElements = 16;

	info.keysize = sizeof(IntermediateResultsHashKey);
	info.entrysize = sizeof(IntermediateResultsHashEntry);
	info.hash = string_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	intermediateResultsHash = hash_create("Intermediate results hash",
										  initialNumberOfElements, &info, hashFlags);

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
		WorkerNode *workerNode = LookupNodeByNodeId(lfirst_int(nodeIdCell));

		workerNodeList = lappend(workerNodeList, workerNode);
		elog(DEBUG1, "%s is broadcasted to %s:%d", resultId, workerNode->workerName,
			 workerNode->workerPort);
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
	IntermediateResultsHashKey key;
	IntermediateResultsHashEntry *entry = NULL;
	bool found = false;

	strlcpy(key.intermediate_result_id, resultId, NAMEDATALEN);
	entry = hash_search(intermediateResultsHash, &key, HASH_ENTER, &found);

	/* use sane defaults */
	if (!found)
	{
		entry->nodeIdList = NIL;
	}

	return entry;
}
