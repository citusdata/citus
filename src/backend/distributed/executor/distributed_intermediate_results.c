/*-------------------------------------------------------------------------
 *
 * distributed_intermediate_results.c
 *   Functions for reading and writing distributed intermediate results.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "access/tupdesc.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/recursive_planning.h"
#include "distributed/redistribution.h"
#include "distributed/remote_commands.h"
#include "distributed/sharding.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/syscache.h"


/*
 * NodePair contains the source and destination node in a NodeToNodeFragmentsTransfer.
 * It is a separate struct to use it as a key in a hash table.
 */
typedef struct NodePair
{
	int sourceNodeId;
	int targetNodeId;
} NodePair;


/*
 * NodeToNodeFragmentsTransfer contains all fragments that need to be fetched from
 * the source node to the destination node in the NodePair.
 */
typedef struct NodeToNodeFragmentsTransfer
{
	NodePair nodes;
	List *fragmentsToFetch;
} NodeToNodeFragmentsTransfer;


/*
 * TargetShardFragment
 */
typedef struct TargetShardFragments
{
	/* index of the target shard, when shards are sorted by minvalue */
	int targetShardIndex;

	/* list of TargetShardFragmentStats, one for each fragment in the target shard */
	List *fragments;
} TargetShardFragments;


/*
 * PredistributionStats contains the statistics on individual fragments when task
 * results are partitioned into fragments using
 * worker_predistribute_query_result.
 */
typedef struct PredistributionStats
{
	int targetShardCount;
	TargetShardFragments *targetShardFragments;
} PredistributionStats;


static RedistributedQueryResult * RedistributeTaskListResult(char *distResultId,
															 List *taskList,
															 int distributionColumnIndex,
															 DistributionScheme *
															 targetDistribution,
															 bool isForWrites);
static void WrapTasksForPredistribution(List *taskList, char *resultPrefix,
										int distributionColumnIndex,
										DistributionScheme *targetDistribution);
static ArrayType * ShardMinValueArrayForDistribution(PartitioningScheme *partitioning);
static char * SourceShardPrefix(char *resultPrefix, uint64 shardId);
static RedistributedQueryResult * CreateRedistributedQueryResult(char *resultPrefix,
																 PredistributionStats *
																 predistributionStats,
																 DistributionScheme *
																 targetDistribution,
																 bool isForWrites,
																 List **
																 fragmentsTransferList);
static PredistributionStats * ExecutePredistributionTasks(List *taskList,
														  DistributionScheme *
														  targetDistribution);
static PredistributionStats * CreatePredistributionStats(int targetShardCount);
static PredistributionStats * TupleStoreToPredistributionStats(
	Tuplestorestate *tupleStore, TupleDesc resultDescriptor,
	DistributionScheme *
	targetDistribution);
static List * BuildFetchTaskListForFragmentTransfers(char *resultPrefix,
													 List *fragmentsTransferList);
static char * BuildQueryStringForFragmentsTransfer(char *resultPrefix,
												   NodeToNodeFragmentsTransfer *
												   fragmentsTransfer);
static char * TargetShardFragmentName(char *resultPrefix,
									  TargetShardFragmentStats *fragmentStats);
static void ExecuteFetchTasks(List *fetchTaskList);
static Tuplestorestate * ExecuteTasksIntoTupleStore(List *taskList, TupleDesc
													resultDescriptor);


RedistributedQueryResult *
RedistributeDistributedPlanResult(char *distResultId, DistributedPlan *distributedPlan,
								  int distributionColumnIndex,
								  DistributionScheme *targetDistribution,
								  bool isForWrites)
{
	/* TODO: check for weird plans (insert select) */

	List *taskList = distributedPlan->workerJob->taskList;

	RedistributedQueryResult *result = RedistributeTaskListResult(distResultId, taskList,
																  distributionColumnIndex,
																  targetDistribution,
																  isForWrites);

	return result;
}


static RedistributedQueryResult *
RedistributeTaskListResult(char *distResultId, List *taskList,
						   int distributionColumnIndex,
						   DistributionScheme *targetDistribution, bool isForWrites)
{
	List *fragmentsTransferList = NIL;

	/* make sure we have a distributed transaction ID and send BEGIN */
	BeginOrContinueCoordinatedTransaction();

	/*
	 * Wrap tasks in worker_predistribute_query_result calls
	 * to generate a fragment for each shard in the target distribution.
	 */
	WrapTasksForPredistribution(taskList, distResultId, distributionColumnIndex,
								targetDistribution);

	/*
	 * Execute the worker_predistribute_query_result tasks and
	 * collect statistics to prune out empty fragments.
	 */
	PredistributionStats *predistributionStats = ExecutePredistributionTasks(taskList,
																			 targetDistribution);

	/*
	 * Create an object that represents redistributed query results. We
	 * derive the fetch tasks from the result.
	 */
	RedistributedQueryResult *result = CreateRedistributedQueryResult(distResultId,
																	  predistributionStats,
																	  targetDistribution,
																	  isForWrites,
																	  &
																	  fragmentsTransferList);

	List *fetchTaskList = BuildFetchTaskListForFragmentTransfers(distResultId,
																 fragmentsTransferList);

	/*
	 * Execute the fetch tasks, after this the result is ready to be used
	 * in distributed queries.
	 */
	ExecuteFetchTasks(fetchTaskList);

	return result;
}


/*
 * WrapTasksForPredistribution wraps the query strings in a given list of tasks
 * in calls to worker_predistribute_query_result, in order to generate
 * a fragment (intermediate result) for each shard in the target distribution.
 * Rows are written to a fragment based on the value in the distribution column
 * of the query result.
 */
static void
WrapTasksForPredistribution(List *taskList, char *resultPrefix, int
							distributionColumnIndex,
							DistributionScheme *targetDistribution)
{
	ListCell *taskCell = NULL;
	PartitioningScheme *partitioning = &(targetDistribution->partitioning);
	ArrayType *splitPointObject = ShardMinValueArrayForDistribution(partitioning);
	StringInfo splitPointString = SplitPointArrayString(splitPointObject,
														partitioning->valueTypeId,
														partitioning->valueTypeMod);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		StringInfo wrappedQuery = makeStringInfo();
		List *shardPlacementList = task->taskPlacementList;

		if (list_length(shardPlacementList) > 1)
		{
			ereport(ERROR, (errmsg("repartitioning is currently only available for "
								   "queries on distributed tables without replication")));
		}

		ShardPlacement *shardPlacement = linitial(shardPlacementList);
		char *taskPrefix = SourceShardPrefix(resultPrefix, task->anchorShardId);

		appendStringInfo(wrappedQuery,
						 "SELECT %d, " UINT64_FORMAT
						 ", partition_index, bytes_written, rows_written "
						 "FROM worker_predistribute_query_result"
						 "(%s,%s,%d,%s)",
						 shardPlacement->nodeId,
						 task->anchorShardId,
						 quote_literal_cstr(taskPrefix),
						 quote_literal_cstr(task->queryString),
						 distributionColumnIndex,
						 splitPointString->data);

		task->queryString = wrappedQuery->data;
	}
}


/*
 * ShardMinValueArrayForDistribution constructs a SQL array for the min values
 * of the partitions.
 */
static ArrayType *
ShardMinValueArrayForDistribution(PartitioningScheme *partitioning)
{
	uint32 partitionIndex = 0;
	Oid typeId = partitioning->valueTypeId;

	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	uint32 minDatumCount = partitioning->partitionCount;
	Datum *minDatumArray = palloc0(partitioning->partitionCount * sizeof(Datum));

	for (partitionIndex = 0; partitionIndex < minDatumCount; partitionIndex++)
	{
		DatumRange *hashRange = &(partitioning->ranges[partitionIndex]);

		minDatumArray[partitionIndex] = hashRange->minValue;
	}

	get_typlenbyvalalign(typeId, &typeLength, &typeByValue, &typeAlignment);

	ArrayType *splitPointObject = construct_array(minDatumArray, minDatumCount, typeId,
												  typeLength, typeByValue, typeAlignment);

	return splitPointObject;
}


static char *
SourceShardPrefix(char *resultPrefix, uint64 shardId)
{
	StringInfo taskPrefix = makeStringInfo();

	appendStringInfo(taskPrefix, "%s.from." UINT64_FORMAT ".to", resultPrefix, shardId);

	return taskPrefix->data;
}


/*
 * ExecutePredistributionTasks executes a list of tasks that were generated using
 * WrapTasksForPredistribution.
 */
static PredistributionStats *
ExecutePredistributionTasks(List *taskList, DistributionScheme *targetDistribution)
{
	TupleDesc resultDescriptor = NULL;
	Tuplestorestate *resultStore = NULL;
	int resultColumnCount = 5;
	PredistributionStats *predistributionStats = NULL;


#if PG_VERSION_NUM >= 120000
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount);
#else
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount, false);
#endif

	TupleDescInitEntry(resultDescriptor, (AttrNumber) 1, "node_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 2, "shard_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 3, "partition_index",
					   INT4OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 4, "bytes_written",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 5, "rows_written",
					   INT8OID, -1, 0);

	resultStore = ExecuteTasksIntoTupleStore(taskList, resultDescriptor);
	predistributionStats = TupleStoreToPredistributionStats(resultStore, resultDescriptor,
															targetDistribution);

	return predistributionStats;
}


static PredistributionStats *
TupleStoreToPredistributionStats(Tuplestorestate *tupleStore, TupleDesc resultDescriptor,
								 DistributionScheme *targetDistribution)
{
	TupleTableSlot *slot = MakeSingleTupleTableSlotCompat(resultDescriptor,
														  &TTSOpsMinimalTuple);
	PartitioningScheme *partitioning = &targetDistribution->partitioning;
	int shardCount = partitioning->partitionCount;

	PredistributionStats *predistributionStats = CreatePredistributionStats(shardCount);

	while (tuplestore_gettupleslot(tupleStore, true, false, slot))
	{
		TargetShardFragmentStats *shardFragmentStats = palloc0(
			sizeof(TargetShardFragmentStats));
		bool isNull = false;

		int sourceNodeId = DatumGetInt32(slot_getattr(slot, 1, &isNull));
		int64 sourceShardId = DatumGetInt64(slot_getattr(slot, 2, &isNull));
		int targetShardIndex = DatumGetInt32(slot_getattr(slot, 3, &isNull));
		int64 byteCount = DatumGetInt64(slot_getattr(slot, 4, &isNull));
		int64 rowCount = DatumGetInt64(slot_getattr(slot, 5, &isNull));

		/* protect against garbage results */
		if (targetShardIndex < 0 || targetShardIndex >= shardCount)
		{
			ereport(ERROR, (errmsg("target shard index %d out of range",
								   targetShardIndex)));
		}

		shardFragmentStats = palloc0(sizeof(TargetShardFragmentStats));
		shardFragmentStats->sourceNodeId = sourceNodeId;
		shardFragmentStats->sourceShardId = sourceShardId;
		shardFragmentStats->targetShardIndex = targetShardIndex;
		shardFragmentStats->byteCount = byteCount;
		shardFragmentStats->rowCount = rowCount;

		TargetShardFragments *fragmentSet =
			&(predistributionStats->targetShardFragments[targetShardIndex]);
		fragmentSet->fragments = lappend(fragmentSet->fragments, shardFragmentStats);

		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);

	return predistributionStats;
}


/*
 * CreatePredistributionStats creates a data structure for holding the statistics
 * returned by executing SQL tasks wrapped in worker_predistribute_query_result
 * calls.
 */
static PredistributionStats *
CreatePredistributionStats(int targetShardCount)
{
	PredistributionStats *predistributionStats = palloc0(sizeof(PredistributionStats));
	int targetShardIndex = 0;

	predistributionStats->targetShardCount = targetShardCount;
	predistributionStats->targetShardFragments =
		palloc0(targetShardCount * sizeof(TargetShardFragments));

	for (targetShardIndex = 0; targetShardIndex < targetShardCount; targetShardIndex++)
	{
		TargetShardFragments *fragmentSet =
			&(predistributionStats->targetShardFragments[targetShardIndex]);

		fragmentSet->targetShardIndex = targetShardIndex;
		fragmentSet->fragments = NIL;
	}

	return predistributionStats;
}


/*
 * CreateRedistributedQueryResult creates a RedistributedQueryResult that contains
 * the reassembled fragment sets which are effectively shards in the temporary
 * distributed table that consist of smaller fragments. Empty fragments are filtered
 * out using the pre-distribution statistics.
 *
 * As part of the process, we also build the list of transfers that need to happen
 * to get the fragments to the right place, grouped by a pair of source and target
 * nodes such that we only open one connection per node pair.
 *
 * TODO: maybe generate the transfer list separately
 */
static RedistributedQueryResult *
CreateRedistributedQueryResult(char *resultPrefix,
							   PredistributionStats *predistributionStats,
							   DistributionScheme *targetDistribution,
							   bool isForWrites,
							   List **fragmentsTransferList)
{
	PartitioningScheme *partitioning = &targetDistribution->partitioning;
	NodeToNodeFragmentsTransfer *fragmentsTransfer = NULL;
	int targetShardIndex = 0;
	int targetShardCount = partitioning->partitionCount;
	HASHCTL info;
	HASH_SEQ_STATUS status;

	ReassembledFragmentSet *reassembledFragmentSets = palloc0(
		sizeof(ReassembledFragmentSet) * targetShardCount);

	RedistributedQueryResult *result = palloc0(sizeof(RedistributedQueryResult));
	result->resultPrefix = pstrdup(resultPrefix);
	result->partitioning = partitioning;
	result->reassembledFragmentSets = reassembledFragmentSets;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(NodePair);
	info.entrysize = sizeof(NodeToNodeFragmentsTransfer);
	info.hcxt = CurrentMemoryContext;
	uint32 hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	HTAB *nodeToNodeTransfers = hash_create("Fragment transfers", 32, &info, hashFlags);

	/*
	 * For each target shard, there is a TargetShardFragments that contains
	 * statistics on all fragments that make up the shard from the time they
	 * were generated on their respective source nodes.
	 *
	 * In this loop we transform TargetShardFragments into ReassembledFragmentSet,
	 * which is tied to a particular (target) node and does not contain empty
	 * fragments. This allows us to construct the appropriate fetch and read tasks.
	 *
	 * Effectively, TargetShardFragments represents the fragments in a target
	 * shard before redistribution and ReassembledFragmentSet represents the
	 * fragments in a target shard after redistribution.
	 */
	for (targetShardIndex = 0; targetShardIndex < targetShardCount; targetShardIndex++)
	{
		TargetShardFragments *targetShardFragments =
			&(predistributionStats->targetShardFragments[targetShardIndex]);
		ReassembledFragmentSet *reassembledFragments =
			&(reassembledFragmentSets[targetShardIndex]);

		ListCell *fragmentCell = NULL;
		int *nodeGroupIds = targetDistribution->groupIds[targetShardIndex];

		/* TODO: consider replication */
		int targetGroupId = nodeGroupIds[0];

		/* TODO: consider follower clusters (isForWrites) */
		WorkerNode *targetNode = PrimaryNodeForGroup(targetGroupId, NULL);

		List *targetShardFragmentStatsList = NIL;

		foreach(fragmentCell, targetShardFragments->fragments)
		{
			TargetShardFragmentStats *fragmentStats = lfirst(fragmentCell);
			NodePair nodePair = {
				fragmentStats->sourceNodeId,
				targetNode->nodeId
			};
			bool foundPair = false;

			if (fragmentStats->rowCount == 0)
			{
				/* no data in fragment */
				continue;
			}

			targetShardFragmentStatsList = lappend(targetShardFragmentStatsList,
												   fragmentStats);

			if (nodePair.sourceNodeId == nodePair.targetNodeId)
			{
				/* fragment is already on the right node, not transfer needed */
				continue;
			}

			fragmentsTransfer = hash_search(nodeToNodeTransfers, &nodePair, HASH_ENTER,
											&foundPair);
			if (!foundPair)
			{
				fragmentsTransfer->fragmentsToFetch = NIL;
			}

			fragmentsTransfer->fragmentsToFetch = lappend(
				fragmentsTransfer->fragmentsToFetch,
				fragmentStats);
		}

		reassembledFragments->nodeId = targetNode->nodeId;
		reassembledFragments->fragments = targetShardFragmentStatsList;
	}

	if (fragmentsTransferList != NULL)
	{
		hash_seq_init(&status, nodeToNodeTransfers);

		while ((fragmentsTransfer = hash_seq_search(&status)) != NULL)
		{
			if (list_length(fragmentsTransfer->fragmentsToFetch) == 0)
			{
				/* all fragments from source to target were empty */
				continue;
			}

			*fragmentsTransferList = lappend(*fragmentsTransferList, fragmentsTransfer);
		}
	}

	return result;
}


/*
 * BuildFetchTaskListForFragmentTransfers expects a list of NodeToNodeFragmentsTransfers
 * and for each transfer it builds an appropriate fetch_intermediate_result task to
 * perform the transfer.
 */
static List *
BuildFetchTaskListForFragmentTransfers(char *resultPrefix, List *fragmentsTransferList)
{
	List *fetchTaskList = NIL;
	ListCell *fragmentsTransferCell = NULL;

	foreach(fragmentsTransferCell, fragmentsTransferList)
	{
		NodeToNodeFragmentsTransfer *fragmentsTransfer = lfirst(fragmentsTransferCell);
		int targetNodeId = fragmentsTransfer->nodes.targetNodeId;
		WorkerNode *workerNode = LookupNodeByNodeId(targetNodeId);

		if (list_length(fragmentsTransfer->fragmentsToFetch) == 0)
		{
			continue;
		}

		ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
		targetPlacement->nodeName = workerNode->workerName;
		targetPlacement->nodePort = workerNode->workerPort;
		targetPlacement->groupId = workerNode->groupId;

		Task *task = CitusMakeNode(Task);
		task->taskType = MODIFY_TASK;
		task->queryString = BuildQueryStringForFragmentsTransfer(resultPrefix,
																 fragmentsTransfer);
		task->taskPlacementList = list_make1(targetPlacement);

		fetchTaskList = lappend(fetchTaskList, task);
	}

	return fetchTaskList;
}


/*
 * BuildQueryStringForFragmentsTransfer builds a fetch_intermediate_result query to
 * perform the given node-to-node transfer by executing the query on the target
 * node.
 */
static char *
BuildQueryStringForFragmentsTransfer(char *resultPrefix,
									 NodeToNodeFragmentsTransfer *fragmentsTransfer)
{
	ListCell *fragmentCell = NULL;
	StringInfo queryString = makeStringInfo();
	StringInfo fragmentNamesArrayString = makeStringInfo();
	int fragmentCount = 0;
	NodePair *nodePair = &fragmentsTransfer->nodes;
	WorkerNode *sourceNode = LookupNodeByNodeId(nodePair->sourceNodeId);

	appendStringInfoString(fragmentNamesArrayString, "ARRAY[");

	foreach(fragmentCell, fragmentsTransfer->fragmentsToFetch)
	{
		TargetShardFragmentStats *fragmentStats = lfirst(fragmentCell);
		char *fragmentName = TargetShardFragmentName(resultPrefix, fragmentStats);

		if (fragmentCount > 0)
		{
			appendStringInfoString(fragmentNamesArrayString, ",");
		}

		appendStringInfoString(fragmentNamesArrayString,
							   quote_literal_cstr(fragmentName));

		fragmentCount++;
	}

	appendStringInfoString(fragmentNamesArrayString, "]::text[]");

	appendStringInfo(queryString,
					 "SELECT %d, bytes FROM fetch_intermediate_results(%s,%s,%d) bytes",
					 nodePair->targetNodeId,
					 fragmentNamesArrayString->data,
					 quote_literal_cstr(sourceNode->workerName),
					 sourceNode->workerPort);

	return queryString->data;
}


static char *
TargetShardFragmentName(char *resultPrefix, TargetShardFragmentStats *fragmentStats)
{
	StringInfo fragmentName = makeStringInfo();
	int targetShardIndex = fragmentStats->targetShardIndex;

	char *sourceShardPrefix = SourceShardPrefix(resultPrefix,
												fragmentStats->sourceShardId);

	appendStringInfo(fragmentName, "%s.%d", sourceShardPrefix, targetShardIndex);

	return fragmentName->data;
}


/*
 * ExecuteFetchTasks executes a list of fetch tasks and discards the results.
 * The fetch task returns the number of bytes written, but this is currently
 * unused.
 */
static void
ExecuteFetchTasks(List *fetchTaskList)
{
	TupleDesc resultDescriptor = NULL;
	int resultColumnCount = 2;

#if PG_VERSION_NUM >= 120000
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount);
#else
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount, false);
#endif

	TupleDescInitEntry(resultDescriptor, (AttrNumber) 1, "node_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 2, "bytes_written",
					   INT8OID, -1, 0);

	ExecuteTasksIntoTupleStore(fetchTaskList, resultDescriptor);
}


static Tuplestorestate *
ExecuteTasksIntoTupleStore(List *taskList, TupleDesc resultDescriptor)
{
	bool hasReturning = true;
	int targetPoolSize = MaxAdaptiveExecutorPoolSize;
	bool randomAccess = true;
	bool interTransactions = false;

	Tuplestorestate *resultStore = tuplestore_begin_heap(randomAccess, interTransactions,
														 work_mem);

	ExecuteTaskListExtended(ROW_MODIFY_READONLY, taskList, resultDescriptor,
							resultStore, hasReturning, targetPoolSize);

	return resultStore;
}


Query *
ReadReassembledFragmentSetQuery(char *resultPrefix, List *targetList,
								ReassembledFragmentSet *fragmentSet)
{
	ListCell *fragmentCell = NULL;
	List *fragmentNames = NIL;

	foreach(fragmentCell, fragmentSet->fragments)
	{
		TargetShardFragmentStats *fragmentStats = lfirst(fragmentCell);

		char *fragmentName = TargetShardFragmentName(resultPrefix, fragmentStats);

		fragmentNames = lappend(fragmentNames, fragmentName);
	}

	Query *query = BuildReadIntermediateResultsArrayQuery(targetList, NIL, fragmentNames);

	return query;
}
