/*-------------------------------------------------------------------------
 *
 * distributed_intermediate_results.c
 *   Functions for reading and writing distributed intermediate results.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/pg_version_constants.h"

#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/transaction_management.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * NodePair contains the source and destination node in a NodeToNodeFragmentsTransfer.
 * It is a separate struct to use it as a key in a hash table.
 */
typedef struct NodePair
{
	uint32 sourceNodeId;
	uint32 targetNodeId;
} NodePair;


/*
 * NodeToNodeFragmentsTransfer contains all fragments that need to be fetched from
 * the source node to the destination node in the NodePair.
 */
typedef struct NodeToNodeFragmentsTransfer
{
	NodePair nodes;
	List *fragmentList;
} NodeToNodeFragmentsTransfer;


/* forward declarations of local functions */
static void WrapTasksForPartitioning(const char *resultIdPrefix, List *selectTaskList,
									 int partitionColumnIndex,
									 CitusTableCacheEntry *targetRelation,
									 bool binaryFormat);
static List * ExecutePartitionTaskList(List *partitionTaskList,
									   CitusTableCacheEntry *targetRelation);
static ArrayType * CreateArrayFromDatums(Datum *datumArray, bool *nullsArray, int
										 datumCount, Oid typeId);
static void ShardMinMaxValueArrays(ShardInterval **shardIntervalArray, int shardCount,
								   Oid intervalTypeId, ArrayType **minValueArray,
								   ArrayType **maxValueArray);
static char * SourceShardPrefix(const char *resultPrefix, uint64 shardId);
static DistributedResultFragment * TupleToDistributedResultFragment(
	TupleTableSlot *tupleSlot, CitusTableCacheEntry *targetRelation);
static Tuplestorestate * ExecuteSelectTasksIntoTupleStore(List *taskList,
														  TupleDesc resultDescriptor,
														  bool errorOnAnyFailure);
static List ** ColocateFragmentsWithRelation(List *fragmentList,
											 CitusTableCacheEntry *targetRelation);
static List * ColocationTransfers(List *fragmentList,
								  CitusTableCacheEntry *targetRelation);
static List * FragmentTransferTaskList(List *fragmentListTransfers);
static char * QueryStringForFragmentsTransfer(
	NodeToNodeFragmentsTransfer *fragmentsTransfer);
static void ExecuteFetchTaskList(List *fetchTaskList);


/*
 * RedistributeTaskListResults partitions the results of given task list using
 * shard ranges and partition method of given targetRelation, and then colocates
 * the result files with shards.
 *
 * If a shard has a replication factor > 1, corresponding result files are copied
 * to all nodes containing that shard.
 *
 * returnValue[shardIndex] is list of cstrings each of which is a resultId which
 * correspond to targetRelation->sortedShardIntervalArray[shardIndex].
 *
 * partitionColumnIndex determines the column in the selectTaskList to use for
 * partitioning.
 */
List **
RedistributeTaskListResults(const char *resultIdPrefix, List *selectTaskList,
							int partitionColumnIndex,
							CitusTableCacheEntry *targetRelation,
							bool binaryFormat)
{
	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	List *fragmentList = PartitionTasklistResults(resultIdPrefix, selectTaskList,
												  partitionColumnIndex,
												  targetRelation, binaryFormat);
	return ColocateFragmentsWithRelation(fragmentList, targetRelation);
}


/*
 * PartitionTasklistResults executes the given task list, and partitions results
 * of each task based on targetRelation's distribution method and intervals.
 * Each of the result partitions are stored in the node where task was executed,
 * and are named as $resultIdPrefix_from_$sourceShardId_to_$targetShardIndex.
 *
 * Result is list of DistributedResultFragment, each of which represents a
 * partition of results. Empty results are omitted. Therefore, if we have N tasks
 * and target relation has M shards, we will have NxM-(number of empty results)
 * fragments.
 *
 * partitionColumnIndex determines the column in the selectTaskList to use for
 * partitioning.
 */
List *
PartitionTasklistResults(const char *resultIdPrefix, List *selectTaskList,
						 int partitionColumnIndex,
						 CitusTableCacheEntry *targetRelation,
						 bool binaryFormat)
{
	if (targetRelation->partitionMethod != DISTRIBUTE_BY_HASH &&
		targetRelation->partitionMethod != DISTRIBUTE_BY_RANGE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("repartitioning results of a tasklist is only supported "
							   "when target relation is hash or range partitioned.")));
	}

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	WrapTasksForPartitioning(resultIdPrefix, selectTaskList,
							 partitionColumnIndex, targetRelation,
							 binaryFormat);
	return ExecutePartitionTaskList(selectTaskList, targetRelation);
}


/*
 * WrapTasksForPartitioning wraps the query for each of the tasks by a call
 * to worker_partition_query_result(). Target list of the wrapped query should
 * match the tuple descriptor in ExecutePartitionTaskList().
 */
static void
WrapTasksForPartitioning(const char *resultIdPrefix, List *selectTaskList,
						 int partitionColumnIndex,
						 CitusTableCacheEntry *targetRelation,
						 bool binaryFormat)
{
	ShardInterval **shardIntervalArray = targetRelation->sortedShardIntervalArray;
	int shardCount = targetRelation->shardIntervalArrayLength;

	ArrayType *minValueArray = NULL;
	ArrayType *maxValueArray = NULL;
	Var *partitionColumn = targetRelation->partitionColumn;
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = 0;
	Oid intervalTypeOutFunc = InvalidOid;
	bool intervalTypeVarlena = false;

	GetIntervalTypeInfo(targetRelation->partitionMethod, partitionColumn,
						&intervalTypeId, &intervalTypeMod);
	getTypeOutputInfo(intervalTypeId, &intervalTypeOutFunc, &intervalTypeVarlena);

	ShardMinMaxValueArrays(shardIntervalArray, shardCount, intervalTypeOutFunc,
						   &minValueArray, &maxValueArray);
	StringInfo minValuesString = ArrayObjectToString(minValueArray, TEXTOID,
													 intervalTypeMod);
	StringInfo maxValuesString = ArrayObjectToString(maxValueArray, TEXTOID,
													 intervalTypeMod);

	Task *selectTask = NULL;
	foreach_ptr(selectTask, selectTaskList)
	{
		List *shardPlacementList = selectTask->taskPlacementList;
		char *taskPrefix = SourceShardPrefix(resultIdPrefix, selectTask->anchorShardId);
		char *partitionMethodString = targetRelation->partitionMethod == 'h' ?
									  "hash" : "range";
		const char *binaryFormatString = binaryFormat ? "true" : "false";
		List *perPlacementQueries = NIL;

		/*
		 * We need to know which placement could successfully execute the query,
		 * so we form a different query per placement, each of which returning
		 * the node id of the placement.
		 */
		ShardPlacement *shardPlacement = NULL;
		foreach_ptr(shardPlacement, shardPlacementList)
		{
			StringInfo wrappedQuery = makeStringInfo();
			appendStringInfo(wrappedQuery,
							 "SELECT %u, partition_index"
							 ", %s || '_' || partition_index::text "
							 ", rows_written "
							 "FROM worker_partition_query_result"
							 "(%s,%s,%d,%s,%s,%s,%s) WHERE rows_written > 0",
							 shardPlacement->nodeId,
							 quote_literal_cstr(taskPrefix),
							 quote_literal_cstr(taskPrefix),
							 quote_literal_cstr(TaskQueryStringForAllPlacements(
													selectTask)),
							 partitionColumnIndex,
							 quote_literal_cstr(partitionMethodString),
							 minValuesString->data, maxValuesString->data,
							 binaryFormatString);
			perPlacementQueries = lappend(perPlacementQueries, wrappedQuery->data);
		}
		SetTaskPerPlacementQueryStrings(selectTask, perPlacementQueries);
	}
}


/*
 * SourceShardPrefix returns result id prefix for partitions which have the
 * given anchor shard id.
 */
static char *
SourceShardPrefix(const char *resultPrefix, uint64 shardId)
{
	StringInfo taskPrefix = makeStringInfo();

	appendStringInfo(taskPrefix, "%s_from_" UINT64_FORMAT "_to", resultPrefix, shardId);

	return taskPrefix->data;
}


/*
 * ShardMinMaxValueArrays returns min values and max values of given shard
 * intervals. Returned arrays are text arrays.
 */
static void
ShardMinMaxValueArrays(ShardInterval **shardIntervalArray, int shardCount,
					   Oid intervalTypeOutFunc, ArrayType **minValueArray,
					   ArrayType **maxValueArray)
{
	Datum *minValues = palloc0(shardCount * sizeof(Datum));
	bool *minValueNulls = palloc0(shardCount * sizeof(bool));
	Datum *maxValues = palloc0(shardCount * sizeof(Datum));
	bool *maxValueNulls = palloc0(shardCount * sizeof(bool));
	for (int shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		minValueNulls[shardIndex] = !shardIntervalArray[shardIndex]->minValueExists;
		maxValueNulls[shardIndex] = !shardIntervalArray[shardIndex]->maxValueExists;

		if (!minValueNulls[shardIndex])
		{
			Datum minValue = shardIntervalArray[shardIndex]->minValue;
			char *minValueStr = DatumGetCString(OidFunctionCall1(intervalTypeOutFunc,
																 minValue));
			minValues[shardIndex] = CStringGetTextDatum(minValueStr);
		}

		if (!maxValueNulls[shardIndex])
		{
			Datum maxValue = shardIntervalArray[shardIndex]->maxValue;
			char *maxValueStr = DatumGetCString(OidFunctionCall1(intervalTypeOutFunc,
																 maxValue));
			maxValues[shardIndex] = CStringGetTextDatum(maxValueStr);
		}
	}

	*minValueArray = CreateArrayFromDatums(minValues, minValueNulls, shardCount, TEXTOID);
	*maxValueArray = CreateArrayFromDatums(maxValues, maxValueNulls, shardCount, TEXTOID);
}


/*
 * CreateArrayFromDatums creates an array consisting of given values and nulls.
 */
static ArrayType *
CreateArrayFromDatums(Datum *datumArray, bool *nullsArray, int datumCount, Oid typeId)
{
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;
	int dimensions[1] = { datumCount };
	int lowerbounds[1] = { 1 };

	get_typlenbyvalalign(typeId, &typeLength, &typeByValue, &typeAlignment);

	ArrayType *datumArrayObject = construct_md_array(datumArray, nullsArray, 1,
													 dimensions,
													 lowerbounds, typeId, typeLength,
													 typeByValue, typeAlignment);

	return datumArrayObject;
}


/*
 * ExecutePartitionTaskList executes the queries formed in WrapTasksForPartitioning(),
 * and returns its results as a list of DistributedResultFragment.
 */
static List *
ExecutePartitionTaskList(List *taskList, CitusTableCacheEntry *targetRelation)
{
	TupleDesc resultDescriptor = NULL;
	Tuplestorestate *resultStore = NULL;
	int resultColumnCount = 4;

#if PG_VERSION_NUM >= PG_VERSION_12
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount);
#else
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount, false);
#endif

	TupleDescInitEntry(resultDescriptor, (AttrNumber) 1, "node_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 2, "partition_index",
					   INT4OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 3, "result_id",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 4, "rows_written",
					   INT8OID, -1, 0);

	bool errorOnAnyFailure = false;
	resultStore = ExecuteSelectTasksIntoTupleStore(taskList, resultDescriptor,
												   errorOnAnyFailure);

	List *fragmentList = NIL;
	TupleTableSlot *slot = MakeSingleTupleTableSlotCompat(resultDescriptor,
														  &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(resultStore, true, false, slot))
	{
		DistributedResultFragment *distributedResultFragment =
			TupleToDistributedResultFragment(slot, targetRelation);

		fragmentList = lappend(fragmentList, distributedResultFragment);

		ExecClearTuple(slot);
	}

	return fragmentList;
}


/*
 * TupleToDistributedResultFragment converts a tuple returned by the query in
 * WrapTasksForPartitioning() to a DistributedResultFragment.
 */
static DistributedResultFragment *
TupleToDistributedResultFragment(TupleTableSlot *tupleSlot,
								 CitusTableCacheEntry *targetRelation)
{
	bool isNull = false;
	uint32 sourceNodeId = DatumGetUInt32(slot_getattr(tupleSlot, 1, &isNull));
	uint32 targetShardIndex = DatumGetUInt32(slot_getattr(tupleSlot, 2, &isNull));
	text *resultId = DatumGetTextP(slot_getattr(tupleSlot, 3, &isNull));
	int64 rowCount = DatumGetInt64(slot_getattr(tupleSlot, 4, &isNull));

	Assert(targetShardIndex < targetRelation->shardIntervalArrayLength);
	ShardInterval *shardInterval =
		targetRelation->sortedShardIntervalArray[targetShardIndex];

	DistributedResultFragment *distributedResultFragment =
		palloc0(sizeof(DistributedResultFragment));

	distributedResultFragment->nodeId = sourceNodeId;
	distributedResultFragment->targetShardIndex = targetShardIndex;
	distributedResultFragment->targetShardId = shardInterval->shardId;
	distributedResultFragment->resultId = text_to_cstring(resultId);
	distributedResultFragment->rowCount = rowCount;

	return distributedResultFragment;
}


/*
 * ExecuteSelectTasksIntoTupleStore executes the given tasks and returns a tuple
 * store containing its results.
 */
static Tuplestorestate *
ExecuteSelectTasksIntoTupleStore(List *taskList, TupleDesc resultDescriptor,
								 bool errorOnAnyFailure)
{
	bool expectResults = true;
	int targetPoolSize = MaxAdaptiveExecutorPoolSize;
	bool randomAccess = true;
	bool interTransactions = false;
	TransactionProperties xactProperties = {
		.errorOnAnyFailure = errorOnAnyFailure,
		.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_REQUIRED,
		.requires2PC = false
	};

	Tuplestorestate *resultStore = tuplestore_begin_heap(randomAccess, interTransactions,
														 work_mem);

	/*
	 * Local execution is not supported because here we use perPlacementQueryStrings.
	 * Local execution does not know how to handle it. One solution is to extract and set
	 * queryStringLazy from perPlacementQueryStrings. The extracted one should be the
	 * query string for the local placement.
	 */
	bool localExecutionSupported = false;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		ROW_MODIFY_READONLY, taskList, targetPoolSize, localExecutionSupported
		);
	executionParams->tupleDescriptor = resultDescriptor;
	executionParams->tupleStore = resultStore;
	executionParams->xactProperties = xactProperties;
	executionParams->expectResults = expectResults;

	ExecuteTaskListExtended(executionParams);

	return resultStore;
}


/*
 * ColocateFragmentsWithRelation moves the fragments in the cluster so they are
 * colocated with the shards of target relation. These transfers are done by
 * calls to fetch_intermediate_results() between nodes.
 *
 * returnValue[shardIndex] is list of result Ids that are colocated with
 * targetRelation->sortedShardIntervalArray[shardIndex] after fetch tasks are
 * done.
 */
static List **
ColocateFragmentsWithRelation(List *fragmentList, CitusTableCacheEntry *targetRelation)
{
	List *fragmentListTransfers = ColocationTransfers(fragmentList, targetRelation);
	List *fragmentTransferTaskList = FragmentTransferTaskList(fragmentListTransfers);

	ExecuteFetchTaskList(fragmentTransferTaskList);

	int shardCount = targetRelation->shardIntervalArrayLength;
	List **shardResultIdList = palloc0(shardCount * sizeof(List *));

	DistributedResultFragment *sourceFragment = NULL;
	foreach_ptr(sourceFragment, fragmentList)
	{
		int shardIndex = sourceFragment->targetShardIndex;

		Assert(shardIndex < shardCount);
		shardResultIdList[shardIndex] = lappend(shardResultIdList[shardIndex],
												sourceFragment->resultId);
	}

	return shardResultIdList;
}


/*
 * ColocationTransfers returns a list of transfers to colocate given fragments with
 * shards of the target relation. These transfers also take into account replicated
 * target relations. This prunes away transfers with same source and target
 */
static List *
ColocationTransfers(List *fragmentList, CitusTableCacheEntry *targetRelation)
{
	HASHCTL transferHashInfo;
	MemSet(&transferHashInfo, 0, sizeof(HASHCTL));
	transferHashInfo.keysize = sizeof(NodePair);
	transferHashInfo.entrysize = sizeof(NodeToNodeFragmentsTransfer);
	transferHashInfo.hcxt = CurrentMemoryContext;
	HTAB *transferHash = hash_create("Fragment Transfer Hash", 32, &transferHashInfo,
									 HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	DistributedResultFragment *fragment = NULL;
	foreach_ptr(fragment, fragmentList)
	{
		List *placementList = ActiveShardPlacementList(fragment->targetShardId);
		ShardPlacement *placement = NULL;
		foreach_ptr(placement, placementList)
		{
			NodePair transferKey = {
				.sourceNodeId = fragment->nodeId,
				.targetNodeId = placement->nodeId
			};

			if (transferKey.sourceNodeId == transferKey.targetNodeId)
			{
				continue;
			}

			bool foundInCache = false;
			NodeToNodeFragmentsTransfer *fragmentListTransfer =
				hash_search(transferHash, &transferKey, HASH_ENTER, &foundInCache);
			if (!foundInCache)
			{
				fragmentListTransfer->nodes = transferKey;
				fragmentListTransfer->fragmentList = NIL;
			}

			fragmentListTransfer->fragmentList =
				lappend(fragmentListTransfer->fragmentList, fragment);
		}
	}

	List *fragmentListTransfers = NIL;
	NodeToNodeFragmentsTransfer *transfer = NULL;
	HASH_SEQ_STATUS hashSeqStatus;

	hash_seq_init(&hashSeqStatus, transferHash);

	while ((transfer = hash_seq_search(&hashSeqStatus)) != NULL)
	{
		fragmentListTransfers = lappend(fragmentListTransfers, transfer);
	}

	return fragmentListTransfers;
}


/*
 * FragmentTransferTaskList returns a list of tasks which performs the given list of
 * transfers. Each of the transfers are done by a SQL call to fetch_intermediate_results.
 * See QueryStringForFragmentsTransfer for how the query is constructed.
 */
static List *
FragmentTransferTaskList(List *fragmentListTransfers)
{
	List *fetchTaskList = NIL;

	NodeToNodeFragmentsTransfer *fragmentsTransfer = NULL;
	foreach_ptr(fragmentsTransfer, fragmentListTransfers)
	{
		uint32 targetNodeId = fragmentsTransfer->nodes.targetNodeId;

		/* these should have already been pruned away in ColocationTransfers */
		Assert(targetNodeId != fragmentsTransfer->nodes.sourceNodeId);

		WorkerNode *workerNode = ForceLookupNodeByNodeId(targetNodeId);

		ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(targetPlacement, workerNode);

		Task *task = CitusMakeNode(Task);
		task->taskType = READ_TASK;
		SetTaskQueryString(task, QueryStringForFragmentsTransfer(fragmentsTransfer));
		task->taskPlacementList = list_make1(targetPlacement);

		fetchTaskList = lappend(fetchTaskList, task);
	}

	return fetchTaskList;
}


/*
 * QueryStringForFragmentsTransfer returns a query which fetches distributed
 * result fragments from source node to target node. See the structure of
 * NodeToNodeFragmentsTransfer for details of how these are decided.
 */
static char *
QueryStringForFragmentsTransfer(NodeToNodeFragmentsTransfer *fragmentsTransfer)
{
	StringInfo queryString = makeStringInfo();
	StringInfo fragmentNamesArrayString = makeStringInfo();
	int fragmentCount = 0;
	NodePair *nodePair = &fragmentsTransfer->nodes;
	WorkerNode *sourceNode = ForceLookupNodeByNodeId(nodePair->sourceNodeId);

	appendStringInfoString(fragmentNamesArrayString, "ARRAY[");

	DistributedResultFragment *fragment = NULL;
	foreach_ptr(fragment, fragmentsTransfer->fragmentList)
	{
		const char *fragmentName = fragment->resultId;

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
					 "SELECT bytes FROM fetch_intermediate_results(%s,%s,%d) bytes",
					 fragmentNamesArrayString->data,
					 quote_literal_cstr(sourceNode->workerName),
					 sourceNode->workerPort);

	ereport(DEBUG3, (errmsg("fetch task on %s:%d: %s", sourceNode->workerName,
							sourceNode->workerPort, queryString->data)));

	return queryString->data;
}


/*
 * ExecuteFetchTaskList executes a list of fetch_intermediate_results() tasks.
 * It ignores the byte_count result of the fetch_intermediate_results() calls.
 */
static void
ExecuteFetchTaskList(List *taskList)
{
	TupleDesc resultDescriptor = NULL;
	Tuplestorestate *resultStore = NULL;
	int resultColumnCount = 1;

#if PG_VERSION_NUM >= PG_VERSION_12
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount);
#else
	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount, false);
#endif

	TupleDescInitEntry(resultDescriptor, (AttrNumber) 1, "byte_count", INT8OID, -1, 0);

	bool errorOnAnyFailure = true;
	resultStore = ExecuteSelectTasksIntoTupleStore(taskList, resultDescriptor,
												   errorOnAnyFailure);

	TupleTableSlot *slot = MakeSingleTupleTableSlotCompat(resultDescriptor,
														  &TTSOpsMinimalTuple);

	while (tuplestore_gettupleslot(resultStore, true, false, slot))
	{
		ExecClearTuple(slot);
	}
}
