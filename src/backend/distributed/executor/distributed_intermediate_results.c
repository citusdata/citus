/*-------------------------------------------------------------------------
 *
 * distributed_intermediate_results.c
 *   Functions for reading and writing distributed intermediate results.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "distributed/intermediate_results.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/transaction_management.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* forward declarations of local functions */
static void WrapTasksForPartitioning(char *resultIdPrefix, List *selectTaskList,
									 DistTableCacheEntry *targetRelation,
									 bool binaryFormat);
static List * ExecutePartitionTaskList(List *partitionTaskList,
									   DistTableCacheEntry *targetRelation);
static ArrayType * CreateArrayFromDatums(Datum *datumArray, bool *nullsArray, int
										 datumCount, Oid typeId);
static void ShardMinMaxValueArrays(ShardInterval **shardIntervalArray, int shardCount,
								   Oid intervalTypeId, ArrayType **minValueArray,
								   ArrayType **maxValueArray);
static char * SourceShardPrefix(char *resultPrefix, uint64 shardId);
static DistributedResultFragment * TupleToDistributedResultFragment(
	TupleTableSlot *tupleSlot, DistTableCacheEntry *targetRelation);
static Tuplestorestate * ExecuteSelectTasksIntoTupleStore(List *taskList, TupleDesc
														  resultDescriptor);


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
 */
List *
PartitionTasklistResults(char *resultIdPrefix, List *selectTaskList,
						 DistTableCacheEntry *targetRelation,
						 bool binaryFormat)
{
	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	WrapTasksForPartitioning(resultIdPrefix, selectTaskList, targetRelation,
							 binaryFormat);
	return ExecutePartitionTaskList(selectTaskList, targetRelation);
}


/*
 * WrapTasksForPartitioning wraps the query for each of the tasks by a call
 * to worker_partition_query_result(). Target list of the wrapped query should
 * match the tuple descriptor in ExecutePartitionTaskList().
 */
static void
WrapTasksForPartitioning(char *resultIdPrefix, List *selectTaskList,
						 DistTableCacheEntry *targetRelation,
						 bool binaryFormat)
{
	ListCell *taskCell = NULL;
	ShardInterval **shardIntervalArray = targetRelation->sortedShardIntervalArray;
	int shardCount = targetRelation->shardIntervalArrayLength;

	ArrayType *minValueArray = NULL;
	ArrayType *maxValueArray = NULL;
	Var *partitionColumn = targetRelation->partitionColumn;
	int partitionColumnIndex = partitionColumn->varoattno - 1;
	Oid intervalTypeId = partitionColumn->vartype;
	int32 intervalTypeMod = partitionColumn->vartypmod;
	Oid intervalTypeOutFunc = InvalidOid;
	bool intervalTypeVarlena = false;
	getTypeOutputInfo(intervalTypeId, &intervalTypeOutFunc, &intervalTypeVarlena);

	ShardMinMaxValueArrays(shardIntervalArray, shardCount, intervalTypeOutFunc,
						   &minValueArray, &maxValueArray);
	StringInfo minValuesString = ArrayObjectToString(minValueArray, TEXTOID,
													 intervalTypeMod);
	StringInfo maxValuesString = ArrayObjectToString(maxValueArray, TEXTOID,
													 intervalTypeMod);

	foreach(taskCell, selectTaskList)
	{
		Task *selectTask = (Task *) lfirst(taskCell);
		StringInfo wrappedQuery = makeStringInfo();
		List *shardPlacementList = selectTask->taskPlacementList;

		ShardPlacement *shardPlacement = linitial(shardPlacementList);
		char *taskPrefix = SourceShardPrefix(resultIdPrefix, selectTask->anchorShardId);
		char *partitionMethodString = targetRelation->partitionMethod == 'h' ?
									  "hash" : "range";
		const char *binaryFormatString = binaryFormat ? "true" : "false";

		appendStringInfo(wrappedQuery,
						 "SELECT %d, partition_index"
						 ", %s || '_' || partition_index::text "
						 ", rows_written "
						 "FROM worker_partition_query_result"
						 "(%s,%s,%d,%s,%s,%s,%s) WHERE rows_written > 0",
						 shardPlacement->nodeId,
						 quote_literal_cstr(taskPrefix),
						 quote_literal_cstr(taskPrefix),
						 quote_literal_cstr(selectTask->queryString),
						 partitionColumnIndex,
						 quote_literal_cstr(partitionMethodString),
						 minValuesString->data, maxValuesString->data,
						 binaryFormatString);

		selectTask->queryString = wrappedQuery->data;
	}
}


/*
 * SourceShardPrefix returns result id prefix for partitions which have the
 * given anchor shard id.
 */
static char *
SourceShardPrefix(char *resultPrefix, uint64 shardId)
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
ExecutePartitionTaskList(List *taskList, DistTableCacheEntry *targetRelation)
{
	TupleDesc resultDescriptor = NULL;
	Tuplestorestate *resultStore = NULL;
	int resultColumnCount = 4;

#if PG_VERSION_NUM >= 120000
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

	resultStore = ExecuteSelectTasksIntoTupleStore(taskList, resultDescriptor);

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
								 DistTableCacheEntry *targetRelation)
{
	bool isNull = false;
	int sourceNodeId = DatumGetInt32(slot_getattr(tupleSlot, 1, &isNull));
	int targetShardIndex = DatumGetInt32(slot_getattr(tupleSlot, 2, &isNull));
	text *resultId = DatumGetTextP(slot_getattr(tupleSlot, 3, &isNull));
	int64 rowCount = DatumGetInt64(slot_getattr(tupleSlot, 4, &isNull));

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
ExecuteSelectTasksIntoTupleStore(List *taskList, TupleDesc resultDescriptor)
{
	bool hasReturning = true;
	int targetPoolSize = MaxAdaptiveExecutorPoolSize;
	bool randomAccess = true;
	bool interTransactions = false;
	TransactionProperties xactProperties = {
		.errorOnAnyFailure = true,
		.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_REQUIRED,
		.requires2PC = false
	};

	Tuplestorestate *resultStore = tuplestore_begin_heap(randomAccess, interTransactions,
														 work_mem);

	ExecuteTaskListExtended(ROW_MODIFY_READONLY, taskList, resultDescriptor,
							resultStore, hasReturning, targetPoolSize, &xactProperties);

	return resultStore;
}
