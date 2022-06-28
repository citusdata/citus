/*-------------------------------------------------------------------------
 *
 * worker_split_copy_udf.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pg_version_compat.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_shard_copy.h"
#include "distributed/worker_split_copy.h"
#include "distributed/intermediate_results.h"
#include "distributed/citus_ruleutils.h"

PG_FUNCTION_INFO_V1(worker_split_copy);

typedef struct SplitCopyInfo
{
	uint64 destinationShardId;              /* destination shard id */
	Datum destinationShardMinHashValue;     /* min hash value of destination shard */
	Datum destinationShardMaxHashValue;     /* max hash value of destination shard */
	uint32_t destinationShardNodeId;        /* node where split child shard is to be placed */
} SplitCopyInfo;

static void ParseSplitCopyInfoDatum(Datum splitCopyInfoDatum,
									SplitCopyInfo **splitCopyInfo);
static DestReceiver ** CreateShardCopyDestReceivers(EState *estate,
													ShardInterval *
													shardIntervalToSplitCopy,
													List *splitCopyInfoList);
static DestReceiver *  CreatePartitionedSplitCopyDestReceiver(EState *executor,
															  ShardInterval *
															  shardIntervalToSplitCopy,
															  List *splitCopyInfoList);
static void BuildMinMaxRangeArrays(List *splitCopyInfoList, ArrayType **minValueArray,
								   ArrayType **maxValueArray);

/*
 * worker_split_copy(source_shard_id bigint, splitCopyInfo citus.split_copy_info[])
 * UDF to split copy shard to list of destination shards.
 * 'source_shard_id' : Source ShardId to split copy.
 * 'splitCopyInfos'   : Array of Split Copy Info (destination_shard's id, min/max ranges and node_id)
 */
Datum
worker_split_copy(PG_FUNCTION_ARGS)
{
	uint64 shardIdToSplitCopy = DatumGetUInt64(PG_GETARG_DATUM(0));
	ShardInterval *shardIntervalToSplitCopy = LoadShardInterval(shardIdToSplitCopy);

	ArrayType *splitCopyInfoArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	if (array_contains_nulls(splitCopyInfoArrayObject))
	{
		ereport(ERROR,
				(errmsg("Shard Copy Info cannot have null values.")));
	}

	ArrayIterator copyInfo_iterator = array_create_iterator(splitCopyInfoArrayObject,
															0 /* slice_ndim */,
															NULL /* mState */);
	Datum copyInfoDatum = 0;
	bool isnull = false;
	List *splitCopyInfoList = NULL;
	while (array_iterate(copyInfo_iterator, &copyInfoDatum, &isnull))
	{
		SplitCopyInfo *splitCopyInfo = NULL;
		ParseSplitCopyInfoDatum(copyInfoDatum, &splitCopyInfo);

		splitCopyInfoList = lappend(splitCopyInfoList, splitCopyInfo);
	}

	EState *executor = CreateExecutorState();
	DestReceiver *splitCopyDestReceiver = CreatePartitionedSplitCopyDestReceiver(executor,
																				 shardIntervalToSplitCopy,
																				 splitCopyInfoList);

	char *sourceShardToCopyName = generate_qualified_relation_name(
		shardIntervalToSplitCopy->relationId);
	AppendShardIdToName(&sourceShardToCopyName, shardIdToSplitCopy);

	StringInfo selectShardQueryForCopy = makeStringInfo();
	appendStringInfo(selectShardQueryForCopy,
					 "SELECT * FROM %s;", sourceShardToCopyName);

	ParamListInfo params = NULL;
	ExecuteQueryStringIntoDestReceiver(selectShardQueryForCopy->data, params,
									   (DestReceiver *) splitCopyDestReceiver);

	FreeExecutorState(executor);

	PG_RETURN_VOID();
}


/* Parse a single SplitCopyInfo Tuple */
static void
ParseSplitCopyInfoDatum(Datum splitCopyInfoDatum, SplitCopyInfo **splitCopyInfo)
{
	HeapTupleHeader dataTuple = DatumGetHeapTupleHeader(splitCopyInfoDatum);

	SplitCopyInfo *copyInfo = palloc0(sizeof(SplitCopyInfo));

	bool isnull = false;
	Datum destinationShardIdDatum = GetAttributeByName(dataTuple, "destination_shard_id",
													   &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_id for split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardId = DatumGetUInt64(destinationShardIdDatum);

	Datum minValueDatum = GetAttributeByName(dataTuple, "destination_shard_min_value",
											 &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_min_value for split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardMinHashValue = minValueDatum;

	Datum maxValueDatum = GetAttributeByName(dataTuple, "destination_shard_max_value",
											 &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_max_value for split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardMaxHashValue = maxValueDatum;

	Datum nodeIdDatum = GetAttributeByName(dataTuple, "destination_shard_node_id",
										   &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_node_id for split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardNodeId = DatumGetInt32(nodeIdDatum);

	*splitCopyInfo = copyInfo;
}


/* Build 'min/max' hash range arrays for PartitionedResultDestReceiver */
static void
BuildMinMaxRangeArrays(List *splitCopyInfoList, ArrayType **minValueArray,
					   ArrayType **maxValueArray)
{
	int partitionCount = list_length(splitCopyInfoList);

	Datum *minValues = palloc0(partitionCount * sizeof(Datum));
	bool *minValueNulls = palloc0(partitionCount * sizeof(bool));
	Datum *maxValues = palloc0(partitionCount * sizeof(Datum));
	bool *maxValueNulls = palloc0(partitionCount * sizeof(bool));

	SplitCopyInfo *splitCopyInfo = NULL;
	int index = 0;
	foreach_ptr(splitCopyInfo, splitCopyInfoList)
	{
		minValues[index] = splitCopyInfo->destinationShardMinHashValue;
		maxValues[index] = splitCopyInfo->destinationShardMaxHashValue;

		/* Caller enforces that min/max values will be not-null */
		minValueNulls[index] = false;
		maxValueNulls[index] = false;
		index++;
	}

	*minValueArray = CreateArrayFromDatums(minValues, minValueNulls, partitionCount,
										   TEXTOID);
	*maxValueArray = CreateArrayFromDatums(maxValues, maxValueNulls, partitionCount,
										   TEXTOID);
}


/*
 * Create underlying ShardCopyDestReceivers for PartitionedResultDestReceiver
 * Each ShardCopyDestReceivers will be responsible for copying tuples from source shard,
 * that fall under its min/max range, to specified destination shard.
 */
static DestReceiver **
CreateShardCopyDestReceivers(EState *estate, ShardInterval *shardIntervalToSplitCopy,
							 List *splitCopyInfoList)
{
	DestReceiver **shardCopyDests = palloc0(splitCopyInfoList->length *
											sizeof(DestReceiver *));

	SplitCopyInfo *splitCopyInfo = NULL;
	int index = 0;
	char *sourceShardNamePrefix = get_rel_name(shardIntervalToSplitCopy->relationId);
	foreach_ptr(splitCopyInfo, splitCopyInfoList)
	{
		char *destinationShardSchemaName = get_namespace_name(get_rel_namespace(
																  shardIntervalToSplitCopy
																  ->relationId));
		char *destinationShardNameCopy = pstrdup(sourceShardNamePrefix);
		AppendShardIdToName(&destinationShardNameCopy, splitCopyInfo->destinationShardId);

		DestReceiver *shardCopyDest = CreateShardCopyDestReceiver(
			estate,
			list_make2(destinationShardSchemaName, destinationShardNameCopy),
			splitCopyInfo->destinationShardNodeId);

		shardCopyDests[index] = shardCopyDest;
		index++;
	}

	return shardCopyDests;
}


/* Create PartitionedSplitCopyDestReceiver along with underlying ShardCopyDestReceivers */
static DestReceiver *
CreatePartitionedSplitCopyDestReceiver(EState *estate,
									   ShardInterval *shardIntervalToSplitCopy,
									   List *splitCopyInfoList)
{
	/* Create underlying ShardCopyDestReceivers */
	DestReceiver **shardCopyDestReceivers = CreateShardCopyDestReceivers(
		estate,
		shardIntervalToSplitCopy,
		splitCopyInfoList);

	/* construct an artificial CitusTableCacheEntry for routing tuples to appropriate ShardCopyReceiver */
	ArrayType *minValuesArray = NULL;
	ArrayType *maxValuesArray = NULL;
	BuildMinMaxRangeArrays(splitCopyInfoList, &minValuesArray, &maxValuesArray);
	char partitionMethod = PartitionMethodViaCatalog(
		shardIntervalToSplitCopy->relationId);
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(
		shardIntervalToSplitCopy->relationId);
	Var *partitionColumn = cacheEntry->partitionColumn;

	CitusTableCacheEntry *shardSearchInfo =
		QueryTupleShardSearchInfo(minValuesArray, maxValuesArray,
								  partitionMethod, partitionColumn);

	/* Construct PartitionedResultDestReceiver from cache and underlying ShardCopyDestReceivers */
	int partitionColumnIndex = partitionColumn->varattno - 1;
	int partitionCount = splitCopyInfoList->length;
	DestReceiver *splitCopyDestReceiver = CreatePartitionedResultDestReceiver(
		partitionColumnIndex,
		partitionCount,
		shardSearchInfo,
		shardCopyDestReceivers,
		true /* lazyStartup */,
		false /* allowNullPartitionColumnValues */);

	return splitCopyDestReceiver;
}
