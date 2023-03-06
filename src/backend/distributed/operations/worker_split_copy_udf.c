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
#include "distributed/citus_ruleutils.h"
#include "distributed/distribution_column.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_shard_copy.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/builtins.h"

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
															  char *partitionColumnName,
															  List *splitCopyInfoList);
static void BuildMinMaxRangeArrays(List *splitCopyInfoList, ArrayType **minValueArray,
								   ArrayType **maxValueArray);
static char * TraceWorkerSplitCopyUdf(char *sourceShardToCopySchemaName,
									  char *sourceShardToCopyPrefix,
									  char *sourceShardToCopyQualifiedName,
									  List *splitCopyInfoList);

/*
 * worker_split_copy(source_shard_id bigint, splitCopyInfo pg_catalog.split_copy_info[])
 * UDF to split copy shard to list of destination shards.
 * 'source_shard_id' : Source ShardId to split copy.
 * 'splitCopyInfos'   : Array of Split Copy Info (destination_shard's id, min/max ranges and node_id)
 */
Datum
worker_split_copy(PG_FUNCTION_ARGS)
{
	uint64 shardIdToSplitCopy = DatumGetUInt64(PG_GETARG_DATUM(0));
	ShardInterval *shardIntervalToSplitCopy = LoadShardInterval(shardIdToSplitCopy);

	text *partitionColumnText = PG_GETARG_TEXT_P(1);
	char *partitionColumnName = text_to_cstring(partitionColumnText);

	ArrayType *splitCopyInfoArrayObject = PG_GETARG_ARRAYTYPE_P(2);
	bool arrayHasNull = ARR_HASNULL(splitCopyInfoArrayObject);
	if (arrayHasNull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg(
							"pg_catalog.split_copy_info array cannot contain null values")));
	}

	const int slice_ndim = 0;
	ArrayMetaState *mState = NULL;
	ArrayIterator copyInfo_iterator = array_create_iterator(splitCopyInfoArrayObject,
															slice_ndim,
															mState);
	Datum copyInfoDatum = 0;
	bool isnull = false;
	List *splitCopyInfoList = NIL;
	while (array_iterate(copyInfo_iterator, &copyInfoDatum, &isnull))
	{
		SplitCopyInfo *splitCopyInfo = NULL;
		ParseSplitCopyInfoDatum(copyInfoDatum, &splitCopyInfo);

		splitCopyInfoList = lappend(splitCopyInfoList, splitCopyInfo);
	}

	EState *executor = CreateExecutorState();
	DestReceiver *splitCopyDestReceiver = CreatePartitionedSplitCopyDestReceiver(executor,
																				 shardIntervalToSplitCopy,
																				 partitionColumnName,
																				 splitCopyInfoList);

	Oid sourceShardToCopySchemaOId = get_rel_namespace(
		shardIntervalToSplitCopy->relationId);
	char *sourceShardToCopySchemaName = get_namespace_name(sourceShardToCopySchemaOId);
	char *sourceShardPrefix = get_rel_name(shardIntervalToSplitCopy->relationId);
	char *sourceShardToCopyName = pstrdup(sourceShardPrefix);
	AppendShardIdToName(&sourceShardToCopyName, shardIdToSplitCopy);
	char *sourceShardToCopyQualifiedName = quote_qualified_identifier(
		sourceShardToCopySchemaName,
		sourceShardToCopyName);

	ereport(LOG, (errmsg("%s", TraceWorkerSplitCopyUdf(sourceShardToCopySchemaName,
													   sourceShardPrefix,
													   sourceShardToCopyQualifiedName,
													   splitCopyInfoList))));

	StringInfo selectShardQueryForCopy = makeStringInfo();
	const char *columnList = CopyableColumnNamesFromRelationName(
		sourceShardToCopySchemaName,
		sourceShardToCopyName);

	appendStringInfo(selectShardQueryForCopy,
					 "SELECT %s FROM %s;", columnList,
					 sourceShardToCopyQualifiedName);

	ParamListInfo params = NULL;
	ExecuteQueryStringIntoDestReceiver(selectShardQueryForCopy->data, params,
									   (DestReceiver *) splitCopyDestReceiver);

	FreeExecutorState(executor);

	PG_RETURN_VOID();
}


/* Trace split copy udf */
static char *
TraceWorkerSplitCopyUdf(char *sourceShardToCopySchemaName,
						char *sourceShardToCopyPrefix,
						char *sourceShardToCopyQualifiedName,
						List *splitCopyInfoList)
{
	StringInfo splitCopyTrace = makeStringInfo();
	appendStringInfo(splitCopyTrace, "performing copy from shard %s to [",
					 sourceShardToCopyQualifiedName);

	/* split copy always has atleast two destinations */
	int index = 1;
	int splitWayCount = list_length(splitCopyInfoList);
	SplitCopyInfo *splitCopyInfo = NULL;
	foreach_ptr(splitCopyInfo, splitCopyInfoList)
	{
		char *shardNameCopy = pstrdup(sourceShardToCopyPrefix);
		AppendShardIdToName(&shardNameCopy, splitCopyInfo->destinationShardId);

		char *shardNameCopyQualifiedName = quote_qualified_identifier(
			sourceShardToCopySchemaName,
			shardNameCopy);

		appendStringInfo(splitCopyTrace, "%s (nodeId: %u)", shardNameCopyQualifiedName,
						 splitCopyInfo->destinationShardNodeId);
		pfree(shardNameCopy);

		if (index < splitWayCount)
		{
			appendStringInfo(splitCopyTrace, ", ");
		}

		index++;
	}

	appendStringInfo(splitCopyTrace, "]");

	return splitCopyTrace->data;
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
							"destination_shard_id for pg_catalog.split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardId = DatumGetUInt64(destinationShardIdDatum);

	Datum minValueDatum = GetAttributeByName(dataTuple, "destination_shard_min_value",
											 &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_min_value for pg_catalog.split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardMinHashValue = minValueDatum;

	Datum maxValueDatum = GetAttributeByName(dataTuple, "destination_shard_max_value",
											 &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_max_value for pg_catalog.split_copy_info cannot be null.")));
	}
	copyInfo->destinationShardMaxHashValue = maxValueDatum;

	Datum nodeIdDatum = GetAttributeByName(dataTuple, "destination_shard_node_id",
										   &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_node_id for pg_catalog.split_copy_info cannot be null.")));
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
		Oid destinationShardSchemaOid = get_rel_namespace(
			shardIntervalToSplitCopy->relationId);
		char *destinationShardSchemaName = get_namespace_name(destinationShardSchemaOid);
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
									   char *partitionColumnName,
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

	/* we currently only support hash-distribution */
	char partitionMethod = DISTRIBUTE_BY_HASH;

	/* synthetically build the partition column by looking at shard columns */
	uint64 shardId = shardIntervalToSplitCopy->shardId;
	bool missingOK = false;
	Oid shardRelationId = LookupShardRelationFromCatalog(shardId, missingOK);
	Var *partitionColumn = BuildDistributionKeyFromColumnName(shardRelationId,
															  partitionColumnName,
															  AccessShareLock);

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
