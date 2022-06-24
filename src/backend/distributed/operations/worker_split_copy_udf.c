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
#include "utils/array.h"
#include "utils/builtins.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_split_copy.h"
#include "distributed/citus_ruleutils.h"

PG_FUNCTION_INFO_V1(worker_split_copy);

static void ParseSplitCopyInfoDatum(Datum splitCopyInfoDatum,
									SplitCopyInfo **splitCopyInfo);

/*
 *
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
	DestReceiver *splitCopyDestReceiver = CreateSplitCopyDestReceiver(executor,
																	  shardIdToSplitCopy,
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
	char *destinationMinHash = text_to_cstring(DatumGetTextP(minValueDatum));
	copyInfo->destinationShardMinHashValue = pg_strtoint32(destinationMinHash);

	Datum maxValueDatum = GetAttributeByName(dataTuple, "destination_shard_max_value",
											 &isnull);
	if (isnull)
	{
		ereport(ERROR, (errmsg(
							"destination_shard_max_value for split_copy_info cannot be null.")));
	}
	char *destinationMaxHash = text_to_cstring(DatumGetTextP(maxValueDatum));
	copyInfo->destinationShardMaxHashValue = pg_strtoint32(destinationMaxHash);

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
