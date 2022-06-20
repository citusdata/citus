/*-------------------------------------------------------------------------
 *
 * worker_split_copy.c
 *	 API implementation for worker shard split copy.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include "c.h"
#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/relation_utils.h"
#include "distributed/worker_split_copy.h"
#include "distributed/worker_shard_copy.h"
#include "distributed/relay_utility.h"

typedef struct SplitCopyDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* Underlying shard copy dest receivers */
	DestReceiver **shardCopyDestReceiverArray;

	/* Split Copy Info */
	SplitCopyInfo **splitCopyInfoArray;

	/* Split factor */
	uint splitFactor;

	/* EState for per-tuple memory allocation */
	EState *executorState;

	/* Source shard Oid */
	Oid sourceShardRelationOid;
} SplitCopyDestReceiver;


static void SplitCopyDestReceiverStartup(DestReceiver *dest, int operation,
												 TupleDesc inputTupleDescriptor);
static bool SplitCopyDestReceiverReceive(TupleTableSlot *slot,
												 DestReceiver *dest);
static void SplitCopyDestReceiverShutdown(DestReceiver *dest);
static void SplitCopyDestReceiverDestroy(DestReceiver *copyDest);

DestReceiver * CreateSplitCopyDestReceiver(EState *executorState, uint64 sourceShardIdToCopy, List* splitCopyInfoList)
{
	SplitCopyDestReceiver *splitCopyDest =
		palloc0(sizeof(SplitCopyDestReceiver));

	/* set up the DestReceiver function pointers */
	splitCopyDest->pub.receiveSlot = SplitCopyDestReceiverReceive;
	splitCopyDest->pub.rStartup = SplitCopyDestReceiverStartup;
	splitCopyDest->pub.rShutdown = SplitCopyDestReceiverShutdown;
	splitCopyDest->pub.rDestroy = SplitCopyDestReceiverDestroy;

	splitCopyDest->executorState = executorState;
	splitCopyDest->splitFactor = splitCopyInfoList->length;
	ShardInterval *shardIntervalToSplitCopy = LoadShardInterval(sourceShardIdToCopy);
	splitCopyDest->sourceShardRelationOid = shardIntervalToSplitCopy->relationId;

	DestReceiver **shardCopyDests = palloc0(splitCopyDest->splitFactor * sizeof(DestReceiver *));
	SplitCopyInfo **splitCopyInfos = palloc0(splitCopyDest->splitFactor * sizeof(SplitCopyInfo *));

	SplitCopyInfo *splitCopyInfo = NULL;
	int index = 0;

	char *sourceShardNamePrefix = get_rel_name(shardIntervalToSplitCopy->relationId);
	foreach_ptr(splitCopyInfo, splitCopyInfoList)
	{
		char *destinationShardSchemaName = get_namespace_name(get_rel_namespace(splitCopyDest->sourceShardRelationOid));;
		char *destinationShardNameCopy = strdup(sourceShardNamePrefix);
		AppendShardIdToName(&destinationShardNameCopy, splitCopyInfo->destinationShardId);

		char *destinationShardFullyQualifiedName =
			quote_qualified_identifier(destinationShardSchemaName, destinationShardNameCopy);

		DestReceiver *shardCopyDest = CreateShardCopyDestReceiver(
			executorState,
			destinationShardFullyQualifiedName,
			splitCopyInfo->destinationShardNodeId);

		shardCopyDests[index] = shardCopyDest;
		splitCopyInfos[index] = splitCopyInfo;
		index++;
	}

	splitCopyDest->shardCopyDestReceiverArray = shardCopyDests;
	splitCopyDest->splitCopyInfoArray = splitCopyInfos;

	return (DestReceiver *) splitCopyDest;
}

static void SplitCopyDestReceiverStartup(DestReceiver *dest, int operation, TupleDesc inputTupleDescriptor)
{
	SplitCopyDestReceiver *copyDest = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < copyDest->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = copyDest->shardCopyDestReceiverArray[index];
		shardCopyDest->rStartup(shardCopyDest, operation, inputTupleDescriptor);
	}
}

static bool SplitCopyDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	SplitCopyDestReceiver *copyDest = (SplitCopyDestReceiver *) dest;

	/* Switch to a per-tuple memory memory context */
	EState *executorState = copyDest->executorState;
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(copyDest->sourceShardRelationOid);
	if (cacheEntry == NULL)
	{
		ereport(ERROR, errmsg("Could not find shard %s for split copy.",
							  get_rel_name(copyDest->sourceShardRelationOid)));
	}

	/* Partition Column Metadata on source shard */
	int partitionColumnIndex = cacheEntry->partitionColumn->varattno - 1;
	FmgrInfo *hashFunction = cacheEntry->hashFunction;

	slot_getallattrs(slot);
	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	/* Partition Column Value cannot be null */
	if (columnNulls[partitionColumnIndex])
	{
		ereport(ERROR, errmsg("Found null partition value for shard %s during split copy.",
							   get_rel_name(copyDest->sourceShardRelationOid)));
	}

	Datum hashedValueDatum = FunctionCall1(hashFunction, columnValues[partitionColumnIndex]);
	int32_t hashedValue = DatumGetInt32(hashedValueDatum);

	for(int index = 0 ; index < copyDest->splitFactor; index++)
	{
		SplitCopyInfo *splitCopyInfo = copyDest->splitCopyInfoArray[index];

		if (splitCopyInfo->destinationShardMinHashValue <= hashedValue &&
			splitCopyInfo->destinationShardMaxHashValue >= hashedValue)
		{
			DestReceiver *shardCopyDestReceiver = copyDest->shardCopyDestReceiverArray[index];
			shardCopyDestReceiver->receiveSlot(slot, shardCopyDestReceiver);
		}
	}

	MemoryContextSwitchTo(oldContext);
	ResetPerTupleExprContext(executorState);

	return true;
}

static void SplitCopyDestReceiverShutdown(DestReceiver *dest)
{
	SplitCopyDestReceiver *copyDest = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < copyDest->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = copyDest->shardCopyDestReceiverArray[index];
		shardCopyDest->rShutdown(shardCopyDest);
	}
}

static void SplitCopyDestReceiverDestroy(DestReceiver *dest)
{
	SplitCopyDestReceiver *copyDest = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < copyDest->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = copyDest->shardCopyDestReceiverArray[index];
		shardCopyDest->rDestroy(shardCopyDest);

		pfree(shardCopyDest);
		pfree(copyDest->splitCopyInfoArray[index]);
	}
}
