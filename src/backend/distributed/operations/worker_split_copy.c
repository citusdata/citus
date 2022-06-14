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
#include "distributed/metadata_cache.h"
#include "distributed/worker_split_copy.h"

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

	/* Source shard name */
	FullRelationName *sourceShardName;

	/* Source shard Oid */
	Oid sourceShardOid;
} SplitCopyDestReceiver;


static void SplitCopyDestReceiverStartup(DestReceiver *dest, int operation,
												 TupleDesc inputTupleDescriptor);
static bool SplitCopyDestReceiverReceive(TupleTableSlot *slot,
												 DestReceiver *dest);
static void SplitCopyDestReceiverShutdown(DestReceiver *dest);
static void SplitCopyDestReceiverDestroy(DestReceiver *copyDest);

DestReceiver * CreateSplitCopyDestReceiver(FullRelationName *sourceShard, List* splitCopyInfoList)
{
	SplitCopyDestReceiver *resultDest =
		palloc0(sizeof(SplitCopyDestReceiver));

	/* set up the DestReceiver function pointers */
	resultDest->pub.receiveSlot = SplitCopyDestReceiverReceive;
	resultDest->pub.rStartup = SplitCopyDestReceiverStartup;
	resultDest->pub.rShutdown = SplitCopyDestReceiverShutdown;
	resultDest->pub.rDestroy = SplitCopyDestReceiverDestroy;

	Oid sourceSchemaOid = get_namespace_oid(sourceShard->schemaName, false /* missing_ok */);
	Oid sourceShardOid = get_relname_relid(sourceShard->relationName, sourceSchemaOid);
	resultDest->sourceShardOid = sourceShardOid;

	// TODO(niupre): Create internal destination receivers for each shard.
	for (int index = 0; index < splitCopyInfoList->length; index++)
	{

	}

	resultDest->splitFactor = splitCopyInfoList->length;

	return (DestReceiver *) resultDest;
}

static void SplitCopyDestReceiverStartup(DestReceiver *dest, int operation, TupleDesc inputTupleDescriptor)
{
	SplitCopyDestReceiver *self = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < self->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = self->shardCopyDestReceiverArray[index];
		shardCopyDest->rStartup(shardCopyDest, operation, inputTupleDescriptor);
	}
}

static bool SplitCopyDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	SplitCopyDestReceiver *self = (SplitCopyDestReceiver *) dest;

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(self->sourceShardOid);
	if (cacheEntry == NULL)
	{
		ereport(ERROR, errmsg("Could not find shard %s for split copy.",
							  self->sourceShardName->relationName));
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
							   self->sourceShardName->relationName));
	}

	Datum hashedValueDatum = FunctionCall1(hashFunction, columnValues[partitionColumnIndex]);
	int32_t hashedValue = DatumGetInt32(hashedValueDatum);

	for(int index = 0 ; index < self->splitFactor; index++)
	{
		SplitCopyInfo *splitCopyInfo = self->splitCopyInfoArray[index];

		if (splitCopyInfo->shardMinValue <= hashedValue &&
			splitCopyInfo->shardMaxValue >= hashedValue)
		{
			DestReceiver *shardCopyDestReceiver = self->shardCopyDestReceiverArray[index];
			shardCopyDestReceiver->receiveSlot(slot, shardCopyDestReceiver);
		}
	}

	return true;
}

static void SplitCopyDestReceiverShutdown(DestReceiver *dest)
{
	SplitCopyDestReceiver *self = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < self->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = self->shardCopyDestReceiverArray[index];
		shardCopyDest->rShutdown(shardCopyDest);
	}
}

static void SplitCopyDestReceiverDestroy(DestReceiver *dest)
{
	SplitCopyDestReceiver *self = (SplitCopyDestReceiver *) dest;

	for (int index = 0; index < self->splitFactor; index++)
	{
		DestReceiver *shardCopyDest = self->shardCopyDestReceiverArray[index];
		shardCopyDest->rDestroy(shardCopyDest);
	}
}
