/*-------------------------------------------------------------------------
 *
 * pgoutput.c
 *		Logical Replication output plugin
 *
 * Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/distributed/shardsplit/pgoutput.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_type.h"
#include "distributed/multi_progress.h"
#include "distributed/worker_protocol.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/origin.h"
#include "replication/pgoutput.h"
#include "utils/inval.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/varlena.h"
#include "distributed/shard_split.h"
#include "distributed/shardsplit_shared_memory.h"
#include "citus_version.h"

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
static LogicalDecodeChangeCB pgoutputChangeCB;
ShardSplitInfo *shardSplitInfoArray = NULL;
int shardSplitInfoArraySize = 0;


/* Plugin callback */
static void split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							Relation relation, ReorderBufferChange *change);

/* Helper methods */
static bool ShouldCommitBeApplied(Relation sourceShardRelation);
static int32_t GetHashValueForIncomingTuple(Relation sourceShardRelation,
											HeapTuple tuple,
											bool *shouldHandleUpdate);

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	char *plugin = "pgoutput";

	LogicalOutputPluginInit plugin_init =
		(LogicalOutputPluginInit) load_external_function(plugin,
														 "_PG_output_plugin_init",
														 false, NULL);

	if (plugin_init == NULL)
	{
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");
	}

	/* ask the output plugin to fill the callback struct */
	plugin_init(cb);

	pgoutputChangeCB = cb->change_cb;

	cb->change_cb = split_change_cb;
}


/*
 * GetHashValueForIncomingTuple returns the hash value of the partition
 * column for the incoming tuple. It also checks if the change should be
 * handled as the incoming committed change would belong to a relation
 * that is not under going split.
 */
static int32_t
GetHashValueForIncomingTuple(Relation sourceShardRelation,
							 HeapTuple tuple,
							 bool *shouldHandleChange)
{
	ShardSplitInfo *shardSplitInfo = NULL;
	int partitionColumnIndex = -1;
	Oid distributedTableOid = InvalidOid;

	Oid sourceShardOid = sourceShardRelation->rd_id;
	for (int i = 0; i < shardSplitInfoArraySize; i++)
	{
		shardSplitInfo = &shardSplitInfoArray[i];
		if (shardSplitInfo->sourceShardOid == sourceShardOid)
		{
			distributedTableOid = shardSplitInfo->distributedTableOid;
			partitionColumnIndex = shardSplitInfo->partitionColumnIndex;
			break;
		}
	}

	/*
	 * The commit can belong to any other table that is not going
	 * under split. Ignore such commit's.
	 */
	if (partitionColumnIndex == -1 ||
		distributedTableOid == InvalidOid)
	{
		/*
		 * TODO(saawasek): Change below warning to DEBUG once more test case
		 * are added.
		 */
		ereport(WARNING, errmsg("Skipping Commit as "
								"Relation: %s isn't splitting",
								RelationGetRelationName(sourceShardRelation)));
		*shouldHandleChange = false;
		return 0;
	}

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableOid);
	if (cacheEntry == NULL)
	{
		ereport(ERROR, errmsg("null entry found for cacheEntry"));
	}

	TupleDesc relationTupleDes = RelationGetDescr(sourceShardRelation);
	bool isNull = false;
	Datum partitionColumnValue = heap_getattr(tuple,
											  partitionColumnIndex + 1,
											  relationTupleDes,
											  &isNull);

	FmgrInfo *hashFunction = cacheEntry->hashFunction;

	/* get hashed value of the distribution value */
	Datum hashedValueDatum = FunctionCall1(hashFunction, partitionColumnValue);
	int32_t hashedValue = DatumGetInt32(hashedValueDatum);

	*shouldHandleChange = true;

	return hashedValue;
}


/*
 * FindTargetRelationOid returns the destination relation Oid for the incoming
 * tuple.
 * sourceShardRelation - Relation on which a commit has happened.
 * tuple               - changed tuple.
 * currentSlotName     - Name of replication slot that is processing this update.
 */
Oid
FindTargetRelationOid(Relation sourceShardRelation,
					  HeapTuple tuple,
					  char *currentSlotName)
{
	Oid targetRelationOid = InvalidOid;
	Oid sourceShardRelationOid = sourceShardRelation->rd_id;

	bool bShouldHandleUpdate = false;
	int hashValue = GetHashValueForIncomingTuple(sourceShardRelation, tuple,
												 &bShouldHandleUpdate);
	if (bShouldHandleUpdate == false)
	{
		return InvalidOid;
	}

	for (int i = 0; i < shardSplitInfoArraySize; i++)
	{
		ShardSplitInfo *shardSplitInfo = &shardSplitInfoArray[i];

		/*
		 * Each commit message is processed by all the configured
		 * replication slots. However, a replication is slot only responsible
		 * for new shard placements belonging to a single node. We check if the
		 * current slot which is processing the commit should emit
		 * a target relation Oid.
		 */
		if (strcmp(shardSplitInfo->slotName, currentSlotName) == 0 &&
			shardSplitInfo->sourceShardOid == sourceShardRelationOid &&
			shardSplitInfo->shardMinValue <= hashValue &&
			shardSplitInfo->shardMaxValue >= hashValue)
		{
			targetRelationOid = shardSplitInfo->splitChildShardOid;
			break;
		}
	}

	return targetRelationOid;
}


/*
 * ShouldCommitBeApplied avoids recursive commit case when source shard and
 * new split child shards are placed on the same node. When the source shard
 * recives a commit(1), the WAL sender processes this commit message. This
 * commit is applied to a child shard which is placed on the same node as a
 * part of replication. This in turn creates one more commit(2).
 * Commit 2 should be skipped as the source shard and destination for commit 2
 * are same and the commit has already been applied.
 *
 * TODO(saawasek): Add the information in Hashmap for performance reasons.
 */
bool
ShouldCommitBeApplied(Relation sourceShardRelation)
{
	ShardSplitInfo *shardSplitInfo = NULL;
	int partitionColumnIndex = -1;
	Oid distributedTableOid = InvalidOid;

	Oid sourceShardOid = sourceShardRelation->rd_id;
	for (int i = 0; i < shardSplitInfoArraySize; i++)
	{
		/* skip the commit when destination is equal to the source */
		shardSplitInfo = &shardSplitInfoArray[i];
		if (shardSplitInfo->splitChildShardOid == sourceShardOid)
		{
			return false;
		}
	}

	return true;
}


/*
 * split_change function emits the incoming tuple change
 * to the appropriate destination shard.
 */
static void
split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	/*
	 * Get ShardSplitInfo array from Shared Memory if not already
	 * initialized. This gets initialized during the replication of
	 * first message.
	 */
	int arraySize = 0;
	if (shardSplitInfoArray == NULL)
	{
		shardSplitInfoArray =
			GetShardSplitInfoSMArrayForSlot(ctx->slot->data.name.data,
											&shardSplitInfoArraySize);
	}

	char *replicationSlotName = ctx->slot->data.name.data;
	if (!ShouldCommitBeApplied(relation))
	{
		return;
	}

	Oid targetRelationOid = InvalidOid;
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
		{
			HeapTuple newTuple = &(change->data.tp.newtuple->tuple);
			targetRelationOid = FindTargetRelationOid(relation, newTuple,
													  replicationSlotName);
			break;
		}

		case REORDER_BUFFER_CHANGE_DELETE:
		{
			HeapTuple oldTuple = &(change->data.tp.oldtuple->tuple);
			targetRelationOid = FindTargetRelationOid(relation, oldTuple,
													  replicationSlotName);

			break;
		}

		/* Only INSERT/DELETE are visible in the replication path of split shard */
		default:
			Assert(false);
	}

	/* Current replication slot is not responsible for handling the change */
	if (targetRelationOid == InvalidOid)
	{
		return;
	}

	Relation targetRelation = RelationIdGetRelation(targetRelationOid);
	pgoutputChangeCB(ctx, txn, targetRelation, change);
	RelationClose(targetRelation);
}
