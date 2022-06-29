/*-------------------------------------------------------------------------
 *
 * shardsplit_decoder.c
 *		Logical Replication output plugin
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/listutils.h"
#include "replication/logical.h"

/*
 * Dynamically-loaded modules are required to include this macro call to check for
 * incompatibility (such as being compiled for a different major PostgreSQL version etc).
 * In a multiple source-file module, the macro call should only appear once.
 */
PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
static LogicalDecodeChangeCB pgoutputChangeCB;

static HTAB *SourceToDestinationShardMap = NULL;

/* Plugin callback */
static void split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							Relation relation, ReorderBufferChange *change);

/* Helper methods */
static int32_t GetHashValueForIncomingTuple(Relation sourceShardRelation,
											HeapTuple tuple,
											int partitionColumIndex,
											Oid distributedTableOid);

static Oid FindTargetRelationOid(Relation sourceShardRelation,
								 HeapTuple tuple,
								 char *currentSlotName);

/*
 * Postgres uses 'pgoutput' as default plugin for logical replication.
 * We want to reuse Postgres pgoutput's functionality as much as possible.
 * Hence we load all the functions of this plugin and override as required.
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	LogicalOutputPluginInit plugin_init =
		(LogicalOutputPluginInit) (void *) load_external_function("pgoutput",
																  "_PG_output_plugin_init",
																  false, NULL);

	if (plugin_init == NULL)
	{
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");
	}

	/* ask the output plugin to fill the callback struct */
	plugin_init(cb);

	/* actual pgoutput callback will be called with the appropriate destination shard */
	pgoutputChangeCB = cb->change_cb;
	cb->change_cb = split_change_cb;
}


/*
 * split_change function emits the incoming tuple change
 * to the appropriate destination shard.
 */
static void
split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	if (!is_publishable_relation(relation))
	{
		return;
	}

	char *replicationSlotName = ctx->slot->data.name.data;

	/*
	 * Initialize SourceToDestinationShardMap if not already initialized.
	 * This gets initialized during the replication of first message.
	 */
	if (SourceToDestinationShardMap == NULL)
	{
		SourceToDestinationShardMap = PopulateSourceToDestinationShardMapForSlot(
			replicationSlotName, TopMemoryContext);
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

		/* updating non-partition column value */
		case REORDER_BUFFER_CHANGE_UPDATE:
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

		/* Only INSERT/DELETE/UPDATE actions are visible in the replication path of split shard */
		default:
			ereport(ERROR, errmsg(
						"Unexpected Action :%d. Expected action is INSERT/DELETE/UPDATE",
						change->action));
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


/*
 * FindTargetRelationOid returns the destination relation Oid for the incoming
 * tuple.
 * sourceShardRelation - Relation on which a commit has happened.
 * tuple               - changed tuple.
 * currentSlotName     - Name of replication slot that is processing this update.
 */
static Oid
FindTargetRelationOid(Relation sourceShardRelation,
					  HeapTuple tuple,
					  char *currentSlotName)
{
	Oid targetRelationOid = InvalidOid;
	Oid sourceShardRelationOid = sourceShardRelation->rd_id;

	/* Get child shard list for source(parent) shard from hashmap*/
	bool found = false;
	SourceToDestinationShardMapEntry *entry =
		(SourceToDestinationShardMapEntry *) hash_search(
			SourceToDestinationShardMap, &sourceShardRelationOid, HASH_FIND, &found);

	/*
	 * Source shard Oid might not exist in the hash map. This can happen
	 * in below cases:
	 * 1) The commit can belong to any other table that is not under going split.
	 * 2) The commit can be recursive in nature. When the source shard
	 * receives a commit(a), the WAL sender processes this commit message. This
	 * commit is applied to a child shard which is placed on the same node as a
	 * part of replication. This in turn creates one more commit(b) which is recursive in nature.
	 * Commit 'b' should be skipped as the source shard and destination for commit 'b'
	 * are same and the commit has already been applied.
	 */
	if (!found)
	{
		return InvalidOid;
	}

	ShardSplitInfo *shardSplitInfo = (ShardSplitInfo *) lfirst(list_head(
																   entry->
																   shardSplitInfoList));
	int hashValue = GetHashValueForIncomingTuple(sourceShardRelation, tuple,
												 shardSplitInfo->partitionColumnIndex,
												 shardSplitInfo->distributedTableOid);

	shardSplitInfo = NULL;
	foreach_ptr(shardSplitInfo, entry->shardSplitInfoList)
	{
		if (shardSplitInfo->shardMinValue <= hashValue &&
			shardSplitInfo->shardMaxValue >= hashValue)
		{
			targetRelationOid = shardSplitInfo->splitChildShardOid;
			break;
		}
	}

	return targetRelationOid;
}


/*
 * GetHashValueForIncomingTuple returns the hash value of the partition
 * column for the incoming tuple.
 */
static int32_t
GetHashValueForIncomingTuple(Relation sourceShardRelation,
							 HeapTuple tuple,
							 int partitionColumnIndex,
							 Oid distributedTableOid)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableOid);
	if (cacheEntry == NULL)
	{
		ereport(ERROR, errmsg(
					"Expected valid Citus Cache entry to be present. But found null"));
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

	return DatumGetInt32(hashedValueDatum);
}
