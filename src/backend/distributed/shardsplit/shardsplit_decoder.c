/*-------------------------------------------------------------------------
 *
 * pgoutput.c
 *		Logical Replication output plugin
 *
 * Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/distributed/shardsplit/shardsplit_decoder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "replication/logical.h"

/*
 * Dynamically-loaded modules are required to include this macro call to check for
 * incompatibility (such as being compiled for a different major PostgreSQL version etc).
 * In a multiple source-file module, the macro call should only appear once.
 */
PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
static LogicalDecodeChangeCB pgoutputChangeCB;

static ShardSplitInfoSMHeader *ShardSplitInfo_SMHeader = NULL;
static ShardSplitInfoForReplicationSlot *ShardSplitInfoForSlot = NULL;

/* Plugin callback */
static void split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							Relation relation, ReorderBufferChange *change);

/* Helper methods */
static bool IsCommitRecursive(Relation sourceShardRelation);
static int32_t GetHashValueForIncomingTuple(Relation sourceShardRelation,
											HeapTuple tuple,
											bool *shouldHandleUpdate);

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
 * GetHashValueForIncomingTuple returns the hash value of the partition
 * column for the incoming tuple. It also checks if the change should be
 * handled as the incoming committed change can belong to a relation
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
	for (int i = ShardSplitInfoForSlot->startIndex; i <= ShardSplitInfoForSlot->endIndex;
		 i++)
	{
		shardSplitInfo = &ShardSplitInfo_SMHeader->splitInfoArray[i];
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
static Oid
FindTargetRelationOid(Relation sourceShardRelation,
					  HeapTuple tuple,
					  char *currentSlotName)
{
	Oid targetRelationOid = InvalidOid;
	Oid sourceShardRelationOid = sourceShardRelation->rd_id;

	bool shouldHandleUpdate = false;
	int hashValue = GetHashValueForIncomingTuple(sourceShardRelation, tuple,
												 &shouldHandleUpdate);
	if (shouldHandleUpdate == false)
	{
		return InvalidOid;
	}

	for (int i = ShardSplitInfoForSlot->startIndex; i <= ShardSplitInfoForSlot->endIndex;
		 i++)
	{
		ShardSplitInfo *shardSplitInfo = &ShardSplitInfo_SMHeader->splitInfoArray[i];

		/*
		 * Each commit message is processed by all the configured replication slots.
		 * A replication slot is responsible for shard placements belonging to unique
		 * table owner and nodeId combination. We check if the current slot which is
		 * processing the commit should emit a target relation Oid.
		 */
		if (shardSplitInfo->sourceShardOid == sourceShardRelationOid &&
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
 * IsCommitRecursive returns true when commit is recursive. When the source shard
 * recives a commit(1), the WAL sender processes this commit message. This
 * commit is applied to a child shard which is placed on the same node as a
 * part of replication. This in turn creates one more commit(2) which is recursive in nature.
 * Commit 2 should be skipped as the source shard and destination for commit 2
 * are same and the commit has already been applied.
 */
bool
IsCommitRecursive(Relation sourceShardRelation)
{
	Oid sourceShardOid = sourceShardRelation->rd_id;
	for (int i = ShardSplitInfoForSlot->startIndex; i <= ShardSplitInfoForSlot->endIndex;
		 i++)
	{
		/* skip the commit when destination is equal to the source */
		ShardSplitInfo *shardSplitInfo = &ShardSplitInfo_SMHeader->splitInfoArray[i];
		if (sourceShardOid == shardSplitInfo->splitChildShardOid)
		{
			return true;
		}
	}

	return false;
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

	/*
	 * Get ShardSplitInfoForSlot if not already initialized.
	 * This gets initialized during the replication of first message.
	 */
	if (ShardSplitInfoForSlot == NULL)
	{
		ShardSplitInfoForSlot = PopulateShardSplitInfoForReplicationSlot(
			ctx->slot->data.name.data);
		ShardSplitInfo_SMHeader = ShardSplitInfoForSlot->shardSplitInfoHeader;
	}

	if (IsCommitRecursive(relation))
	{
		return;
	}

	Oid targetRelationOid = InvalidOid;
	char *replicationSlotName = ctx->slot->data.name.data;
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
