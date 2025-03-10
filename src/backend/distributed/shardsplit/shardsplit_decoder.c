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

#include "catalog/pg_namespace.h"
#include "replication/logical.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "pg_version_constants.h"

#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_shared_memory.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
static LogicalDecodeChangeCB pgOutputPluginChangeCB;

#define InvalidRepOriginId 0

static HTAB *SourceToDestinationShardMap = NULL;
static bool replication_origin_filter_cb(LogicalDecodingContext *ctx, RepOriginId
										 origin_id);

/* Plugin callback */
static void shard_split_change_cb(LogicalDecodingContext *ctx,
								  ReorderBufferTXN *txn,
								  Relation relation, ReorderBufferChange *change);

/* Helper methods */
static int32_t GetHashValueForIncomingTuple(Relation sourceShardRelation,
											HeapTuple tuple,
											int partitionColumIndex,
											Oid distributedTableOid);

static Oid FindTargetRelationOid(Relation sourceShardRelation,
								 HeapTuple tuple,
								 char *currentSlotName);

static HeapTuple GetTupleForTargetSchema(HeapTuple sourceRelationTuple,
										 TupleDesc sourceTupleDesc,
										 TupleDesc targetTupleDesc);

/*
 * Postgres uses 'pgoutput' as default plugin for logical replication.
 * We want to reuse Postgres pgoutput's functionality as much as possible.
 * Hence we load all the functions of this plugin and override as required.
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	LogicalOutputPluginInit plugin_init =
		(LogicalOutputPluginInit) (void *)
		load_external_function("pgoutput",
							   "_PG_output_plugin_init",
							   false, NULL);

	if (plugin_init == NULL)
	{
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");
	}

	/* ask the output plugin to fill the callback struct */
	plugin_init(cb);

	/* actual pgoutput callback will be called with the appropriate destination shard */
	pgOutputPluginChangeCB = cb->change_cb;
	cb->change_cb = shard_split_change_cb;
	cb->filter_by_origin_cb = replication_origin_filter_cb;
}


/*
 * replication_origin_filter_cb call back function filters out publication of changes
 * originated from any other node other than the current node. This is
 * identified by the "origin_id" of the changes. The origin_id is set to
 * a non-zero value in the origin node as part of WAL replication for internal
 * operations like shard split/moves/create_distributed_table etc.
 */
static bool
replication_origin_filter_cb(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	return  (origin_id != InvalidRepOriginId);
}


/*
 * update_replication_progress is copied from Postgres 15. We use it to send keepalive
 * messages when we are filtering out the wal changes resulting from the initial copy.
 * If we do not send out messages long enough, wal reciever will time out.
 * Postgres 16 has refactored this code such that keepalive messages are sent during
 * reordering phase which is above change_cb. So we do not need to send keepalive in
 * change_cb.
 */
#if (PG_VERSION_NUM < PG_VERSION_16)
static void
update_replication_progress(LogicalDecodingContext *ctx, bool skipped_xact)
{
	static int changes_count = 0;

	/*
	 * We don't want to try sending a keepalive message after processing each
	 * change as that can have overhead. Tests revealed that there is no
	 * noticeable overhead in doing it after continuously processing 100 or so
	 * changes.
	 */
#define CHANGES_THRESHOLD 100

	/*
	 * After continuously processing CHANGES_THRESHOLD changes, we
	 * try to send a keepalive message if required.
	 */
	if (ctx->end_xact || ++changes_count >= CHANGES_THRESHOLD)
	{
		OutputPluginUpdateProgress(ctx, skipped_xact);
		changes_count = 0;
	}
}


#endif

/*
 * shard_split_change_cb function emits the incoming tuple change
 * to the appropriate destination shard.
 */
static void
shard_split_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					  Relation relation, ReorderBufferChange *change)
{
	/*
	 * If Citus has not been loaded yet, pass the changes
	 * through to the undrelying decoder plugin.
	 */
	if (!CitusHasBeenLoaded())
	{
		pgOutputPluginChangeCB(ctx, txn, relation, change);
		return;
	}

#if (PG_VERSION_NUM < PG_VERSION_16)

	/* Send replication keepalive. */
	update_replication_progress(ctx, false);
#endif

	/* check if the relation is publishable.*/
	if (!is_publishable_relation(relation))
	{
		return;
	}

	char *replicationSlotName = ctx->slot->data.name.data;
	if (replicationSlotName == NULL)
	{
		elog(ERROR, "Replication slot name is NULL!");
		return;
	}

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

#if PG_VERSION_NUM >= PG_VERSION_17
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
		{
			HeapTuple newTuple = change->data.tp.newtuple;
			targetRelationOid = FindTargetRelationOid(relation, newTuple,
													  replicationSlotName);
			break;
		}

		/* updating non-partition column value */
		case REORDER_BUFFER_CHANGE_UPDATE:
		{
			HeapTuple newTuple = change->data.tp.newtuple;
			targetRelationOid = FindTargetRelationOid(relation, newTuple,
													  replicationSlotName);
			break;
		}

		case REORDER_BUFFER_CHANGE_DELETE:
		{
			HeapTuple oldTuple = change->data.tp.oldtuple;
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
#else
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
#endif

	/* Current replication slot is not responsible for handling the change */
	if (targetRelationOid == InvalidOid)
	{
		return;
	}

	Relation targetRelation = RelationIdGetRelation(targetRelationOid);

	/*
	 * If any columns from source relation have been dropped, then the tuple needs to
	 * be formatted according to the target relation.
	 */
	TupleDesc sourceRelationDesc = RelationGetDescr(relation);
	TupleDesc targetRelationDesc = RelationGetDescr(targetRelation);
	if (sourceRelationDesc->natts > targetRelationDesc->natts)
	{
#if PG_VERSION_NUM >= PG_VERSION_17
		switch (change->action)
		{
			case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple sourceRelationNewTuple = change->data.tp.newtuple;
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchema(
					sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.newtuple = targetRelationNewTuple;
				break;
			}

			case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple sourceRelationNewTuple = change->data.tp.newtuple;
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchema(
					sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.newtuple = targetRelationNewTuple;

				/*
				 * Format oldtuple according to the target relation. If the column values of replica
				 * identiy change, then the old tuple is non-null and needs to be formatted according
				 * to the target relation schema.
				 */
				if (change->data.tp.oldtuple != NULL)
				{
					HeapTuple sourceRelationOldTuple = change->data.tp.oldtuple;
					HeapTuple targetRelationOldTuple = GetTupleForTargetSchema(
						sourceRelationOldTuple,
						sourceRelationDesc,
						targetRelationDesc);

					change->data.tp.oldtuple = targetRelationOldTuple;
				}
				break;
			}

			case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple sourceRelationOldTuple = change->data.tp.oldtuple;
				HeapTuple targetRelationOldTuple = GetTupleForTargetSchema(
					sourceRelationOldTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.oldtuple = targetRelationOldTuple;
				break;
			}

			/* Only INSERT/DELETE/UPDATE actions are visible in the replication path of split shard */
			default:
				ereport(ERROR, errmsg(
							"Unexpected Action :%d. Expected action is INSERT/DELETE/UPDATE",
							change->action));
		}
#else
		switch (change->action)
		{
			case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple sourceRelationNewTuple = &(change->data.tp.newtuple->tuple);
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchema(
					sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.newtuple->tuple = *targetRelationNewTuple;
				break;
			}

			case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple sourceRelationNewTuple = &(change->data.tp.newtuple->tuple);
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchema(
					sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.newtuple->tuple = *targetRelationNewTuple;

				/*
				 * Format oldtuple according to the target relation. If the column values of replica
				 * identiy change, then the old tuple is non-null and needs to be formatted according
				 * to the target relation schema.
				 */
				if (change->data.tp.oldtuple != NULL)
				{
					HeapTuple sourceRelationOldTuple = &(change->data.tp.oldtuple->tuple);
					HeapTuple targetRelationOldTuple = GetTupleForTargetSchema(
						sourceRelationOldTuple,
						sourceRelationDesc,
						targetRelationDesc);

					change->data.tp.oldtuple->tuple = *targetRelationOldTuple;
				}
				break;
			}

			case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple sourceRelationOldTuple = &(change->data.tp.oldtuple->tuple);
				HeapTuple targetRelationOldTuple = GetTupleForTargetSchema(
					sourceRelationOldTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.oldtuple->tuple = *targetRelationOldTuple;
				break;
			}

			/* Only INSERT/DELETE/UPDATE actions are visible in the replication path of split shard */
			default:
				ereport(ERROR, errmsg(
							"Unexpected Action :%d. Expected action is INSERT/DELETE/UPDATE",
							change->action));
		}
#endif
	}

	pgOutputPluginChangeCB(ctx, txn, targetRelation, change);
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
	foreach_declared_ptr(shardSplitInfo, entry->shardSplitInfoList)
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
	TupleDesc relationTupleDes = RelationGetDescr(sourceShardRelation);
	Form_pg_attribute partitionColumn = TupleDescAttr(relationTupleDes,
													  partitionColumnIndex);

	bool isNull = false;
	Datum partitionColumnValue = heap_getattr(tuple,
											  partitionColumnIndex + 1,
											  relationTupleDes,
											  &isNull);

	TypeCacheEntry *typeEntry = lookup_type_cache(partitionColumn->atttypid,
												  TYPECACHE_HASH_PROC_FINFO);

	/* get hashed value of the distribution value */
	Datum hashedValueDatum = FunctionCall1Coll(&(typeEntry->hash_proc_finfo),
											   typeEntry->typcollation,
											   partitionColumnValue);

	return DatumGetInt32(hashedValueDatum);
}


/*
 * GetTupleForTargetSchema returns a tuple with the schema of the target relation.
 * If some columns within the source relations are dropped, we would have to reformat
 * the tuple to match the schema of the target relation.
 *
 * Consider the below scenario:
 * Session1 : Drop column followed by create_distributed_table_concurrently
 * Session2 : Concurrent insert workload
 *
 * The child shards created by create_distributed_table_concurrently will have less columns
 * than the source shard because some column were dropped.
 * The incoming tuple from session2 will have more columns as the writes
 * happened on source shard. But now the tuple needs to be applied on child shard. So we need to format
 * it according to child schema.
 */
static HeapTuple
GetTupleForTargetSchema(HeapTuple sourceRelationTuple,
						TupleDesc sourceRelDesc,
						TupleDesc targetRelDesc)
{
	/* Deform the tuple */
	Datum *oldValues = (Datum *) palloc0(sourceRelDesc->natts * sizeof(Datum));
	bool *oldNulls = (bool *) palloc0(sourceRelDesc->natts * sizeof(bool));
	heap_deform_tuple(sourceRelationTuple, sourceRelDesc, oldValues,
					  oldNulls);


	/* Create new tuple by skipping dropped columns */
	int nextAttributeIndex = 0;
	Datum *newValues = (Datum *) palloc0(targetRelDesc->natts * sizeof(Datum));
	bool *newNulls = (bool *) palloc0(targetRelDesc->natts * sizeof(bool));
	for (int i = 0; i < sourceRelDesc->natts; i++)
	{
		if (TupleDescAttr(sourceRelDesc, i)->attisdropped)
		{
			continue;
		}

		newValues[nextAttributeIndex] = oldValues[i];
		newNulls[nextAttributeIndex] = oldNulls[i];
		nextAttributeIndex++;
	}

	HeapTuple targetRelationTuple = heap_form_tuple(targetRelDesc, newValues, newNulls);
	return targetRelationTuple;
}
