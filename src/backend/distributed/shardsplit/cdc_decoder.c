/*-------------------------------------------------------------------------
 *
 * cdc_decoder.c
 *		CDC Decoder plugin for Citus
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "common/hashfn.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_namespace.h"
#include "distributed/cdc_decoder.h"
#include "distributed/relay_utility.h"
#include "distributed/worker_protocol.h"
#include "distributed/metadata_cache.h"

static LogicalDecodeChangeCB ouputPluginChangeCB;


static bool replication_origin_filter_cb(LogicalDecodingContext *ctx, RepOriginId
										 origin_id);

static void HandleSchemaChangesInRelation(Relation relation, Relation targetRelation,
										  ReorderBufferChange *change);

static void translateAndPublishRelationForCDC(LogicalDecodingContext *ctx,
											  ReorderBufferTXN *txn,
											  Relation relation,
											  ReorderBufferChange *change, Oid shardId,
											  Oid targetRelationid);

typedef struct
{
	Oid shardId;
	Oid distributedTableId;
	bool isReferenceTable;
	bool isNull;
} ShardIdHashEntry;

static HTAB *shardToDistributedTableMap = NULL;


/*
 * InitShardToDistributedTableMap initializes the hash table that is used to
 * translate the changes in the shard table to the changes in the distributed table.
 */
static void
InitShardToDistributedTableMap()
{
	HASHCTL info;
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ShardIdHashEntry);
	info.hash = oid_hash;
	info.hcxt = CurrentMemoryContext;

	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	shardToDistributedTableMap = hash_create("CDC Decoder translation hash table", 1024,
											 &info, hashFlags);
}


/*
 * AddShardIdToHashTable adds the shardId to the hash table.
 */
static Oid
AddShardIdToHashTable(Oid shardId, ShardIdHashEntry *entry)
{
	entry->shardId = shardId;
	entry->distributedTableId = LookupShardRelationFromCatalog(shardId, true);
	entry->isReferenceTable = PartitionMethodViaCatalog(entry->distributedTableId) == 'n';
	return entry->distributedTableId;
}


static Oid
LookupDistributedTableIdForShardId(Oid shardId, bool *isReferenceTable)
{
	bool found;
	Oid distributedTableId = InvalidOid;
	ShardIdHashEntry *entry = (ShardIdHashEntry *) hash_search(shardToDistributedTableMap, &shardId,
											 HASH_FIND | HASH_ENTER, &found);
	if (found)
	{
		distributedTableId = entry->distributedTableId;
	}
	else
	{
		distributedTableId = AddShardIdToHashTable(shardId, entry);
	}
	*isReferenceTable = entry->isReferenceTable;
	return distributedTableId;
}


/*
 * InitCDCDecoder is called by from the shard split decoder plugin's init function.
 * It sets the call back function for filtering out changes originated from other nodes.
 */
void
InitCDCDecoder(OutputPluginCallbacks *cb, LogicalDecodeChangeCB changeCB)
{
	elog(LOG, "Initializing CDC decoder");
	cb->filter_by_origin_cb = replication_origin_filter_cb;
	ouputPluginChangeCB = changeCB;
	InitShardToDistributedTableMap();
}


/*
 * replication_origin_filter_cb call back function filters out publication of changes
 * originated from any other node other than the current node. This is
 * identified by the "origin_id" of the changes. The origin_id is set to
 * a non-zero value in the origin node as part of WAL replication.
 */
static bool
replication_origin_filter_cb(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	return  (origin_id != InvalidRepOriginId);
}


/*
 * This function is responsible for translating the changes in the shard table to
 * the changes in the shell table and publishing the changes as a change to the
 * distributed table so that CDD clients are not aware of the shard tables. It also
 * handles schema changes to the distributed table.
 */
static void
translateAndPublishRelationForCDC(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
								  Relation relation, ReorderBufferChange *change, Oid
								  shardId, Oid targetRelationid)
{
	/* Get the distributed table's relation for this shard.*/
	Relation targetRelation = RelationIdGetRelation(targetRelationid);

	/* Get the tuple descuptors for both shard and shell tables to check for any Schema changes.*/
	TupleDesc sourceRelationDesc = RelationGetDescr(relation);
	TupleDesc targetRelationDesc = RelationGetDescr(targetRelation);

	/*
	 * Check if there has been a schema change (such as a dropped column), by comparing
	 * the number of attributes in the shard table and the shell table.
	 */
	if (targetRelationDesc->natts > sourceRelationDesc->natts)
	{
		/* Shell table has some dropped attributes, so format the change to include dropped columns. */
		HandleSchemaChangesInRelation(relation, targetRelation, change);
	}
	else
	{
		/* Shard table has some dropped attributes, so exclude dropped columns to match shell table*/
		HandleSchemaChangesInRelation(targetRelation, relation, change);
	}

	/*
	 * Publish the change to the shard table as the change in the distributed table,
	 * so that the CDC client can see the change in the distributed table,
	 * instead of the shard table, by calling the pgoutput's callback function.
	 */
	ouputPluginChangeCB(ctx, txn, targetRelation, change);
	RelationClose(targetRelation);
}


/*
 * PublishChangesIfCdcSlot checks if the current slot is a CDC slot. If so, it publishes
 * the changes as the change for the distributed table instead of shard.
 * If not, it returns false. It also skips the Citus metadata tables.
 */
void
PublishDistributedTableChanges(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							   Relation relation, ReorderBufferChange *change)
{
	char *shardRelationName = RelationGetRelationName(relation);

	/* Skip publishing CDC changes for any system relations in pg_catalog*/
	if (relation->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
	{
		return;
	}

	/* Check if the relation is a distributed table by checking for shard name.	*/
	uint64 shardId = ExtractShardIdFromTableName(shardRelationName, true);

	/* If this relation is not distributed, call the pgoutput's callback and return. */
	if (shardId == INVALID_SHARD_ID)
	{
		ouputPluginChangeCB(ctx, txn, relation, change);
		return;
	}

	bool isReferenceTable = false;
	Oid distRelationId = LookupDistributedTableIdForShardId(shardId, &isReferenceTable);
	if (distRelationId == InvalidOid)
	{
		ouputPluginChangeCB(ctx, txn, relation, change);
		return;
	}

	/* Publish changes for reference table only from the coordinator node. */
	if (isReferenceTable && !IsCoordinator())
	{
		return;
	}

	/* translate and publish from shard relation to distributed table relation for CDC. */
	translateAndPublishRelationForCDC(ctx, txn, relation, change, shardId,
									  distRelationId);
}


/*
 * GetTupleForTargetSchemaForCdc returns a heap tuple with the data from sourceRelationTuple
 * to match the schema in targetRelDesc. This is needed to remove the dropped columns from
 * the sourceRelDesc before publishing the changes to the CDC client.
 */
static HeapTuple
GetTupleForTargetSchemaForCdc(HeapTuple sourceRelationTuple,
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
	for (int i = 0; i < targetRelDesc->natts; i++)
	{
		if (TupleDescAttr(targetRelDesc, i)->attisdropped)
		{
			Datum nullDatum = (Datum) 0;
			newValues[i] = nullDatum;
			newNulls[i] = true;
		}
		else
		{
			newValues[i] = oldValues[nextAttributeIndex];
			newNulls[i] = oldNulls[nextAttributeIndex];
			nextAttributeIndex++;
		}
	}

	HeapTuple targetRelationTuple = heap_form_tuple(targetRelDesc, newValues, newNulls);
	return targetRelationTuple;
}


/*
 * HandleSchemaChangesInRelation handles schema changes in the relation.
 */
static void
HandleSchemaChangesInRelation(Relation sourceRelation, Relation targetRelation,
							  ReorderBufferChange *change)
{
	TupleDesc sourceRelationDesc = RelationGetDescr(sourceRelation);
	TupleDesc targetRelationDesc = RelationGetDescr(targetRelation);
	if (targetRelationDesc->natts > sourceRelationDesc->natts)
	{
		switch (change->action)
		{
			case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple sourceRelationNewTuple = &(change->data.tp.newtuple->tuple);
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchemaForCdc(
					sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);

				change->data.tp.newtuple->tuple = *targetRelationNewTuple;
				break;
			}

			case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple sourceRelationNewTuple = &(change->data.tp.newtuple->tuple);
				HeapTuple targetRelationNewTuple = GetTupleForTargetSchemaForCdc(
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
					HeapTuple targetRelationOldTuple = GetTupleForTargetSchemaForCdc(
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
				HeapTuple targetRelationOldTuple = GetTupleForTargetSchemaForCdc(
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
	}
}
