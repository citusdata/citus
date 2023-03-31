/*-------------------------------------------------------------------------
 *
 * cdc_decoder.c
 *		CDC Decoder plugin for Citus
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "cdc_decoder_utils.h"
#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_publication.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
static LogicalDecodeChangeCB ouputPluginChangeCB;

static void InitShardToDistributedTableMap(void);

static void PublishDistributedTableChanges(LogicalDecodingContext *ctx,
										   ReorderBufferTXN *txn,
										   Relation relation,
										   ReorderBufferChange *change);


static bool replication_origin_filter_cb(LogicalDecodingContext *ctx, RepOriginId
										 origin_id);

static void TranslateChangesIfSchemaChanged(Relation relation, Relation targetRelation,
											ReorderBufferChange *change);

static void TranslateAndPublishRelationForCDC(LogicalDecodingContext *ctx,
											  ReorderBufferTXN *txn,
											  Relation relation,
											  ReorderBufferChange *change, Oid shardId,
											  Oid targetRelationid);

typedef struct
{
	uint64 shardId;
	Oid distributedTableId;
	bool isReferenceTable;
	bool isNull;
} ShardIdHashEntry;

static HTAB *shardToDistributedTableMap = NULL;

static void cdc_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change);


/* build time macro for base decoder plugin name for CDC and Shard Split. */
#ifndef DECODER
#define DECODER "pgoutput"
#endif

#define DECODER_INIT_FUNCTION_NAME "_PG_output_plugin_init"

/*
 * Postgres uses 'pgoutput' as default plugin for logical replication.
 * We want to reuse Postgres pgoutput's functionality as much as possible.
 * Hence we load all the functions of this plugin and override as required.
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	elog(LOG, "Initializing CDC decoder");

	/*
	 * We build custom .so files whose name matches common decoders (pgoutput, wal2json)
	 * and place them in $libdir/citus_decoders/ such that administrators can configure
	 * dynamic_library_path to include this directory, and users can then use the
	 * regular decoder names when creating replications slots.
	 *
	 * To load the original decoder, we need to remove citus_decoders/ from the
	 * dynamic_library_path.
	 */
	char *originalDLP = Dynamic_library_path;
	Dynamic_library_path = RemoveCitusDecodersFromPaths(Dynamic_library_path);

	LogicalOutputPluginInit plugin_init =
		(LogicalOutputPluginInit) (void *)
		load_external_function(DECODER,
							   DECODER_INIT_FUNCTION_NAME,
							   false, NULL);

	if (plugin_init == NULL)
	{
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");
	}

	/* in case this session is used for different replication slots */
	Dynamic_library_path = originalDLP;

	/* ask the output plugin to fill the callback struct */
	plugin_init(cb);

	/* Initialize the Shard Id to Distributed Table id mapping hash table.*/
	InitShardToDistributedTableMap();

	/* actual pgoutput callback function will be called  */
	ouputPluginChangeCB = cb->change_cb;
	cb->change_cb = cdc_change_cb;
	cb->filter_by_origin_cb = replication_origin_filter_cb;
}


/*
 * shard_split_and_cdc_change_cb function emits the incoming tuple change
 * to the appropriate destination shard.
 */
static void
cdc_change_cb(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	/*
	 * If Citus has not been loaded yet, pass the changes
	 * through to the undrelying decoder plugin.
	 */
	if (!CdcCitusHasBeenLoaded())
	{
		ouputPluginChangeCB(ctx, txn, relation, change);
		return;
	}

	/* check if the relation is publishable.*/
	if (!is_publishable_relation(relation))
	{
		return;
	}

	PublishDistributedTableChanges(ctx, txn, relation, change);
}


/*
 * InitShardToDistributedTableMap initializes the hash table that is used to
 * translate the changes in the shard table to the changes in the distributed table.
 */
static void
InitShardToDistributedTableMap()
{
	HASHCTL info;
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(ShardIdHashEntry);
	info.hash = tag_hash;
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
	entry->distributedTableId = CdcLookupShardRelationFromCatalog(shardId, true);
	entry->isReferenceTable = CdcPartitionMethodViaCatalog(entry->distributedTableId) ==
							  'n';
	return entry->distributedTableId;
}


static Oid
LookupDistributedTableIdForShardId(Oid shardId, bool *isReferenceTable)
{
	bool found;
	Oid distributedTableId = InvalidOid;
	ShardIdHashEntry *entry = (ShardIdHashEntry *) hash_search(shardToDistributedTableMap,
															   &shardId,
															   HASH_FIND | HASH_ENTER,
															   &found);
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
 * This function is responsible for translating the changes in the shard table to
 * the changes in the shell table and publishing the changes as a change to the
 * distributed table so that CDD clients are not aware of the shard tables. It also
 * handles schema changes to the distributed table.
 */
static void
TranslateAndPublishRelationForCDC(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
								  Relation relation, ReorderBufferChange *change, Oid
								  shardId, Oid targetRelationid)
{
	/* Get the distributed table's relation for this shard.*/
	Relation targetRelation = RelationIdGetRelation(targetRelationid);

	/*
	 * Check if there has been a schema change (such as a dropped column), by comparing
	 * the number of attributes in the shard table and the shell table.
	 */
	TranslateChangesIfSchemaChanged(relation, targetRelation, change);

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
static void
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
	uint64 shardId = CdcExtractShardIdFromTableName(shardRelationName, true);

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
	if (isReferenceTable && !CdcIsCoordinator())
	{
		return;
	}

	/* translate and publish from shard relation to distributed table relation for CDC. */
	TranslateAndPublishRelationForCDC(ctx, txn, relation, change, shardId,
									  distRelationId);
}


/*
 * GetTupleForTargetSchemaForCdc returns a heap tuple with the data from sourceRelationTuple
 * to match the schema in targetRelDesc. Either or both source and target relations may have
 * dropped columns. This function handles it by adding NULL values for dropped columns in
 * target relation and skipping dropped columns in source relation. It returns a heap tuple
 * adjusted to the current schema of the target relation.
 */
static HeapTuple
GetTupleForTargetSchemaForCdc(HeapTuple sourceRelationTuple,
							  TupleDesc sourceRelDesc,
							  TupleDesc targetRelDesc)
{
	/* Allocate memory for sourceValues and sourceNulls arrays. */
	Datum *sourceValues = (Datum *) palloc0(sourceRelDesc->natts * sizeof(Datum));
	bool *sourceNulls = (bool *) palloc0(sourceRelDesc->natts * sizeof(bool));

	/* Deform the source tuple to sourceValues and sourceNulls arrays. */
	heap_deform_tuple(sourceRelationTuple, sourceRelDesc, sourceValues,
					  sourceNulls);

	/* This is the next field to Read in the source relation */
	uint32 sourceIndex = 0;
	uint32 targetIndex = 0;

	/* Allocate memory for sourceValues and sourceNulls arrays. */
	Datum *targetValues = (Datum *) palloc0(targetRelDesc->natts * sizeof(Datum));
	bool *targetNulls = (bool *) palloc0(targetRelDesc->natts * sizeof(bool));

	/* Loop through all source and target attributes one by one and handle any dropped attributes.*/
	while (targetIndex < targetRelDesc->natts)
	{
		/* If this target attribute has been dropped, add a NULL attribute in targetValues and continue.*/
		if (TupleDescAttr(targetRelDesc, targetIndex)->attisdropped)
		{
			Datum nullDatum = (Datum) 0;
			targetValues[targetIndex] = nullDatum;
			targetNulls[targetIndex] = true;
			targetIndex++;
		}
		/* If this source attribute has been dropped, just skip this source attribute.*/
		else if (TupleDescAttr(sourceRelDesc, sourceIndex)->attisdropped)
		{
			sourceIndex++;
			continue;
		}
		/* If both source and target attributes are not dropped, add the attribute field to targetValues. */
		else if (sourceIndex < sourceRelDesc->natts)
		{
			targetValues[targetIndex] = sourceValues[sourceIndex];
			targetNulls[targetIndex] = sourceNulls[sourceIndex];
			sourceIndex++;
			targetIndex++;
		}
		else
		{
			/* If there are no more source fields, add a NULL field in targetValues. */
			Datum nullDatum = (Datum) 0;
			targetValues[targetIndex] = nullDatum;
			targetNulls[targetIndex] = true;
			targetIndex++;
		}
	}

	/* Form a new tuple from the target values created by the above loop. */
	HeapTuple targetRelationTuple = heap_form_tuple(targetRelDesc, targetValues,
													targetNulls);
	return targetRelationTuple;
}


/* HasSchemaChanged function returns if there any schema changes between source and target relations.*/
static bool
HasSchemaChanged(TupleDesc sourceRelationDesc, TupleDesc targetRelationDesc)
{
	bool hasSchemaChanged = (sourceRelationDesc->natts != targetRelationDesc->natts);
	if (hasSchemaChanged)
	{
		return true;
	}

	for (uint32 i = 0; i < sourceRelationDesc->natts; i++)
	{
		if (TupleDescAttr(sourceRelationDesc, i)->attisdropped ||
			TupleDescAttr(targetRelationDesc, i)->attisdropped)
		{
			hasSchemaChanged = true;
			break;
		}
	}

	return hasSchemaChanged;
}


/*
 * TranslateChangesIfSchemaChanged translates the tuples ReorderBufferChange
 * if there is a schema change between source and target relations.
 */
static void
TranslateChangesIfSchemaChanged(Relation sourceRelation, Relation targetRelation,
								ReorderBufferChange *change)
{
	TupleDesc sourceRelationDesc = RelationGetDescr(sourceRelation);
	TupleDesc targetRelationDesc = RelationGetDescr(targetRelation);

	/* if there are no changes between source and target relations, return. */
	if (!HasSchemaChanged(sourceRelationDesc, targetRelationDesc))
	{
		return;
	}

	/* Check the ReorderBufferChange's action type and handle them accordingly.*/
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
		{
			/* For insert action, only new tuple should always be translated*/
			HeapTuple sourceRelationNewTuple = &(change->data.tp.newtuple->tuple);
			HeapTuple targetRelationNewTuple = GetTupleForTargetSchemaForCdc(
				sourceRelationNewTuple, sourceRelationDesc, targetRelationDesc);
			change->data.tp.newtuple->tuple = *targetRelationNewTuple;
			break;
		}

		/*
		 * For update changes both old and new tuples need to be translated for target relation
		 * if the REPLICA IDENTITY is set to FULL. Otherwise, only the new tuple needs to be
		 * translated for target relation.
		 */
		case REORDER_BUFFER_CHANGE_UPDATE:
		{
			/* For update action, new tuple should always be translated*/
			/* Get the new tuple from the ReorderBufferChange, and translate it to target relation. */
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
			/* For delete action, only old tuple should be translated*/
			HeapTuple sourceRelationOldTuple = &(change->data.tp.oldtuple->tuple);
			HeapTuple targetRelationOldTuple = GetTupleForTargetSchemaForCdc(
				sourceRelationOldTuple,
				sourceRelationDesc,
				targetRelationDesc);

			change->data.tp.oldtuple->tuple = *targetRelationOldTuple;
			break;
		}

		default:
		{
			/* Do nothing for other action types. */
			break;
		}
	}
}
