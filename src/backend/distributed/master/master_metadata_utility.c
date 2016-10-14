/*-------------------------------------------------------------------------
 *
 * master_metadata_utility.c
 *    Routines for reading and modifying master node's metadata.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "distributed/citus_nodes.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_shard_placement.h"
#include "distributed/relay_utility.h"
#include "distributed/worker_manager.h"
#include "nodes/makefuncs.h"
#include "parser/scansup.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Local functions forward declarations */
static uint64 * AllocateUint64(uint64 value);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_stage_shard_row);
PG_FUNCTION_INFO_V1(master_stage_shard_placement_row);


/*
 * LoadShardIntervalList returns a list of shard intervals related for a given
 * distributed table. The function returns an empty list if no shards can be
 * found for the given relation.
 * Since LoadShardIntervalList relies on sortedShardIntervalArray, it returns
 * a shard interval list whose elements are sorted on shardminvalue. Shard intervals
 * with uninitialized shard min/max values are placed in the end of the list.
 */
List *
LoadShardIntervalList(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
	List *shardList = NIL;
	int i = 0;

	for (i = 0; i < cacheEntry->shardIntervalArrayLength; i++)
	{
		ShardInterval *newShardInterval = NULL;
		newShardInterval = (ShardInterval *) palloc0(sizeof(ShardInterval));

		CopyShardInterval(cacheEntry->sortedShardIntervalArray[i], newShardInterval);

		shardList = lappend(shardList, newShardInterval);
	}

	return shardList;
}


/*
 * ShardIntervalCount returns number of shard intervals for a given distributed table.
 * The function returns 0 if table is not distributed, or no shards can be found for
 * the given relation id.
 */
int
ShardIntervalCount(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
	int shardIntervalCount = 0;

	if (cacheEntry->isDistributedTable)
	{
		shardIntervalCount = cacheEntry->shardIntervalArrayLength;
	}

	return shardIntervalCount;
}


/*
 * LoadShardList reads list of shards for given relationId from pg_dist_shard,
 * and returns the list of found shardIds.
 * Since LoadShardList relies on sortedShardIntervalArray, it returns a shard
 * list whose elements are sorted on shardminvalue. Shards with uninitialized
 * shard min/max values are placed in the end of the list.
 */
List *
LoadShardList(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
	List *shardList = NIL;
	int i = 0;

	for (i = 0; i < cacheEntry->shardIntervalArrayLength; i++)
	{
		ShardInterval *currentShardInterval = cacheEntry->sortedShardIntervalArray[i];
		uint64 *shardIdPointer = AllocateUint64(currentShardInterval->shardId);

		shardList = lappend(shardList, shardIdPointer);
	}

	return shardList;
}


/* Allocates eight bytes, and copies given value's contents those bytes. */
static uint64 *
AllocateUint64(uint64 value)
{
	uint64 *allocatedValue = (uint64 *) palloc0(sizeof(uint64));
	Assert(sizeof(uint64) >= 8);

	(*allocatedValue) = value;

	return allocatedValue;
}


/*
 * CopyShardInterval copies fields from the specified source ShardInterval
 * into the fields of the provided destination ShardInterval.
 */
void
CopyShardInterval(ShardInterval *srcInterval, ShardInterval *destInterval)
{
	destInterval->type = srcInterval->type;
	destInterval->relationId = srcInterval->relationId;
	destInterval->storageType = srcInterval->storageType;
	destInterval->valueTypeId = srcInterval->valueTypeId;
	destInterval->valueTypeLen = srcInterval->valueTypeLen;
	destInterval->valueByVal = srcInterval->valueByVal;
	destInterval->minValueExists = srcInterval->minValueExists;
	destInterval->maxValueExists = srcInterval->maxValueExists;
	destInterval->shardId = srcInterval->shardId;

	destInterval->minValue = 0;
	if (destInterval->minValueExists)
	{
		destInterval->minValue = datumCopy(srcInterval->minValue,
										   srcInterval->valueByVal,
										   srcInterval->valueTypeLen);
	}

	destInterval->maxValue = 0;
	if (destInterval->maxValueExists)
	{
		destInterval->maxValue = datumCopy(srcInterval->maxValue,
										   srcInterval->valueByVal,
										   srcInterval->valueTypeLen);
	}
}


/*
 * ShardLength finds shard placements for the given shardId, extracts the length
 * of a finalized shard, and returns the shard's length. This function errors
 * out if we cannot find any finalized shard placements for the given shardId.
 */
uint64
ShardLength(uint64 shardId)
{
	uint64 shardLength = 0;

	List *shardPlacementList = FinalizedShardPlacementList(shardId);
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find length of shard " UINT64_FORMAT, shardId),
						errdetail("Could not find any shard placements for the shard.")));
	}
	else
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) linitial(shardPlacementList);
		shardLength = shardPlacement->shardLength;
	}

	return shardLength;
}


/*
 * NodeHasActiveShardPlacements returns whether any active shards are placed on this node
 */
bool
NodeHasActiveShardPlacements(char *nodeName, int32 nodePort)
{
	const int scanKeyCount = 3;
	const bool indexOK = false;

	bool hasFinalizedPlacements = false;

	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[scanKeyCount];

	Relation pgShardPlacement = heap_open(DistShardPlacementRelationId(),
										  AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_placement_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_shard_placement_nodeport,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(nodePort));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_shard_placement_shardstate,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(FILE_FINALIZED));

	scanDescriptor = systable_beginscan(pgShardPlacement,
										DistShardPlacementNodeidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	hasFinalizedPlacements = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);
	heap_close(pgShardPlacement, AccessShareLock);

	return hasFinalizedPlacements;
}


/*
 * FinalizedShardPlacementList finds shard placements for the given shardId from
 * system catalogs, chooses placements that are in finalized state, and returns
 * these shard placements in a new list.
 */
List *
FinalizedShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	List *shardPlacementList = ShardPlacementList(shardId);

	ListCell *shardPlacementCell = NULL;
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		if (shardPlacement->shardState == FILE_FINALIZED)
		{
			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}

	return finalizedPlacementList;
}


/*
 * ShardPlacementList finds shard placements for the given shardId from system
 * catalogs, converts these placements to their in-memory representation, and
 * returns the converted shard placements in a new list.
 */
List *
ShardPlacementList(uint64 shardId)
{
	List *shardPlacementList = NIL;
	Relation pgShardPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;

	pgShardPlacement = heap_open(DistShardPlacementRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_placement_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgShardPlacement,
										DistShardPlacementShardidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgShardPlacement);

		ShardPlacement *placement = TupleToShardPlacement(tupleDescriptor, heapTuple);
		shardPlacementList = lappend(shardPlacementList, placement);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgShardPlacement, AccessShareLock);

	/* if no shard placements are found, warn the user */
	if (shardPlacementList == NIL)
	{
		ereport(WARNING, (errmsg("could not find any shard placements for shardId "
								 UINT64_FORMAT, shardId)));
	}

	return shardPlacementList;
}


/*
 * TupleToShardPlacement takes in a heap tuple from pg_dist_shard_placement,
 * and converts this tuple to in-memory struct. The function assumes the
 * caller already has locks on the tuple, and doesn't perform any locking.
 */
ShardPlacement *
TupleToShardPlacement(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	ShardPlacement *shardPlacement = NULL;
	bool isNull = false;

	Datum placementId = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_placementid,
									 tupleDescriptor, &isNull);
	Datum shardId = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_shardid,
								 tupleDescriptor, &isNull);
	Datum shardLength = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_shardlength,
									 tupleDescriptor, &isNull);
	Datum shardState = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_shardstate,
									tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_nodeport,
								  tupleDescriptor, &isNull);
	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_shard_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_shard_placement_tuple")));
	}

	shardPlacement = CitusMakeNode(ShardPlacement);
	shardPlacement->placementId = DatumGetInt64(placementId);
	shardPlacement->shardId = DatumGetInt64(shardId);
	shardPlacement->shardLength = DatumGetInt64(shardLength);
	shardPlacement->shardState = DatumGetUInt32(shardState);
	shardPlacement->nodeName = TextDatumGetCString(nodeName);
	shardPlacement->nodePort = DatumGetInt64(nodePort);

	return shardPlacement;
}


/*
 * InsertShardRow opens the shard system catalog, and inserts a new row with the
 * given values into that system catalog. Note that we allow the user to pass in
 * null min/max values in case they are creating an empty shard.
 */
void
InsertShardRow(Oid relationId, uint64 shardId, char storageType,
			   text *shardMinValue, text *shardMaxValue)
{
	Relation pgDistShard = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_shard];
	bool isNulls[Natts_pg_dist_shard];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_shard_logicalrelid - 1] = ObjectIdGetDatum(relationId);
	values[Anum_pg_dist_shard_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_shard_shardstorage - 1] = CharGetDatum(storageType);

	/* dropped shardalias column must also be set; it is still part of the tuple */
	isNulls[Anum_pg_dist_shard_shardalias_DROPPED - 1] = true;

	/* check if shard min/max values are null */
	if (shardMinValue != NULL && shardMaxValue != NULL)
	{
		values[Anum_pg_dist_shard_shardminvalue - 1] = PointerGetDatum(shardMinValue);
		values[Anum_pg_dist_shard_shardmaxvalue - 1] = PointerGetDatum(shardMaxValue);
	}
	else
	{
		isNulls[Anum_pg_dist_shard_shardminvalue - 1] = true;
		isNulls[Anum_pg_dist_shard_shardmaxvalue - 1] = true;
	}

	/* open shard relation and insert new tuple */
	pgDistShard = heap_open(DistShardRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistShard);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistShard, heapTuple);
	CatalogUpdateIndexes(pgDistShard, heapTuple);

	/* invalidate previous cache entry and close relation */
	CitusInvalidateRelcacheByRelid(relationId);

	CommandCounterIncrement();
	heap_close(pgDistShard, RowExclusiveLock);
}


/*
 * InsertShardPlacementRow opens the shard placement system catalog, and inserts
 * a new row with the given values into that system catalog. If placementId is
 * INVALID_PLACEMENT_ID, a new placement id will be assigned.
 */
void
InsertShardPlacementRow(uint64 shardId, uint64 placementId,
						char shardState, uint64 shardLength,
						char *nodeName, uint32 nodePort)
{
	Relation pgDistShardPlacement = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_shard_placement];
	bool isNulls[Natts_pg_dist_shard_placement];

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	if (placementId == INVALID_PLACEMENT_ID)
	{
		placementId = master_get_new_placementid(NULL);
	}
	values[Anum_pg_dist_shard_placement_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_shard_placement_shardstate - 1] = CharGetDatum(shardState);
	values[Anum_pg_dist_shard_placement_shardlength - 1] = Int64GetDatum(shardLength);
	values[Anum_pg_dist_shard_placement_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_shard_placement_nodeport - 1] = Int64GetDatum(nodePort);
	values[Anum_pg_dist_shard_placement_placementid - 1] = Int64GetDatum(placementId);

	/* open shard placement relation and insert new tuple */
	pgDistShardPlacement = heap_open(DistShardPlacementRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistShardPlacement);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistShardPlacement, heapTuple);
	CatalogUpdateIndexes(pgDistShardPlacement, heapTuple);

	CommandCounterIncrement();
	heap_close(pgDistShardPlacement, RowExclusiveLock);
}


/*
 * DeleteShardRow opens the shard system catalog, finds the unique row that has
 * the given shardId, and deletes this row.
 */
void
DeleteShardRow(uint64 shardId)
{
	Relation pgDistShard = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	Form_pg_dist_shard pgDistShardForm = NULL;
	Oid distributedRelationId = InvalidOid;

	pgDistShard = heap_open(DistShardRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardShardidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}

	pgDistShardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
	distributedRelationId = pgDistShardForm->logicalrelid;

	simple_heap_delete(pgDistShard, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	/* invalidate previous cache entry */
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	CommandCounterIncrement();
	heap_close(pgDistShard, RowExclusiveLock);
}


/*
 * DeleteShardPlacementRow opens the shard placement system catalog, finds the
 * first (unique) row that corresponds to the given shardId and worker node, and
 * deletes this row.
 */
uint64
DeleteShardPlacementRow(uint64 shardId, char *workerName, uint32 workerPort)
{
	Relation pgDistShardPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	bool heapTupleFound = false;
	TupleDesc tupleDescriptor = NULL;
	int64 placementId = INVALID_PLACEMENT_ID;
	bool isNull = false;

	pgDistShardPlacement = heap_open(DistShardPlacementRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistShardPlacement);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_placement_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShardPlacement,
										DistShardPlacementShardidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		ShardPlacement *placement = TupleToShardPlacement(tupleDescriptor, heapTuple);
		if (strncmp(placement->nodeName, workerName, WORKER_LENGTH) == 0 &&
			placement->nodePort == workerPort)
		{
			heapTupleFound = true;
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* if we couldn't find the shard placement to delete, error out */
	if (!heapTupleFound)
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   UINT64_FORMAT " on node \"%s:%u\"",
							   shardId, workerName, workerPort)));
	}

	placementId = heap_getattr(heapTuple, Anum_pg_dist_shard_placement_placementid,
							   tupleDescriptor, &isNull);
	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_shard_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_shard_placement_tuple")));
	}

	simple_heap_delete(pgDistShardPlacement, &heapTuple->t_self);
	systable_endscan(scanDescriptor);

	CommandCounterIncrement();
	heap_close(pgDistShardPlacement, RowExclusiveLock);

	return placementId;
}


/*
 * UpdateShardPlacementState sets the shardState for the placement identified
 * by placementId.
 */
void
UpdateShardPlacementState(uint64 placementId, char shardState)
{
	Relation pgDistShardPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	Datum values[Natts_pg_dist_shard_placement];
	bool isnull[Natts_pg_dist_shard_placement];
	bool replace[Natts_pg_dist_shard_placement];

	pgDistShardPlacement = heap_open(DistShardPlacementRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistShardPlacement);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(placementId));

	scanDescriptor = systable_beginscan(pgDistShardPlacement,
										DistShardPlacementPlacementidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   UINT64_FORMAT,
							   placementId)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_shard_placement_shardstate - 1] = CharGetDatum(shardState);
	isnull[Anum_pg_dist_shard_placement_shardstate - 1] = false;
	replace[Anum_pg_dist_shard_placement_shardstate - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);
	simple_heap_update(pgDistShardPlacement, &heapTuple->t_self, heapTuple);

	CatalogUpdateIndexes(pgDistShardPlacement, heapTuple);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistShardPlacement, NoLock);
}


/*
 * Check that the current user has `mode` permissions on relationId, error out
 * if not. Superusers always have such permissions.
 */
void
EnsureTablePermissions(Oid relationId, AclMode mode)
{
	AclResult aclresult;

	aclresult = pg_class_aclcheck(relationId, GetUserId(), mode);

	if (aclresult != ACLCHECK_OK)
	{
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   get_rel_name(relationId));
	}
}


/*
 * Check that the current user has owner rights to relationId, error out if
 * not. Superusers are regarded as owners.
 */
void
EnsureTableOwner(Oid relationId)
{
	if (!pg_class_ownercheck(relationId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   get_rel_name(relationId));
	}
}


/*
 * EnsureSuperUser check that the current user is a superuser and errors out if not.
 */
void
EnsureSuperUser(void)
{
	if (!superuser())
	{
		ereport(ERROR, (errmsg("operation is not allowed"),
						errhint("Run the command with a superuser.")));
	}
}


/*
 * Return a table's owner as a string.
 */
char *
TableOwner(Oid relationId)
{
	Oid userId = InvalidOid;
	HeapTuple tuple;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
						errmsg("relation with OID %u does not exist", relationId)));
	}

	userId = ((Form_pg_class) GETSTRUCT(tuple))->relowner;

	ReleaseSysCache(tuple);

	return GetUserNameFromId(userId, false);
}


/*
 * master_stage_shard_row() inserts a row into pg_dist_shard, after performing
 * basic permission checks.
 *
 * TODO: This function only exists for csql's \stage, and should not otherwise
 * be used. Once \stage is removed, it'll be removed too.
 */
Datum
master_stage_shard_row(PG_FUNCTION_ARGS)
{
	Oid distributedRelationId = InvalidOid;
	uint64 shardId = 0;
	char storageType = 0;
	text *shardMinValue = NULL;
	text *shardMaxValue = NULL;
	Relation relation;

	/*
	 * Have to check arguments for NULLness as it can't be declared STRICT
	 * because of min/max arguments, which have to be NULLable for new shards.
	 */
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation cannot be null")));
	}
	else if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard cannot be null")));
	}
	else if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("storage type cannot be null")));
	}

	distributedRelationId = PG_GETARG_OID(0);
	shardId = PG_GETARG_INT64(1);
	storageType = PG_GETARG_CHAR(2);

	if (!PG_ARGISNULL(3))
	{
		shardMinValue = PG_GETARG_TEXT_P(3);
	}

	if (!PG_ARGISNULL(4))
	{
		shardMaxValue = PG_GETARG_TEXT_P(4);
	}

	relation = heap_open(distributedRelationId, RowExclusiveLock);

	/*
	 * Check permissions on relation. Note we require ACL_INSERT and not owner
	 * rights - it'd be worse for security to require every user performing
	 * data loads to be made a table owner - besides being more complex to set
	 * up.
	 */
	EnsureTablePermissions(distributedRelationId, ACL_INSERT);

	/* and finally actually insert the row */
	InsertShardRow(distributedRelationId, shardId, storageType,
				   shardMinValue, shardMaxValue);

	heap_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * master_stage_shard_placement_row() inserts a row into
 * pg_dist_shard_placment, after performing some basic checks.
 *
 * TODO: This function only exists for csql's \stage, and should not otherwise
 * be used. Once \stage is removed, it'll be removed too.
 */
Datum
master_stage_shard_placement_row(PG_FUNCTION_ARGS)
{
	uint64 shardId = PG_GETARG_INT64(0);
	int32 shardState = PG_GETARG_INT32(1);
	int32 shardLength = PG_GETARG_INT64(2);
	char *nodeName = text_to_cstring(PG_GETARG_TEXT_P(3));
	int nodePort = PG_GETARG_INT32(4);

	Oid distributedRelationId = InvalidOid;
	Relation relation = NULL;

	Relation pgDistShard = heap_open(DistShardRelationId(), RowExclusiveLock);
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;

	/* Lookup which table the shardid belongs to */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardShardidIndexId(), true,
										NULL, scanKeyCount, scanKey);
	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		Form_pg_dist_shard pgDistShardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
		distributedRelationId = pgDistShardForm->logicalrelid;
	}
	else
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}
	systable_endscan(scanDescriptor);

	relation = heap_open(distributedRelationId, RowExclusiveLock);

	/*
	 * Check permissions on relation. Note we require ACL_INSERT and not owner
	 * rights - it'd be worse for security to require every user performing
	 * data loads to be made a table owner - besides being more complex to set
	 * up.
	 */
	EnsureTablePermissions(distributedRelationId, ACL_INSERT);

	/* finally insert placement */
	InsertShardPlacementRow(shardId, INVALID_PLACEMENT_ID, shardState, shardLength,
							nodeName, nodePort);

	heap_close(relation, NoLock);
	heap_close(pgDistShard, NoLock);

	PG_RETURN_VOID();
}
