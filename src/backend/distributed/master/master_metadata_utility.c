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
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_shard_placement.h"
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
 * LoadShardAlias finds the row for given relation and shardId in pg_dist_shard,
 * finds the shard alias in this row if any, and then deep copies this alias.
 */
char *
LoadShardAlias(Oid relationId, uint64 shardId)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	Datum shardAliasDatum = 0;
	bool shardAliasNull = false;
	char *shardAlias = NULL;

	Relation pgDistShard = heap_open(DistShardRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistShard);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardShardidIndexId(), true,
										NULL, scanKeyCount, scanKey);

	/*
	 * Normally, we should have at most one tuple here as we have a unique index
	 * on shardId. However, if users want to drop this uniqueness constraint,
	 * and look up the shardalias based on the relation and shardId pair, we
	 * still allow that. We don't have any users relaying on this feature. Thus,
	 * we may consider to remove this check.
	 */
	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_dist_shard pgDistShardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
		if (pgDistShardForm->logicalrelid == relationId)
		{
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* if no tuple found, error out */
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for relationId: %u "
							   "and shard " UINT64_FORMAT, relationId, shardId)));
	}

	/* if shard alias exists, deep copy cstring */
	shardAliasDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_shardalias,
								   tupleDescriptor, &shardAliasNull);
	if (!shardAliasNull)
	{
		shardAlias = TextDatumGetCString(shardAliasDatum);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, AccessShareLock);

	return shardAlias;
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
 * TupleToShardPlacement takes in a heap tuple from pg_dist_shard_placement, and
 * converts this tuple to an equivalent struct in memory. The function assumes
 * the caller already has locks on the tuple, and doesn't perform any locking.
 */
ShardPlacement *
TupleToShardPlacement(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	ShardPlacement *shardPlacement = NULL;
	bool isNull = false;

	Oid tupleOid = HeapTupleGetOid(heapTuple);
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

	Assert(!HeapTupleHasNulls(heapTuple));

	shardPlacement = CitusMakeNode(ShardPlacement);
	shardPlacement->tupleOid = tupleOid;
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

	/* check if shard min/max values are null */
	if (shardMinValue != NULL && shardMaxValue != NULL)
	{
		values[Anum_pg_dist_shard_shardminvalue - 1] = PointerGetDatum(shardMinValue);
		values[Anum_pg_dist_shard_shardmaxvalue - 1] = PointerGetDatum(shardMaxValue);

		/* we always set shard alias to null */
		isNulls[Anum_pg_dist_shard_shardalias - 1] = true;
	}
	else
	{
		isNulls[Anum_pg_dist_shard_shardminvalue - 1] = true;
		isNulls[Anum_pg_dist_shard_shardmaxvalue - 1] = true;
		isNulls[Anum_pg_dist_shard_shardalias - 1] = true;
	}

	/* open shard relation and insert new tuple */
	pgDistShard = heap_open(DistShardRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistShard);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistShard, heapTuple);
	CatalogUpdateIndexes(pgDistShard, heapTuple);
	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistShard, RowExclusiveLock);
	CitusInvalidateRelcacheByRelid(relationId);
}


/*
 * InsertShardPlacementRow opens the shard placement system catalog, and inserts
 * a new row with the given values into that system catalog.
 */
void
InsertShardPlacementRow(uint64 shardId, char shardState, uint64 shardLength,
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

	values[Anum_pg_dist_shard_placement_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_shard_placement_shardstate - 1] = CharGetDatum(shardState);
	values[Anum_pg_dist_shard_placement_shardlength - 1] = Int64GetDatum(shardLength);
	values[Anum_pg_dist_shard_placement_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_shard_placement_nodeport - 1] = Int64GetDatum(nodePort);

	/* open shard placement relation and insert new tuple */
	pgDistShardPlacement = heap_open(DistShardPlacementRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistShardPlacement);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistShardPlacement, heapTuple);
	CatalogUpdateIndexes(pgDistShardPlacement, heapTuple);
	CommandCounterIncrement();

	/* close relation */
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
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, RowExclusiveLock);

	/* invalidate previous cache entry */
	CitusInvalidateRelcacheByRelid(distributedRelationId);
}


/*
 * DeleteShardPlacementRow opens the shard placement system catalog, finds the
 * first (unique) row that corresponds to the given shardId and worker node, and
 * deletes this row.
 */
void
DeleteShardPlacementRow(uint64 shardId, char *workerName, uint32 workerPort)
{
	Relation pgDistShardPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	bool heapTupleFound = false;

	pgDistShardPlacement = heap_open(DistShardPlacementRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_placement_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShardPlacement,
										DistShardPlacementShardidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistShardPlacement);

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

	simple_heap_delete(pgDistShardPlacement, &heapTuple->t_self);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistShardPlacement, RowExclusiveLock);
}


/*
 * BuildDistributionKeyFromColumnName builds a simple distribution key consisting
 * only out of a reference to the column of name columnName. Errors out if the
 * specified column does not exist or is not suitable to be used as a
 * distribution column.
 */
Node *
BuildDistributionKeyFromColumnName(Relation distributedRelation, char *columnName)
{
	HeapTuple columnTuple = NULL;
	Form_pg_attribute columnForm = NULL;
	Var *column = NULL;
	char *tableName = RelationGetRelationName(distributedRelation);

	/* it'd probably better to downcase identifiers consistent with SQL case folding */
	truncate_identifier(columnName, strlen(columnName), true);

	/* lookup column definition */
	columnTuple = SearchSysCacheAttName(RelationGetRelid(distributedRelation),
										columnName);
	if (!HeapTupleIsValid(columnTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" of relation \"%s\" does not exist",
							   columnName, tableName)));
	}

	columnForm = (Form_pg_attribute) GETSTRUCT(columnTuple);

	/* check if the column may be referenced in the distribution key */
	if (columnForm->attnum <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot reference system column \"%s\" in relation \"%s\"",
							   columnName, tableName)));
	}

	/* build Var referencing only the chosen distribution column */
	column = makeVar(1, columnForm->attnum, columnForm->atttypid,
					 columnForm->atttypmod, columnForm->attcollation, 0);

	ReleaseSysCache(columnTuple);

	return (Node *) column;
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

#if (PG_VERSION_NUM < 90500)
	return GetUserNameFromId(userId);
#else
	return GetUserNameFromId(userId, false);
#endif
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
	InsertShardPlacementRow(shardId, shardState, shardLength, nodeName, nodePort);

	heap_close(relation, NoLock);
	heap_close(pgDistShard, NoLock);

	PG_RETURN_VOID();
}
