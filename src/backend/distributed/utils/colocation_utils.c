/*-------------------------------------------------------------------------
 *
 * colocation_utils.c
 *
 * This file contains functions to perform useful operations on co-located tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/shardinterval_utils.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"


/*
 * TableColocationId function returns co-location id of given table. This function errors
 * out if given table is not distributed.
 */
uint64
TableColocationId(Oid distributedTableId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);

	return cacheEntry->colocationId;
}


/*
 * TablesColocated function checks whether given two tables are co-located and
 * returns true if they are co-located. A table is always co-located with itself.
 * If given two tables are different and they are not distributed, this function
 * errors out.
 */
bool
TablesColocated(Oid leftDistributedTableId, Oid rightDistributedTableId)
{
	uint64 leftColocationId = INVALID_COLOCATION_ID;
	uint64 rightColocationId = INVALID_COLOCATION_ID;

	if (leftDistributedTableId == rightDistributedTableId)
	{
		return true;
	}

	leftColocationId = TableColocationId(leftDistributedTableId);
	rightColocationId = TableColocationId(rightDistributedTableId);
	if (leftColocationId == INVALID_COLOCATION_ID ||
		rightColocationId == INVALID_COLOCATION_ID)
	{
		return false;
	}

	return leftColocationId == rightColocationId;
}


/*
 * ShardsColocated function checks whether given two shards are co-located and
 * returns true if they are co-located. Two shards are co-located either;
 * - They are same (A shard is always co-located with itself).
 * OR
 * - Tables are hash partitioned.
 * - Tables containing the shards are co-located.
 * - Min/Max values of the shards are same.
 */
bool
ShardsColocated(ShardInterval *leftShardInterval, ShardInterval *rightShardInterval)
{
	bool tablesColocated = TablesColocated(leftShardInterval->relationId,
										   rightShardInterval->relationId);

	if (tablesColocated)
	{
		/*
		 * We do min/max value check here to decide whether two shards are co=located,
		 * instead we can simply use FindShardIntervalIndex function on both shards then
		 * but do index check, but we avoid it because this way it is more cheaper.
		 *
		 * Having co-located tables implies that tables are partitioned by hash partition
		 * therefore it is safe to use DatumGetInt32 here.
		 */
		int32 leftShardMinValue = DatumGetInt32(leftShardInterval->minValue);
		int32 leftShardMaxValue = DatumGetInt32(leftShardInterval->maxValue);
		int32 rightShardMinValue = DatumGetInt32(rightShardInterval->minValue);
		int32 rightShardMaxValue = DatumGetInt32(rightShardInterval->maxValue);

		bool minValuesEqual = leftShardMinValue == rightShardMinValue;
		bool maxValuesEqual = leftShardMaxValue == rightShardMaxValue;

		return minValuesEqual && maxValuesEqual;
	}

	return false;
}


/*
 * ColocatedTableList function returns list of relation ids which are co-located
 * with given table. If given table is not hash distributed, co-location is not
 * valid for that table and it is only co-located with itself.
 */
List *
ColocatedTableList(Oid distributedTableId)
{
	int tableColocationId = TableColocationId(distributedTableId);
	List *colocatedTableList = NIL;

	Relation pgDistPartition = NULL;
	TupleDesc tupleDescriptor = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	bool indexOK = true;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	/*
	 * If distribution type of the table is not hash, the table is only co-located
	 * with itself.
	 */
	if (tableColocationId == INVALID_COLOCATION_ID)
	{
		colocatedTableList = lappend_oid(colocatedTableList, distributedTableId);
		return colocatedTableList;
	}

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_colocationid,
				BTEqualStrategyNumber, F_INT8EQ, ObjectIdGetDatum(tableColocationId));

	pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(pgDistPartition);
	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionColocationidIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Oid colocatedTableId = heap_getattr(heapTuple,
											Anum_pg_dist_partition_logicalrelid,
											tupleDescriptor, &isNull);

		colocatedTableList = lappend_oid(colocatedTableList, colocatedTableId);
		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, AccessShareLock);

	return colocatedTableList;
}


/*
 * ColocatedShardPlacementList function returns list of shard intervals which are
 * co-located with given shard. If given shard is belong to append or range distributed
 * table, co-location is not valid for that shard. Therefore such shard is only co-located
 * with itself.
 */
List *
ColocatedShardPlacementList(ShardInterval *shardInterval)
{
	Oid distributedTableId = shardInterval->relationId;
	List *colocatedShardList = NIL;
	int shardIntervalIndex = -1;
	List *colocatedTableList = NIL;
	ListCell *colocatedTableCell = NULL;

	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;

	/*
	 * If distribution type of the table is not hash, each shard of the table is only
	 * co-located with itself.
	 */
	if (partitionMethod != DISTRIBUTE_BY_HASH)
	{
		colocatedShardList = lappend(colocatedShardList, shardInterval);
		return colocatedShardList;
	}

	shardIntervalIndex = FindShardIntervalIndex(shardInterval);
	colocatedTableList = ColocatedTableList(distributedTableId);

	/* FindShardIntervalIndex have to find index of given shard */
	Assert(shardIntervalIndex >= 0);

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);
		DistTableCacheEntry *colocatedTableCacheEntry =
			DistributedTableCacheEntry(colocatedTableId);
		ShardInterval *colocatedShardInterval = NULL;

		/*
		 * Since we iterate over co-located tables, shard count of each table should be
		 * same and greater than shardIntervalIndex.
		 */
		Assert(cacheEntry->shardIntervalArrayLength ==
			   colocatedTableCacheEntry->shardIntervalArrayLength);

		colocatedShardInterval =
			colocatedTableCacheEntry->sortedShardIntervalArray[shardIntervalIndex];

		colocatedShardList = lappend(colocatedShardList, colocatedShardInterval);
	}

	Assert(list_length(colocatedTableList) == list_length(colocatedShardList));

	return colocatedShardList;
}
