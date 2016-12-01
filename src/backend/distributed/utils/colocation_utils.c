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
#include "access/xact.h"
#include "catalog/indexing.h"
#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* local function forward declarations */
static void MarkTablesColocated(Oid sourceRelationId, Oid targetRelationId);
static void ErrorIfShardPlacementsNotColocated(Oid leftRelationId, Oid rightRelationId);
static bool ShardsIntervalsEqual(ShardInterval *leftShardInterval,
								 ShardInterval *rightShardInterval);
static int CompareShardPlacementsByNode(const void *leftElement,
										const void *rightElement);
static void UpdateRelationColocationGroup(Oid distributedRelationId, uint32 colocationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(mark_tables_colocated);


/*
 * mark_tables_colocated puts target tables to same colocation group with the
 * source table. If the source table is in INVALID_COLOCATION_ID group, then it
 * creates a new colocation group and assigns all tables to this new colocation
 * group.
 */
Datum
mark_tables_colocated(PG_FUNCTION_ARGS)
{
	Oid sourceRelationId = PG_GETARG_OID(0);
	ArrayType *relationIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);
	Datum *relationIdDatumArray = NULL;
	int relationIndex = 0;

	int relationCount = ArrayObjectCount(relationIdArrayObject);
	if (relationCount < 1)
	{
		ereport(ERROR, (errmsg("at least one target table is required for this "
							   "operation")));
	}

	relationIdDatumArray = DeconstructArrayObject(relationIdArrayObject);

	for (relationIndex = 0; relationIndex < relationCount; relationIndex++)
	{
		Oid nextRelationOid = DatumGetObjectId(relationIdDatumArray[relationIndex]);
		MarkTablesColocated(sourceRelationId, nextRelationOid);
	}

	PG_RETURN_VOID();
}


/*
 * MarkTablesColocated puts both tables to same colocation group. If the
 * source table is in INVALID_COLOCATION_ID group, then it creates a new
 * colocation group and assigns both tables to same colocation group. Otherwise,
 * it adds the target table to colocation group of the source table.
 */
static void
MarkTablesColocated(Oid sourceRelationId, Oid targetRelationId)
{
	uint32 sourceColocationId = INVALID_COLOCATION_ID;
	Relation pgDistColocation = NULL;
	Var *sourceDistributionColumn = NULL;
	Var *targetDistributionColumn = NULL;
	Oid sourceDistributionColumnType = InvalidOid;
	Oid targetDistributionColumnType = InvalidOid;
	bool defaultColocationGroup = false;

	CheckHashPartitionedTable(sourceRelationId);
	CheckHashPartitionedTable(targetRelationId);

	sourceDistributionColumn = PartitionKey(sourceRelationId);
	sourceDistributionColumnType = sourceDistributionColumn->vartype;

	targetDistributionColumn = PartitionKey(targetRelationId);
	targetDistributionColumnType = targetDistributionColumn->vartype;

	if (sourceDistributionColumnType != targetDistributionColumnType)
	{
		char *sourceRelationName = get_rel_name(sourceRelationId);
		char *targetRelationName = get_rel_name(targetRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, targetRelationName),
						errdetail("Distribution column types don't match for "
								  "%s and %s.", sourceRelationName,
								  targetRelationName)));
	}

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	/* check if shard placements are colocated */
	ErrorIfShardPlacementsNotColocated(sourceRelationId, targetRelationId);

	/*
	 * Get colocation group of the source table, if the source table does not
	 * have a colocation group, create a new one, and set it for the source table.
	 */
	sourceColocationId = TableColocationId(sourceRelationId);
	if (sourceColocationId == INVALID_COLOCATION_ID)
	{
		uint32 shardCount = ShardIntervalCount(sourceRelationId);
		uint32 shardReplicationFactor = TableShardReplicationFactor(sourceRelationId);
		uint32 defaultColocationId = INVALID_COLOCATION_ID;

		/* check if there is a default colocation group */
		defaultColocationId = DefaultColocationGroupId(shardCount, shardReplicationFactor,
													   sourceDistributionColumnType);
		if (defaultColocationId == INVALID_COLOCATION_ID)
		{
			defaultColocationGroup = true;
		}

		sourceColocationId = CreateColocationGroup(shardCount, shardReplicationFactor,
												   sourceDistributionColumnType,
												   defaultColocationGroup);
		UpdateRelationColocationGroup(sourceRelationId, sourceColocationId);
	}

	/* finally set colocation group for the target relation */
	UpdateRelationColocationGroup(targetRelationId, sourceColocationId);

	heap_close(pgDistColocation, NoLock);
}


/*
 * ErrorIfShardPlacementsNotColocated checks if the shard placements of the
 * given two relations are physically colocated. It errors out in any of
 * following cases:
 * 1.Shard counts are different,
 * 2.Shard intervals don't match
 * 3.Matching shard intervals have different number of shard placements
 * 4.Shard placements are not colocated (not on the same node)
 * 5.Shard placements have different health states
 *
 * Note that, this functions assumes that both tables are hash distributed.
 */
static void
ErrorIfShardPlacementsNotColocated(Oid leftRelationId, Oid rightRelationId)
{
	List *leftShardIntervalList = NIL;
	List *rightShardIntervalList = NIL;
	ListCell *leftShardIntervalCell = NULL;
	ListCell *rightShardIntervalCell = NULL;
	char *leftRelationName = NULL;
	char *rightRelationName = NULL;
	uint32 leftShardCount = 0;
	uint32 rightShardCount = 0;

	/* get sorted shard interval lists for both tables */
	leftShardIntervalList = LoadShardIntervalList(leftRelationId);
	rightShardIntervalList = LoadShardIntervalList(rightRelationId);

	/* prevent concurrent placement changes */
	LockShardListMetadata(leftShardIntervalList, ShareLock);
	LockShardListMetadata(rightShardIntervalList, ShareLock);

	leftRelationName = get_rel_name(leftRelationId);
	rightRelationName = get_rel_name(rightRelationId);

	leftShardCount = list_length(leftShardIntervalList);
	rightShardCount = list_length(rightShardIntervalList);

	if (leftShardCount != rightShardCount)
	{
		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   leftRelationName, rightRelationName),
						errdetail("Shard counts don't match for %s and %s.",
								  leftRelationName, rightRelationName)));
	}

	/* compare shard intervals one by one */
	forboth(leftShardIntervalCell, leftShardIntervalList,
			rightShardIntervalCell, rightShardIntervalList)
	{
		ShardInterval *leftInterval = (ShardInterval *) lfirst(leftShardIntervalCell);
		ShardInterval *rightInterval = (ShardInterval *) lfirst(rightShardIntervalCell);

		List *leftPlacementList = NIL;
		List *rightPlacementList = NIL;
		List *sortedLeftPlacementList = NIL;
		List *sortedRightPlacementList = NIL;
		ListCell *leftPlacementCell = NULL;
		ListCell *rightPlacementCell = NULL;

		uint64 leftShardId = leftInterval->shardId;
		uint64 rightShardId = rightInterval->shardId;

		bool shardsIntervalsEqual = ShardsIntervalsEqual(leftInterval, rightInterval);
		if (!shardsIntervalsEqual)
		{
			ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
								   leftRelationName, rightRelationName),
							errdetail("Shard intervals don't match for %s and %s.",
									  leftRelationName, rightRelationName)));
		}

		leftPlacementList = ShardPlacementList(leftShardId);
		rightPlacementList = ShardPlacementList(rightShardId);

		if (list_length(leftPlacementList) != list_length(rightPlacementList))
		{
			ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
								   leftRelationName, rightRelationName),
							errdetail("Shard %ld of %s and shard %ld of %s "
									  "have different number of shard placements.",
									  leftShardId, leftRelationName,
									  rightShardId, rightRelationName)));
		}

		/* sort shard placements according to the node */
		sortedLeftPlacementList = SortList(leftPlacementList,
										   CompareShardPlacementsByNode);
		sortedRightPlacementList = SortList(rightPlacementList,
											CompareShardPlacementsByNode);

		/* compare shard placements one by one */
		forboth(leftPlacementCell, sortedLeftPlacementList,
				rightPlacementCell, sortedRightPlacementList)
		{
			ShardPlacement *leftPlacement =
				(ShardPlacement *) lfirst(leftPlacementCell);
			ShardPlacement *rightPlacement =
				(ShardPlacement *) lfirst(rightPlacementCell);
			int nodeCompare = 0;

			/*
			 * If shard placements are on different nodes, these shard
			 * placements are not colocated.
			 */
			nodeCompare = CompareShardPlacementsByNode((void *) &leftPlacement,
													   (void *) &rightPlacement);
			if (nodeCompare != 0)
			{
				ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
									   leftRelationName, rightRelationName),
								errdetail("Shard %ld of %s and shard %ld of %s "
										  "are not colocated.",
										  leftShardId, leftRelationName,
										  rightShardId, rightRelationName)));
			}

			/* we also don't allow colocated shards to be in different shard states */
			if (leftPlacement->shardState != rightPlacement->shardState)
			{
				ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
									   leftRelationName, rightRelationName),
								errdetail("%s and %s have shard placements in "
										  "different shard states.",
										  leftRelationName, rightRelationName)));
			}
		}
	}
}


/*
 * ShardsIntervalsEqual checks if two shard intervals of hash distributed
 * tables are equal. Note that, this function doesn't work with non-hash
 * partitioned table's shards.
 *
 * We do min/max value check here to decide whether two shards are colocated,
 * instead we can simply use FindShardIntervalIndex function on both shards then
 * but do index check, but we avoid it because this way it is more cheaper.
 */
static bool
ShardsIntervalsEqual(ShardInterval *leftShardInterval, ShardInterval *rightShardInterval)
{
	int32 leftShardMinValue = DatumGetInt32(leftShardInterval->minValue);
	int32 leftShardMaxValue = DatumGetInt32(leftShardInterval->maxValue);
	int32 rightShardMinValue = DatumGetInt32(rightShardInterval->minValue);
	int32 rightShardMaxValue = DatumGetInt32(rightShardInterval->maxValue);

	bool minValuesEqual = leftShardMinValue == rightShardMinValue;
	bool maxValuesEqual = leftShardMaxValue == rightShardMaxValue;

	return minValuesEqual && maxValuesEqual;
}


/*
 * CompareShardPlacementsByNode compares two shard placements by their nodename
 * and nodeport.
 */
static int
CompareShardPlacementsByNode(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	char *leftNodeName = leftPlacement->nodeName;
	char *rightNodeName = rightPlacement->nodeName;

	uint32 leftNodePort = leftPlacement->nodePort;
	uint32 rightNodePort = rightPlacement->nodePort;

	/* first compare node names */
	int nodeNameCompare = strncmp(leftNodeName, rightNodeName, WORKER_LENGTH);
	if (nodeNameCompare != 0)
	{
		return nodeNameCompare;
	}

	/* if node names are same, check node ports */
	if (leftNodePort < rightNodePort)
	{
		return -1;
	}
	else if (leftNodePort > rightNodePort)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


/*
 * UpdateRelationColocationGroup updates colocation group in pg_dist_partition
 * for the given relation.
 */
static void
UpdateRelationColocationGroup(Oid distributedRelationId, uint32 colocationId)
{
	Relation pgDistPartition = NULL;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	SysScanDesc scanDescriptor = NULL;
	bool indexOK = true;
	int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	Datum values[Natts_pg_dist_partition];
	bool isNull[Natts_pg_dist_partition];
	bool replace[Natts_pg_dist_partition];

	pgDistPartition = heap_open(DistPartitionRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistPartition);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributedRelationId));

	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionLogicalRelidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		char *distributedRelationName = get_rel_name(distributedRelationId);
		ereport(ERROR, (errmsg("could not find valid entry for relation %s",
							   distributedRelationName)));
	}

	memset(values, 0, sizeof(replace));
	memset(isNull, false, sizeof(isNull));
	memset(replace, false, sizeof(replace));

	values[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	isNull[Anum_pg_dist_partition_colocationid - 1] = false;
	replace[Anum_pg_dist_partition_colocationid - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isNull, replace);
	simple_heap_update(pgDistPartition, &heapTuple->t_self, heapTuple);

	CatalogUpdateIndexes(pgDistPartition, heapTuple);
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, NoLock);
}


/*
 * TableColocationId function returns co-location id of given table. This function
 * errors out if given table is not distributed.
 */
uint32
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
	uint32 leftColocationId = INVALID_COLOCATION_ID;
	uint32 rightColocationId = INVALID_COLOCATION_ID;

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
		bool shardIntervalEqual = ShardsIntervalsEqual(leftShardInterval,
													   rightShardInterval);
		return shardIntervalEqual;
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
	uint32 tableColocationId = TableColocationId(distributedTableId);
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
				BTEqualStrategyNumber, F_INT4EQ, ObjectIdGetDatum(tableColocationId));

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
 * ColocatedShardIntervalList function returns list of shard intervals which are
 * co-located with given shard. If given shard is belong to append or range distributed
 * table, co-location is not valid for that shard. Therefore such shard is only co-located
 * with itself.
 */
List *
ColocatedShardIntervalList(ShardInterval *shardInterval)
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


/*
 * ColocatedTableId returns an arbitrary table which belongs to given colocation
 * group. If there is not such a colocation group, it returns invalid oid.
 */
Oid
ColocatedTableId(Oid colocationId)
{
	Oid colocatedTableId = InvalidOid;
	Relation pgDistPartition = NULL;
	TupleDesc tupleDescriptor = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	bool indexOK = true;
	bool isNull = false;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, ObjectIdGetDatum(colocationId));

	/* prevent DELETE statements */
	pgDistPartition = heap_open(DistPartitionRelationId(), ShareLock);
	tupleDescriptor = RelationGetDescr(pgDistPartition);
	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionColocationidIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		colocatedTableId = heap_getattr(heapTuple, Anum_pg_dist_partition_logicalrelid,
										tupleDescriptor, &isNull);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, ShareLock);

	return colocatedTableId;
}


/*
 * ColocatedShardIdInRelation returns shardId of the shard from given relation, so that
 * returned shard is co-located with given shard.
 */
uint64
ColocatedShardIdInRelation(Oid relationId, int shardIndex)
{
	DistTableCacheEntry *tableCacheEntry = DistributedTableCacheEntry(relationId);

	return tableCacheEntry->sortedShardIntervalArray[shardIndex]->shardId;
}
