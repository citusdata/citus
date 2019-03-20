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
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "distributed/colocation_utils.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* local function forward declarations */
static void MarkTablesColocated(Oid sourceRelationId, Oid targetRelationId);
static void ErrorIfShardPlacementsNotColocated(Oid leftRelationId, Oid rightRelationId);
static bool ShardsIntervalsEqual(ShardInterval *leftShardInterval,
								 ShardInterval *rightShardInterval);
static bool HashPartitionedShardIntervalsEqual(ShardInterval *leftShardInterval,
											   ShardInterval *rightShardInterval);
static int CompareShardPlacementsByNode(const void *leftElement,
										const void *rightElement);
static void UpdateRelationColocationGroup(Oid distributedRelationId, uint32 colocationId);
static List * ColocationGroupTableList(Oid colocationId);
static void DeleteColocationGroup(uint32 colocationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(mark_tables_colocated);
PG_FUNCTION_INFO_V1(get_colocated_shard_array);


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

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureTableOwner(sourceRelationId);

	relationIdDatumArray = DeconstructArrayObject(relationIdArrayObject);

	for (relationIndex = 0; relationIndex < relationCount; relationIndex++)
	{
		Oid nextRelationOid = DatumGetObjectId(relationIdDatumArray[relationIndex]);

		/* we require that the user either owns all tables or is superuser */
		EnsureTableOwner(nextRelationOid);

		MarkTablesColocated(sourceRelationId, nextRelationOid);
	}

	PG_RETURN_VOID();
}


/*
 * get_colocated_shards_array returns array of shards ids which are co-located with given
 * shard.
 */
Datum
get_colocated_shard_array(PG_FUNCTION_ARGS)
{
	uint32 shardId = PG_GETARG_UINT32(0);
	ShardInterval *shardInterval = LoadShardInterval(shardId);

	ArrayType *colocatedShardsArrayType = NULL;
	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	ListCell *colocatedShardCell = NULL;
	int colocatedShardCount = list_length(colocatedShardList);
	Datum *colocatedShardsDatumArray = palloc0(colocatedShardCount * sizeof(Datum));
	Oid arrayTypeId = OIDOID;
	int colocatedShardIndex = 0;

	/* sort to get consistent output */
	colocatedShardList = SortList(colocatedShardList, CompareShardIntervalsById);

	foreach(colocatedShardCell, colocatedShardList)
	{
		ShardInterval *colocatedShardInterval = (ShardInterval *) lfirst(
			colocatedShardCell);
		uint64 colocatedShardId = colocatedShardInterval->shardId;

		Datum colocatedShardDatum = Int64GetDatum(colocatedShardId);

		colocatedShardsDatumArray[colocatedShardIndex] = colocatedShardDatum;
		colocatedShardIndex++;
	}

	colocatedShardsArrayType = DatumArrayToArrayType(colocatedShardsDatumArray,
													 colocatedShardCount, arrayTypeId);

	PG_RETURN_ARRAYTYPE_P(colocatedShardsArrayType);
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
	uint32 targetColocationId = INVALID_COLOCATION_ID;
	Relation pgDistColocation = NULL;

	CheckReplicationModel(sourceRelationId, targetRelationId);
	CheckDistributionColumnType(sourceRelationId, targetRelationId);

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

		Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
		Oid sourceDistributionColumnType = InvalidOid;

		/* reference tables has NULL distribution column */
		if (sourceDistributionColumn != NULL)
		{
			sourceDistributionColumnType = sourceDistributionColumn->vartype;
		}

		sourceColocationId = CreateColocationGroup(shardCount, shardReplicationFactor,
												   sourceDistributionColumnType);
		UpdateRelationColocationGroup(sourceRelationId, sourceColocationId);
	}

	targetColocationId = TableColocationId(targetRelationId);

	/* finally set colocation group for the target relation */
	UpdateRelationColocationGroup(targetRelationId, sourceColocationId);

	/* if there is not any remaining table in the colocation group, delete it */
	DeleteColocationGroupIfNoTablesBelong(targetColocationId);

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
							errdetail("Shard " UINT64_FORMAT
									  " of %s and shard " UINT64_FORMAT
									  " of %s have different number of shard placements.",
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
								errdetail("Shard " UINT64_FORMAT " of %s and shard "
										  UINT64_FORMAT " of %s are not colocated.",
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
 * ShardsIntervalsEqual checks if two shard intervals of distributed
 * tables are equal.
 *
 * Notes on the function:
 * (i)   The function returns true if both shard intervals are the same.
 * (ii)  The function returns false even if the shard intervals equal, but,
 *       their distribution method are different.
 * (iii) The function returns false for append and range partitioned tables
 *       excluding (i) case.
 * (iv)  For reference tables, all shards are equal (i.e., same replication factor
 *       and shard min/max values). Thus, always return true for shards of reference
 *       tables.
 */
static bool
ShardsIntervalsEqual(ShardInterval *leftShardInterval, ShardInterval *rightShardInterval)
{
	char leftIntervalPartitionMethod = PartitionMethod(leftShardInterval->relationId);
	char rightIntervalPartitionMethod = PartitionMethod(rightShardInterval->relationId);

	/* if both shards are  the same, return true */
	if (leftShardInterval->shardId == rightShardInterval->shardId)
	{
		return true;
	}

	/* if partition methods are not the same, shards cannot be considered as co-located */
	leftIntervalPartitionMethod = PartitionMethod(leftShardInterval->relationId);
	rightIntervalPartitionMethod = PartitionMethod(rightShardInterval->relationId);
	if (leftIntervalPartitionMethod != rightIntervalPartitionMethod)
	{
		return false;
	}

	if (leftIntervalPartitionMethod == DISTRIBUTE_BY_HASH)
	{
		return HashPartitionedShardIntervalsEqual(leftShardInterval, rightShardInterval);
	}
	else if (leftIntervalPartitionMethod == DISTRIBUTE_BY_NONE)
	{
		/*
		 * Reference tables has only a single shard and all reference tables
		 * are always co-located with each other.
		 */

		return true;
	}

	/* append and range partitioned shard never co-located */
	return false;
}


/*
 * HashPartitionedShardIntervalsEqual checks if two shard intervals of hash distributed
 * tables are equal. Note that, this function doesn't work with non-hash
 * partitioned table's shards.
 *
 * We do min/max value check here to decide whether two shards are colocated,
 * instead we can simply use ShardIndex function on both shards then
 * but do index check, but we avoid it because this way it is more cheaper.
 */
static bool
HashPartitionedShardIntervalsEqual(ShardInterval *leftShardInterval,
								   ShardInterval *rightShardInterval)
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

	/* if node names are same, check node ports */
	if (leftPlacement->nodeId < rightPlacement->nodeId)
	{
		return -1;
	}
	else if (leftPlacement->nodeId > rightPlacement->nodeId)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


/*
 * ColocationId searches pg_dist_colocation for shard count, replication factor
 * and distribution column type. If a matching entry is found, it returns the
 * colocation id, otherwise it returns INVALID_COLOCATION_ID.
 */
uint32
ColocationId(int shardCount, int replicationFactor, Oid distributionColumnType)
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	HeapTuple colocationTuple = NULL;
	SysScanDesc scanDescriptor;
	const int scanKeyCount = 3;
	ScanKeyData scanKey[3];
	bool indexOK = true;

	Relation pgDistColocation = heap_open(DistColocationRelationId(), AccessShareLock);

	/* set scan arguments */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_colocation_shardcount,
				BTEqualStrategyNumber, F_INT4EQ, UInt32GetDatum(shardCount));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_colocation_replicationfactor,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(replicationFactor));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_colocation_distributioncolumntype,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributionColumnType));

	scanDescriptor = systable_beginscan(pgDistColocation,
										DistColocationConfigurationIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	colocationTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(colocationTuple))
	{
		Form_pg_dist_colocation colocationForm =
			(Form_pg_dist_colocation) GETSTRUCT(colocationTuple);

		colocationId = colocationForm->colocationid;
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistColocation, AccessShareLock);

	return colocationId;
}


/*
 * CreateColocationGroup creates a new colocation id and writes it into
 * pg_dist_colocation with the given configuration. It also returns the created
 * colocation id.
 */
uint32
CreateColocationGroup(int shardCount, int replicationFactor, Oid distributionColumnType)
{
	uint32 colocationId = GetNextColocationId();
	Relation pgDistColocation = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_colocation];
	bool isNulls[Natts_pg_dist_colocation];

	/* form new colocation tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_colocation_colocationid - 1] = UInt32GetDatum(colocationId);
	values[Anum_pg_dist_colocation_shardcount - 1] = UInt32GetDatum(shardCount);
	values[Anum_pg_dist_colocation_replicationfactor - 1] =
		UInt32GetDatum(replicationFactor);
	values[Anum_pg_dist_colocation_distributioncolumntype - 1] =
		ObjectIdGetDatum(distributionColumnType);

	/* open colocation relation and insert the new tuple */
	pgDistColocation = heap_open(DistColocationRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistColocation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistColocation, heapTuple);

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();
	heap_close(pgDistColocation, RowExclusiveLock);

	return colocationId;
}


/*
 * GetNextColocationId allocates and returns a unique colocationId for the
 * colocation group to be created. This allocation occurs both in shared memory
 * and in write ahead logs; writing to logs avoids the risk of having
 * colocationId collisions.
 *
 * Please note that the caller is still responsible for finalizing colocationId
 * with the master node. Further note that this function relies on an internal
 * sequence created in initdb to generate unique identifiers.
 */
uint32
GetNextColocationId()
{
	text *sequenceName = cstring_to_text(COLOCATIONID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum colocationIdDatum = 0;
	uint32 colocationId = INVALID_COLOCATION_ID;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique colocation id from sequence */
	colocationIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	colocationId = DatumGetUInt32(colocationIdDatum);

	return colocationId;
}


/*
 * CheckReplicationModel checks if given relations are from the same
 * replication model. Otherwise, it errors out.
 */
void
CheckReplicationModel(Oid sourceRelationId, Oid targetRelationId)
{
	DistTableCacheEntry *sourceTableEntry = NULL;
	DistTableCacheEntry *targetTableEntry = NULL;
	char sourceReplicationModel = 0;
	char targetReplicationModel = 0;

	sourceTableEntry = DistributedTableCacheEntry(sourceRelationId);
	sourceReplicationModel = sourceTableEntry->replicationModel;

	targetTableEntry = DistributedTableCacheEntry(targetRelationId);
	targetReplicationModel = targetTableEntry->replicationModel;

	if (sourceReplicationModel != targetReplicationModel)
	{
		char *sourceRelationName = get_rel_name(sourceRelationId);
		char *targetRelationName = get_rel_name(targetRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, targetRelationName),
						errdetail("Replication models don't match for %s and %s.",
								  sourceRelationName, targetRelationName)));
	}
}


/*
 * CheckDistributionColumnType checks if distribution column types of relations
 * are same. Otherwise, it errors out.
 */
void
CheckDistributionColumnType(Oid sourceRelationId, Oid targetRelationId)
{
	Var *sourceDistributionColumn = NULL;
	Var *targetDistributionColumn = NULL;
	Oid sourceDistributionColumnType = InvalidOid;
	Oid targetDistributionColumnType = InvalidOid;

	/* reference tables have NULL distribution column */
	sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	if (sourceDistributionColumn == NULL)
	{
		sourceDistributionColumnType = InvalidOid;
	}
	else
	{
		sourceDistributionColumnType = sourceDistributionColumn->vartype;
	}

	/* reference tables have NULL distribution column */
	targetDistributionColumn = DistPartitionKey(targetRelationId);
	if (targetDistributionColumn == NULL)
	{
		targetDistributionColumnType = InvalidOid;
	}
	else
	{
		targetDistributionColumnType = targetDistributionColumn->vartype;
	}

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
	bool shouldSyncMetadata = false;
	bool indexOK = true;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
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

	memset(values, 0, sizeof(values));
	memset(isNull, false, sizeof(isNull));
	memset(replace, false, sizeof(replace));

	values[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	isNull[Anum_pg_dist_partition_colocationid - 1] = false;
	replace[Anum_pg_dist_partition_colocationid - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isNull, replace);


	CatalogTupleUpdate(pgDistPartition, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(distributedRelationId);

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, NoLock);

	shouldSyncMetadata = ShouldSyncTableMetadata(distributedRelationId);
	if (shouldSyncMetadata)
	{
		char *updateColocationIdCommand = ColocationIdUpdateCommand(distributedRelationId,
																	colocationId);

		SendCommandToWorkers(WORKERS_WITH_METADATA, updateColocationIdCommand);
	}
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

	/*
	 * If distribution type of the table is not hash, the table is only co-located
	 * with itself.
	 */
	if (tableColocationId == INVALID_COLOCATION_ID)
	{
		colocatedTableList = lappend_oid(colocatedTableList, distributedTableId);
		return colocatedTableList;
	}

	colocatedTableList = ColocationGroupTableList(tableColocationId);

	return colocatedTableList;
}


/*
 * ColocationGroupTableList returns the list of tables in the given colocation
 * group. If the colocation group is INVALID_COLOCATION_ID, it returns NIL.
 */
static List *
ColocationGroupTableList(Oid colocationId)
{
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
	if (colocationId == INVALID_COLOCATION_ID)
	{
		return NIL;
	}

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, ObjectIdGetDatum(colocationId));

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
	 * If distribution type of the table is not hash or reference, each shard of
	 * the shard is only co-located with itself.
	 */
	if ((partitionMethod == DISTRIBUTE_BY_APPEND) ||
		(partitionMethod == DISTRIBUTE_BY_RANGE))
	{
		ShardInterval *copyShardInterval = CitusMakeNode(ShardInterval);
		CopyShardInterval(shardInterval, copyShardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);

		return colocatedShardList;
	}

	shardIntervalIndex = ShardIndex(shardInterval);
	colocatedTableList = ColocatedTableList(distributedTableId);

	/* ShardIndex have to find index of given shard */
	Assert(shardIntervalIndex >= 0);

	foreach(colocatedTableCell, colocatedTableList)
	{
		Oid colocatedTableId = lfirst_oid(colocatedTableCell);
		DistTableCacheEntry *colocatedTableCacheEntry =
			DistributedTableCacheEntry(colocatedTableId);
		ShardInterval *colocatedShardInterval = NULL;
		ShardInterval *copyShardInterval = NULL;

		/*
		 * Since we iterate over co-located tables, shard count of each table should be
		 * same and greater than shardIntervalIndex.
		 */
		Assert(cacheEntry->shardIntervalArrayLength ==
			   colocatedTableCacheEntry->shardIntervalArrayLength);

		colocatedShardInterval =
			colocatedTableCacheEntry->sortedShardIntervalArray[shardIntervalIndex];

		copyShardInterval = CitusMakeNode(ShardInterval);
		CopyShardInterval(colocatedShardInterval, copyShardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);
	}

	Assert(list_length(colocatedTableList) == list_length(colocatedShardList));

	return colocatedShardList;
}


/*
 * ColocatedTableId returns an arbitrary table which belongs to given colocation
 * group. If there is not such a colocation group, it returns invalid oid.
 *
 * This function also takes an AccessShareLock on the co-colocated table to
 * guarantee that the table isn't dropped for the remainder of the transaction.
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

	/*
	 * We may have a distributed table whose colocation id is INVALID_COLOCATION_ID.
	 * In this case, we do not want to send that table's id as colocated table id.
	 */
	if (colocationId == INVALID_COLOCATION_ID)
	{
		return colocatedTableId;
	}

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, ObjectIdGetDatum(colocationId));

	pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(pgDistPartition);
	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionColocationidIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Relation colocatedRelation = NULL;

		colocatedTableId = heap_getattr(heapTuple, Anum_pg_dist_partition_logicalrelid,
										tupleDescriptor, &isNull);

		/*
		 * Make sure the relation isn't dropped for the remainder of
		 * the transaction.
		 */
		LockRelationOid(colocatedTableId, AccessShareLock);

		/*
		 * The relation might have been dropped just before we locked it.
		 * Let's look it up.
		 */
		colocatedRelation = RelationIdGetRelation(colocatedTableId);
		if (RelationIsValid(colocatedRelation))
		{
			/* relation still exists, we can use it */
			RelationClose(colocatedRelation);
			break;
		}

		/* relation was dropped, try the next one */
		colocatedTableId = InvalidOid;

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, AccessShareLock);

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


/*
 * DeleteColocationGroupIfNoTablesBelong function deletes given co-location group if there
 * is no relation in that co-location group. A co-location group may become empty after
 * mark_tables_colocated or upgrade_reference_table UDF calls. In that case we need to
 * remove empty co-location group to prevent orphaned co-location groups.
 */
void
DeleteColocationGroupIfNoTablesBelong(uint32 colocationId)
{
	if (colocationId != INVALID_COLOCATION_ID)
	{
		List *colocatedTableList = ColocationGroupTableList(colocationId);
		int colocatedTableCount = list_length(colocatedTableList);

		if (colocatedTableCount == 0)
		{
			DeleteColocationGroup(colocationId);
		}
	}
}


/*
 * DeleteColocationGroup deletes the colocation group from pg_dist_colocation.
 */
static void
DeleteColocationGroup(uint32 colocationId)
{
	Relation pgDistColocation = NULL;
	SysScanDesc scanDescriptor = NULL;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = false;
	HeapTuple heapTuple = NULL;

	pgDistColocation = heap_open(DistColocationRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_colocation_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, UInt32GetDatum(colocationId));

	scanDescriptor = systable_beginscan(pgDistColocation, InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	/* if a record is found, delete it */
	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		simple_heap_delete(pgDistColocation, &(heapTuple->t_self));

		CitusInvalidateRelcacheByRelid(DistColocationRelationId());
		CommandCounterIncrement();
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistColocation, RowExclusiveLock);
}
