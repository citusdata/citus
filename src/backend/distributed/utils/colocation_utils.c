/*-------------------------------------------------------------------------
 *
 * colocation_utils.c
 *
 * This file contains functions to perform useful operations on co-located tables.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/utils/array_type.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"


/* local function forward declarations */
static void MarkTablesColocated(Oid sourceRelationId, Oid targetRelationId);
static bool ShardsIntervalsEqual(ShardInterval *leftShardInterval,
								 ShardInterval *rightShardInterval);
static bool HashPartitionedShardIntervalsEqual(ShardInterval *leftShardInterval,
											   ShardInterval *rightShardInterval);
static int CompareShardPlacementsByNode(const void *leftElement,
										const void *rightElement);
static uint32 CreateColocationGroupForRelation(Oid sourceRelationId);
static void BreakColocation(Oid sourceRelationId);
static uint32 SingleShardTableGetNodeId(Oid relationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(mark_tables_colocated);
PG_FUNCTION_INFO_V1(get_colocated_shard_array);
PG_FUNCTION_INFO_V1(update_distributed_table_colocation);


/*
 * mark_tables_colocated puts target tables to same colocation group with the
 * source table. If the source table is in INVALID_COLOCATION_ID group, then it
 * creates a new colocation group and assigns all tables to this new colocation
 * group.
 */
Datum
mark_tables_colocated(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid sourceRelationId = PG_GETARG_OID(0);
	ArrayType *relationIdArrayObject = PG_GETARG_ARRAYTYPE_P(1);

	int relationCount = ArrayObjectCount(relationIdArrayObject);
	if (relationCount < 1)
	{
		ereport(ERROR, (errmsg("at least one target table is required for this "
							   "operation")));
	}

	EnsureTableOwner(sourceRelationId);

	Datum *relationIdDatumArray = DeconstructArrayObject(relationIdArrayObject);

	for (int relationIndex = 0; relationIndex < relationCount; relationIndex++)
	{
		Oid nextRelationOid = DatumGetObjectId(relationIdDatumArray[relationIndex]);

		/* we require that the user either owns all tables or is superuser */
		EnsureTableOwner(nextRelationOid);

		MarkTablesColocated(sourceRelationId, nextRelationOid);
	}

	PG_RETURN_VOID();
}


/*
 * update_distributed_table_colocation updates the colocation of a table.
 * if colocate_with -> 'none' then the table is assigned a new
 * colocation group.
 */
Datum
update_distributed_table_colocation(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid targetRelationId = PG_GETARG_OID(0);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(1);

	EnsureTableOwner(targetRelationId);
	ErrorIfTenantTable(targetRelationId, TenantOperationNames[TENANT_UPDATE_COLOCATION]);

	char *colocateWithTableName = text_to_cstring(colocateWithTableNameText);
	if (IsColocateWithNone(colocateWithTableName))
	{
		EnsureHashOrSingleShardDistributedTable(targetRelationId);
		BreakColocation(targetRelationId);
	}
	else
	{
		Oid colocateWithTableId = ResolveRelationId(colocateWithTableNameText, false);
		ErrorIfTenantTable(colocateWithTableId,
						   TenantOperationNames[TENANT_COLOCATE_WITH]);
		EnsureTableOwner(colocateWithTableId);
		MarkTablesColocated(colocateWithTableId, targetRelationId);
	}
	PG_RETURN_VOID();
}


/*
 * IsColocateWithNone returns true if the given table is
 * the special keyword "none".
 */
bool
IsColocateWithNone(char *colocateWithTableName)
{
	return pg_strncasecmp(colocateWithTableName, "none", NAMEDATALEN) == 0;
}


/*
 * IsColocateWithDefault returns true if the given table is
 * the special keyword "default".
 */
bool
IsColocateWithDefault(char *colocateWithTableName)
{
	return pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0;
}


/*
 * BreakColocation breaks the colocations of the given relation id.
 * If t1, t2 and t3 are colocated and we call this function with t2,
 * t1 and t3 will stay colocated but t2 will have a new colocation id.
 * Note that this function does not move any data around for the new colocation.
 */
static void
BreakColocation(Oid sourceRelationId)
{
	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	Relation pgDistColocation = table_open(DistColocationRelationId(), ExclusiveLock);

	uint32 oldColocationId = TableColocationId(sourceRelationId);
	CreateColocationGroupForRelation(sourceRelationId);

	/* if there is not any remaining table in the old colocation group, delete it */
	DeleteColocationGroupIfNoTablesBelong(oldColocationId);

	table_close(pgDistColocation, NoLock);
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

	List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
	int colocatedShardCount = list_length(colocatedShardList);
	Datum *colocatedShardsDatumArray = palloc0(colocatedShardCount * sizeof(Datum));
	Oid arrayTypeId = OIDOID;
	int colocatedShardIndex = 0;

	ShardInterval *colocatedShardInterval = NULL;
	foreach_declared_ptr(colocatedShardInterval, colocatedShardList)
	{
		uint64 colocatedShardId = colocatedShardInterval->shardId;

		Datum colocatedShardDatum = Int64GetDatum(colocatedShardId);

		colocatedShardsDatumArray[colocatedShardIndex] = colocatedShardDatum;
		colocatedShardIndex++;
	}

	ArrayType *colocatedShardsArrayType = DatumArrayToArrayType(colocatedShardsDatumArray,
																colocatedShardCount,
																arrayTypeId);

	PG_RETURN_ARRAYTYPE_P(colocatedShardsArrayType);
}


/*
 * CreateColocationGroupForRelation creates colocation entry in
 * pg_dist_colocation and updated the colocation id in pg_dist_partition
 * for the given relation.
 */
static uint32
CreateColocationGroupForRelation(Oid sourceRelationId)
{
	uint32 shardCount = ShardIntervalCount(sourceRelationId);
	uint32 shardReplicationFactor = TableShardReplicationFactor(sourceRelationId);

	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	Oid sourceDistributionColumnType = InvalidOid;
	Oid sourceDistributionColumnCollation = InvalidOid;

	/* reference tables has NULL distribution column */
	if (sourceDistributionColumn != NULL)
	{
		sourceDistributionColumnType = sourceDistributionColumn->vartype;
		sourceDistributionColumnCollation = sourceDistributionColumn->varcollid;
	}

	uint32 sourceColocationId = CreateColocationGroup(shardCount, shardReplicationFactor,
													  sourceDistributionColumnType,
													  sourceDistributionColumnCollation);
	bool localOnly = false;
	UpdateRelationColocationGroup(sourceRelationId, sourceColocationId, localOnly);
	return sourceColocationId;
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
	if (IsCitusTableType(sourceRelationId, CITUS_LOCAL_TABLE) ||
		IsCitusTableType(targetRelationId, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errmsg("local tables cannot be colocated with "
							   "other tables")));
	}

	EnsureHashOrSingleShardDistributedTable(sourceRelationId);
	EnsureHashOrSingleShardDistributedTable(targetRelationId);
	CheckReplicationModel(sourceRelationId, targetRelationId);
	CheckDistributionColumnType(sourceRelationId, targetRelationId);

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	Relation pgDistColocation = table_open(DistColocationRelationId(), ExclusiveLock);

	/* check if shard placements are colocated */
	ErrorIfShardPlacementsNotColocated(sourceRelationId, targetRelationId);

	/*
	 * Get colocation group of the source table, if the source table does not
	 * have a colocation group, create a new one, and set it for the source table.
	 */
	uint32 sourceColocationId = TableColocationId(sourceRelationId);
	if (sourceColocationId == INVALID_COLOCATION_ID)
	{
		sourceColocationId = CreateColocationGroupForRelation(sourceRelationId);
	}

	uint32 targetColocationId = TableColocationId(targetRelationId);

	/* finally set colocation group for the target relation */
	bool localOnly = false;
	UpdateRelationColocationGroup(targetRelationId, sourceColocationId, localOnly);

	/* if there is not any remaining table in the colocation group, delete it */
	DeleteColocationGroupIfNoTablesBelong(targetColocationId);

	table_close(pgDistColocation, NoLock);
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
void
ErrorIfShardPlacementsNotColocated(Oid leftRelationId, Oid rightRelationId)
{
	/* get sorted shard interval lists for both tables */
	List *leftShardIntervalList = LoadShardIntervalList(leftRelationId);
	List *rightShardIntervalList = LoadShardIntervalList(rightRelationId);

	/* prevent concurrent placement changes */
	LockShardListMetadata(leftShardIntervalList, ShareLock);
	LockShardListMetadata(rightShardIntervalList, ShareLock);

	char *leftRelationName = get_rel_name(leftRelationId);
	char *rightRelationName = get_rel_name(rightRelationId);

	uint32 leftShardCount = list_length(leftShardIntervalList);
	uint32 rightShardCount = list_length(rightShardIntervalList);

	if (leftShardCount != rightShardCount)
	{
		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   leftRelationName, rightRelationName),
						errdetail("Shard counts don't match for %s and %s.",
								  leftRelationName, rightRelationName)));
	}

	/* compare shard intervals one by one */
	ShardInterval *leftInterval = NULL;
	ShardInterval *rightInterval = NULL;
	forboth_ptr(leftInterval, leftShardIntervalList,
				rightInterval, rightShardIntervalList)
	{
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

		List *leftPlacementList = ShardPlacementList(leftShardId);
		List *rightPlacementList = ShardPlacementList(rightShardId);

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
		List *sortedLeftPlacementList = SortList(leftPlacementList,
												 CompareShardPlacementsByNode);
		List *sortedRightPlacementList = SortList(rightPlacementList,
												  CompareShardPlacementsByNode);

		/* compare shard placements one by one */
		ShardPlacement *leftPlacement = NULL;
		ShardPlacement *rightPlacement = NULL;
		forboth_ptr(leftPlacement, sortedLeftPlacementList,
					rightPlacement, sortedRightPlacementList)
		{
			/*
			 * If shard placements are on different nodes, these shard
			 * placements are not colocated.
			 */
			int nodeCompare = CompareShardPlacementsByNode((void *) &leftPlacement,
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

	/* if both shards are the same, return true */
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

	if (IsCitusTableType(leftShardInterval->relationId, HASH_DISTRIBUTED))
	{
		return HashPartitionedShardIntervalsEqual(leftShardInterval, rightShardInterval);
	}
	else if (!HasDistributionKey(leftShardInterval->relationId))
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
 * ColocationId searches pg_dist_colocation for shard count, replication factor,
 * distribution column type, and distribution column collation. If a matching entry
 * is found, it returns the colocation id, otherwise returns INVALID_COLOCATION_ID.
 */
uint32
ColocationId(int shardCount, int replicationFactor, Oid distributionColumnType, Oid
			 distributionColumnCollation)
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	const int scanKeyCount = 4;
	ScanKeyData scanKey[4];
	bool indexOK = true;

	Relation pgDistColocation = table_open(DistColocationRelationId(), AccessShareLock);

	/* set scan arguments */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_colocation_distributioncolumntype,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributionColumnType));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_colocation_shardcount,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(shardCount));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_colocation_replicationfactor,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(replicationFactor));
	ScanKeyInit(&scanKey[3], Anum_pg_dist_colocation_distributioncolumncollation,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(
					distributionColumnCollation));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistColocation,
													DistColocationConfigurationIndexId(),
													indexOK, NULL, scanKeyCount, scanKey);

	HeapTuple colocationTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(colocationTuple))
	{
		Form_pg_dist_colocation colocationForm =
			(Form_pg_dist_colocation) GETSTRUCT(colocationTuple);

		/* avoid chosing a colocation group that belongs to a tenant schema */
		if (IsTenantSchemaColocationGroup(colocationForm->colocationid))
		{
			colocationTuple = systable_getnext(scanDescriptor);
			continue;
		}

		if (colocationId == INVALID_COLOCATION_ID || colocationId >
			colocationForm->colocationid)
		{
			/*
			 * We assign the smallest colocation id among all the matches so that we
			 * assign the same colocation group for similar distributed tables
			 */
			colocationId = colocationForm->colocationid;
		}
		colocationTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistColocation, AccessShareLock);

	return colocationId;
}


/*
 * AcquireColocationDefaultLock serializes concurrent creation of a colocation entry
 * for default group.
 */
void
AcquireColocationDefaultLock(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_CREATE_COLOCATION_DEFAULT);

	(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
}


/*
 * ReleaseColocationDefaultLock releases the lock for concurrent creation of a colocation entry
 * for default group.
 */
void
ReleaseColocationDefaultLock(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;

	SET_LOCKTAG_CITUS_OPERATION(tag, CITUS_CREATE_COLOCATION_DEFAULT);

	LockRelease(&tag, ExclusiveLock, sessionLock);
}


/*
 * CreateColocationGroup creates a new colocation id and writes it into
 * pg_dist_colocation with the given configuration. It also returns the created
 * colocation id.
 */
uint32
CreateColocationGroup(int shardCount, int replicationFactor, Oid distributionColumnType,
					  Oid distributionColumnCollation)
{
	uint32 colocationId = GetNextColocationId();

	InsertColocationGroupLocally(colocationId, shardCount, replicationFactor,
								 distributionColumnType, distributionColumnCollation);

	SyncNewColocationGroupToNodes(colocationId, shardCount, replicationFactor,
								  distributionColumnType, distributionColumnCollation);

	return colocationId;
}


/*
 * InsertColocationGroupLocally inserts a record into pg_dist_colocation.
 */
void
InsertColocationGroupLocally(uint32 colocationId, int shardCount, int replicationFactor,
							 Oid distributionColumnType,
							 Oid distributionColumnCollation)
{
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
	values[Anum_pg_dist_colocation_distributioncolumncollation - 1] =
		ObjectIdGetDatum(distributionColumnCollation);

	/* open colocation relation and insert the new tuple */
	Relation pgDistColocation = table_open(DistColocationRelationId(), RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistColocation);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistColocation, heapTuple);

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();
	table_close(pgDistColocation, NoLock);
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

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique colocation id from sequence */
	Datum colocationIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	uint32 colocationId = DatumGetUInt32(colocationIdDatum);

	return colocationId;
}


/*
 * CheckReplicationModel checks if given relations are from the same
 * replication model. Otherwise, it errors out.
 */
void
CheckReplicationModel(Oid sourceRelationId, Oid targetRelationId)
{
	CitusTableCacheEntry *sourceTableEntry = GetCitusTableCacheEntry(sourceRelationId);
	char sourceReplicationModel = sourceTableEntry->replicationModel;

	CitusTableCacheEntry *targetTableEntry = GetCitusTableCacheEntry(targetRelationId);
	char targetReplicationModel = targetTableEntry->replicationModel;

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
	/* reference tables have NULL distribution column */
	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);

	/* reference tables have NULL distribution column */
	Var *targetDistributionColumn = DistPartitionKey(targetRelationId);

	EnsureColumnTypeEquality(sourceRelationId, targetRelationId,
							 sourceDistributionColumn, targetDistributionColumn);
}


/*
 * EnsureColumnTypeEquality checks if distribution column types and collations
 * of the given columns are same. The function sets the boolean pointers.
 */
void
EnsureColumnTypeEquality(Oid sourceRelationId, Oid targetRelationId,
						 Var *sourceDistributionColumn, Var *targetDistributionColumn)
{
	Oid sourceDistributionColumnType = InvalidOid;
	Oid targetDistributionColumnType = InvalidOid;
	Oid sourceDistributionColumnCollation = InvalidOid;
	Oid targetDistributionColumnCollation = InvalidOid;

	if (sourceDistributionColumn != NULL)
	{
		sourceDistributionColumnType = sourceDistributionColumn->vartype;
		sourceDistributionColumnCollation = sourceDistributionColumn->varcollid;
	}

	if (targetDistributionColumn != NULL)
	{
		targetDistributionColumnType = targetDistributionColumn->vartype;
		targetDistributionColumnCollation = targetDistributionColumn->varcollid;
	}

	bool columnTypesSame = sourceDistributionColumnType == targetDistributionColumnType;
	bool columnCollationsSame =
		sourceDistributionColumnCollation == targetDistributionColumnCollation;

	if (!columnTypesSame)
	{
		char *sourceRelationName = get_rel_name(sourceRelationId);
		char *targetRelationName = get_rel_name(targetRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, targetRelationName),
						errdetail("Distribution column types don't match for "
								  "%s and %s.", sourceRelationName,
								  targetRelationName)));
	}

	if (!columnCollationsSame)
	{
		char *sourceRelationName = get_rel_name(sourceRelationId);
		char *targetRelationName = get_rel_name(targetRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, targetRelationName),
						errdetail(
							"Distribution column collations don't match for "
							"%s and %s.", sourceRelationName,
							targetRelationName)));
	}
}


/*
 * UpdateRelationColocationGroup updates colocation group in pg_dist_partition
 * for the given relation.
 *
 * When localOnly is true, the function does not propagate changes to the
 * metadata workers.
 */
void
UpdateRelationColocationGroup(Oid distributedRelationId, uint32 colocationId,
							  bool localOnly)
{
	bool indexOK = true;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	Datum values[Natts_pg_dist_partition];
	bool isNull[Natts_pg_dist_partition];
	bool replace[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributedRelationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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
	table_close(pgDistPartition, NoLock);

	bool shouldSyncMetadata = ShouldSyncTableMetadata(distributedRelationId);
	if (shouldSyncMetadata && !localOnly)
	{
		char *updateColocationIdCommand = ColocationIdUpdateCommand(distributedRelationId,
																	colocationId);

		SendCommandToWorkersWithMetadata(updateColocationIdCommand);
	}
}


/*
 * TableColocationId function returns co-location id of given table. This function
 * errors out if given table is not distributed.
 */
uint32
TableColocationId(Oid distributedTableId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);

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
	if (leftDistributedTableId == rightDistributedTableId)
	{
		return true;
	}

	uint32 leftColocationId = TableColocationId(leftDistributedTableId);
	uint32 rightColocationId = TableColocationId(rightDistributedTableId);
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

	int count = 0;
	colocatedTableList = ColocationGroupTableList(tableColocationId, count);

	return colocatedTableList;
}


/*
 * ColocationGroupTableList returns the list of tables in the given colocation
 * group. If the colocation group is INVALID_COLOCATION_ID, it returns NIL.
 *
 * If count is zero then the command is executed for all rows that it applies to.
 * If count is greater than zero, then no more than count rows will be retrieved;
 * execution stops when the count is reached, much like adding a LIMIT clause
 * to the query.
 */
List *
ColocationGroupTableList(uint32 colocationId, uint32 count)
{
	List *colocatedTableList = NIL;
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
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(colocationId));

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionColocationidIndexId(),
													indexOK, NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNullArray[Natts_pg_dist_partition];
		Datum datumArray[Natts_pg_dist_partition];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		Oid colocatedTableId = DatumGetObjectId(
			datumArray[Anum_pg_dist_partition_logicalrelid - 1]);

		colocatedTableList = lappend_oid(colocatedTableList, colocatedTableId);
		heapTuple = systable_getnext(scanDescriptor);

		if (count == 0)
		{
			/* fetch all rows */
			continue;
		}
		else if (list_length(colocatedTableList) >= count)
		{
			/* we are done */
			break;
		}
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, AccessShareLock);

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

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);

	/*
	 * If distribution type of the table is append or range, each shard of
	 * the shard is only co-located with itself.
	 */
	if (IsCitusTableTypeCacheEntry(cacheEntry, APPEND_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(cacheEntry, RANGE_DISTRIBUTED))
	{
		ShardInterval *copyShardInterval = CopyShardInterval(shardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);

		return colocatedShardList;
	}

	int shardIntervalIndex = ShardIndex(shardInterval);
	List *colocatedTableList = ColocatedTableList(distributedTableId);

	/* ShardIndex have to find index of given shard */
	Assert(shardIntervalIndex >= 0);

	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
	{
		CitusTableCacheEntry *colocatedTableCacheEntry =
			GetCitusTableCacheEntry(colocatedTableId);

		/*
		 * Since we iterate over co-located tables, shard count of each table should be
		 * same and greater than shardIntervalIndex.
		 */
		Assert(cacheEntry->shardIntervalArrayLength ==
			   colocatedTableCacheEntry->shardIntervalArrayLength);

		ShardInterval *colocatedShardInterval =
			colocatedTableCacheEntry->sortedShardIntervalArray[shardIntervalIndex];

		ShardInterval *copyShardInterval = CopyShardInterval(colocatedShardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);
	}

	Assert(list_length(colocatedTableList) == list_length(colocatedShardList));

	return SortList(colocatedShardList, CompareShardIntervalsById);
}


/*
 * ColocatedNonPartitionShardIntervalList function returns list of shard intervals
 * which are co-located with given shard, except partitions. If given shard is belong
 * to append or range distributed table, co-location is not valid for that shard.
 * Therefore such shard is only co-located with itself.
 */
List *
ColocatedNonPartitionShardIntervalList(ShardInterval *shardInterval)
{
	Oid distributedTableId = shardInterval->relationId;
	List *colocatedShardList = NIL;

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);

	/*
	 * If distribution type of the table is append or range, each shard of the shard
	 * is only co-located with itself. We don't expect this case to happen, since
	 * distributing partitioned tables in only supported for hash-distributed tables.
	 * Therefore, currently we can't cover here with a test.
	 */
	if (IsCitusTableTypeCacheEntry(cacheEntry, APPEND_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(cacheEntry, RANGE_DISTRIBUTED))
	{
		ShardInterval *copyShardInterval = CopyShardInterval(shardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);

		return colocatedShardList;
	}

	ereport(DEBUG1, (errmsg("skipping child tables for relation named: %s",
							get_rel_name(distributedTableId))));

	int shardIntervalIndex = ShardIndex(shardInterval);
	List *colocatedTableList = ColocatedTableList(distributedTableId);

	/* ShardIndex have to find index of given shard */
	Assert(shardIntervalIndex >= 0);

	Oid colocatedTableId = InvalidOid;
	foreach_declared_oid(colocatedTableId, colocatedTableList)
	{
		if (PartitionTable(colocatedTableId))
		{
			continue;
		}

		CitusTableCacheEntry *colocatedTableCacheEntry =
			GetCitusTableCacheEntry(colocatedTableId);

		/*
		 * Since we iterate over co-located tables, shard count of each table should be
		 * same and greater than shardIntervalIndex.
		 */
		Assert(cacheEntry->shardIntervalArrayLength ==
			   colocatedTableCacheEntry->shardIntervalArrayLength);

		ShardInterval *colocatedShardInterval =
			colocatedTableCacheEntry->sortedShardIntervalArray[shardIntervalIndex];

		ShardInterval *copyShardInterval = CopyShardInterval(colocatedShardInterval);

		colocatedShardList = lappend(colocatedShardList, copyShardInterval);
	}

	return SortList(colocatedShardList, CompareShardIntervalsById);
}


/*
 * ColocatedTableId returns an arbitrary table which belongs to given colocation
 * group. If there is not such a colocation group, it returns invalid oid.
 *
 * This function also takes an AccessShareLock on the co-colocated table to
 * guarantee that the table isn't dropped for the remainder of the transaction.
 */
Oid
ColocatedTableId(int32 colocationId)
{
	Oid colocatedTableId = InvalidOid;
	bool indexOK = true;
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
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(colocationId));

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionColocationidIndexId(),
													indexOK, NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNullArray[Natts_pg_dist_partition];
		Datum datumArray[Natts_pg_dist_partition];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		colocatedTableId = DatumGetObjectId(
			datumArray[Anum_pg_dist_partition_logicalrelid - 1]);

		/*
		 * Make sure the relation isn't dropped for the remainder of
		 * the transaction.
		 */
		LockRelationOid(colocatedTableId, AccessShareLock);

		/*
		 * The relation might have been dropped just before we locked it.
		 * Let's look it up.
		 */
		Relation colocatedRelation = RelationIdGetRelation(colocatedTableId);
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
	table_close(pgDistPartition, AccessShareLock);

	return colocatedTableId;
}


/*
 * SingleShardTableColocationNodeId takes a colocation id that presumably
 * belongs to colocation group used to colocate a set of single-shard
 * tables and returns id of the node that stores / is expected to store
 * the shards within the colocation group.
 */
uint32
SingleShardTableColocationNodeId(uint32 colocationId)
{
	List *tablesInColocationGroup = ColocationGroupTableList(colocationId, 0);
	if (list_length(tablesInColocationGroup) == 0)
	{
		int workerNodeIndex =
			EmptySingleShardTableColocationDecideNodeId(colocationId);
		List *workerNodeList = DistributedTablePlacementNodeList(RowShareLock);
		WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList, workerNodeIndex);

		return workerNode->nodeId;
	}
	else
	{
		Oid colocatedTableId = ColocatedTableId(colocationId);
		return SingleShardTableGetNodeId(colocatedTableId);
	}
}


/*
 * SingleShardTableGetNodeId returns id of the node that stores shard of
 * given single-shard table.
 */
static uint32
SingleShardTableGetNodeId(Oid relationId)
{
	if (!IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED))
	{
		ereport(ERROR, (errmsg("table is not a single-shard distributed table")));
	}

	int64 shardId = GetFirstShardId(relationId);
	List *shardPlacementList = ShardPlacementList(shardId);
	if (list_length(shardPlacementList) != 1)
	{
		ereport(ERROR, (errmsg("table shard does not have a single shard placement")));
	}

	return ((ShardPlacement *) linitial(shardPlacementList))->nodeId;
}


/*
 * ColocatedShardIdInRelation returns shardId of the shard from given relation, so that
 * returned shard is co-located with given shard.
 */
uint64
ColocatedShardIdInRelation(Oid relationId, int shardIndex)
{
	CitusTableCacheEntry *tableCacheEntry = GetCitusTableCacheEntry(relationId);

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
		int count = 1;
		List *colocatedTableList = ColocationGroupTableList(colocationId, count);
		int colocatedTableCount = list_length(colocatedTableList);

		if (colocatedTableCount == 0)
		{
			DeleteColocationGroup(colocationId);
		}
	}
}


/*
 * DeleteColocationGroup deletes the colocation group from pg_dist_colocation
 * throughout the cluster and dissociates the tenant schema if any.
 */
void
DeleteColocationGroup(uint32 colocationId)
{
	DeleteColocationGroupLocally(colocationId);
	SyncDeleteColocationGroupToNodes(colocationId);
}


/*
 * DeleteColocationGroupLocally deletes the colocation group from pg_dist_colocation.
 */
void
DeleteColocationGroupLocally(uint32 colocationId)
{
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = false;

	Relation pgDistColocation = table_open(DistColocationRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_colocation_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(colocationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistColocation, InvalidOid, indexOK,
													NULL, scanKeyCount, scanKey);

	/* if a record is found, delete it */
	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		/*
		 * simple_heap_delete() expects that the caller has at least an
		 * AccessShareLock on primary key index.
		 *
		 * XXX: This does not seem required, do we really need to acquire this lock?
		 * Postgres doesn't acquire such locks on indexes before deleting catalog tuples.
		 * Linking here the reasons we added this lock acquirement:
		 * https://github.com/citusdata/citus/pull/2851#discussion_r306569462
		 * https://github.com/citusdata/citus/pull/2855#discussion_r313628554
		 * https://github.com/citusdata/citus/issues/1890
		 */
		Relation replicaIndex =
			index_open(RelationGetPrimaryKeyIndex(pgDistColocation),
					   AccessShareLock);
		simple_heap_delete(pgDistColocation, &(heapTuple->t_self));

		CitusInvalidateRelcacheByRelid(DistColocationRelationId());
		CommandCounterIncrement();
		table_close(replicaIndex, AccessShareLock);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistColocation, NoLock);
}


/*
 * FindColocateWithColocationId tries to find a colocation ID for a given
 * colocate_with clause passed to create_distributed_table.
 */
uint32
FindColocateWithColocationId(Oid relationId, char replicationModel,
							 Oid distributionColumnType,
							 Oid distributionColumnCollation,
							 int shardCount, bool shardCountIsStrict,
							 char *colocateWithTableName)
{
	uint32 colocationId = INVALID_COLOCATION_ID;

	if (IsColocateWithDefault(colocateWithTableName))
	{
		/* check for default colocation group */
		colocationId = ColocationId(shardCount, ShardReplicationFactor,
									distributionColumnType,
									distributionColumnCollation);

		/*
		 * if the shardCount is strict then we check if the shard count
		 * of the colocated table is actually shardCount
		 */
		if (shardCountIsStrict && colocationId != INVALID_COLOCATION_ID)
		{
			Oid colocatedTableId = ColocatedTableId(colocationId);

			if (colocatedTableId != InvalidOid)
			{
				CitusTableCacheEntry *cacheEntry =
					GetCitusTableCacheEntry(colocatedTableId);
				int colocatedTableShardCount = cacheEntry->shardIntervalArrayLength;

				if (colocatedTableShardCount != shardCount)
				{
					colocationId = INVALID_COLOCATION_ID;
				}
			}
		}
	}
	else if (!IsColocateWithNone(colocateWithTableName))
	{
		text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
		Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText, false);

		EnsureTableCanBeColocatedWith(relationId, replicationModel,
									  distributionColumnType, sourceRelationId);

		colocationId = TableColocationId(sourceRelationId);
	}

	return colocationId;
}


/*
 * EnsureTableCanBeColocatedWith checks whether a given replication model and
 * distribution column type is suitable to distribute a table to be colocated
 * with given source table.
 *
 * We only pass relationId to provide meaningful error messages.
 */
void
EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
							  Oid distributionColumnType, Oid sourceRelationId)
{
	CitusTableCacheEntry *sourceTableEntry = GetCitusTableCacheEntry(sourceRelationId);

	if (IsCitusTableTypeCacheEntry(sourceTableEntry, APPEND_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(sourceTableEntry, RANGE_DISTRIBUTED) ||
		IsCitusTableTypeCacheEntry(sourceTableEntry, CITUS_LOCAL_TABLE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation"),
						errdetail("Currently, colocate_with option is not supported "
								  "with append / range distributed tables and local "
								  "tables added to metadata.")));
	}

	char sourceReplicationModel = sourceTableEntry->replicationModel;
	if (sourceReplicationModel != replicationModel)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Replication models don't match for %s and %s.",
								  sourceRelationName, relationName)));
	}

	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	Oid sourceDistributionColumnType = !sourceDistributionColumn ? InvalidOid :
									   sourceDistributionColumn->vartype;
	if (sourceDistributionColumnType != distributionColumnType)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Distribution column types don't match for "
								  "%s and %s.", sourceRelationName,
								  relationName)));
	}

	/* prevent colocating regular tables with tenant tables */
	Oid sourceRelationSchemaId = get_rel_namespace(sourceRelationId);
	Oid targetRelationSchemaId = get_rel_namespace(relationId);
	if (IsTenantSchema(sourceRelationSchemaId) &&
		sourceRelationSchemaId != targetRelationSchemaId)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);
		char *sourceRelationSchemaName = get_namespace_name(sourceRelationSchemaId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Cannot colocate tables with distributed schema tables"
								  " by using colocate_with option."),
						errhint("Consider using \"CREATE TABLE\" statement "
								"to create this table as a single-shard distributed "
								"table in the same schema to automatically colocate "
								"it with %s.%s",
								sourceRelationSchemaName, sourceRelationName)));
	}
}
