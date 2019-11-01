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
#include "libpq-fe.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/connection_management.h"
#include "distributed/citus_nodes.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_placement.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
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


/* Local functions forward declarations */
static uint64 * AllocateUint64(uint64 value);
static void RecordDistributedRelationDependencies(Oid distributedRelationId,
												  Node *distributionKey);
static GroupShardPlacement * TupleToGroupShardPlacement(TupleDesc tupleDesc,
														HeapTuple heapTuple);
static uint64 DistributedTableSize(Oid relationId, char *sizeQuery);
static uint64 DistributedTableSizeOnWorker(WorkerNode *workerNode, Oid relationId,
										   char *sizeQuery);
static List * ShardIntervalsOnWorkerGroup(WorkerNode *workerNode, Oid relationId);
static StringInfo GenerateSizeQueryOnMultiplePlacements(Oid distributedRelationId,
														List *shardIntervalList,
														char *sizeQuery);
static void ErrorIfNotSuitableToGetSize(Oid relationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_table_size);
PG_FUNCTION_INFO_V1(citus_total_relation_size);
PG_FUNCTION_INFO_V1(citus_relation_size);


/*
 * citus_total_relation_size accepts a table name and returns a distributed table
 * and its indexes' total relation size.
 */
Datum
citus_total_relation_size(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	uint64 totalRelationSize = 0;
	char *tableSizeFunction = PG_TOTAL_RELATION_SIZE_FUNCTION;

	CheckCitusVersion(ERROR);

	if (CStoreTable(relationId))
	{
		tableSizeFunction = CSTORE_TABLE_SIZE_FUNCTION;
	}

	totalRelationSize = DistributedTableSize(relationId, tableSizeFunction);

	PG_RETURN_INT64(totalRelationSize);
}


/*
 * citus_table_size accepts a table name and returns a distributed table's total
 * relation size.
 */
Datum
citus_table_size(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	uint64 tableSize = 0;
	char *tableSizeFunction = PG_TABLE_SIZE_FUNCTION;

	CheckCitusVersion(ERROR);

	if (CStoreTable(relationId))
	{
		tableSizeFunction = CSTORE_TABLE_SIZE_FUNCTION;
	}

	tableSize = DistributedTableSize(relationId, tableSizeFunction);

	PG_RETURN_INT64(tableSize);
}


/*
 * citus_relation_size accept a table name and returns a relation's 'main'
 * fork's size.
 */
Datum
citus_relation_size(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	uint64 relationSize = 0;
	char *tableSizeFunction = PG_RELATION_SIZE_FUNCTION;

	CheckCitusVersion(ERROR);

	if (CStoreTable(relationId))
	{
		tableSizeFunction = CSTORE_TABLE_SIZE_FUNCTION;
	}

	relationSize = DistributedTableSize(relationId, tableSizeFunction);

	PG_RETURN_INT64(relationSize);
}


/*
 * DistributedTableSize is helper function for each kind of citus size functions.
 * It first checks whether the table is distributed and size query can be run on
 * it. Connection to each node has to be established to get the size of the table.
 */
static uint64
DistributedTableSize(Oid relationId, char *sizeQuery)
{
	Relation relation = NULL;
	List *workerNodeList = NULL;
	ListCell *workerNodeCell = NULL;
	uint64 totalRelationSize = 0;

	if (XactModificationLevel == XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("citus size functions cannot be called in transaction"
							   " blocks which contain multi-shard data modifications")));
	}

	relation = try_relation_open(relationId, AccessShareLock);

	if (relation == NULL)
	{
		ereport(ERROR,
				(errmsg("could not compute table size: relation does not exist")));
	}

	ErrorIfNotSuitableToGetSize(relationId);

	workerNodeList = ActiveReadableNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		uint64 relationSizeOnNode = DistributedTableSizeOnWorker(workerNode, relationId,
																 sizeQuery);
		totalRelationSize += relationSizeOnNode;
	}

	heap_close(relation, AccessShareLock);

	return totalRelationSize;
}


/*
 * DistributedTableSizeOnWorker gets the workerNode and relationId to calculate
 * size of that relation on the given workerNode by summing up the size of each
 * shard placement.
 */
static uint64
DistributedTableSizeOnWorker(WorkerNode *workerNode, Oid relationId, char *sizeQuery)
{
	StringInfo tableSizeQuery = NULL;
	StringInfo tableSizeStringInfo = NULL;
	char *workerNodeName = workerNode->workerName;
	uint32 workerNodePort = workerNode->workerPort;
	char *tableSizeString;
	uint64 tableSize = 0;
	MultiConnection *connection = NULL;
	uint32 connectionFlag = 0;
	PGresult *result = NULL;
	int queryResult = 0;
	List *sizeList = NIL;
	bool raiseErrors = true;

	List *shardIntervalsOnNode = ShardIntervalsOnWorkerGroup(workerNode, relationId);

	tableSizeQuery = GenerateSizeQueryOnMultiplePlacements(relationId,
														   shardIntervalsOnNode,
														   sizeQuery);

	connection = GetNodeConnection(connectionFlag, workerNodeName, workerNodePort);
	queryResult = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data, &result);

	if (queryResult != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot get the size because of a connection error")));
	}

	sizeList = ReadFirstColumnAsText(result);
	tableSizeStringInfo = (StringInfo) linitial(sizeList);
	tableSizeString = tableSizeStringInfo->data;
	tableSize = atol(tableSizeString);

	PQclear(result);
	ClearResults(connection, raiseErrors);

	return tableSize;
}


/*
 * GroupShardPlacementsForTableOnGroup accepts a relationId and a group and returns a list
 * of GroupShardPlacement's representing all of the placements for the table which reside
 * on the group.
 */
List *
GroupShardPlacementsForTableOnGroup(Oid relationId, int32 groupId)
{
	DistTableCacheEntry *distTableCacheEntry = DistributedTableCacheEntry(relationId);
	List *resultList = NIL;

	int shardIndex = 0;
	int shardIntervalArrayLength = distTableCacheEntry->shardIntervalArrayLength;

	for (shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		GroupShardPlacement *placementArray =
			distTableCacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements =
			distTableCacheEntry->arrayOfPlacementArrayLengths[shardIndex];
		int placementIndex = 0;

		for (placementIndex = 0; placementIndex < numberOfPlacements; placementIndex++)
		{
			GroupShardPlacement *placement = &placementArray[placementIndex];

			if (placement->groupId == groupId)
			{
				resultList = lappend(resultList, placement);
			}
		}
	}

	return resultList;
}


/*
 * ShardIntervalsOnWorkerGroup accepts a WorkerNode and returns a list of the shard
 * intervals of the given table which are placed on the group the node is a part of.
 *
 * DO NOT modify the shard intervals returned by this function, they are not copies but
 * pointers.
 */
static List *
ShardIntervalsOnWorkerGroup(WorkerNode *workerNode, Oid relationId)
{
	DistTableCacheEntry *distTableCacheEntry = DistributedTableCacheEntry(relationId);
	List *shardIntervalList = NIL;
	int shardIndex = 0;
	int shardIntervalArrayLength = distTableCacheEntry->shardIntervalArrayLength;

	for (shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		GroupShardPlacement *placementArray =
			distTableCacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements =
			distTableCacheEntry->arrayOfPlacementArrayLengths[shardIndex];
		int placementIndex = 0;

		for (placementIndex = 0; placementIndex < numberOfPlacements; placementIndex++)
		{
			GroupShardPlacement *placement = &placementArray[placementIndex];
			uint64 shardId = placement->shardId;
			bool metadataLock = false;

			metadataLock = TryLockShardDistributionMetadata(shardId, ShareLock);

			/* if the lock is not acquired warn the user */
			if (metadataLock == false)
			{
				ereport(WARNING, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
								  errmsg("lock is not acquired, size of shard "
										 UINT64_FORMAT " will be ignored", shardId)));
				continue;
			}

			if (placement->groupId == workerNode->groupId)
			{
				ShardInterval *shardInterval =
					distTableCacheEntry->sortedShardIntervalArray[shardIndex];
				shardIntervalList = lappend(shardIntervalList, shardInterval);
			}
		}
	}

	return shardIntervalList;
}


/*
 * GenerateSizeQueryOnMultiplePlacements generates a select size query to get
 * size of multiple tables from the relation with distributedRelationId. Note
 * that, different size functions supported by PG are also supported by this
 * function changing the size query given as the last parameter to function.
 * Format of sizeQuery is pg_*_size(%s). Examples of it can be found in the
 * master_protocol.h
 */
static StringInfo
GenerateSizeQueryOnMultiplePlacements(Oid distributedRelationId, List *shardIntervalList,
									  char *sizeQuery)
{
	Oid schemaId = get_rel_namespace(distributedRelationId);
	char *schemaName = get_namespace_name(schemaId);

	StringInfo selectQuery = makeStringInfo();
	ListCell *shardIntervalCell = NULL;

	appendStringInfo(selectQuery, "SELECT ");

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		char *shardName = get_rel_name(distributedRelationId);
		char *shardQualifiedName = NULL;
		char *quotedShardName = NULL;
		AppendShardIdToName(&shardName, shardId);

		shardQualifiedName = quote_qualified_identifier(schemaName, shardName);
		quotedShardName = quote_literal_cstr(shardQualifiedName);

		appendStringInfo(selectQuery, sizeQuery, quotedShardName);
		appendStringInfo(selectQuery, " + ");
	}

	/*
	 * Add 0 as a last size, it handles empty list case and makes size control checks
	 * unnecessary which would have implemented without this line.
	 */
	appendStringInfo(selectQuery, "0;");

	return selectQuery;
}


/*
 * ErrorIfNotSuitableToGetSize determines whether the table is suitable to find
 * its' size with internal functions.
 */
static void
ErrorIfNotSuitableToGetSize(Oid relationId)
{
	if (!IsDistributedTable(relationId))
	{
		char *relationName = get_rel_name(relationId);
		char *escapedQueryString = quote_literal_cstr(relationName);
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot calculate the size because relation %s is not "
							   "distributed", escapedQueryString)));
	}

	if (PartitionMethod(relationId) == DISTRIBUTE_BY_HASH &&
		!SingleReplicatedTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot calculate the size because replication factor "
							   "is greater than 1")));
	}
}


/*
 * CompareShardPlacementsByWorker compares two shard placements by their
 * worker node name and port.
 */
int
CompareShardPlacementsByWorker(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	int nodeNameCmp = strncmp(leftPlacement->nodeName, rightPlacement->nodeName,
							  WORKER_LENGTH);
	if (nodeNameCmp != 0)
	{
		return nodeNameCmp;
	}
	else if (leftPlacement->nodePort > rightPlacement->nodePort)
	{
		return 1;
	}
	else if (leftPlacement->nodePort < rightPlacement->nodePort)
	{
		return -1;
	}

	return 0;
}


/*
 * TableShardReplicationFactor returns the current replication factor of the
 * given relation by looking into shard placements. It errors out if there
 * are different number of shard placements for different shards. It also
 * errors out if the table does not have any shards.
 */
uint32
TableShardReplicationFactor(Oid relationId)
{
	uint32 replicationCount = 0;
	ListCell *shardCell = NULL;

	List *shardIntervalList = LoadShardIntervalList(relationId);
	foreach(shardCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
		uint64 shardId = shardInterval->shardId;

		List *shardPlacementList = ShardPlacementList(shardId);
		uint32 shardPlacementCount = list_length(shardPlacementList);

		/*
		 * Get the replication count of the first shard in the list, and error
		 * out if there is a shard with different replication count.
		 */
		if (replicationCount == 0)
		{
			replicationCount = shardPlacementCount;
		}
		else if (replicationCount != shardPlacementCount)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot find the replication factor of the "
								   "table %s", relationName),
							errdetail("The shard " UINT64_FORMAT
									  " has different shards replication counts from "
									  "other shards.", shardId)));
		}
	}

	/* error out if the table does not have any shards */
	if (replicationCount == 0)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot find the replication factor of the "
							   "table %s", relationName),
						errdetail("The table %s does not have any shards.",
								  relationName)));
	}

	return replicationCount;
}


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
	destInterval->shardIndex = srcInterval->shardIndex;

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
 * CopyShardPlacement copies the values of the source placement into the
 * target placement.
 */
void
CopyShardPlacement(ShardPlacement *srcPlacement, ShardPlacement *destPlacement)
{
	/* first copy all by-value fields */
	memcpy(destPlacement, srcPlacement, sizeof(ShardPlacement));

	/* and then the fields pointing to external values */
	if (srcPlacement->nodeName)
	{
		destPlacement->nodeName = pstrdup(srcPlacement->nodeName);
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
 * NodeGroupHasShardPlacements returns whether any active shards are placed on the group
 */
bool
NodeGroupHasShardPlacements(int32 groupId, bool onlyConsiderActivePlacements)
{
	const int scanKeyCount = (onlyConsiderActivePlacements ? 2 : 1);
	const bool indexOK = false;

	bool hasFinalizedPlacements = false;

	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[2];

	Relation pgPlacement = heap_open(DistPlacementRelationId(),
									 AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));
	if (onlyConsiderActivePlacements)
	{
		ScanKeyInit(&scanKey[1], Anum_pg_dist_placement_shardstate,
					BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(FILE_FINALIZED));
	}

	scanDescriptor = systable_beginscan(pgPlacement,
										DistPlacementGroupidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	hasFinalizedPlacements = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);
	heap_close(pgPlacement, NoLock);

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

	return SortList(finalizedPlacementList, CompareShardPlacementsByWorker);
}


/*
 * FinalizedShardPlacement finds a shard placement for the given shardId from
 * system catalog, chooses a placement that is in finalized state and returns
 * that shard placement. If this function cannot find a healthy shard placement
 * and missingOk is set to false it errors out.
 */
ShardPlacement *
FinalizedShardPlacement(uint64 shardId, bool missingOk)
{
	List *finalizedPlacementList = FinalizedShardPlacementList(shardId);
	ShardPlacement *shardPlacement = NULL;

	if (list_length(finalizedPlacementList) == 0)
	{
		if (!missingOk)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("could not find any healthy placement for shard "
								   UINT64_FORMAT, shardId)));
		}

		return shardPlacement;
	}

	shardPlacement = (ShardPlacement *) linitial(finalizedPlacementList);

	return shardPlacement;
}


/*
 * BuildShardPlacementList finds shard placements for the given shardId from
 * system catalogs, converts these placements to their in-memory
 * representation, and returns the converted shard placements in a new list.
 *
 * This probably only should be called from metadata_cache.c.  Resides here
 * because it shares code with other routines in this file.
 */
List *
BuildShardPlacementList(ShardInterval *shardInterval)
{
	int64 shardId = shardInterval->shardId;
	List *shardPlacementList = NIL;
	Relation pgPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;

	pgPlacement = heap_open(DistPlacementRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgPlacement,
										DistPlacementShardidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgPlacement);

		GroupShardPlacement *placement =
			TupleToGroupShardPlacement(tupleDescriptor, heapTuple);

		shardPlacementList = lappend(shardPlacementList, placement);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgPlacement, NoLock);

	return shardPlacementList;
}


/*
 * BuildShardPlacementListForGroup finds shard placements for the given groupId
 * from system catalogs, converts these placements to their in-memory
 * representation, and returns the converted shard placements in a new list.
 */
List *
AllShardPlacementsOnNodeGroup(int32 groupId)
{
	List *shardPlacementList = NIL;
	Relation pgPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;

	pgPlacement = heap_open(DistPlacementRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	scanDescriptor = systable_beginscan(pgPlacement,
										DistPlacementGroupidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgPlacement);

		GroupShardPlacement *placement =
			TupleToGroupShardPlacement(tupleDescriptor, heapTuple);

		shardPlacementList = lappend(shardPlacementList, placement);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgPlacement, NoLock);

	return shardPlacementList;
}


/*
 * TupleToGroupShardPlacement takes in a heap tuple from pg_dist_placement,
 * and converts this tuple to in-memory struct. The function assumes the
 * caller already has locks on the tuple, and doesn't perform any locking.
 */
static GroupShardPlacement *
TupleToGroupShardPlacement(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	GroupShardPlacement *shardPlacement = NULL;
	bool isNullArray[Natts_pg_dist_placement];
	Datum datumArray[Natts_pg_dist_placement];

	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_placement tuple")));
	}

	/*
	 * We use heap_deform_tuple() instead of heap_getattr() to expand tuple
	 * to contain missing values when ALTER TABLE ADD COLUMN happens.
	 */
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	shardPlacement = CitusMakeNode(GroupShardPlacement);
	shardPlacement->placementId = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_placementid - 1]);
	shardPlacement->shardId = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_shardid - 1]);
	shardPlacement->shardLength = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_shardlength - 1]);
	shardPlacement->shardState = DatumGetUInt32(
		datumArray[Anum_pg_dist_placement_shardstate - 1]);
	shardPlacement->groupId = DatumGetInt32(
		datumArray[Anum_pg_dist_placement_groupid - 1]);

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

	CatalogTupleInsert(pgDistShard, heapTuple);

	/* invalidate previous cache entry and close relation */
	CitusInvalidateRelcacheByRelid(relationId);

	CommandCounterIncrement();
	heap_close(pgDistShard, NoLock);
}


/*
 * InsertShardPlacementRow opens the shard placement system catalog, and inserts
 * a new row with the given values into that system catalog. If placementId is
 * INVALID_PLACEMENT_ID, a new placement id will be assigned.Then, returns the
 * placement id of the added shard placement.
 */
uint64
InsertShardPlacementRow(uint64 shardId, uint64 placementId,
						char shardState, uint64 shardLength,
						int32 groupId)
{
	Relation pgDistPlacement = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_placement];
	bool isNulls[Natts_pg_dist_placement];

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	if (placementId == INVALID_PLACEMENT_ID)
	{
		placementId = master_get_new_placementid(NULL);
	}
	values[Anum_pg_dist_placement_placementid - 1] = Int64GetDatum(placementId);
	values[Anum_pg_dist_placement_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_placement_shardstate - 1] = CharGetDatum(shardState);
	values[Anum_pg_dist_placement_shardlength - 1] = Int64GetDatum(shardLength);
	values[Anum_pg_dist_placement_groupid - 1] = Int32GetDatum(groupId);

	/* open shard placement relation and insert new tuple */
	pgDistPlacement = heap_open(DistPlacementRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistPlacement);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistPlacement, heapTuple);

	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();
	heap_close(pgDistPlacement, NoLock);

	return placementId;
}


/*
 * InsertIntoPgDistPartition inserts a new tuple into pg_dist_partition.
 */
void
InsertIntoPgDistPartition(Oid relationId, char distributionMethod,
						  Var *distributionColumn, uint32 colocationId,
						  char replicationModel)
{
	Relation pgDistPartition = NULL;
	char *distributionColumnString = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_partition];
	bool newNulls[Natts_pg_dist_partition];

	/* open system catalog and insert new tuple */
	pgDistPartition = heap_open(DistPartitionRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_partition_logicalrelid - 1] =
		ObjectIdGetDatum(relationId);
	newValues[Anum_pg_dist_partition_partmethod - 1] =
		CharGetDatum(distributionMethod);
	newValues[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	newValues[Anum_pg_dist_partition_repmodel - 1] = CharGetDatum(replicationModel);

	/* set partkey column to NULL for reference tables */
	if (distributionMethod != DISTRIBUTE_BY_NONE)
	{
		distributionColumnString = nodeToString((Node *) distributionColumn);

		newValues[Anum_pg_dist_partition_partkey - 1] =
			CStringGetTextDatum(distributionColumnString);
	}
	else
	{
		newValues[Anum_pg_dist_partition_partkey - 1] = PointerGetDatum(NULL);
		newNulls[Anum_pg_dist_partition_partkey - 1] = true;
	}

	newTuple = heap_form_tuple(RelationGetDescr(pgDistPartition), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	CatalogTupleInsert(pgDistPartition, newTuple);

	CitusInvalidateRelcacheByRelid(relationId);

	RecordDistributedRelationDependencies(relationId, (Node *) distributionColumn);

	CommandCounterIncrement();
	heap_close(pgDistPartition, NoLock);
}


/*
 * RecordDistributedRelationDependencies creates the dependency entries
 * necessary for a distributed relation in addition to the preexisting ones
 * for a normal relation.
 *
 * We create one dependency from the (now distributed) relation to the citus
 * extension to prevent the extension from being dropped while distributed
 * tables exist. Furthermore a dependency from pg_dist_partition's
 * distribution clause to the underlying columns is created, but it's marked
 * as being owned by the relation itself. That means the entire table can be
 * dropped, but the column itself can't. Neither can the type of the
 * distribution column be changed (c.f. ATExecAlterColumnType).
 */
static void
RecordDistributedRelationDependencies(Oid distributedRelationId, Node *distributionKey)
{
	ObjectAddress relationAddr = { 0, 0, 0 };
	ObjectAddress citusExtensionAddr = { 0, 0, 0 };

	relationAddr.classId = RelationRelationId;
	relationAddr.objectId = distributedRelationId;
	relationAddr.objectSubId = 0;

	citusExtensionAddr.classId = ExtensionRelationId;
	citusExtensionAddr.objectId = get_extension_oid("citus", false);
	citusExtensionAddr.objectSubId = 0;

	/* dependency from table entry to extension */
	recordDependencyOn(&relationAddr, &citusExtensionAddr, DEPENDENCY_NORMAL);
}


/*
 * DeletePartitionRow removes the row from pg_dist_partition where the logicalrelid
 * field equals to distributedRelationId. Then, the function invalidates the
 * metadata cache.
 */
void
DeletePartitionRow(Oid distributedRelationId)
{
	Relation pgDistPartition = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	pgDistPartition = heap_open(DistPartitionRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributedRelationId));

	scanDescriptor = systable_beginscan(pgDistPartition, InvalidOid, false, NULL,
										scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for partition %d",
							   distributedRelationId)));
	}

	simple_heap_delete(pgDistPartition, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	/* invalidate the cache */
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	heap_close(pgDistPartition, NoLock);
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
	heap_close(pgDistShard, NoLock);
}


/*
 * DeleteShardPlacementRow opens the shard placement system catalog, finds the placement
 * with the given placementId, and deletes it.
 */
void
DeleteShardPlacementRow(uint64 placementId)
{
	Relation pgDistPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	bool isNull = false;
	uint64 shardId = 0;

	pgDistPlacement = heap_open(DistPlacementRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistPlacement);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(placementId));

	scanDescriptor = systable_beginscan(pgDistPlacement,
										DistPlacementPlacementidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   INT64_FORMAT, placementId)));
	}

	shardId = heap_getattr(heapTuple, Anum_pg_dist_placement_shardid,
						   tupleDescriptor, &isNull);
	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_placement tuple")));
	}

	simple_heap_delete(pgDistPlacement, &heapTuple->t_self);
	systable_endscan(scanDescriptor);

	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();
	heap_close(pgDistPlacement, NoLock);
}


/*
 * UpdateShardPlacementState sets the shardState for the placement identified
 * by placementId.
 */
void
UpdateShardPlacementState(uint64 placementId, char shardState)
{
	Relation pgDistPlacement = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	Datum values[Natts_pg_dist_placement];
	bool isnull[Natts_pg_dist_placement];
	bool replace[Natts_pg_dist_placement];
	uint64 shardId = INVALID_SHARD_ID;
	bool colIsNull = false;

	pgDistPlacement = heap_open(DistPlacementRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistPlacement);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(placementId));

	scanDescriptor = systable_beginscan(pgDistPlacement,
										DistPlacementPlacementidIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   UINT64_FORMAT,
							   placementId)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_placement_shardstate - 1] = CharGetDatum(shardState);
	isnull[Anum_pg_dist_placement_shardstate - 1] = false;
	replace[Anum_pg_dist_placement_shardstate - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistPlacement, &heapTuple->t_self, heapTuple);

	shardId = DatumGetInt64(heap_getattr(heapTuple,
										 Anum_pg_dist_placement_shardid,
										 tupleDescriptor, &colIsNull));
	Assert(!colIsNull);
	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistPlacement, NoLock);
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
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relationId));
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
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}
}


/*
 * Check that the current user has owner rights to the schema, error out if
 * not. Superusers are regarded as owners.
 */
void
EnsureSchemaOwner(Oid schemaId)
{
	if (!pg_namespace_ownercheck(schemaId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA,
					   get_namespace_name(schemaId));
	}
}


/*
 * Check that the current user has owner rights to sequenceRelationId, error out if
 * not. Superusers are regarded as owners.
 */
void
EnsureSequenceOwner(Oid sequenceOid)
{
	if (!pg_class_ownercheck(sequenceOid, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SEQUENCE,
					   get_rel_name(sequenceOid));
	}
}


/*
 * Check that the current user has owner rights to functionId, error out if
 * not. Superusers are regarded as owners. Functions and procedures are
 * treated equally.
 */
void
EnsureFunctionOwner(Oid functionId)
{
	if (!pg_proc_ownercheck(functionId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION,
					   get_func_name(functionId));
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
