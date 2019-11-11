/*-------------------------------------------------------------------------
 *
 * metadata_cache.c
 *	  Distributed table metadata cache
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "stdint.h"
#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "citus_version.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/function_utils.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/pg_dist_local_group.h"
#include "distributed/pg_dist_node_metadata.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_placement.h"
#include "distributed/shared_library_init.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/* user configuration */
int ReadFromSecondaries = USE_SECONDARY_NODES_NEVER;

/*
 * ShardCacheEntry represents an entry in the shardId -> ShardInterval cache.
 * To avoid duplicating data and invalidation logic between this cache and the
 * DistTableCache, this only points into the DistTableCacheEntry of the
 * shard's distributed table.
 */
typedef struct ShardCacheEntry
{
	/* hash key, needs to be first */
	int64 shardId;

	/*
	 * Cache entry for the distributed table a shard belongs to, possibly not
	 * valid.
	 */
	DistTableCacheEntry *tableEntry;

	/*
	 * Offset in tableEntry->sortedShardIntervalArray, only valid if
	 * tableEntry->isValid.  We don't store pointers to the individual shard
	 * placements because that'd make invalidation a bit more complicated, and
	 * because there's simply no need.
	 */
	int shardIndex;
} ShardCacheEntry;


/*
 * State which should be cleared upon DROP EXTENSION.  When the configuration
 * changes, e.g. because the extension is dropped, these summarily get set to
 * 0.
 */
typedef struct MetadataCacheData
{
	bool extensionLoaded;
	Oid distShardRelationId;
	Oid distPlacementRelationId;
	Oid distNodeRelationId;
	Oid distNodeNodeIdIndexId;
	Oid distLocalGroupRelationId;
	Oid distObjectRelationId;
	Oid distObjectPrimaryKeyIndexId;
	Oid distColocationRelationId;
	Oid distColocationConfigurationIndexId;
	Oid distColocationColocationidIndexId;
	Oid distPartitionRelationId;
	Oid distPartitionLogicalRelidIndexId;
	Oid distPartitionColocationidIndexId;
	Oid distShardLogicalRelidIndexId;
	Oid distShardShardidIndexId;
	Oid distPlacementShardidIndexId;
	Oid distPlacementPlacementidIndexId;
	Oid distPlacementGroupidIndexId;
	Oid distTransactionRelationId;
	Oid distTransactionGroupIndexId;
	Oid distTransactionRecordIndexId;
	Oid citusCatalogNamespaceId;
	Oid copyFormatTypeId;
	Oid readIntermediateResultFuncId;
	Oid extraDataContainerFuncId;
	Oid workerHashFunctionId;
	Oid anyValueFunctionId;
	Oid textSendAsJsonbFunctionId;
	Oid extensionOwner;
	Oid binaryCopyFormatId;
	Oid textCopyFormatId;
	Oid primaryNodeRoleId;
	Oid secondaryNodeRoleId;
	Oid unavailableNodeRoleId;
	Oid pgTableIsVisibleFuncId;
	Oid citusTableIsVisibleFuncId;
	bool databaseNameValid;
	char databaseName[NAMEDATALEN];
} MetadataCacheData;


static MetadataCacheData MetadataCache;

/* Citus extension version variables */
bool EnableVersionChecks = true; /* version checks are enabled */

static bool citusVersionKnownCompatible = false;

/* Hash table for informations about each partition */
static HTAB *DistTableCacheHash = NULL;

/* Hash table for informations about each shard */
static HTAB *DistShardCacheHash = NULL;
static MemoryContext MetadataCacheMemoryContext = NULL;

/* Hash table for information about each object */
static HTAB *DistObjectCacheHash = NULL;

/* Hash table for informations about worker nodes */
static HTAB *WorkerNodeHash = NULL;
static WorkerNode **WorkerNodeArray = NULL;
static int WorkerNodeCount = 0;
static bool workerNodeHashValid = false;

/* default value is -1, for coordinator it's 0 and for worker nodes > 0 */
static int32 LocalGroupId = -1;

/* built first time through in InitializeDistCache */
static ScanKeyData DistPartitionScanKey[1];
static ScanKeyData DistShardScanKey[1];
static ScanKeyData DistObjectScanKey[3];


/* local function forward declarations */
static bool IsDistributedTableViaCatalog(Oid relationId);
static ShardCacheEntry * LookupShardCacheEntry(int64 shardId);
static DistTableCacheEntry * LookupDistTableCacheEntry(Oid relationId);
static void BuildDistTableCacheEntry(DistTableCacheEntry *cacheEntry);
static void BuildCachedShardList(DistTableCacheEntry *cacheEntry);
static ShardInterval ** SortShardIntervalArray(ShardInterval **shardIntervalArray,
											   int shardCount,
											   FmgrInfo *
											   shardIntervalSortCompareFunction);
static void PrepareWorkerNodeCache(void);
static bool HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray,
										  int shardCount);
static bool CheckInstalledVersion(int elevel);
static char * AvailableExtensionVersion(void);
static char * InstalledExtensionVersion(void);
static bool HasOverlappingShardInterval(ShardInterval **shardIntervalArray,
										int shardIntervalArrayLength,
										FmgrInfo *shardIntervalSortCompareFunction);
static void InitializeCaches(void);
static void InitializeDistCache(void);
static void InitializeDistObjectCache(void);
static void InitializeWorkerNodeCache(void);
static void RegisterForeignKeyGraphCacheCallbacks(void);
static void RegisterWorkerNodeCacheCallbacks(void);
static void RegisterLocalGroupIdCacheCallbacks(void);
static uint32 WorkerNodeHashCode(const void *key, Size keySize);
static void ResetDistTableCacheEntry(DistTableCacheEntry *cacheEntry);
static void CreateDistTableCache(void);
static void CreateDistObjectCache(void);
static void InvalidateForeignRelationGraphCacheCallback(Datum argument, Oid relationId);
static void InvalidateDistRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateLocalGroupIdRelationCacheCallback(Datum argument, Oid relationId);
static HeapTuple LookupDistPartitionTuple(Relation pgDistPartition, Oid relationId);
static List * LookupDistShardTuples(Oid relationId);
static void GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
									  Oid *columnTypeId, int32 *columnTypeMod,
									  Oid *intervalTypeId, int32 *intervalTypeMod);
static ShardInterval * TupleToShardInterval(HeapTuple heapTuple,
											TupleDesc tupleDescriptor, Oid intervalTypeId,
											int32 intervalTypeMod);
static void CachedNamespaceLookup(const char *nspname, Oid *cachedOid);
static void CachedRelationLookup(const char *relationName, Oid *cachedOid);
static void CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace,
										  Oid *cachedOid);
static ShardPlacement * ResolveGroupShardPlacement(
	GroupShardPlacement *groupShardPlacement, ShardCacheEntry *shardEntry);
static Oid LookupEnumValueId(Oid typeId, char *valueName);
static void InvalidateDistTableCache(void);
static void InvalidateDistObjectCache(void);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_dist_partition_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_shard_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_placement_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_node_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_local_group_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_authinfo_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_object_cache_invalidate);
PG_FUNCTION_INFO_V1(role_exists);
PG_FUNCTION_INFO_V1(authinfo_valid);
PG_FUNCTION_INFO_V1(poolinfo_valid);


/*
 * EnsureModificationsCanRun checks if the current node is in recovery mode or
 * citus.use_secondary_nodes is 'always'. If either is true the function errors out.
 */
void
EnsureModificationsCanRun(void)
{
	if (RecoveryInProgress() && !WritableStandbyCoordinator)
	{
		ereport(ERROR, (errmsg("writing to worker nodes is not currently allowed"),
						errdetail("the database is in recovery mode")));
	}

	if (ReadFromSecondaries == USE_SECONDARY_NODES_ALWAYS)
	{
		ereport(ERROR, (errmsg("writing to worker nodes is not currently allowed"),
						errdetail("citus.use_secondary_nodes is set to 'always'")));
	}
}


/*
 * IsDistributedTable returns whether relationId is a distributed relation or
 * not.
 */
bool
IsDistributedTable(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = NULL;

	cacheEntry = LookupDistTableCacheEntry(relationId);

	/*
	 * If extension hasn't been created, or has the wrong version and the
	 * table isn't a distributed one, LookupDistTableCacheEntry() will return NULL.
	 */
	if (!cacheEntry)
	{
		return false;
	}

	return cacheEntry->isDistributedTable;
}


/*
 * IsDistributedTableViaCatalog returns whether the given relation is a
 * distributed table or not.
 *
 * It does so by searching pg_dist_partition, explicitly bypassing caches,
 * because this function is designed to be used in cases where accessing
 * metadata tables is not safe.
 *
 * NB: Currently this still hardcodes pg_dist_partition logicalrelid column
 * offset and the corresponding index.  If we ever come close to changing
 * that, we'll have to work a bit harder.
 */
static bool
IsDistributedTableViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	Relation pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionLogicalRelidIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	partitionTuple = systable_getnext(scanDescriptor);
	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, AccessShareLock);

	return HeapTupleIsValid(partitionTuple);
}


/*
 * DistributedTableList returns a list that includes all the valid distributed table
 * cache entries.
 */
List *
DistributedTableList(void)
{
	List *distTableOidList = NIL;
	List *distributedTableList = NIL;
	ListCell *distTableOidCell = NULL;

	Assert(CitusHasBeenLoaded() && CheckCitusVersion(WARNING));

	/* first, we need to iterate over pg_dist_partition */
	distTableOidList = DistTableOidList();

	foreach(distTableOidCell, distTableOidList)
	{
		DistTableCacheEntry *cacheEntry = NULL;
		Oid relationId = lfirst_oid(distTableOidCell);

		cacheEntry = DistributedTableCacheEntry(relationId);

		distributedTableList = lappend(distributedTableList, cacheEntry);
	}

	return distributedTableList;
}


/*
 * LoadShardInterval returns the, cached, metadata about a shard.
 *
 * The return value is a copy of the cached ShardInterval struct and may
 * therefore be modified and/or freed.
 */
ShardInterval *
LoadShardInterval(uint64 shardId)
{
	ShardInterval *shardInterval = NULL;
	ShardInterval *sourceShardInterval = NULL;
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;

	shardEntry = LookupShardCacheEntry(shardId);

	tableEntry = shardEntry->tableEntry;

	Assert(tableEntry->isDistributedTable);

	/* the offset better be in a valid range */
	Assert(shardEntry->shardIndex < tableEntry->shardIntervalArrayLength);

	sourceShardInterval = tableEntry->sortedShardIntervalArray[shardEntry->shardIndex];

	/* copy value to return */
	shardInterval = (ShardInterval *) palloc0(sizeof(ShardInterval));
	CopyShardInterval(sourceShardInterval, shardInterval);

	return shardInterval;
}


/*
 * RelationIdOfShard returns the relationId of the given
 * shardId.
 */
Oid
RelationIdForShard(uint64 shardId)
{
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;

	shardEntry = LookupShardCacheEntry(shardId);

	tableEntry = shardEntry->tableEntry;

	Assert(tableEntry->isDistributedTable);

	return tableEntry->relationId;
}


/*
 * ReferenceTableShardId returns true if the given shardId belongs to
 * a reference table.
 */
bool
ReferenceTableShardId(uint64 shardId)
{
	ShardCacheEntry *shardEntry = LookupShardCacheEntry(shardId);
	DistTableCacheEntry *tableEntry = shardEntry->tableEntry;

	return (tableEntry->partitionMethod == DISTRIBUTE_BY_NONE);
}


/*
 * LoadGroupShardPlacement returns the cached shard placement metadata
 *
 * The return value is a copy of the cached GroupShardPlacement struct and may
 * therefore be modified and/or freed.
 */
GroupShardPlacement *
LoadGroupShardPlacement(uint64 shardId, uint64 placementId)
{
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;

	GroupShardPlacement *placementArray = NULL;
	int numberOfPlacements = 0;

	int i = 0;

	shardEntry = LookupShardCacheEntry(shardId);
	tableEntry = shardEntry->tableEntry;

	/* the offset better be in a valid range */
	Assert(shardEntry->shardIndex < tableEntry->shardIntervalArrayLength);

	placementArray = tableEntry->arrayOfPlacementArrays[shardEntry->shardIndex];
	numberOfPlacements = tableEntry->arrayOfPlacementArrayLengths[shardEntry->shardIndex];

	for (i = 0; i < numberOfPlacements; i++)
	{
		if (placementArray[i].placementId == placementId)
		{
			GroupShardPlacement *shardPlacement = CitusMakeNode(GroupShardPlacement);

			memcpy(shardPlacement, &placementArray[i], sizeof(GroupShardPlacement));

			return shardPlacement;
		}
	}

	ereport(ERROR, (errmsg("could not find valid entry for shard placement "
						   UINT64_FORMAT, placementId)));
}


/*
 * LoadShardPlacement returns a shard placement for the primary node.
 */
ShardPlacement *
LoadShardPlacement(uint64 shardId, uint64 placementId)
{
	ShardCacheEntry *shardEntry = NULL;
	GroupShardPlacement *groupPlacement = NULL;
	ShardPlacement *nodePlacement = NULL;

	shardEntry = LookupShardCacheEntry(shardId);
	groupPlacement = LoadGroupShardPlacement(shardId, placementId);
	nodePlacement = ResolveGroupShardPlacement(groupPlacement, shardEntry);

	return nodePlacement;
}


/*
 * FindShardPlacementOnGroup returns the shard placement for the given shard
 * on the given group, or returns NULL if no placement for the shard exists
 * on the group.
 */
ShardPlacement *
FindShardPlacementOnGroup(int32 groupId, uint64 shardId)
{
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;
	GroupShardPlacement *placementArray = NULL;
	int numberOfPlacements = 0;
	ShardPlacement *placementOnNode = NULL;
	int placementIndex = 0;

	shardEntry = LookupShardCacheEntry(shardId);
	tableEntry = shardEntry->tableEntry;
	placementArray = tableEntry->arrayOfPlacementArrays[shardEntry->shardIndex];
	numberOfPlacements = tableEntry->arrayOfPlacementArrayLengths[shardEntry->shardIndex];

	for (placementIndex = 0; placementIndex < numberOfPlacements; placementIndex++)
	{
		GroupShardPlacement *placement = &placementArray[placementIndex];

		if (placement->groupId == groupId)
		{
			placementOnNode = ResolveGroupShardPlacement(placement, shardEntry);
			break;
		}
	}

	return placementOnNode;
}


/*
 * ResolveGroupShardPlacement takes a GroupShardPlacement and adds additional data to it,
 * such as the node we should consider it to be on.
 */
static ShardPlacement *
ResolveGroupShardPlacement(GroupShardPlacement *groupShardPlacement,
						   ShardCacheEntry *shardEntry)
{
	DistTableCacheEntry *tableEntry = shardEntry->tableEntry;
	int shardIndex = shardEntry->shardIndex;
	ShardInterval *shardInterval = tableEntry->sortedShardIntervalArray[shardIndex];

	ShardPlacement *shardPlacement = CitusMakeNode(ShardPlacement);
	int32 groupId = groupShardPlacement->groupId;
	WorkerNode *workerNode = LookupNodeForGroup(groupId);

	/* copy everything into shardPlacement but preserve the header */
	memcpy((((CitusNode *) shardPlacement) + 1),
		   (((CitusNode *) groupShardPlacement) + 1),
		   sizeof(GroupShardPlacement) - sizeof(CitusNode));

	shardPlacement->nodeName = pstrdup(workerNode->workerName);
	shardPlacement->nodePort = workerNode->workerPort;
	shardPlacement->nodeId = workerNode->nodeId;

	/* fill in remaining fields */
	Assert(tableEntry->partitionMethod != 0);
	shardPlacement->partitionMethod = tableEntry->partitionMethod;
	shardPlacement->colocationGroupId = tableEntry->colocationId;
	if (tableEntry->partitionMethod == DISTRIBUTE_BY_HASH)
	{
		Assert(shardInterval->minValueExists);
		Assert(shardInterval->valueTypeId == INT4OID);

		/*
		 * Use the lower boundary of the interval's range to identify
		 * it for colocation purposes. That remains meaningful even if
		 * a concurrent session splits a shard.
		 */
		shardPlacement->representativeValue = DatumGetInt32(shardInterval->minValue);
	}
	else
	{
		shardPlacement->representativeValue = 0;
	}

	return shardPlacement;
}


/*
 * LookupNodeByNodeId returns a worker node by nodeId or NULL if the node
 * cannot be found.
 */
WorkerNode *
LookupNodeByNodeId(uint32 nodeId)
{
	int workerNodeIndex = 0;

	PrepareWorkerNodeCache();

	for (workerNodeIndex = 0; workerNodeIndex < WorkerNodeCount; workerNodeIndex++)
	{
		WorkerNode *workerNode = WorkerNodeArray[workerNodeIndex];
		if (workerNode->nodeId == nodeId)
		{
			WorkerNode *workerNodeCopy = palloc0(sizeof(WorkerNode));
			memcpy(workerNodeCopy, workerNode, sizeof(WorkerNode));

			return workerNodeCopy;
		}
	}

	return NULL;
}


/*
 * LookupNodeForGroup searches the WorkerNodeHash for a worker which is a member of the
 * given group and also readable (a primary if we're reading from primaries, a secondary
 * if we're reading from secondaries). If such a node does not exist it emits an
 * appropriate error message.
 */
WorkerNode *
LookupNodeForGroup(int32 groupId)
{
	bool foundAnyNodes = false;
	int workerNodeIndex = 0;

	PrepareWorkerNodeCache();

	for (workerNodeIndex = 0; workerNodeIndex < WorkerNodeCount; workerNodeIndex++)
	{
		WorkerNode *workerNode = WorkerNodeArray[workerNodeIndex];
		int32 workerNodeGroupId = workerNode->groupId;
		if (workerNodeGroupId != groupId)
		{
			continue;
		}

		foundAnyNodes = true;

		if (WorkerNodeIsReadable(workerNode))
		{
			return workerNode;
		}
	}

	if (!foundAnyNodes)
	{
		ereport(ERROR, (errmsg("there is a shard placement in node group %d but "
							   "there are no nodes in that group", groupId)));
	}

	switch (ReadFromSecondaries)
	{
		case USE_SECONDARY_NODES_NEVER:
		{
			ereport(ERROR, (errmsg("node group %d does not have a primary node",
								   groupId)));
			return NULL;
		}

		case USE_SECONDARY_NODES_ALWAYS:
		{
			ereport(ERROR, (errmsg("node group %d does not have a secondary node",
								   groupId)));
			return NULL;
		}

		default:
		{
			ereport(FATAL, (errmsg("unrecognized value for use_secondary_nodes")));
			return NULL;
		}
	}
}


/*
 * ShardPlacementList returns the list of placements for the given shard from
 * the cache.
 *
 * The returned list is deep copied from the cache and thus can be modified
 * and pfree()d freely.
 */
List *
ShardPlacementList(uint64 shardId)
{
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;
	GroupShardPlacement *placementArray = NULL;
	int numberOfPlacements = 0;
	List *placementList = NIL;
	int i = 0;

	shardEntry = LookupShardCacheEntry(shardId);
	tableEntry = shardEntry->tableEntry;

	/* the offset better be in a valid range */
	Assert(shardEntry->shardIndex < tableEntry->shardIntervalArrayLength);

	placementArray = tableEntry->arrayOfPlacementArrays[shardEntry->shardIndex];
	numberOfPlacements = tableEntry->arrayOfPlacementArrayLengths[shardEntry->shardIndex];

	for (i = 0; i < numberOfPlacements; i++)
	{
		GroupShardPlacement *groupShardPlacement = &placementArray[i];
		ShardPlacement *shardPlacement = ResolveGroupShardPlacement(groupShardPlacement,
																	shardEntry);

		placementList = lappend(placementList, shardPlacement);
	}

	/* if no shard placements are found, warn the user */
	if (numberOfPlacements == 0)
	{
		ereport(WARNING, (errmsg("could not find any shard placements for shardId "
								 UINT64_FORMAT, shardId)));
	}

	return placementList;
}


/*
 * LookupShardCacheEntry returns the cache entry belonging to a shard, or
 * errors out if that shard is unknown.
 */
static ShardCacheEntry *
LookupShardCacheEntry(int64 shardId)
{
	ShardCacheEntry *shardEntry = NULL;
	bool foundInCache = false;
	bool recheck = false;

	Assert(CitusHasBeenLoaded() && CheckCitusVersion(WARNING));

	InitializeCaches();

	/* lookup cache entry */
	shardEntry = hash_search(DistShardCacheHash, &shardId, HASH_FIND, &foundInCache);

	if (!foundInCache)
	{
		/*
		 * A possible reason for not finding an entry in the cache is that the
		 * distributed table's cache entry hasn't been accessed. Thus look up
		 * the distributed table, and build the cache entry.  Afterwards we
		 * know that the shard has to be in the cache if it exists.  If the
		 * shard does *not* exist LookupShardRelation() will error out.
		 */
		Oid relationId = LookupShardRelation(shardId, false);

		/* trigger building the cache for the shard id */
		LookupDistTableCacheEntry(relationId);

		recheck = true;
	}
	else
	{
		/*
		 * We might have some concurrent metadata changes. In order to get the changes,
		 * we first need to accept the cache invalidation messages.
		 */
		AcceptInvalidationMessages();

		if (!shardEntry->tableEntry->isValid)
		{
			Oid oldRelationId = shardEntry->tableEntry->relationId;
			Oid currentRelationId = LookupShardRelation(shardId, false);

			/*
			 * The relation OID to which the shard belongs could have changed,
			 * most notably when the extension is dropped and a shard ID is
			 * reused. Reload the cache entries for both old and new relation
			 * ID and then look up the shard entry again.
			 */
			LookupDistTableCacheEntry(oldRelationId);
			LookupDistTableCacheEntry(currentRelationId);

			recheck = true;
		}
	}

	/*
	 * If we (re-)loaded the table cache, re-search the shard cache - the
	 * shard index might have changed.  If we still can't find the entry, it
	 * can't exist.
	 */
	if (recheck)
	{
		shardEntry = hash_search(DistShardCacheHash, &shardId, HASH_FIND, &foundInCache);

		if (!foundInCache)
		{
			ereport(ERROR, (errmsg("could not find valid entry for shard "
								   UINT64_FORMAT, shardId)));
		}
	}

	return shardEntry;
}


/*
 * DistributedTableCacheEntry looks up a pg_dist_partition entry for a
 * relation.
 *
 * Errors out if no relation matching the criteria could be found.
 */
DistTableCacheEntry *
DistributedTableCacheEntry(Oid distributedRelationId)
{
	DistTableCacheEntry *cacheEntry =
		LookupDistTableCacheEntry(distributedRelationId);

	if (cacheEntry && cacheEntry->isDistributedTable)
	{
		return cacheEntry;
	}
	else
	{
		char *relationName = get_rel_name(distributedRelationId);
		ereport(ERROR, (errmsg("relation %s is not distributed", relationName)));
	}
}


/*
 * LookupDistTableCacheEntry returns the distributed table metadata for the
 * passed relationId. For efficiency it caches lookups.
 */
static DistTableCacheEntry *
LookupDistTableCacheEntry(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = NULL;
	bool foundInCache = false;
	void *hashKey = (void *) &relationId;

	/*
	 * Can't be a distributed relation if the extension hasn't been loaded
	 * yet. As we can't do lookups in nonexistent tables, directly return NULL
	 * here.
	 */
	if (!CitusHasBeenLoaded())
	{
		return NULL;
	}

	InitializeCaches();

	/*
	 * If the version is not known to be compatible, perform thorough check,
	 * unless such checks are disabled.
	 */
	if (!citusVersionKnownCompatible && EnableVersionChecks)
	{
		bool isDistributed = IsDistributedTableViaCatalog(relationId);
		int reportLevel = DEBUG1;

		/*
		 * If there's a version-mismatch, and we're dealing with a distributed
		 * table, we have to error out as we can't return a valid entry.  We
		 * want to check compatibility in the non-distributed case as well, so
		 * future lookups can use the cache if compatible.
		 */
		if (isDistributed)
		{
			reportLevel = ERROR;
		}

		if (!CheckCitusVersion(reportLevel))
		{
			/* incompatible, can't access cache, so return before doing so */
			return NULL;
		}
	}

	cacheEntry = hash_search(DistTableCacheHash, hashKey, HASH_ENTER, &foundInCache);

	/* return valid matches */
	if (foundInCache)
	{
		/*
		 * We might have some concurrent metadata changes. In order to get the changes,
		 * we first need to accept the cache invalidation messages.
		 */
		AcceptInvalidationMessages();

		if (cacheEntry->isValid)
		{
			return cacheEntry;
		}

		/* free the content of old, invalid, entries */
		ResetDistTableCacheEntry(cacheEntry);
	}

	/* zero out entry, but not the key part */
	memset(((char *) cacheEntry) + sizeof(Oid), 0,
		   sizeof(DistTableCacheEntry) - sizeof(Oid));

	/*
	 * We disable interrupts while creating the cache entry because loading
	 * shard metadata can take a while, and if statement_timeout is too low,
	 * this will get canceled on each call and we won't be able to run any
	 * queries on the table.
	 */
	HOLD_INTERRUPTS();

	/* actually fill out entry */
	BuildDistTableCacheEntry(cacheEntry);

	/* and finally mark as valid */
	cacheEntry->isValid = true;

	RESUME_INTERRUPTS();

	return cacheEntry;
}


/*
 * LookupDistObjectCacheEntry returns the distributed table metadata for the
 * passed relationId. For efficiency it caches lookups.
 */
DistObjectCacheEntry *
LookupDistObjectCacheEntry(Oid classid, Oid objid, int32 objsubid)
{
	DistObjectCacheEntry *cacheEntry = NULL;
	bool foundInCache = false;
	DistObjectCacheEntryKey hashKey;
	Relation pgDistObjectRel = NULL;
	TupleDesc pgDistObjectTupleDesc = NULL;
	ScanKeyData pgDistObjectKey[3];
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;

	memset(&hashKey, 0, sizeof(DistObjectCacheEntryKey));
	hashKey.classid = classid;
	hashKey.objid = objid;
	hashKey.objsubid = objsubid;

	/*
	 * Can't be a distributed relation if the extension hasn't been loaded
	 * yet. As we can't do lookups in nonexistent tables, directly return NULL
	 * here.
	 */
	if (!CitusHasBeenLoaded())
	{
		return NULL;
	}

	InitializeCaches();

	cacheEntry = hash_search(DistObjectCacheHash, &hashKey, HASH_ENTER, &foundInCache);

	/* return valid matches */
	if (foundInCache)
	{
		/*
		 * We might have some concurrent metadata changes. In order to get the changes,
		 * we first need to accept the cache invalidation messages.
		 */
		AcceptInvalidationMessages();

		if (cacheEntry->isValid)
		{
			return cacheEntry;
		}

		/*
		 * This is where we'd free the old entry's out of band data if it had any.
		 * Right now we don't have anything to free.
		 */
	}

	/* zero out entry, but not the key part */
	memset(((char *) cacheEntry), 0, sizeof(DistObjectCacheEntry));
	cacheEntry->key.classid = classid;
	cacheEntry->key.objid = objid;
	cacheEntry->key.objsubid = objsubid;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);
	pgDistObjectTupleDesc = RelationGetDescr(pgDistObjectRel);

	ScanKeyInit(&pgDistObjectKey[0], Anum_pg_dist_object_classid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
	ScanKeyInit(&pgDistObjectKey[1], Anum_pg_dist_object_objid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
	ScanKeyInit(&pgDistObjectKey[2], Anum_pg_dist_object_objsubid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

	pgDistObjectScan = systable_beginscan(pgDistObjectRel, DistObjectPrimaryKeyIndexId(),
										  true, NULL, 3, pgDistObjectKey);
	pgDistObjectTup = systable_getnext(pgDistObjectScan);

	if (HeapTupleIsValid(pgDistObjectTup))
	{
		Datum datumArray[Natts_pg_dist_object];
		bool isNullArray[Natts_pg_dist_object];

		heap_deform_tuple(pgDistObjectTup, pgDistObjectTupleDesc, datumArray,
						  isNullArray);

		cacheEntry->isValid = true;
		cacheEntry->isDistributed = true;

		cacheEntry->distributionArgIndex =
			DatumGetInt32(datumArray[Anum_pg_dist_object_distribution_argument_index -
									 1]);
		cacheEntry->colocationId =
			DatumGetInt32(datumArray[Anum_pg_dist_object_colocationid - 1]);
	}
	else
	{
		cacheEntry->isValid = true;
		cacheEntry->isDistributed = false;
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return cacheEntry;
}


/*
 * BuildDistTableCacheEntry is a helper routine for
 * LookupDistTableCacheEntry() for building the cache contents.
 */
static void
BuildDistTableCacheEntry(DistTableCacheEntry *cacheEntry)
{
	HeapTuple distPartitionTuple = NULL;
	Relation pgDistPartition = NULL;
	Datum partitionKeyDatum = 0;
	Datum replicationModelDatum = 0;
	MemoryContext oldContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	bool partitionKeyIsNull = false;
	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);
	distPartitionTuple =
		LookupDistPartitionTuple(pgDistPartition, cacheEntry->relationId);

	/* not a distributed table, done */
	if (distPartitionTuple == NULL)
	{
		cacheEntry->isDistributedTable = false;
		heap_close(pgDistPartition, NoLock);
		return;
	}

	cacheEntry->isDistributedTable = true;

	tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(distPartitionTuple, tupleDescriptor, datumArray, isNullArray);

	cacheEntry->partitionMethod = datumArray[Anum_pg_dist_partition_partmethod - 1];
	partitionKeyDatum = datumArray[Anum_pg_dist_partition_partkey - 1];
	partitionKeyIsNull = isNullArray[Anum_pg_dist_partition_partkey - 1];

	/* note that for reference tables partitionKeyisNull is true */
	if (!partitionKeyIsNull)
	{
		Node *partitionNode = NULL;

		oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

		/* get the string representation of the partition column Var */
		cacheEntry->partitionKeyString = TextDatumGetCString(partitionKeyDatum);

		/* convert the string to a Node and ensure it is a Var */
		partitionNode = stringToNode(cacheEntry->partitionKeyString);
		Assert(IsA(partitionNode, Var));

		cacheEntry->partitionColumn = (Var *) partitionNode;

		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		cacheEntry->partitionKeyString = NULL;
	}

	cacheEntry->colocationId = datumArray[Anum_pg_dist_partition_colocationid - 1];
	if (isNullArray[Anum_pg_dist_partition_colocationid - 1])
	{
		cacheEntry->colocationId = INVALID_COLOCATION_ID;
	}

	replicationModelDatum = datumArray[Anum_pg_dist_partition_repmodel - 1];
	if (isNullArray[Anum_pg_dist_partition_repmodel - 1])
	{
		/*
		 * repmodel is NOT NULL but before ALTER EXTENSION citus UPGRADE the column
		 * doesn't exist
		 */
		cacheEntry->replicationModel = 'c';
	}
	else
	{
		cacheEntry->replicationModel = DatumGetChar(replicationModelDatum);
	}

	heap_freetuple(distPartitionTuple);

	BuildCachedShardList(cacheEntry);

	/* we only need hash functions for hash distributed tables */
	if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
	{
		TypeCacheEntry *typeEntry = NULL;
		FmgrInfo *hashFunction = NULL;
		Var *partitionColumn = cacheEntry->partitionColumn;

		typeEntry = lookup_type_cache(partitionColumn->vartype,
									  TYPECACHE_HASH_PROC_FINFO);

		hashFunction = MemoryContextAllocZero(MetadataCacheMemoryContext,
											  sizeof(FmgrInfo));

		fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo),
					   MetadataCacheMemoryContext);

		cacheEntry->hashFunction = hashFunction;

		/* check the shard distribution for hash partitioned tables */
		cacheEntry->hasUniformHashDistribution =
			HasUniformHashDistribution(cacheEntry->sortedShardIntervalArray,
									   cacheEntry->shardIntervalArrayLength);
	}
	else
	{
		cacheEntry->hashFunction = NULL;
	}

	oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

	cacheEntry->referencedRelationsViaForeignKey = ReferencedRelationIdList(
		cacheEntry->relationId);
	cacheEntry->referencingRelationsViaForeignKey = ReferencingRelationIdList(
		cacheEntry->relationId);

	MemoryContextSwitchTo(oldContext);

	heap_close(pgDistPartition, NoLock);
}


/*
 * BuildCachedShardList() is a helper routine for BuildDistTableCacheEntry()
 * building up the list of shards in a distributed relation.
 */
static void
BuildCachedShardList(DistTableCacheEntry *cacheEntry)
{
	ShardInterval **shardIntervalArray = NULL;
	ShardInterval **sortedShardIntervalArray = NULL;
	FmgrInfo *shardIntervalCompareFunction = NULL;
	FmgrInfo *shardColumnCompareFunction = NULL;
	List *distShardTupleList = NIL;
	int shardIntervalArrayLength = 0;
	int shardIndex = 0;
	Oid columnTypeId = InvalidOid;
	int32 columnTypeMod = -1;
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = -1;

	GetPartitionTypeInputInfo(cacheEntry->partitionKeyString,
							  cacheEntry->partitionMethod,
							  &columnTypeId,
							  &columnTypeMod,
							  &intervalTypeId,
							  &intervalTypeMod);

	distShardTupleList = LookupDistShardTuples(cacheEntry->relationId);
	shardIntervalArrayLength = list_length(distShardTupleList);
	if (shardIntervalArrayLength > 0)
	{
		Relation distShardRelation = heap_open(DistShardRelationId(), AccessShareLock);
		TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);
		ListCell *distShardTupleCell = NULL;
		int arrayIndex = 0;

		shardIntervalArray = MemoryContextAllocZero(MetadataCacheMemoryContext,
													shardIntervalArrayLength *
													sizeof(ShardInterval *));

		cacheEntry->arrayOfPlacementArrays =
			MemoryContextAllocZero(MetadataCacheMemoryContext,
								   shardIntervalArrayLength *
								   sizeof(GroupShardPlacement *));
		cacheEntry->arrayOfPlacementArrayLengths =
			MemoryContextAllocZero(MetadataCacheMemoryContext,
								   shardIntervalArrayLength *
								   sizeof(int));

		foreach(distShardTupleCell, distShardTupleList)
		{
			HeapTuple shardTuple = lfirst(distShardTupleCell);
			ShardInterval *shardInterval = TupleToShardInterval(shardTuple,
																distShardTupleDesc,
																intervalTypeId,
																intervalTypeMod);
			ShardInterval *newShardInterval = NULL;
			MemoryContext oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

			newShardInterval = (ShardInterval *) palloc0(sizeof(ShardInterval));
			CopyShardInterval(shardInterval, newShardInterval);
			shardIntervalArray[arrayIndex] = newShardInterval;

			MemoryContextSwitchTo(oldContext);

			heap_freetuple(shardTuple);

			arrayIndex++;
		}

		heap_close(distShardRelation, AccessShareLock);
	}

	/* look up value comparison function */
	if (columnTypeId != InvalidOid)
	{
		/* allocate the comparison function in the cache context */
		MemoryContext oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

		shardColumnCompareFunction = GetFunctionInfo(columnTypeId, BTREE_AM_OID,
													 BTORDER_PROC);
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		shardColumnCompareFunction = NULL;
	}

	/* look up interval comparison function */
	if (intervalTypeId != InvalidOid)
	{
		/* allocate the comparison function in the cache context */
		MemoryContext oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

		shardIntervalCompareFunction = GetFunctionInfo(intervalTypeId, BTREE_AM_OID,
													   BTORDER_PROC);
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		shardIntervalCompareFunction = NULL;
	}

	/* reference tables has a single shard which is not initialized */
	if (cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE)
	{
		cacheEntry->hasUninitializedShardInterval = true;
		cacheEntry->hasOverlappingShardInterval = true;

		/*
		 * Note that during create_reference_table() call,
		 * the reference table do not have any shards.
		 */
		if (shardIntervalArrayLength > 1)
		{
			char *relationName = get_rel_name(cacheEntry->relationId);

			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("reference table \"%s\" has more than 1 shard",
								   relationName)));
		}

		/* since there is a zero or one shard, it is already sorted */
		sortedShardIntervalArray = shardIntervalArray;
	}
	else
	{
		/* sort the interval array */
		sortedShardIntervalArray = SortShardIntervalArray(shardIntervalArray,
														  shardIntervalArrayLength,
														  shardIntervalCompareFunction);

		/* check if there exists any shard intervals with no min/max values */
		cacheEntry->hasUninitializedShardInterval =
			HasUninitializedShardInterval(sortedShardIntervalArray,
										  shardIntervalArrayLength);

		if (!cacheEntry->hasUninitializedShardInterval)
		{
			cacheEntry->hasOverlappingShardInterval =
				HasOverlappingShardInterval(sortedShardIntervalArray,
											shardIntervalArrayLength,
											shardIntervalCompareFunction);
		}
		else
		{
			cacheEntry->hasOverlappingShardInterval = true;
		}

		/*
		 * If table is hash-partitioned and has shards, there never should be
		 * any uninitalized shards.  Historically we've not prevented that for
		 * range partitioned tables, but it might be a good idea to start
		 * doing so.
		 */
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH &&
			cacheEntry->hasUninitializedShardInterval)
		{
			ereport(ERROR, (errmsg("hash partitioned table has uninitialized shards")));
		}
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH &&
			cacheEntry->hasOverlappingShardInterval)
		{
			ereport(ERROR, (errmsg("hash partitioned table has overlapping shards")));
		}
	}

	/*
	 * We set these here, so ResetDistTableCacheEntry() can see what has been
	 * entered into DistShardCacheHash even if the following loop is interrupted
	 * by throwing errors, etc.
	 */
	cacheEntry->sortedShardIntervalArray = sortedShardIntervalArray;
	cacheEntry->shardIntervalArrayLength = 0;

	/* maintain shardId->(table,ShardInterval) cache */
	for (shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardCacheEntry *shardEntry = NULL;
		ShardInterval *shardInterval = sortedShardIntervalArray[shardIndex];
		bool foundInCache = false;
		List *placementList = NIL;
		MemoryContext oldContext = NULL;
		ListCell *placementCell = NULL;
		GroupShardPlacement *placementArray = NULL;
		int placementOffset = 0;
		int numberOfPlacements = 0;

		shardEntry = hash_search(DistShardCacheHash, &shardInterval->shardId, HASH_ENTER,
								 &foundInCache);
		if (foundInCache)
		{
			ereport(ERROR, (errmsg("cached metadata for shard " UINT64_FORMAT
								   " is inconsistent",
								   shardInterval->shardId),
							errhint("Reconnect and try again.")));
		}

		/*
		 * We should increment this only after we are sure this hasn't already
		 * been assigned to any other relations. ResetDistTableCacheEntry()
		 * depends on this.
		 */
		cacheEntry->shardIntervalArrayLength++;

		shardEntry->shardIndex = shardIndex;
		shardEntry->tableEntry = cacheEntry;

		/* build list of shard placements */
		placementList = BuildShardPlacementList(shardInterval);
		numberOfPlacements = list_length(placementList);

		/* and copy that list into the cache entry */
		oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);
		placementArray = palloc0(numberOfPlacements * sizeof(GroupShardPlacement));
		foreach(placementCell, placementList)
		{
			GroupShardPlacement *srcPlacement =
				(GroupShardPlacement *) lfirst(placementCell);

			placementArray[placementOffset] = *srcPlacement;
			placementOffset++;
		}
		MemoryContextSwitchTo(oldContext);

		cacheEntry->arrayOfPlacementArrays[shardIndex] = placementArray;
		cacheEntry->arrayOfPlacementArrayLengths[shardIndex] = numberOfPlacements;

		/* store the shard index in the ShardInterval */
		shardInterval->shardIndex = shardIndex;
	}

	cacheEntry->shardColumnCompareFunction = shardColumnCompareFunction;
	cacheEntry->shardIntervalCompareFunction = shardIntervalCompareFunction;
}


/*
 * SortedShardIntervalArray sorts the input shardIntervalArray. Shard intervals with
 * no min/max values are placed at the end of the array.
 */
static ShardInterval **
SortShardIntervalArray(ShardInterval **shardIntervalArray, int shardCount,
					   FmgrInfo *shardIntervalSortCompareFunction)
{
	/* short cut if there are no shard intervals in the array */
	if (shardCount == 0)
	{
		return shardIntervalArray;
	}

	/* if a shard doesn't have min/max values, it's placed in the end of the array */
	qsort_arg(shardIntervalArray, shardCount, sizeof(ShardInterval *),
			  (qsort_arg_comparator) CompareShardIntervals,
			  (void *) shardIntervalSortCompareFunction);

	return shardIntervalArray;
}


/*
 * HasUniformHashDistribution determines whether the given list of sorted shards
 * has a uniform hash distribution, as produced by master_create_worker_shards for
 * hash partitioned tables.
 */
bool
HasUniformHashDistribution(ShardInterval **shardIntervalArray,
						   int shardIntervalArrayLength)
{
	uint64 hashTokenIncrement = 0;
	int shardIndex = 0;

	/* if there are no shards, there is no uniform distribution */
	if (shardIntervalArrayLength == 0)
	{
		return false;
	}

	/* calculate the hash token increment */
	hashTokenIncrement = HASH_TOKEN_COUNT / shardIntervalArrayLength;

	for (shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);

		if (shardIndex == (shardIntervalArrayLength - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		if (DatumGetInt32(shardInterval->minValue) != shardMinHashToken ||
			DatumGetInt32(shardInterval->maxValue) != shardMaxHashToken)
		{
			return false;
		}
	}

	return true;
}


/*
 * HasUninitializedShardInterval returns true if all the elements of the
 * sortedShardIntervalArray has min/max values. Callers of the function must
 * ensure that input shard interval array is sorted on shardminvalue and uninitialized
 * shard intervals are at the end of the array.
 */
static bool
HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray, int shardCount)
{
	bool hasUninitializedShardInterval = false;
	ShardInterval *lastShardInterval = NULL;

	if (shardCount == 0)
	{
		return hasUninitializedShardInterval;
	}

	Assert(sortedShardIntervalArray != NULL);

	/*
	 * Since the shard interval array is sorted, and uninitialized ones stored
	 * in the end of the array, checking the last element is enough.
	 */
	lastShardInterval = sortedShardIntervalArray[shardCount - 1];
	if (!lastShardInterval->minValueExists || !lastShardInterval->maxValueExists)
	{
		hasUninitializedShardInterval = true;
	}

	return hasUninitializedShardInterval;
}


/*
 * HasOverlappingShardInterval determines whether the given list of sorted
 * shards has overlapping ranges.
 */
static bool
HasOverlappingShardInterval(ShardInterval **shardIntervalArray,
							int shardIntervalArrayLength,
							FmgrInfo *shardIntervalSortCompareFunction)
{
	int shardIndex = 0;
	ShardInterval *lastShardInterval = NULL;
	Datum comparisonDatum = 0;
	int comparisonResult = 0;

	/* zero/a single shard can't overlap */
	if (shardIntervalArrayLength < 2)
	{
		return false;
	}

	lastShardInterval = shardIntervalArray[0];
	for (shardIndex = 1; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *curShardInterval = shardIntervalArray[shardIndex];

		/* only called if !hasUninitializedShardInterval */
		Assert(lastShardInterval->minValueExists && lastShardInterval->maxValueExists);
		Assert(curShardInterval->minValueExists && curShardInterval->maxValueExists);

		comparisonDatum = CompareCall2(shardIntervalSortCompareFunction,
									   lastShardInterval->maxValue,
									   curShardInterval->minValue);
		comparisonResult = DatumGetInt32(comparisonDatum);

		if (comparisonResult >= 0)
		{
			return true;
		}

		lastShardInterval = curShardInterval;
	}

	return false;
}


/*
 * CitusHasBeenLoaded returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false. The result is cached as this is called very frequently.
 */
bool
CitusHasBeenLoaded(void)
{
	/* recheck presence until citus has been loaded */
	if (!MetadataCache.extensionLoaded || creating_extension)
	{
		bool extensionPresent = false;
		bool extensionScriptExecuted = true;

		Oid extensionOid = get_extension_oid("citus", true);
		if (extensionOid != InvalidOid)
		{
			extensionPresent = true;
		}

		if (extensionPresent)
		{
			/* check if Citus extension objects are still being created */
			if (creating_extension && CurrentExtensionObject == extensionOid)
			{
				extensionScriptExecuted = false;
			}

			/*
			 * Whenever the extension exists, even when currently creating it,
			 * we need the infrastructure to run citus in this database to be
			 * ready.
			 */
			StartupCitusBackend();
		}

		/* we disable extension features during pg_upgrade */
		MetadataCache.extensionLoaded = extensionPresent &&
										extensionScriptExecuted &&
										!IsBinaryUpgrade;

		if (MetadataCache.extensionLoaded)
		{
			/*
			 * InvalidateDistRelationCacheCallback resets state such as extensionLoaded
			 * when it notices changes to pg_dist_partition (which usually indicate
			 * `DROP EXTENSION citus;` has been run)
			 *
			 * Ensure InvalidateDistRelationCacheCallback will notice those changes
			 * by caching pg_dist_partition's oid.
			 *
			 * We skip these checks during upgrade since pg_dist_partition is not
			 * present during early stages of upgrade operation.
			 */
			DistPartitionRelationId();


			/*
			 * This needs to be initialized so we can receive foreign relation graph
			 * invalidation messages in InvalidateForeignRelationGraphCacheCallback().
			 * See the comments of InvalidateForeignKeyGraph for more context.
			 */
			DistColocationRelationId();

			/*
			 * We also reset citusVersionKnownCompatible, so it will be re-read in
			 * case of extension update.
			 */
			citusVersionKnownCompatible = false;
		}
	}

	return MetadataCache.extensionLoaded;
}


/*
 * CheckCitusVersion checks whether there is a version mismatch between the
 * available version and the loaded version or between the installed version
 * and the loaded version. Returns true if compatible, false otherwise.
 *
 * As a side effect, this function also sets citusVersionKnownCompatible global
 * variable to true which reduces version check cost of next calls.
 */
bool
CheckCitusVersion(int elevel)
{
	if (citusVersionKnownCompatible ||
		!CitusHasBeenLoaded() ||
		!EnableVersionChecks)
	{
		return true;
	}

	if (CheckAvailableVersion(elevel) && CheckInstalledVersion(elevel))
	{
		citusVersionKnownCompatible = true;
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * CheckAvailableVersion compares CITUS_EXTENSIONVERSION and the currently
 * available version from the citus.control file. If they are not compatible,
 * this function logs an error with the specified elevel and returns false,
 * otherwise it returns true.
 */
bool
CheckAvailableVersion(int elevel)
{
	char *availableVersion = NULL;

	if (!EnableVersionChecks)
	{
		return true;
	}

	availableVersion = AvailableExtensionVersion();

	if (!MajorVersionsCompatible(availableVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from latest "
								"available extension version"),
						 errdetail("Loaded library requires %s, but the latest control "
								   "file specifies %s.", CITUS_MAJORVERSION,
								   availableVersion),
						 errhint("Restart the database to load the latest Citus "
								 "library.")));
		return false;
	}

	return true;
}


/*
 * CheckInstalledVersion compares CITUS_EXTENSIONVERSION and the the
 * extension's current version from the pg_extemsion catalog table. If they
 * are not compatible, this function logs an error with the specified elevel,
 * otherwise it returns true.
 */
static bool
CheckInstalledVersion(int elevel)
{
	char *installedVersion = NULL;

	Assert(CitusHasBeenLoaded());
	Assert(EnableVersionChecks);

	installedVersion = InstalledExtensionVersion();

	if (!MajorVersionsCompatible(installedVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from installed "
								"extension version"),
						 errdetail("Loaded library requires %s, but the installed "
								   "extension version is %s.", CITUS_MAJORVERSION,
								   installedVersion),
						 errhint("Run ALTER EXTENSION citus UPDATE and try again.")));
		return false;
	}

	return true;
}


/*
 * MajorVersionsCompatible checks whether both versions are compatible. They
 * are if major and minor version numbers match, the schema version is
 * ignored.  Returns true if compatible, false otherwise.
 */
bool
MajorVersionsCompatible(char *leftVersion, char *rightVersion)
{
	const char schemaVersionSeparator = '-';

	char *leftSeperatorPosition = strchr(leftVersion, schemaVersionSeparator);
	char *rightSeperatorPosition = strchr(rightVersion, schemaVersionSeparator);
	int leftComparisionLimit = 0;
	int rightComparisionLimit = 0;

	if (leftSeperatorPosition != NULL)
	{
		leftComparisionLimit = leftSeperatorPosition - leftVersion;
	}
	else
	{
		leftComparisionLimit = strlen(leftVersion);
	}

	if (rightSeperatorPosition != NULL)
	{
		rightComparisionLimit = rightSeperatorPosition - rightVersion;
	}
	else
	{
		rightComparisionLimit = strlen(leftVersion);
	}

	/* we can error out early if hypens are not in the same position */
	if (leftComparisionLimit != rightComparisionLimit)
	{
		return false;
	}

	return strncmp(leftVersion, rightVersion, leftComparisionLimit) == 0;
}


/*
 * AvailableExtensionVersion returns the Citus version from citus.control file. It also
 * saves the result, thus consecutive calls to CitusExtensionAvailableVersion will
 * not read the citus.control file again.
 */
static char *
AvailableExtensionVersion(void)
{
	ReturnSetInfo *extensionsResultSet = NULL;
	TupleTableSlot *tupleTableSlot = NULL;
	LOCAL_FCINFO(fcinfo, 0);
	FmgrInfo flinfo;
	EState *estate = NULL;

	bool hasTuple = false;
	bool goForward = true;
	bool doCopy = false;
	char *availableExtensionVersion;

	InitializeCaches();

	estate = CreateExecutorState();
	extensionsResultSet = makeNode(ReturnSetInfo);
	extensionsResultSet->econtext = GetPerTupleExprContext(estate);
	extensionsResultSet->allowedModes = SFRM_Materialize;

	fmgr_info(F_PG_AVAILABLE_EXTENSIONS, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 0, InvalidOid, NULL,
							 (Node *) extensionsResultSet);

	/* pg_available_extensions returns result set containing all available extensions */
	(*pg_available_extensions)(fcinfo);

	tupleTableSlot = MakeSingleTupleTableSlotCompat(extensionsResultSet->setDesc,
													&TTSOpsMinimalTuple);
	hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward, doCopy,
									   tupleTableSlot);
	while (hasTuple)
	{
		Datum extensionNameDatum = 0;
		char *extensionName = NULL;
		bool isNull = false;

		extensionNameDatum = slot_getattr(tupleTableSlot, 1, &isNull);
		extensionName = NameStr(*DatumGetName(extensionNameDatum));
		if (strcmp(extensionName, "citus") == 0)
		{
			MemoryContext oldMemoryContext = NULL;
			Datum availableVersion = slot_getattr(tupleTableSlot, 2, &isNull);

			/* we will cache the result of citus version to prevent catalog access */
			oldMemoryContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

			availableExtensionVersion = text_to_cstring(DatumGetTextPP(availableVersion));

			MemoryContextSwitchTo(oldMemoryContext);

			ExecClearTuple(tupleTableSlot);
			ExecDropSingleTupleTableSlot(tupleTableSlot);

			return availableExtensionVersion;
		}

		ExecClearTuple(tupleTableSlot);
		hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
										   doCopy, tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("citus extension is not found")));

	return NULL;
}


/*
 * InstalledExtensionVersion returns the Citus version in PostgreSQL pg_extension table.
 */
static char *
InstalledExtensionVersion(void)
{
	Relation relation = NULL;
	SysScanDesc scandesc;
	ScanKeyData entry[1];
	HeapTuple extensionTuple = NULL;
	char *installedExtensionVersion = NULL;

	InitializeCaches();

	relation = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
								  NULL, 1, entry);

	extensionTuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(extensionTuple))
	{
		MemoryContext oldMemoryContext = NULL;
		int extensionIndex = Anum_pg_extension_extversion;
		TupleDesc tupleDescriptor = RelationGetDescr(relation);
		bool isNull = false;

		Datum installedVersion = heap_getattr(extensionTuple, extensionIndex,
											  tupleDescriptor, &isNull);

		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("citus extension version is null")));
		}

		/* we will cache the result of citus version to prevent catalog access */
		oldMemoryContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

		installedExtensionVersion = text_to_cstring(DatumGetTextPP(installedVersion));

		MemoryContextSwitchTo(oldMemoryContext);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension is not loaded")));
	}

	systable_endscan(scandesc);

	heap_close(relation, AccessShareLock);

	return installedExtensionVersion;
}


/* return oid of pg_dist_shard relation */
Oid
DistShardRelationId(void)
{
	CachedRelationLookup("pg_dist_shard",
						 &MetadataCache.distShardRelationId);

	return MetadataCache.distShardRelationId;
}


/* return oid of pg_dist_placement relation */
Oid
DistPlacementRelationId(void)
{
	CachedRelationLookup("pg_dist_placement",
						 &MetadataCache.distPlacementRelationId);

	return MetadataCache.distPlacementRelationId;
}


/* return oid of pg_dist_node relation */
Oid
DistNodeRelationId(void)
{
	CachedRelationLookup("pg_dist_node",
						 &MetadataCache.distNodeRelationId);

	return MetadataCache.distNodeRelationId;
}


/* return oid of pg_dist_node's primary key index */
Oid
DistNodeNodeIdIndexId(void)
{
	CachedRelationLookup("pg_dist_node_pkey",
						 &MetadataCache.distNodeNodeIdIndexId);

	return MetadataCache.distNodeNodeIdIndexId;
}


/* return oid of pg_dist_local_group relation */
Oid
DistLocalGroupIdRelationId(void)
{
	CachedRelationLookup("pg_dist_local_group",
						 &MetadataCache.distLocalGroupRelationId);

	return MetadataCache.distLocalGroupRelationId;
}


/* return the oid of citus namespace */
Oid
CitusCatalogNamespaceId(void)
{
	CachedNamespaceLookup("citus", &MetadataCache.citusCatalogNamespaceId);
	return MetadataCache.citusCatalogNamespaceId;
}


/* return oid of pg_dist_shard relation */
Oid
DistObjectRelationId(void)
{
	CachedRelationNamespaceLookup("pg_dist_object", CitusCatalogNamespaceId(),
								  &MetadataCache.distObjectRelationId);

	return MetadataCache.distObjectRelationId;
}


/* return oid of pg_dist_object_pkey */
Oid
DistObjectPrimaryKeyIndexId(void)
{
	CachedRelationNamespaceLookup("pg_dist_object_pkey",
								  CitusCatalogNamespaceId(),
								  &MetadataCache.distObjectPrimaryKeyIndexId);

	return MetadataCache.distObjectPrimaryKeyIndexId;
}


/* return oid of pg_dist_colocation relation */
Oid
DistColocationRelationId(void)
{
	CachedRelationLookup("pg_dist_colocation",
						 &MetadataCache.distColocationRelationId);

	return MetadataCache.distColocationRelationId;
}


/* return oid of pg_dist_colocation_configuration_index index */
Oid
DistColocationConfigurationIndexId(void)
{
	CachedRelationLookup("pg_dist_colocation_configuration_index",
						 &MetadataCache.distColocationConfigurationIndexId);

	return MetadataCache.distColocationConfigurationIndexId;
}


/* return oid of pg_dist_colocation_pkey index */
Oid
DistColocationColocationidIndexId(void)
{
	CachedRelationLookup("pg_dist_colocation_pkey",
						 &MetadataCache.distColocationColocationidIndexId);

	return MetadataCache.distColocationColocationidIndexId;
}


/* return oid of pg_dist_partition relation */
Oid
DistPartitionRelationId(void)
{
	CachedRelationLookup("pg_dist_partition",
						 &MetadataCache.distPartitionRelationId);

	return MetadataCache.distPartitionRelationId;
}


/* return oid of pg_dist_partition_logical_relid_index index */
Oid
DistPartitionLogicalRelidIndexId(void)
{
	CachedRelationLookup("pg_dist_partition_logical_relid_index",
						 &MetadataCache.distPartitionLogicalRelidIndexId);

	return MetadataCache.distPartitionLogicalRelidIndexId;
}


/* return oid of pg_dist_partition_colocationid_index index */
Oid
DistPartitionColocationidIndexId(void)
{
	CachedRelationLookup("pg_dist_partition_colocationid_index",
						 &MetadataCache.distPartitionColocationidIndexId);

	return MetadataCache.distPartitionColocationidIndexId;
}


/* return oid of pg_dist_shard_logical_relid_index index */
Oid
DistShardLogicalRelidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_logical_relid_index",
						 &MetadataCache.distShardLogicalRelidIndexId);

	return MetadataCache.distShardLogicalRelidIndexId;
}


/* return oid of pg_dist_shard_shardid_index index */
Oid
DistShardShardidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_shardid_index",
						 &MetadataCache.distShardShardidIndexId);

	return MetadataCache.distShardShardidIndexId;
}


/* return oid of pg_dist_placement_shardid_index */
Oid
DistPlacementShardidIndexId(void)
{
	CachedRelationLookup("pg_dist_placement_shardid_index",
						 &MetadataCache.distPlacementShardidIndexId);

	return MetadataCache.distPlacementShardidIndexId;
}


/* return oid of pg_dist_placement_placementid_index */
Oid
DistPlacementPlacementidIndexId(void)
{
	CachedRelationLookup("pg_dist_placement_placementid_index",
						 &MetadataCache.distPlacementPlacementidIndexId);

	return MetadataCache.distPlacementPlacementidIndexId;
}


/* return oid of pg_dist_transaction relation */
Oid
DistTransactionRelationId(void)
{
	CachedRelationLookup("pg_dist_transaction",
						 &MetadataCache.distTransactionRelationId);

	return MetadataCache.distTransactionRelationId;
}


/* return oid of pg_dist_transaction_group_index */
Oid
DistTransactionGroupIndexId(void)
{
	CachedRelationLookup("pg_dist_transaction_group_index",
						 &MetadataCache.distTransactionGroupIndexId);

	return MetadataCache.distTransactionGroupIndexId;
}


/* return oid of pg_dist_transaction_unique_constraint */
Oid
DistTransactionRecordIndexId(void)
{
	CachedRelationLookup("pg_dist_transaction_unique_constraint",
						 &MetadataCache.distTransactionRecordIndexId);

	return MetadataCache.distTransactionRecordIndexId;
}


/* return oid of pg_dist_placement_groupid_index */
Oid
DistPlacementGroupidIndexId(void)
{
	CachedRelationLookup("pg_dist_placement_groupid_index",
						 &MetadataCache.distPlacementGroupidIndexId);

	return MetadataCache.distPlacementGroupidIndexId;
}


/* return oid of the read_intermediate_result(text,citus_copy_format) function */
Oid
CitusReadIntermediateResultFuncId(void)
{
	if (MetadataCache.readIntermediateResultFuncId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("read_intermediate_result"));
		Oid copyFormatTypeOid = CitusCopyFormatTypeId();
		Oid paramOids[2] = { TEXTOID, copyFormatTypeOid };
		bool missingOK = false;

		MetadataCache.readIntermediateResultFuncId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return MetadataCache.readIntermediateResultFuncId;
}


/* return oid of the citus.copy_format enum type */
Oid
CitusCopyFormatTypeId(void)
{
	if (MetadataCache.copyFormatTypeId == InvalidOid)
	{
		char *typeName = "citus_copy_format";
		MetadataCache.copyFormatTypeId = GetSysCacheOid2Compat(TYPENAMENSP,
															   Anum_pg_enum_oid,
															   PointerGetDatum(typeName),
															   PG_CATALOG_NAMESPACE);
	}

	return MetadataCache.copyFormatTypeId;
}


/* return oid of the 'binary' citus_copy_format enum value */
Oid
BinaryCopyFormatId(void)
{
	if (MetadataCache.binaryCopyFormatId == InvalidOid)
	{
		Oid copyFormatTypeId = CitusCopyFormatTypeId();
		MetadataCache.binaryCopyFormatId = LookupEnumValueId(copyFormatTypeId, "binary");
	}

	return MetadataCache.binaryCopyFormatId;
}


/* return oid of the 'text' citus_copy_format enum value */
Oid
TextCopyFormatId(void)
{
	if (MetadataCache.textCopyFormatId == InvalidOid)
	{
		Oid copyFormatTypeId = CitusCopyFormatTypeId();
		MetadataCache.textCopyFormatId = LookupEnumValueId(copyFormatTypeId, "text");
	}

	return MetadataCache.textCopyFormatId;
}


/* return oid of the citus_extradata_container(internal) function */
Oid
CitusExtraDataContainerFuncId(void)
{
	List *nameList = NIL;
	Oid paramOids[1] = { INTERNALOID };

	if (MetadataCache.extraDataContainerFuncId == InvalidOid)
	{
		nameList = list_make2(makeString("pg_catalog"),
							  makeString("citus_extradata_container"));
		MetadataCache.extraDataContainerFuncId =
			LookupFuncName(nameList, 1, paramOids, false);
	}

	return MetadataCache.extraDataContainerFuncId;
}


/* return oid of the worker_hash function */
Oid
CitusWorkerHashFunctionId(void)
{
	if (MetadataCache.workerHashFunctionId == InvalidOid)
	{
		Oid citusExtensionOid = get_extension_oid("citus", false);
		Oid citusSchemaOid = get_extension_schema(citusExtensionOid);
		char *citusSchemaName = get_namespace_name(citusSchemaOid);
		const int argCount = 1;

		MetadataCache.workerHashFunctionId =
			FunctionOid(citusSchemaName, "worker_hash", argCount);
	}

	return MetadataCache.workerHashFunctionId;
}


/* return oid of the any_value aggregate function */
Oid
CitusAnyValueFunctionId(void)
{
	if (MetadataCache.anyValueFunctionId == InvalidOid)
	{
		const int argCount = 1;
		MetadataCache.anyValueFunctionId =
			FunctionOid("pg_catalog", "any_value", argCount);
	}

	return MetadataCache.anyValueFunctionId;
}


/* return oid of the citus_text_send_as_jsonb(text) function */
Oid
CitusTextSendAsJsonbFunctionId(void)
{
	if (MetadataCache.textSendAsJsonbFunctionId == InvalidOid)
	{
		List *nameList = list_make2(makeString("pg_catalog"),
									makeString("citus_text_send_as_jsonb"));
		Oid paramOids[1] = { TEXTOID };

		MetadataCache.textSendAsJsonbFunctionId =
			LookupFuncName(nameList, 1, paramOids, false);
	}

	return MetadataCache.textSendAsJsonbFunctionId;
}


/*
 * PgTableVisibleFuncId returns oid of the pg_table_is_visible function.
 */
Oid
PgTableVisibleFuncId(void)
{
	if (MetadataCache.pgTableIsVisibleFuncId == InvalidOid)
	{
		const int argCount = 1;

		MetadataCache.pgTableIsVisibleFuncId =
			FunctionOid("pg_catalog", "pg_table_is_visible", argCount);
	}

	return MetadataCache.pgTableIsVisibleFuncId;
}


/*
 * CitusTableVisibleFuncId returns oid of the citus_table_is_visible function.
 */
Oid
CitusTableVisibleFuncId(void)
{
	if (MetadataCache.citusTableIsVisibleFuncId == InvalidOid)
	{
		const int argCount = 1;

		MetadataCache.citusTableIsVisibleFuncId =
			FunctionOid("pg_catalog", "citus_table_is_visible", argCount);
	}

	return MetadataCache.citusTableIsVisibleFuncId;
}


/*
 * CurrentDatabaseName gets the name of the current database and caches
 * the result.
 *
 * Given that the database name cannot be changed when there is at least
 * one session connected to it, we do not need to implement any invalidation
 * mechanism.
 */
char *
CurrentDatabaseName(void)
{
	if (!MetadataCache.databaseNameValid)
	{
		char *databaseName = get_database_name(MyDatabaseId);
		if (databaseName == NULL)
		{
			return NULL;
		}

		strlcpy(MetadataCache.databaseName, databaseName, NAMEDATALEN);
		MetadataCache.databaseNameValid = true;
	}

	return MetadataCache.databaseName;
}


/*
 * CitusExtensionOwner() returns the owner of the 'citus' extension. That user
 * is, amongst others, used to perform actions a normal user might not be
 * allowed to perform.
 */
extern Oid
CitusExtensionOwner(void)
{
	Relation relation = NULL;
	SysScanDesc scandesc;
	ScanKeyData entry[1];
	HeapTuple extensionTuple = NULL;
	Form_pg_extension extensionForm = NULL;

	if (MetadataCache.extensionOwner != InvalidOid)
	{
		return MetadataCache.extensionOwner;
	}

	relation = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
								  NULL, 1, entry);

	extensionTuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(extensionTuple))
	{
		extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);

		/*
		 * For some operations Citus requires superuser permissions; we use
		 * the extension owner for that. The extension owner is guaranteed to
		 * be a superuser (otherwise C functions can't be created), but it'd
		 * be possible to change the owner. So check that this still a
		 * superuser.
		 */
		if (!superuser_arg(extensionForm->extowner))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("citus extension needs to be owned by superuser")));
		}
		MetadataCache.extensionOwner = extensionForm->extowner;
		Assert(OidIsValid(MetadataCache.extensionOwner));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension not loaded")));
	}

	systable_endscan(scandesc);

	heap_close(relation, AccessShareLock);

	return MetadataCache.extensionOwner;
}


/*
 * CitusExtensionOwnerName returns the name of the owner of the extension.
 */
char *
CitusExtensionOwnerName(void)
{
	Oid superUserId = CitusExtensionOwner();

	return GetUserNameFromId(superUserId, false);
}


/* return the username of the currently active role */
char *
CurrentUserName(void)
{
	Oid userId = GetUserId();

	return GetUserNameFromId(userId, false);
}


/*
 * LookupTypeOid returns the Oid of the "pg_catalog.{typeNameString}" type, or
 * InvalidOid if it does not exist.
 */
static Oid
LookupTypeOid(char *typeNameString)
{
	Value *schemaName = makeString("pg_catalog");
	Value *typeName = makeString(typeNameString);
	List *qualifiedName = list_make2(schemaName, typeName);
	TypeName *enumTypeName = makeTypeNameFromNameList(qualifiedName);

	Oid nodeRoleTypId;

	/* typenameTypeId but instead of raising an error return InvalidOid */
	Type tup = LookupTypeName(NULL, enumTypeName, NULL, false);
	if (tup == NULL)
	{
		return InvalidOid;
	}

#if PG_VERSION_NUM >= 120000
	nodeRoleTypId = ((Form_pg_type) GETSTRUCT(tup))->oid;
#else
	nodeRoleTypId = HeapTupleGetOid(tup);
#endif
	ReleaseSysCache(tup);

	return nodeRoleTypId;
}


/*
 * LookupStringEnumValueId returns the Oid of the value in "pg_catalog.{enumName}"
 * which matches the provided valueName, or InvalidOid if the enum doesn't exist yet.
 */
static Oid
LookupStringEnumValueId(char *enumName, char *valueName)
{
	Oid enumTypeId = LookupTypeOid(enumName);

	if (enumTypeId == InvalidOid)
	{
		return InvalidOid;
	}
	else
	{
		Oid valueId = LookupEnumValueId(enumTypeId, valueName);
		return valueId;
	}
}


/*
 * LookupEnumValueId looks up the OID of an enum value.
 */
static Oid
LookupEnumValueId(Oid typeId, char *valueName)
{
	Datum typeIdDatum = ObjectIdGetDatum(typeId);
	Datum valueDatum = CStringGetDatum(valueName);
	Datum valueIdDatum = DirectFunctionCall2(enum_in, valueDatum, typeIdDatum);
	Oid valueId = DatumGetObjectId(valueIdDatum);

	return valueId;
}


/* return the Oid of the 'primary' nodeRole enum value */
Oid
PrimaryNodeRoleId(void)
{
	if (!MetadataCache.primaryNodeRoleId)
	{
		MetadataCache.primaryNodeRoleId = LookupStringEnumValueId("noderole", "primary");
	}

	return MetadataCache.primaryNodeRoleId;
}


/* return the Oid of the 'secodary' nodeRole enum value */
Oid
SecondaryNodeRoleId(void)
{
	if (!MetadataCache.secondaryNodeRoleId)
	{
		MetadataCache.secondaryNodeRoleId = LookupStringEnumValueId("noderole",
																	"secondary");
	}

	return MetadataCache.secondaryNodeRoleId;
}


/* return the Oid of the 'unavailable' nodeRole enum value */
Oid
UnavailableNodeRoleId(void)
{
	if (!MetadataCache.unavailableNodeRoleId)
	{
		MetadataCache.unavailableNodeRoleId = LookupStringEnumValueId("noderole",
																	  "unavailable");
	}

	return MetadataCache.unavailableNodeRoleId;
}


/*
 * master_dist_partition_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_partition are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_partition_cache_invalidate(PG_FUNCTION_ARGS)
{
	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	HeapTuple newTuple = NULL;
	HeapTuple oldTuple = NULL;
	Oid oldLogicalRelationId = InvalidOid;
	Oid newLogicalRelationId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	newTuple = triggerData->tg_newtuple;
	oldTuple = triggerData->tg_trigtuple;

	/* collect logicalrelid for OLD and NEW tuple */
	if (oldTuple != NULL)
	{
		Form_pg_dist_partition distPart = (Form_pg_dist_partition) GETSTRUCT(oldTuple);

		oldLogicalRelationId = distPart->logicalrelid;
	}

	if (newTuple != NULL)
	{
		Form_pg_dist_partition distPart = (Form_pg_dist_partition) GETSTRUCT(newTuple);

		newLogicalRelationId = distPart->logicalrelid;
	}

	/*
	 * Invalidate relcache for the relevant relation(s). In theory
	 * logicalrelid should never change, but it doesn't hurt to be
	 * paranoid.
	 */
	if (oldLogicalRelationId != InvalidOid &&
		oldLogicalRelationId != newLogicalRelationId)
	{
		CitusInvalidateRelcacheByRelid(oldLogicalRelationId);
	}

	if (newLogicalRelationId != InvalidOid)
	{
		CitusInvalidateRelcacheByRelid(newLogicalRelationId);
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_shard_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_shard are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_shard_cache_invalidate(PG_FUNCTION_ARGS)
{
	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	HeapTuple newTuple = NULL;
	HeapTuple oldTuple = NULL;
	Oid oldLogicalRelationId = InvalidOid;
	Oid newLogicalRelationId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	newTuple = triggerData->tg_newtuple;
	oldTuple = triggerData->tg_trigtuple;

	/* collect logicalrelid for OLD and NEW tuple */
	if (oldTuple != NULL)
	{
		Form_pg_dist_shard distShard = (Form_pg_dist_shard) GETSTRUCT(oldTuple);

		oldLogicalRelationId = distShard->logicalrelid;
	}

	if (newTuple != NULL)
	{
		Form_pg_dist_shard distShard = (Form_pg_dist_shard) GETSTRUCT(newTuple);

		newLogicalRelationId = distShard->logicalrelid;
	}

	/*
	 * Invalidate relcache for the relevant relation(s). In theory
	 * logicalrelid should never change, but it doesn't hurt to be
	 * paranoid.
	 */
	if (oldLogicalRelationId != InvalidOid &&
		oldLogicalRelationId != newLogicalRelationId)
	{
		CitusInvalidateRelcacheByRelid(oldLogicalRelationId);
	}

	if (newLogicalRelationId != InvalidOid)
	{
		CitusInvalidateRelcacheByRelid(newLogicalRelationId);
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_placement_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_placement are
 * changed on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_placement_cache_invalidate(PG_FUNCTION_ARGS)
{
	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	HeapTuple newTuple = NULL;
	HeapTuple oldTuple = NULL;
	Oid oldShardId = InvalidOid;
	Oid newShardId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	newTuple = triggerData->tg_newtuple;
	oldTuple = triggerData->tg_trigtuple;

	/* collect shardid for OLD and NEW tuple */
	if (oldTuple != NULL)
	{
		Form_pg_dist_placement distPlacement =
			(Form_pg_dist_placement) GETSTRUCT(oldTuple);

		oldShardId = distPlacement->shardid;
	}

	if (newTuple != NULL)
	{
		Form_pg_dist_placement distPlacement =
			(Form_pg_dist_placement) GETSTRUCT(newTuple);

		newShardId = distPlacement->shardid;
	}

	/*
	 * Invalidate relcache for the relevant relation(s). In theory shardId
	 * should never change, but it doesn't hurt to be paranoid.
	 */
	if (oldShardId != InvalidOid &&
		oldShardId != newShardId)
	{
		CitusInvalidateRelcacheByShardId(oldShardId);
	}

	if (newShardId != InvalidOid)
	{
		CitusInvalidateRelcacheByShardId(newShardId);
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_node_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_node are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_node_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_authinfo_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_authinfo are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_authinfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	/* no-op in community edition */

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_local_group_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_local_group are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_local_group_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	CitusInvalidateRelcacheByRelid(DistLocalGroupIdRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_object_cache_invalidate is a trigger function that performs relcache
 * invalidation when the contents of pg_dist_object are changed on the SQL
 * level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
master_dist_object_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CheckCitusVersion(ERROR);

	CitusInvalidateRelcacheByRelid(DistObjectRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * InitializeCaches() registers invalidation handlers for metadata_cache.c's
 * caches.
 */
static void
InitializeCaches(void)
{
	static bool performedInitialization = false;

	if (!performedInitialization)
	{
		MetadataCacheMemoryContext = NULL;

		/*
		 * If either of dist table cache or shard cache
		 * allocation and initializations fail due to an exception
		 * that is caused by OOM or any other reason,
		 * we reset the flag, and delete the shard cache memory
		 * context to reclaim partially allocated memory.
		 *
		 * Command will continue to fail since we re-throw the exception.
		 */
		PG_TRY();
		{
			/* set first, to avoid recursion dangers */
			performedInitialization = true;

			/* make sure we've initialized CacheMemoryContext */
			if (CacheMemoryContext == NULL)
			{
				CreateCacheMemoryContext();
			}

			MetadataCacheMemoryContext = AllocSetContextCreate(
				CacheMemoryContext,
				"MetadataCacheMemoryContext",
				ALLOCSET_DEFAULT_SIZES);

			InitializeDistCache();
			RegisterForeignKeyGraphCacheCallbacks();
			RegisterWorkerNodeCacheCallbacks();
			RegisterLocalGroupIdCacheCallbacks();
		}
		PG_CATCH();
		{
			performedInitialization = false;

			if (MetadataCacheMemoryContext != NULL)
			{
				MemoryContextDelete(MetadataCacheMemoryContext);
			}

			MetadataCacheMemoryContext = NULL;
			DistTableCacheHash = NULL;
			DistShardCacheHash = NULL;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}


/* initialize the infrastructure for the metadata cache */
static void
InitializeDistCache(void)
{
	HASHCTL info;

	/* build initial scan keys, copied for every relation scan */
	memset(&DistPartitionScanKey, 0, sizeof(DistPartitionScanKey));

	fmgr_info_cxt(F_OIDEQ,
				  &DistPartitionScanKey[0].sk_func,
				  MetadataCacheMemoryContext);
	DistPartitionScanKey[0].sk_strategy = BTEqualStrategyNumber;
	DistPartitionScanKey[0].sk_subtype = InvalidOid;
	DistPartitionScanKey[0].sk_collation = InvalidOid;
	DistPartitionScanKey[0].sk_attno = Anum_pg_dist_partition_logicalrelid;

	memset(&DistShardScanKey, 0, sizeof(DistShardScanKey));

	fmgr_info_cxt(F_OIDEQ,
				  &DistShardScanKey[0].sk_func,
				  MetadataCacheMemoryContext);
	DistShardScanKey[0].sk_strategy = BTEqualStrategyNumber;
	DistShardScanKey[0].sk_subtype = InvalidOid;
	DistShardScanKey[0].sk_collation = InvalidOid;
	DistShardScanKey[0].sk_attno = Anum_pg_dist_shard_logicalrelid;

	CreateDistTableCache();

	InitializeDistObjectCache();

	/* initialize the per-shard hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardCacheEntry);
	info.hash = tag_hash;
	info.hcxt = MetadataCacheMemoryContext;
	DistShardCacheHash =
		hash_create("Shard Cache", 32 * 64, &info,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateDistRelationCacheCallback,
								  (Datum) 0);
}


static void
InitializeDistObjectCache(void)
{
	/* build initial scan keys, copied for every relation scan */
	memset(&DistObjectScanKey, 0, sizeof(DistObjectScanKey));

	fmgr_info_cxt(F_OIDEQ,
				  &DistObjectScanKey[0].sk_func,
				  MetadataCacheMemoryContext);
	DistObjectScanKey[0].sk_strategy = BTEqualStrategyNumber;
	DistObjectScanKey[0].sk_subtype = InvalidOid;
	DistObjectScanKey[0].sk_collation = InvalidOid;
	DistObjectScanKey[0].sk_attno = Anum_pg_dist_object_classid;

	fmgr_info_cxt(F_OIDEQ,
				  &DistObjectScanKey[1].sk_func,
				  MetadataCacheMemoryContext);
	DistObjectScanKey[1].sk_strategy = BTEqualStrategyNumber;
	DistObjectScanKey[1].sk_subtype = InvalidOid;
	DistObjectScanKey[1].sk_collation = InvalidOid;
	DistObjectScanKey[1].sk_attno = Anum_pg_dist_object_objid;

	fmgr_info_cxt(F_INT4EQ,
				  &DistObjectScanKey[2].sk_func,
				  MetadataCacheMemoryContext);
	DistObjectScanKey[2].sk_strategy = BTEqualStrategyNumber;
	DistObjectScanKey[2].sk_subtype = InvalidOid;
	DistObjectScanKey[2].sk_collation = InvalidOid;
	DistObjectScanKey[2].sk_attno = Anum_pg_dist_object_objsubid;

	CreateDistObjectCache();
}


/*
 * GetWorkerNodeHash returns the worker node data as a hash with the nodename and
 * nodeport as a key.
 *
 * The hash is returned from the cache, if the cache is not (yet) valid, it is first
 * rebuilt.
 */
HTAB *
GetWorkerNodeHash(void)
{
	PrepareWorkerNodeCache();

	return WorkerNodeHash;
}


/*
 * PrepareWorkerNodeCache makes sure the worker node data from pg_dist_node is cached,
 * if it is not already cached.
 */
static void
PrepareWorkerNodeCache(void)
{
	InitializeCaches(); /* ensure relevant callbacks are registered */

	/*
	 * Simulate a SELECT from pg_dist_node, ensure pg_dist_node doesn't change while our
	 * caller is using WorkerNodeHash.
	 */
	LockRelationOid(DistNodeRelationId(), AccessShareLock);

	/*
	 * We might have some concurrent metadata changes. In order to get the changes,
	 * we first need to accept the cache invalidation messages.
	 */
	AcceptInvalidationMessages();

	if (!workerNodeHashValid)
	{
		InitializeWorkerNodeCache();

		workerNodeHashValid = true;
	}
}


/*
 * InitializeWorkerNodeCache initialize the infrastructure for the worker node cache.
 * The function reads the worker nodes from the metadata table, adds them to the hash and
 * finally registers an invalidation callback.
 */
static void
InitializeWorkerNodeCache(void)
{
	HTAB *newWorkerNodeHash = NULL;
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	HASHCTL info;
	int hashFlags = 0;
	long maxTableSize = (long) MaxWorkerNodesTracked;
	bool includeNodesFromOtherClusters = false;
	int newWorkerNodeCount = 0;
	WorkerNode **newWorkerNodeArray = NULL;
	int workerNodeIndex = 0;

	InitializeCaches();

	/*
	 * Create the hash that holds the worker nodes. The key is the combination of
	 * nodename and nodeport, instead of the unique nodeid because worker nodes are
	 * searched by the nodename and nodeport in every physical plan creation.
	 */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint32) + WORKER_LENGTH + sizeof(uint32);
	info.entrysize = sizeof(WorkerNode);
	info.hcxt = MetadataCacheMemoryContext;
	info.hash = WorkerNodeHashCode;
	info.match = WorkerNodeCompare;
	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE;

	newWorkerNodeHash = hash_create("Worker Node Hash", maxTableSize, &info, hashFlags);

	/* read the list from pg_dist_node */
	workerNodeList = ReadWorkerNodes(includeNodesFromOtherClusters);

	newWorkerNodeCount = list_length(workerNodeList);
	newWorkerNodeArray = MemoryContextAlloc(MetadataCacheMemoryContext,
											sizeof(WorkerNode *) * newWorkerNodeCount);

	/* iterate over the worker node list */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = NULL;
		WorkerNode *currentNode = lfirst(workerNodeCell);
		void *hashKey = NULL;
		bool handleFound = false;

		/* search for the worker node in the hash, and then insert the values */
		hashKey = (void *) currentNode;
		workerNode = (WorkerNode *) hash_search(newWorkerNodeHash, hashKey,
												HASH_ENTER, &handleFound);

		/* fill the newly allocated workerNode in the cache */
		strlcpy(workerNode->workerName, currentNode->workerName, WORKER_LENGTH);
		workerNode->workerPort = currentNode->workerPort;
		workerNode->groupId = currentNode->groupId;
		workerNode->nodeId = currentNode->nodeId;
		strlcpy(workerNode->workerRack, currentNode->workerRack, WORKER_LENGTH);
		workerNode->hasMetadata = currentNode->hasMetadata;
		workerNode->metadataSynced = currentNode->metadataSynced;
		workerNode->isActive = currentNode->isActive;
		workerNode->nodeRole = currentNode->nodeRole;
		workerNode->shouldHaveShards = currentNode->shouldHaveShards;
		strlcpy(workerNode->nodeCluster, currentNode->nodeCluster, NAMEDATALEN);

		newWorkerNodeArray[workerNodeIndex++] = workerNode;

		if (handleFound)
		{
			ereport(WARNING, (errmsg("multiple lines for worker node: \"%s:%u\"",
									 workerNode->workerName,
									 workerNode->workerPort)));
		}

		/* we do not need the currentNode anymore */
		pfree(currentNode);
	}

	/* now, safe to destroy the old hash */
	hash_destroy(WorkerNodeHash);

	if (WorkerNodeArray != NULL)
	{
		pfree(WorkerNodeArray);
	}

	WorkerNodeCount = newWorkerNodeCount;
	WorkerNodeArray = newWorkerNodeArray;
	WorkerNodeHash = newWorkerNodeHash;
}


/*
 * RegisterForeignKeyGraphCacheCallbacks registers callbacks required for
 * the foreign key graph cache.
 */
static void
RegisterForeignKeyGraphCacheCallbacks(void)
{
	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateForeignRelationGraphCacheCallback,
								  (Datum) 0);
}


/*
 * RegisterWorkerNodeCacheCallbacks registers the callbacks required for the
 * worker node cache.  It's separate from InitializeWorkerNodeCache so the
 * callback can be registered early, before the metadata tables exist.
 */
static void
RegisterWorkerNodeCacheCallbacks(void)
{
	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateNodeRelationCacheCallback,
								  (Datum) 0);
}


/*
 * GetLocalGroupId returns the group identifier of the local node. The function assumes
 * that pg_dist_local_node_group has exactly one row and has at least one column.
 * Otherwise, the function errors out.
 */
int32
GetLocalGroupId(void)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	int32 groupId = 0;
	Relation pgDistLocalGroupId = NULL;
	Oid localGroupTableOid = InvalidOid;

	InitializeCaches();

	/*
	 * Already set the group id, no need to read the heap again.
	 */
	if (LocalGroupId != -1)
	{
		return LocalGroupId;
	}

	localGroupTableOid = get_relname_relid("pg_dist_local_group", PG_CATALOG_NAMESPACE);
	if (localGroupTableOid == InvalidOid)
	{
		return 0;
	}

	pgDistLocalGroupId = heap_open(localGroupTableOid, AccessShareLock);

	scanDescriptor = systable_beginscan(pgDistLocalGroupId,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistLocalGroupId);

	heapTuple = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Datum groupIdDatum = heap_getattr(heapTuple,
										  Anum_pg_dist_local_groupid,
										  tupleDescriptor, &isNull);

		groupId = DatumGetInt32(groupIdDatum);

		/* set the local cache variable */
		LocalGroupId = groupId;
	}
	else
	{
		/*
		 * Upgrade is happening. When upgrading postgres, pg_dist_local_group is
		 * temporarily empty before citus_finish_pg_upgrade() finishes execution.
		 */
		groupId = GROUP_ID_UPGRADING;
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistLocalGroupId, AccessShareLock);

	return groupId;
}


/*
 * RegisterLocalGroupIdCacheCallbacks registers the callbacks required to
 * maintain LocalGroupId at a consistent value. It's separate from
 * GetLocalGroupId so the callback can be registered early, before metadata
 * tables exist.
 */
static void
RegisterLocalGroupIdCacheCallbacks(void)
{
	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateLocalGroupIdRelationCacheCallback,
								  (Datum) 0);
}


/*
 * WorkerNodeHashCode computes the hash code for a worker node from the node's
 * host name and port number. Nodes that only differ by their rack locations
 * hash to the same value.
 */
static uint32
WorkerNodeHashCode(const void *key, Size keySize)
{
	const WorkerNode *worker = (const WorkerNode *) key;
	const char *workerName = worker->workerName;
	const uint32 *workerPort = &(worker->workerPort);

	/* standard hash function outlined in Effective Java, Item 8 */
	uint32 result = 17;
	result = 37 * result + string_hash(workerName, WORKER_LENGTH);
	result = 37 * result + tag_hash(workerPort, sizeof(uint32));
	return result;
}


/*
 * ResetDistTableCacheEntry frees any out-of-band memory used by a cache entry,
 * but does not free the entry itself.
 */
static void
ResetDistTableCacheEntry(DistTableCacheEntry *cacheEntry)
{
	int shardIndex = 0;

	if (cacheEntry->partitionKeyString != NULL)
	{
		pfree(cacheEntry->partitionKeyString);
		cacheEntry->partitionKeyString = NULL;
	}

	if (cacheEntry->shardIntervalCompareFunction != NULL)
	{
		pfree(cacheEntry->shardIntervalCompareFunction);
		cacheEntry->shardIntervalCompareFunction = NULL;
	}

	if (cacheEntry->hashFunction)
	{
		pfree(cacheEntry->hashFunction);
		cacheEntry->hashFunction = NULL;
	}

	if (cacheEntry->partitionColumn != NULL)
	{
		pfree(cacheEntry->partitionColumn);
		cacheEntry->partitionColumn = NULL;
	}

	if (cacheEntry->shardIntervalArrayLength == 0)
	{
		return;
	}

	for (shardIndex = 0; shardIndex < cacheEntry->shardIntervalArrayLength;
		 shardIndex++)
	{
		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
		GroupShardPlacement *placementArray =
			cacheEntry->arrayOfPlacementArrays[shardIndex];
		bool valueByVal = shardInterval->valueByVal;
		bool foundInCache = false;

		/* delete the shard's placements */
		if (placementArray != NULL)
		{
			pfree(placementArray);
		}

		/* delete per-shard cache-entry */
		hash_search(DistShardCacheHash, &shardInterval->shardId, HASH_REMOVE,
					&foundInCache);
		Assert(foundInCache);

		/* delete data pointed to by ShardInterval */
		if (!valueByVal)
		{
			if (shardInterval->minValueExists)
			{
				pfree(DatumGetPointer(shardInterval->minValue));
			}

			if (shardInterval->maxValueExists)
			{
				pfree(DatumGetPointer(shardInterval->maxValue));
			}
		}

		/* and finally the ShardInterval itself */
		pfree(shardInterval);
	}

	if (cacheEntry->sortedShardIntervalArray)
	{
		pfree(cacheEntry->sortedShardIntervalArray);
		cacheEntry->sortedShardIntervalArray = NULL;
	}
	if (cacheEntry->arrayOfPlacementArrayLengths)
	{
		pfree(cacheEntry->arrayOfPlacementArrayLengths);
		cacheEntry->arrayOfPlacementArrayLengths = NULL;
	}
	if (cacheEntry->arrayOfPlacementArrays)
	{
		pfree(cacheEntry->arrayOfPlacementArrays);
		cacheEntry->arrayOfPlacementArrays = NULL;
	}
	if (cacheEntry->referencedRelationsViaForeignKey)
	{
		list_free(cacheEntry->referencedRelationsViaForeignKey);
		cacheEntry->referencedRelationsViaForeignKey = NIL;
	}
	if (cacheEntry->referencingRelationsViaForeignKey)
	{
		list_free(cacheEntry->referencingRelationsViaForeignKey);
		cacheEntry->referencingRelationsViaForeignKey = NIL;
	}

	cacheEntry->shardIntervalArrayLength = 0;
	cacheEntry->hasUninitializedShardInterval = false;
	cacheEntry->hasUniformHashDistribution = false;
	cacheEntry->hasOverlappingShardInterval = false;
}


/*
 * InvalidateForeignRelationGraphCacheCallback invalidates the foreign key relation
 * graph and entire distributed cache entries.
 */
static void
InvalidateForeignRelationGraphCacheCallback(Datum argument, Oid relationId)
{
	if (relationId == MetadataCache.distColocationRelationId)
	{
		SetForeignConstraintRelationshipGraphInvalid();
		InvalidateDistTableCache();
	}
}


/*
 * InvalidateForeignKeyGraph is used to invalidate the cached foreign key
 * graph (see ForeignKeyRelationGraph @ utils/foreign_key_relationship.c).
 *
 * To invalidate the foreign key graph, we hack around relcache invalidation
 * callbacks. Given that there is no metadata table associated with the foreign
 * key graph cache, we use pg_dist_colocation, which is never invalidated for
 * other purposes.
 *
 * We acknowledge that it is not a very intiutive way of implementing this cache
 * invalidation, but, seems acceptable for now. If this becomes problematic, we
 * could try using a magic oid where we're sure that no relation would ever use
 * that oid.
 */
void
InvalidateForeignKeyGraph(void)
{
	CitusInvalidateRelcacheByRelid(DistColocationRelationId());

	/* bump command counter to force invalidation to take effect */
	CommandCounterIncrement();
}


/*
 * InvalidateDistRelationCacheCallback flushes cache entries when a relation
 * is updated (or flushes the entire cache).
 */
static void
InvalidateDistRelationCacheCallback(Datum argument, Oid relationId)
{
	/* invalidate either entire cache or a specific entry */
	if (relationId == InvalidOid)
	{
		InvalidateDistTableCache();
		InvalidateDistObjectCache();
	}
	else
	{
		void *hashKey = (void *) &relationId;
		bool foundInCache = false;


		DistTableCacheEntry *cacheEntry = hash_search(DistTableCacheHash, hashKey,
													  HASH_FIND, &foundInCache);
		if (foundInCache)
		{
			cacheEntry->isValid = false;
		}

		/*
		 * If pg_dist_partition is being invalidated drop all state
		 * This happens pretty rarely, but most importantly happens during
		 * DROP EXTENSION citus;
		 */
		if (relationId == MetadataCache.distPartitionRelationId)
		{
			InvalidateMetadataSystemCache();
		}

		if (relationId == MetadataCache.distObjectRelationId)
		{
			InvalidateDistObjectCache();
		}
	}
}


/*
 * InvalidateDistTableCache marks all DistTableCacheHash entries invalid.
 */
static void
InvalidateDistTableCache(void)
{
	DistTableCacheEntry *cacheEntry = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, DistTableCacheHash);

	while ((cacheEntry = (DistTableCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		cacheEntry->isValid = false;
	}
}


/*
 * InvalidateDistObjectCache marks all DistObjectCacheHash entries invalid.
 */
static void
InvalidateDistObjectCache(void)
{
	DistObjectCacheEntry *cacheEntry = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, DistObjectCacheHash);

	while ((cacheEntry = (DistObjectCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		cacheEntry->isValid = false;
	}
}


/*
 * FlushDistTableCache flushes the entire distributed relation cache, frees
 * all entries, and recreates the cache.
 */
void
FlushDistTableCache(void)
{
	DistTableCacheEntry *cacheEntry = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, DistTableCacheHash);

	while ((cacheEntry = (DistTableCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		ResetDistTableCacheEntry(cacheEntry);
	}

	hash_destroy(DistTableCacheHash);
	CreateDistTableCache();
}


/* CreateDistTableCache initializes the per-table hash table */
static void
CreateDistTableCache(void)
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(DistTableCacheEntry);
	info.hash = tag_hash;
	info.hcxt = MetadataCacheMemoryContext;
	DistTableCacheHash =
		hash_create("Distributed Relation Cache", 32, &info,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}


/* CreateDistObjectCache initializes the per-object hash table */
static void
CreateDistObjectCache(void)
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(DistObjectCacheEntryKey);
	info.entrysize = sizeof(DistObjectCacheEntry);
	info.hash = tag_hash;
	info.hcxt = MetadataCacheMemoryContext;
	DistObjectCacheHash =
		hash_create("Distributed Object Cache", 32, &info,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}


/*
 * InvalidateMetadataSystemCache resets all the cached OIDs and the extensionLoaded flag,
 * and invalidates the worker node, ConnParams, and local group ID caches.
 */
void
InvalidateMetadataSystemCache(void)
{
	InvalidateConnParamsHashEntries();

	memset(&MetadataCache, 0, sizeof(MetadataCache));
	workerNodeHashValid = false;
	LocalGroupId = -1;
}


/*
 * DistTableOidList iterates over the pg_dist_partition table and returns
 * a list that consists of the logicalrelids.
 */
List *
DistTableOidList(void)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	List *distTableOidList = NIL;
	TupleDesc tupleDescriptor = NULL;

	Relation pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan(pgDistPartition,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistPartition);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Oid relationId = InvalidOid;
		Datum relationIdDatum = heap_getattr(heapTuple,
											 Anum_pg_dist_partition_logicalrelid,
											 tupleDescriptor, &isNull);
		relationId = DatumGetObjectId(relationIdDatum);
		distTableOidList = lappend_oid(distTableOidList, relationId);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistPartition, AccessShareLock);

	return distTableOidList;
}


/*
 * InvalidateNodeRelationCacheCallback destroys the WorkerNodeHash when
 * any change happens on pg_dist_node table. It also set WorkerNodeHash to
 * NULL, which allows consequent accesses to the hash read from the
 * pg_dist_node from scratch.
 */
static void
InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId)
{
	if (relationId == InvalidOid || relationId == MetadataCache.distNodeRelationId)
	{
		workerNodeHashValid = false;
	}
}


/*
 * InvalidateLocalGroupIdRelationCacheCallback sets the LocalGroupId to
 * the default value.
 */
static void
InvalidateLocalGroupIdRelationCacheCallback(Datum argument, Oid relationId)
{
	/* when invalidation happens simply set the LocalGroupId to the default value */
	if (relationId == InvalidOid || relationId == MetadataCache.distLocalGroupRelationId)
	{
		LocalGroupId = -1;
	}
}


/*
 * LookupDistPartitionTuple searches pg_dist_partition for relationId's entry
 * and returns that or, if no matching entry was found, NULL.
 */
static HeapTuple
LookupDistPartitionTuple(Relation pgDistPartition, Oid relationId)
{
	HeapTuple distPartitionTuple = NULL;
	HeapTuple currentPartitionTuple = NULL;
	SysScanDesc scanDescriptor;
	ScanKeyData scanKey[1];

	/* copy scankey to local copy, it will be modified during the scan */
	memcpy(scanKey, DistPartitionScanKey, sizeof(DistPartitionScanKey));

	/* set scan arguments */
	scanKey[0].sk_argument = ObjectIdGetDatum(relationId);

	scanDescriptor = systable_beginscan(pgDistPartition,
										DistPartitionLogicalRelidIndexId(),
										true, NULL, 1, scanKey);

	currentPartitionTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(currentPartitionTuple))
	{
		distPartitionTuple = heap_copytuple(currentPartitionTuple);
	}

	systable_endscan(scanDescriptor);

	return distPartitionTuple;
}


/*
 * LookupDistShardTuples returns a list of all dist_shard tuples for the
 * specified relation.
 */
static List *
LookupDistShardTuples(Oid relationId)
{
	Relation pgDistShard = NULL;
	List *distShardTupleList = NIL;
	HeapTuple currentShardTuple = NULL;
	SysScanDesc scanDescriptor;
	ScanKeyData scanKey[1];

	pgDistShard = heap_open(DistShardRelationId(), AccessShareLock);

	/* copy scankey to local copy, it will be modified during the scan */
	memcpy(scanKey, DistShardScanKey, sizeof(DistShardScanKey));

	/* set scan arguments */
	scanKey[0].sk_argument = ObjectIdGetDatum(relationId);

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardLogicalRelidIndexId(), true,
										NULL, 1, scanKey);

	currentShardTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(currentShardTuple))
	{
		HeapTuple shardTupleCopy = heap_copytuple(currentShardTuple);
		distShardTupleList = lappend(distShardTupleList, shardTupleCopy);

		currentShardTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, AccessShareLock);

	return distShardTupleList;
}


/*
 * LookupShardRelation returns the logical relation oid a shard belongs to.
 *
 * Errors out if the shardId does not exist and missingOk is false. Returns
 * InvalidOid if the shardId does not exist and missingOk is true.
 */
Oid
LookupShardRelation(int64 shardId, bool missingOk)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	Form_pg_dist_shard shardForm = NULL;
	Relation pgDistShard = heap_open(DistShardRelationId(), AccessShareLock);
	Oid relationId = InvalidOid;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardShardidIndexId(), true,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple) && !missingOk)
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}

	if (!HeapTupleIsValid(heapTuple))
	{
		relationId = InvalidOid;
	}
	else
	{
		shardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
		relationId = shardForm->logicalrelid;
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, NoLock);

	return relationId;
}


/*
 * GetPartitionTypeInputInfo populates output parameters with the interval type
 * identifier and modifier for the specified partition key/method combination.
 */
static void
GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
						  Oid *columnTypeId, int32 *columnTypeMod,
						  Oid *intervalTypeId, int32 *intervalTypeMod)
{
	*columnTypeId = InvalidOid;
	*columnTypeMod = -1;
	*intervalTypeId = InvalidOid;
	*intervalTypeMod = -1;

	switch (partitionMethod)
	{
		case DISTRIBUTE_BY_APPEND:
		case DISTRIBUTE_BY_RANGE:
		{
			Node *partitionNode = stringToNode(partitionKeyString);
			Var *partitionColumn = (Var *) partitionNode;
			Assert(IsA(partitionNode, Var));

			*intervalTypeId = partitionColumn->vartype;
			*intervalTypeMod = partitionColumn->vartypmod;
			*columnTypeId = partitionColumn->vartype;
			*columnTypeMod = partitionColumn->vartypmod;
			break;
		}

		case DISTRIBUTE_BY_HASH:
		{
			Node *partitionNode = stringToNode(partitionKeyString);
			Var *partitionColumn = (Var *) partitionNode;
			Assert(IsA(partitionNode, Var));

			*intervalTypeId = INT4OID;
			*columnTypeId = partitionColumn->vartype;
			*columnTypeMod = partitionColumn->vartypmod;
			break;
		}

		case DISTRIBUTE_BY_NONE:
		{
			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported table partition type: %c",
								   partitionMethod)));
		}
	}
}


/*
 * TupleToShardInterval transforms the specified dist_shard tuple into a new
 * ShardInterval using the provided descriptor and partition type information.
 */
static ShardInterval *
TupleToShardInterval(HeapTuple heapTuple, TupleDesc tupleDescriptor, Oid
					 intervalTypeId,
					 int32 intervalTypeMod)
{
	ShardInterval *shardInterval = NULL;
	bool minValueNull = false;
	bool maxValueNull = false;
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	Datum datumArray[Natts_pg_dist_shard];
	bool isNullArray[Natts_pg_dist_shard];
	Datum minValueTextDatum = 0;
	Datum maxValueTextDatum = 0;
	Oid relationId = InvalidOid;
	int64 shardId = InvalidOid;
	char storageType = InvalidOid;
	Datum minValue = 0;
	Datum maxValue = 0;
	bool minValueExists = false;
	bool maxValueExists = false;
	int16 intervalTypeLen = 0;
	bool intervalByVal = false;
	char intervalAlign = '0';
	char intervalDelim = '0';

	/*
	 * We use heap_deform_tuple() instead of heap_getattr() to expand tuple
	 * to contain missing values when ALTER TABLE ADD COLUMN happens.
	 */
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	relationId = DatumGetObjectId(datumArray[Anum_pg_dist_shard_logicalrelid -
											 1]);
	shardId = DatumGetInt64(datumArray[Anum_pg_dist_shard_shardid - 1]);
	storageType = DatumGetChar(datumArray[Anum_pg_dist_shard_shardstorage - 1]);
	minValueTextDatum = datumArray[Anum_pg_dist_shard_shardminvalue - 1];
	maxValueTextDatum = datumArray[Anum_pg_dist_shard_shardmaxvalue - 1];

	minValueNull = isNullArray[Anum_pg_dist_shard_shardminvalue - 1];
	maxValueNull = isNullArray[Anum_pg_dist_shard_shardmaxvalue - 1];

	if (!minValueNull && !maxValueNull)
	{
		char *minValueString = TextDatumGetCString(minValueTextDatum);
		char *maxValueString = TextDatumGetCString(maxValueTextDatum);

		/* TODO: move this up the call stack to avoid per-tuple invocation? */
		get_type_io_data(intervalTypeId, IOFunc_input, &intervalTypeLen,
						 &intervalByVal,
						 &intervalAlign, &intervalDelim, &typeIoParam,
						 &inputFunctionId);

		/* finally convert min/max values to their actual types */
		minValue = OidInputFunctionCall(inputFunctionId, minValueString,
										typeIoParam, intervalTypeMod);
		maxValue = OidInputFunctionCall(inputFunctionId, maxValueString,
										typeIoParam, intervalTypeMod);

		minValueExists = true;
		maxValueExists = true;
	}

	shardInterval = CitusMakeNode(ShardInterval);
	shardInterval->relationId = relationId;
	shardInterval->storageType = storageType;
	shardInterval->valueTypeId = intervalTypeId;
	shardInterval->valueTypeLen = intervalTypeLen;
	shardInterval->valueByVal = intervalByVal;
	shardInterval->minValueExists = minValueExists;
	shardInterval->maxValueExists = maxValueExists;
	shardInterval->minValue = minValue;
	shardInterval->maxValue = maxValue;
	shardInterval->shardId = shardId;

	return shardInterval;
}


/*
 * CachedNamespaceLookup performs a cached lookup for the namespace (schema), with the
 * result cached in cachedOid.
 */
static void
CachedNamespaceLookup(const char *nspname, Oid *cachedOid)
{
	/* force callbacks to be registered, so we always get notified upon changes */
	InitializeCaches();

	if (*cachedOid == InvalidOid)
	{
		*cachedOid = get_namespace_oid(nspname, true);

		if (*cachedOid == InvalidOid)
		{
			ereport(ERROR, (errmsg(
								"cache lookup failed for namespace %s, called too early?",
								nspname)));
		}
	}
}


/*
 * CachedRelationLookup performs a cached lookup for the relation
 * relationName, with the result cached in *cachedOid.
 */
static void
CachedRelationLookup(const char *relationName, Oid *cachedOid)
{
	CachedRelationNamespaceLookup(relationName, PG_CATALOG_NAMESPACE, cachedOid);
}


static void
CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace,
							  Oid *cachedOid)
{
	/* force callbacks to be registered, so we always get notified upon changes */
	InitializeCaches();

	if (*cachedOid == InvalidOid)
	{
		*cachedOid = get_relname_relid(relationName, relnamespace);

		if (*cachedOid == InvalidOid)
		{
			ereport(ERROR, (errmsg(
								"cache lookup failed for %s, called too early?",
								relationName)));
		}
	}
}


/*
 * Register a relcache invalidation for a non-shared relation.
 *
 * We ignore the case that there's no corresponding pg_class entry - that
 * happens if we register a relcache invalidation (e.g. for a
 * pg_dist_partition deletion) after the relation has been dropped. That's ok,
 * because in those cases we're guaranteed to already have registered an
 * invalidation for the target relation.
 */
void
CitusInvalidateRelcacheByRelid(Oid relationId)
{
	HeapTuple classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));

	if (HeapTupleIsValid(classTuple))
	{
		CacheInvalidateRelcacheByTuple(classTuple);
		ReleaseSysCache(classTuple);
	}
}


/*
 * Register a relcache invalidation for the distributed relation associated
 * with the shard.
 */
void
CitusInvalidateRelcacheByShardId(int64 shardId)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	Form_pg_dist_shard shardForm = NULL;
	Relation pgDistShard = heap_open(DistShardRelationId(), AccessShareLock);

	/*
	 * Load shard, to find the associated relation id. Can't use
	 * LoadShardInterval directly because that'd fail if the shard doesn't
	 * exist anymore, which we can't have. Also lower overhead is desirable
	 * here.
	 */

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	scanDescriptor = systable_beginscan(pgDistShard,
										DistShardShardidIndexId(), true,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		shardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
		CitusInvalidateRelcacheByRelid(shardForm->logicalrelid);
	}
	else
	{
		/*
		 * Couldn't find associated relation. That can primarily happen in two cases:
		 *
		 * 1) A placement row is inserted before the shard row. That's fine,
		 *	  since we don't need invalidations via placements in that case.
		 *
		 * 2) The shard has been deleted, but some placements were
		 *    unreachable, and the user is manually deleting the rows. Not
		 *    much point in WARNING or ERRORing in that case either, there's
		 *    nothing to invalidate.
		 *
		 * Hence we just emit a DEBUG5 message.
		 */
		ereport(DEBUG5, (errmsg(
							 "could not find distributed relation to invalidate for "
							 "shard "INT64_FORMAT, shardId)));
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, NoLock);

	/* bump command counter, to force invalidation to take effect */
	CommandCounterIncrement();
}


/*
 * DistNodeMetadata returns the single metadata jsonb object stored in
 * pg_dist_node_metadata.
 */
Datum
DistNodeMetadata(void)
{
	Datum metadata = 0;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	const int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	Oid metadataTableOid = InvalidOid;
	Relation pgDistNodeMetadata = NULL;
	TupleDesc tupleDescriptor = NULL;

	metadataTableOid = get_relname_relid("pg_dist_node_metadata",
										 PG_CATALOG_NAMESPACE);
	if (metadataTableOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("pg_dist_node_metadata was not found")));
	}

	pgDistNodeMetadata = heap_open(metadataTableOid, AccessShareLock);
	scanDescriptor = systable_beginscan(pgDistNodeMetadata,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);
	tupleDescriptor = RelationGetDescr(pgDistNodeMetadata);

	heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		metadata = heap_getattr(heapTuple, Anum_pg_dist_node_metadata_metadata,
								tupleDescriptor, &isNull);
		Assert(!isNull);
	}
	else
	{
		ereport(ERROR, (errmsg(
							"could not find any entries in pg_dist_metadata")));
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNodeMetadata, AccessShareLock);

	return metadata;
}


/*
 * role_exists is a check constraint which ensures that roles referenced in the
 * pg_dist_authinfo catalog actually exist (at least at the time of insertion).
 */
Datum
role_exists(PG_FUNCTION_ARGS)
{
	Name roleName = PG_GETARG_NAME(0);
	bool roleExists = SearchSysCacheExists1(AUTHNAME, NameGetDatum(roleName));

	PG_RETURN_BOOL(roleExists);
}


/*
 * authinfo_valid is a check constraint which errors on all rows, intended for
 * use in prohibiting writes to pg_dist_authinfo in Citus Community.
 */
Datum
authinfo_valid(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot write to pg_dist_authinfo"),
					errdetail(
						"Citus Community Edition does not support the use of "
						"custom authentication options."),
					errhint(
						"To learn more about using advanced authentication schemes "
						"with Citus, please contact us at "
						"https://citusdata.com/about/contact_us")));
}


/*
 * poolinfo_valid is a check constraint which errors on all rows, intended for
 * use in prohibiting writes to pg_dist_poolinfo in Citus Community.
 */
Datum
poolinfo_valid(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot write to pg_dist_poolinfo"),
					errdetail(
						"Citus Community Edition does not support the use of "
						"pooler options."),
					errhint("To learn more about using advanced pooling schemes "
							"with Citus, please contact us at "
							"https://citusdata.com/about/contact_us")));
}
