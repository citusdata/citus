/*-------------------------------------------------------------------------
 *
 * metadata_cache.c
 *	  Distributed table metadata cache
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "citus_version.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_local_group.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_shard_placement.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


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


/* state which should be cleared upon DROP EXTENSION */
static bool extensionLoaded = false;
static Oid distShardRelationId = InvalidOid;
static Oid distShardPlacementRelationId = InvalidOid;
static Oid distNodeRelationId = InvalidOid;
static Oid distLocalGroupRelationId = InvalidOid;
static Oid distColocationRelationId = InvalidOid;
static Oid distColocationConfigurationIndexId = InvalidOid;
static Oid distColocationColocationidIndexId = InvalidOid;
static Oid distPartitionRelationId = InvalidOid;
static Oid distPartitionLogicalRelidIndexId = InvalidOid;
static Oid distPartitionColocationidIndexId = InvalidOid;
static Oid distShardLogicalRelidIndexId = InvalidOid;
static Oid distShardShardidIndexId = InvalidOid;
static Oid distShardPlacementShardidIndexId = InvalidOid;
static Oid distShardPlacementPlacementidIndexId = InvalidOid;
static Oid distShardPlacementNodeidIndexId = InvalidOid;
static Oid distTransactionRelationId = InvalidOid;
static Oid distTransactionGroupIndexId = InvalidOid;
static Oid extraDataContainerFuncId = InvalidOid;
static Oid workerHashFunctionId = InvalidOid;

/* Citus extension version variables */
bool EnableVersionChecks = true; /* version checks are enabled */

static bool citusVersionKnownCompatible = false;

/* Hash table for informations about each partition */
static HTAB *DistTableCacheHash = NULL;

/* Hash table for informations about each shard */
static HTAB *DistShardCacheHash = NULL;

/* Hash table for informations about worker nodes */
static HTAB *WorkerNodeHash = NULL;
static bool workerNodeHashValid = false;

static bool invalidationRegistered = false;

/* default value is -1, for coordinator it's 0 and for worker nodes > 0 */
static int LocalGroupId = -1;

/* built first time through in InitializePartitionCache */
static ScanKeyData DistPartitionScanKey[1];
static ScanKeyData DistShardScanKey[1];


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
static bool HasUniformHashDistribution(ShardInterval **shardIntervalArray,
									   int shardIntervalArrayLength);
static bool HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray,
										  int shardCount);
static bool CheckInstalledVersion(int elevel);
static char * AvailableExtensionVersion(void);
static char * InstalledExtensionVersion(void);
static bool HasOverlappingShardInterval(ShardInterval **shardIntervalArray,
										int shardIntervalArrayLength,
										FmgrInfo *shardIntervalSortCompareFunction);
static void InitializeDistTableCache(void);
static void InitializeWorkerNodeCache(void);
static uint32 WorkerNodeHashCode(const void *key, Size keySize);
static void ResetDistTableCacheEntry(DistTableCacheEntry *cacheEntry);
static void InvalidateDistRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateLocalGroupIdRelationCacheCallback(Datum argument, Oid relationId);
static HeapTuple LookupDistPartitionTuple(Relation pgDistPartition, Oid relationId);
static List * LookupDistShardTuples(Oid relationId);
static Oid LookupShardRelation(int64 shardId);
static void GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
									  Oid *columnTypeId, int32 *columnTypeMod,
									  Oid *intervalTypeId, int32 *intervalTypeMod);
static ShardInterval * TupleToShardInterval(HeapTuple heapTuple,
											TupleDesc tupleDescriptor, Oid intervalTypeId,
											int32 intervalTypeMod);
static void CachedRelationLookup(const char *relationName, Oid *cachedOid);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_dist_partition_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_shard_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_placement_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_node_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_local_group_cache_invalidate);


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
	ScanKeyData scanKey[scanKeyCount];
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
 * LoadShardPlacement returns the, cached, metadata about a shard placement.
 *
 * The return value is a copy of the cached ShardPlacement struct and may
 * therefore be modified and/or freed.
 */
ShardPlacement *
LoadShardPlacement(uint64 shardId, uint64 placementId)
{
	ShardCacheEntry *shardEntry = NULL;
	DistTableCacheEntry *tableEntry = NULL;

	ShardPlacement *placementArray = NULL;
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
			ShardPlacement *shardPlacement = CitusMakeNode(ShardPlacement);
			CopyShardPlacement(&placementArray[i], shardPlacement);

			return shardPlacement;
		}
	}

	ereport(ERROR, (errmsg("could not find valid entry for shard placement "
						   UINT64_FORMAT, placementId)));
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
	ShardPlacement *placementArray = NULL;
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
		/* copy placement into target context */
		ShardPlacement *placement = CitusMakeNode(ShardPlacement);
		CopyShardPlacement(&placementArray[i], placement);

		placementList = lappend(placementList, placement);
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

	/* probably not reachable */
	if (DistShardCacheHash == NULL)
	{
		InitializeDistTableCache();
	}

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
		Oid relationId = LookupShardRelation(shardId);

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
			/*
			 * The cache entry might not be valid right now. Reload cache entry
			 * and recheck (as the offset might have changed).
			 */
			LookupDistTableCacheEntry(shardEntry->tableEntry->relationId);
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
	DistTableCacheEntry *cacheEntry = NULL;

	cacheEntry = LookupDistTableCacheEntry(distributedRelationId);

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

	if (DistTableCacheHash == NULL)
	{
		InitializeDistTableCache();
	}

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

	/* actually fill out entry */
	BuildDistTableCacheEntry(cacheEntry);

	/* and finally mark as valid */
	cacheEntry->isValid = true;

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
	Form_pg_dist_partition partitionForm = NULL;
	Datum partitionKeyDatum = 0;
	Datum replicationModelDatum = 0;
	MemoryContext oldContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	bool isNull = false;
	bool partitionKeyIsNull = false;

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
	partitionForm = (Form_pg_dist_partition) GETSTRUCT(distPartitionTuple);

	cacheEntry->partitionMethod = partitionForm->partmethod;

	partitionKeyDatum = heap_getattr(distPartitionTuple,
									 Anum_pg_dist_partition_partkey,
									 tupleDescriptor,
									 &partitionKeyIsNull);

	/* note that for reference tables partitionKeyisNull is true */
	if (!partitionKeyIsNull)
	{
		oldContext = MemoryContextSwitchTo(CacheMemoryContext);
		cacheEntry->partitionKeyString = TextDatumGetCString(partitionKeyDatum);
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		cacheEntry->partitionKeyString = NULL;
	}

	cacheEntry->colocationId = heap_getattr(distPartitionTuple,
											Anum_pg_dist_partition_colocationid,
											tupleDescriptor,
											&isNull);
	if (isNull)
	{
		cacheEntry->colocationId = INVALID_COLOCATION_ID;
	}

	replicationModelDatum = heap_getattr(distPartitionTuple,
										 Anum_pg_dist_partition_repmodel,
										 tupleDescriptor,
										 &isNull);
	if (isNull)
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
		Node *partitionNode = stringToNode(cacheEntry->partitionKeyString);
		Var *partitionColumn = (Var *) partitionNode;
		FmgrInfo *hashFunction = NULL;

		Assert(IsA(partitionNode, Var));
		typeEntry = lookup_type_cache(partitionColumn->vartype,
									  TYPECACHE_HASH_PROC_FINFO);

		hashFunction = MemoryContextAllocZero(CacheMemoryContext,
											  sizeof(FmgrInfo));

		fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo), CacheMemoryContext);

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

		shardIntervalArray = MemoryContextAllocZero(CacheMemoryContext,
													shardIntervalArrayLength *
													sizeof(ShardInterval *));

		cacheEntry->arrayOfPlacementArrays =
			MemoryContextAllocZero(CacheMemoryContext,
								   shardIntervalArrayLength *
								   sizeof(ShardPlacement *));
		cacheEntry->arrayOfPlacementArrayLengths =
			MemoryContextAllocZero(CacheMemoryContext,
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
			MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);

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
		MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);

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
		MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);

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


	/* maintain shardId->(table,ShardInterval) cache */
	for (shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardCacheEntry *shardEntry = NULL;
		ShardInterval *shardInterval = sortedShardIntervalArray[shardIndex];
		bool foundInCache = false;
		List *placementList = NIL;
		MemoryContext oldContext = NULL;
		ListCell *placementCell = NULL;
		ShardPlacement *placementArray = NULL;
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

		shardEntry->shardIndex = shardIndex;
		shardEntry->tableEntry = cacheEntry;

		/* build list of shard placements */
		placementList = BuildShardPlacementList(shardInterval);
		numberOfPlacements = list_length(placementList);

		/* and copy that list into the cache entry */
		oldContext = MemoryContextSwitchTo(CacheMemoryContext);
		placementArray = palloc0(numberOfPlacements * sizeof(ShardPlacement));
		foreach(placementCell, placementList)
		{
			ShardPlacement *srcPlacement = (ShardPlacement *) lfirst(placementCell);
			ShardPlacement *dstPlacement = &placementArray[placementOffset];

			CopyShardPlacement(srcPlacement, dstPlacement);

			/* fill in remaining fields */
			Assert(cacheEntry->partitionMethod != 0);
			dstPlacement->partitionMethod = cacheEntry->partitionMethod;
			dstPlacement->colocationGroupId = cacheEntry->colocationId;
			if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
			{
				Assert(shardInterval->minValueExists);
				Assert(shardInterval->valueTypeId == INT4OID);

				/*
				 * Use the lower boundary of the interval's range to identify
				 * it for colocation purposes. That remains meaningful even if
				 * a concurrent session splits a shard.
				 */
				dstPlacement->representativeValue =
					DatumGetInt32(shardInterval->minValue);
			}
			else
			{
				dstPlacement->representativeValue = 0;
			}
			placementOffset++;
		}
		MemoryContextSwitchTo(oldContext);

		cacheEntry->arrayOfPlacementArrays[shardIndex] = placementArray;
		cacheEntry->arrayOfPlacementArrayLengths[shardIndex] = numberOfPlacements;
	}

	cacheEntry->shardIntervalArrayLength = shardIntervalArrayLength;
	cacheEntry->sortedShardIntervalArray = sortedShardIntervalArray;
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
	ShardInterval **sortedShardIntervalArray = NULL;

	/* short cut if there are no shard intervals in the array */
	if (shardCount == 0)
	{
		return shardIntervalArray;
	}

	/* if a shard doesn't have min/max values, it's placed in the end of the array */
	qsort_arg(shardIntervalArray, shardCount, sizeof(ShardInterval *),
			  (qsort_arg_comparator) CompareShardIntervals,
			  (void *) shardIntervalSortCompareFunction);

	sortedShardIntervalArray = shardIntervalArray;

	return sortedShardIntervalArray;
}


/*
 * HasUniformHashDistribution determines whether the given list of sorted shards
 * has a uniform hash distribution, as produced by master_create_worker_shards for
 * hash partitioned tables.
 */
static bool
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
	if (!extensionLoaded || creating_extension)
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
		}

		/* we disable extension features during pg_upgrade */
		extensionLoaded = extensionPresent && extensionScriptExecuted && !IsBinaryUpgrade;

		if (extensionLoaded)
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
			 * We also reset citusVersionKnownCompatible, so it will be re-read in
			 * case of extension update.
			 */
			citusVersionKnownCompatible = false;
		}
	}

	return extensionLoaded;
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
	FunctionCallInfoData *fcinfo = NULL;
	FmgrInfo *flinfo = NULL;
	int argumentCount = 0;
	EState *estate = NULL;

	bool hasTuple = false;
	bool goForward = true;
	bool doCopy = false;
	char *availableExtensionVersion;

	estate = CreateExecutorState();
	extensionsResultSet = makeNode(ReturnSetInfo);
	extensionsResultSet->econtext = GetPerTupleExprContext(estate);
	extensionsResultSet->allowedModes = SFRM_Materialize;

	fcinfo = palloc0(sizeof(FunctionCallInfoData));
	flinfo = palloc0(sizeof(FmgrInfo));

	fmgr_info(F_PG_AVAILABLE_EXTENSIONS, flinfo);
	InitFunctionCallInfoData(*fcinfo, flinfo, argumentCount, InvalidOid, NULL,
							 (Node *) extensionsResultSet);

	/* pg_available_extensions returns result set containing all available extensions */
	(*pg_available_extensions)(fcinfo);

	tupleTableSlot = MakeSingleTupleTableSlot(extensionsResultSet->setDesc);
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
			if (CacheMemoryContext == NULL)
			{
				CreateCacheMemoryContext();
			}
			oldMemoryContext = MemoryContextSwitchTo(CacheMemoryContext);

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
		if (CacheMemoryContext == NULL)
		{
			CreateCacheMemoryContext();
		}

		oldMemoryContext = MemoryContextSwitchTo(CacheMemoryContext);

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
	CachedRelationLookup("pg_dist_shard", &distShardRelationId);

	return distShardRelationId;
}


/* return oid of pg_dist_shard_placement relation */
Oid
DistShardPlacementRelationId(void)
{
	CachedRelationLookup("pg_dist_shard_placement", &distShardPlacementRelationId);

	return distShardPlacementRelationId;
}


/* return oid of pg_dist_node relation */
Oid
DistNodeRelationId(void)
{
	CachedRelationLookup("pg_dist_node", &distNodeRelationId);

	return distNodeRelationId;
}


/* return oid of pg_dist_local_group relation */
Oid
DistLocalGroupIdRelationId(void)
{
	CachedRelationLookup("pg_dist_local_group", &distLocalGroupRelationId);

	return distLocalGroupRelationId;
}


/* return oid of pg_dist_colocation relation */
Oid
DistColocationRelationId(void)
{
	CachedRelationLookup("pg_dist_colocation", &distColocationRelationId);

	return distColocationRelationId;
}


/* return oid of pg_dist_colocation_configuration_index index */
Oid
DistColocationConfigurationIndexId(void)
{
	CachedRelationLookup("pg_dist_colocation_configuration_index",
						 &distColocationConfigurationIndexId);

	return distColocationConfigurationIndexId;
}


/* return oid of pg_dist_colocation_pkey index */
Oid
DistColocationColocationidIndexId(void)
{
	CachedRelationLookup("pg_dist_colocation_pkey",
						 &distColocationColocationidIndexId);

	return distColocationColocationidIndexId;
}


/* return oid of pg_dist_partition relation */
Oid
DistPartitionRelationId(void)
{
	CachedRelationLookup("pg_dist_partition", &distPartitionRelationId);

	return distPartitionRelationId;
}


/* return oid of pg_dist_partition_logical_relid_index index */
Oid
DistPartitionLogicalRelidIndexId(void)
{
	CachedRelationLookup("pg_dist_partition_logical_relid_index",
						 &distPartitionLogicalRelidIndexId);

	return distPartitionLogicalRelidIndexId;
}


/* return oid of pg_dist_partition_colocationid_index index */
Oid
DistPartitionColocationidIndexId(void)
{
	CachedRelationLookup("pg_dist_partition_colocationid_index",
						 &distPartitionColocationidIndexId);

	return distPartitionColocationidIndexId;
}


/* return oid of pg_dist_shard_logical_relid_index index */
Oid
DistShardLogicalRelidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_logical_relid_index",
						 &distShardLogicalRelidIndexId);

	return distShardLogicalRelidIndexId;
}


/* return oid of pg_dist_shard_shardid_index index */
Oid
DistShardShardidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_shardid_index", &distShardShardidIndexId);

	return distShardShardidIndexId;
}


/* return oid of pg_dist_shard_placement_shardid_index */
Oid
DistShardPlacementShardidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_placement_shardid_index",
						 &distShardPlacementShardidIndexId);

	return distShardPlacementShardidIndexId;
}


/* return oid of pg_dist_shard_placement_shardid_index */
Oid
DistShardPlacementPlacementidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_placement_placementid_index",
						 &distShardPlacementPlacementidIndexId);

	return distShardPlacementPlacementidIndexId;
}


/* return oid of pg_dist_transaction relation */
Oid
DistTransactionRelationId(void)
{
	CachedRelationLookup("pg_dist_transaction", &distTransactionRelationId);

	return distTransactionRelationId;
}


/* return oid of pg_dist_transaction_group_index */
Oid
DistTransactionGroupIndexId(void)
{
	CachedRelationLookup("pg_dist_transaction_group_index",
						 &distTransactionGroupIndexId);

	return distTransactionGroupIndexId;
}


/* return oid of pg_dist_shard_placement_nodeid_index */
Oid
DistShardPlacementNodeidIndexId(void)
{
	CachedRelationLookup("pg_dist_shard_placement_nodeid_index",
						 &distShardPlacementNodeidIndexId);

	return distShardPlacementNodeidIndexId;
}


/* return oid of the citus_extradata_container(internal) function */
Oid
CitusExtraDataContainerFuncId(void)
{
	List *nameList = NIL;
	Oid paramOids[1] = { INTERNALOID };

	if (extraDataContainerFuncId == InvalidOid)
	{
		nameList = list_make2(makeString("pg_catalog"),
							  makeString("citus_extradata_container"));
		extraDataContainerFuncId = LookupFuncName(nameList, 1, paramOids, false);
	}

	return extraDataContainerFuncId;
}


/* return oid of the worker_hash function */
Oid
CitusWorkerHashFunctionId(void)
{
	if (workerHashFunctionId == InvalidOid)
	{
		Oid citusExtensionOid = get_extension_oid("citus", false);
		Oid citusSchemaOid = get_extension_schema(citusExtensionOid);
		char *citusSchemaName = get_namespace_name(citusSchemaOid);
		const int argCount = 1;

		workerHashFunctionId = FunctionOid(citusSchemaName, "worker_hash", argCount);
	}

	return workerHashFunctionId;
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
	static Oid extensionOwner = InvalidOid;

	if (extensionOwner != InvalidOid)
	{
		return extensionOwner;
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
		extensionOwner = extensionForm->extowner;
		Assert(OidIsValid(extensionOwner));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension not loaded")));
	}

	systable_endscan(scandesc);

	heap_close(relation, AccessShareLock);

	return extensionOwner;
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


/* return the  username of the currently active role */
char *
CurrentUserName(void)
{
	Oid userId = GetUserId();

	return GetUserNameFromId(userId, false);
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
 * master_dist_placmeent_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_shard_placement are
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
		Form_pg_dist_shard_placement distPlacement =
			(Form_pg_dist_shard_placement) GETSTRUCT(oldTuple);

		oldShardId = distPlacement->shardid;
	}

	if (newTuple != NULL)
	{
		Form_pg_dist_shard_placement distPlacement =
			(Form_pg_dist_shard_placement) GETSTRUCT(newTuple);

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


/* initialize the infrastructure for the metadata cache */
static void
InitializeDistTableCache(void)
{
	HASHCTL info;

	/* make sure we've initialized CacheMemoryContext */
	if (CacheMemoryContext == NULL)
	{
		CreateCacheMemoryContext();
	}

	/* build initial scan keys, copied for every relation scan */
	memset(&DistPartitionScanKey, 0, sizeof(DistPartitionScanKey));

	fmgr_info_cxt(F_OIDEQ,
				  &DistPartitionScanKey[0].sk_func,
				  CacheMemoryContext);
	DistPartitionScanKey[0].sk_strategy = BTEqualStrategyNumber;
	DistPartitionScanKey[0].sk_subtype = InvalidOid;
	DistPartitionScanKey[0].sk_collation = InvalidOid;
	DistPartitionScanKey[0].sk_attno = Anum_pg_dist_partition_logicalrelid;

	memset(&DistShardScanKey, 0, sizeof(DistShardScanKey));

	fmgr_info_cxt(F_OIDEQ,
				  &DistShardScanKey[0].sk_func,
				  CacheMemoryContext);
	DistShardScanKey[0].sk_strategy = BTEqualStrategyNumber;
	DistShardScanKey[0].sk_subtype = InvalidOid;
	DistShardScanKey[0].sk_collation = InvalidOid;
	DistShardScanKey[0].sk_attno = Anum_pg_dist_shard_logicalrelid;

	/* initialize the per-table hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(DistTableCacheEntry);
	info.hash = tag_hash;
	DistTableCacheHash =
		hash_create("Distributed Relation Cache", 32, &info,
					HASH_ELEM | HASH_FUNCTION);

	/* initialize the per-shard hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardCacheEntry);
	info.hash = tag_hash;
	DistShardCacheHash =
		hash_create("Shard Cache", 32 * 64, &info,
					HASH_ELEM | HASH_FUNCTION);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateDistRelationCacheCallback,
								  (Datum) 0);
}


/*
 * GetWorkerNodeHash is a wrapper around InitializeWorkerNodeCache(). It
 * triggers InitializeWorkerNodeCache when the workerHash is invalid. Otherwise,
 * it returns the hash.
 */
HTAB *
GetWorkerNodeHash(void)
{
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

	return WorkerNodeHash;
}


/*
 * InitializeWorkerNodeCache initialize the infrastructure for the worker node cache.
 * The function reads the worker nodes from the metadata table, adds them to the hash and
 * finally registers an invalidation callback.
 */
static void
InitializeWorkerNodeCache(void)
{
	static bool invalidationRegistered = false;
	HTAB *oldWorkerNodeHash = NULL;
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	HASHCTL info;
	int hashFlags = 0;
	long maxTableSize = (long) MaxWorkerNodesTracked;

	/* make sure we've initialized CacheMemoryContext */
	if (CacheMemoryContext == NULL)
	{
		CreateCacheMemoryContext();
	}

	/*
	 * Create the hash that holds the worker nodes. The key is the combination of
	 * nodename and nodeport, instead of the unique nodeid because worker nodes are
	 * searched by the nodename and nodeport in every physical plan creation.
	 */
	memset(&info, 0, sizeof(info));
	info.keysize = +sizeof(uint32) + WORKER_LENGTH + sizeof(uint32);
	info.entrysize = sizeof(WorkerNode);
	info.hcxt = CacheMemoryContext;
	info.hash = WorkerNodeHashCode;
	info.match = WorkerNodeCompare;
	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE;

	oldWorkerNodeHash = WorkerNodeHash;
	WorkerNodeHash = hash_create("Worker Node Hash",
								 maxTableSize,
								 &info, hashFlags);

	/* read the list from pg_dist_node */
	workerNodeList = ReadWorkerNodes();

	/* iterate over the worker node list */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = NULL;
		WorkerNode *currentNode = lfirst(workerNodeCell);
		void *hashKey = NULL;
		bool handleFound = false;

		/* search for the worker node in the hash, and then insert the values */
		hashKey = (void *) currentNode;
		workerNode = (WorkerNode *) hash_search(WorkerNodeHash, hashKey,
												HASH_ENTER, &handleFound);

		/* fill the newly allocated workerNode in the cache */
		strlcpy(workerNode->workerName, currentNode->workerName, WORKER_LENGTH);
		workerNode->workerPort = currentNode->workerPort;
		workerNode->groupId = currentNode->groupId;
		workerNode->nodeId = currentNode->nodeId;
		strlcpy(workerNode->workerRack, currentNode->workerRack, WORKER_LENGTH);
		workerNode->hasMetadata = currentNode->hasMetadata;
		workerNode->isActive = currentNode->isActive;

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
	hash_destroy(oldWorkerNodeHash);

	/* prevent multiple invalidation registrations */
	if (!invalidationRegistered)
	{
		/* Watch for invalidation events. */
		CacheRegisterRelcacheCallback(InvalidateNodeRelationCacheCallback,
									  (Datum) 0);

		invalidationRegistered = true;
	}
}


/*
 * GetLocalGroupId returns the group identifier of the local node. The function assumes
 * that pg_dist_local_node_group has exactly one row and has at least one column.
 * Otherwise, the function errors out.
 */
int
GetLocalGroupId(void)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	Oid groupId = InvalidOid;
	Relation pgDistLocalGroupId = NULL;
	Oid localGroupTableOid = InvalidOid;

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

		groupId = DatumGetUInt32(groupIdDatum);
	}
	else
	{
		elog(ERROR, "could not find any entries in pg_dist_local_group");
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistLocalGroupId, AccessShareLock);

	/* prevent multiple invalidation registrations */
	if (!invalidationRegistered)
	{
		/* Watch for invalidation events. */
		CacheRegisterRelcacheCallback(InvalidateLocalGroupIdRelationCacheCallback,
									  (Datum) 0);

		invalidationRegistered = true;
	}

	/* set the local cache variable */
	LocalGroupId = groupId;

	return groupId;
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

	if (cacheEntry->shardIntervalArrayLength == 0)
	{
		return;
	}

	for (shardIndex = 0; shardIndex < cacheEntry->shardIntervalArrayLength;
		 shardIndex++)
	{
		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
		ShardPlacement *placementArray = cacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements = cacheEntry->arrayOfPlacementArrayLengths[shardIndex];
		bool valueByVal = shardInterval->valueByVal;
		bool foundInCache = false;
		int placementIndex = 0;

		/* delete the shard's placements */
		for (placementIndex = 0;
			 placementIndex < numberOfPlacements;
			 placementIndex++)
		{
			ShardPlacement *placement = &placementArray[placementIndex];

			if (placement->nodeName)
			{
				pfree(placement->nodeName);
			}

			/* placement itself is deleted as part of the array */
		}
		pfree(placementArray);

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

	cacheEntry->shardIntervalArrayLength = 0;
	cacheEntry->hasUninitializedShardInterval = false;
	cacheEntry->hasUniformHashDistribution = false;
	cacheEntry->hasOverlappingShardInterval = false;
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
		DistTableCacheEntry *cacheEntry = NULL;
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, DistTableCacheHash);

		while ((cacheEntry = (DistTableCacheEntry *) hash_seq_search(&status)) != NULL)
		{
			cacheEntry->isValid = false;
		}
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
	}

	/*
	 * If pg_dist_partition is being invalidated drop all state
	 * This happens pretty rarely, but most importantly happens during
	 * DROP EXTENSION citus;
	 */
	if (relationId != InvalidOid && relationId == distPartitionRelationId)
	{
		extensionLoaded = false;
		distShardRelationId = InvalidOid;
		distShardPlacementRelationId = InvalidOid;
		distLocalGroupRelationId = InvalidOid;
		distNodeRelationId = InvalidOid;
		distColocationRelationId = InvalidOid;
		distColocationConfigurationIndexId = InvalidOid;
		distColocationColocationidIndexId = InvalidOid;
		distPartitionRelationId = InvalidOid;
		distPartitionLogicalRelidIndexId = InvalidOid;
		distPartitionColocationidIndexId = InvalidOid;
		distShardLogicalRelidIndexId = InvalidOid;
		distShardShardidIndexId = InvalidOid;
		distShardPlacementShardidIndexId = InvalidOid;
		distShardPlacementPlacementidIndexId = InvalidOid;
		distTransactionRelationId = InvalidOid;
		distTransactionGroupIndexId = InvalidOid;
		extraDataContainerFuncId = InvalidOid;
		workerHashFunctionId = InvalidOid;
	}
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
	if (relationId == InvalidOid || relationId == distNodeRelationId)
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
	if (relationId == InvalidOid || relationId == distLocalGroupRelationId)
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

	scanDescriptor = systable_beginscan(pgDistShard, DistShardLogicalRelidIndexId(), true,
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
 * Errors out if the shardId does not exist.
 */
static Oid
LookupShardRelation(int64 shardId)
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
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}

	shardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
	relationId = shardForm->logicalrelid;

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
TupleToShardInterval(HeapTuple heapTuple, TupleDesc tupleDescriptor, Oid intervalTypeId,
					 int32 intervalTypeMod)
{
	ShardInterval *shardInterval = NULL;
	bool isNull = false;
	bool minValueNull = false;
	bool maxValueNull = false;
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	Datum relationIdDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_logicalrelid,
										 tupleDescriptor, &isNull);
	Datum shardIdDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_shardid,
									  tupleDescriptor, &isNull);
	Datum storageTypeDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_shardstorage,
										  tupleDescriptor, &isNull);

	Datum minValueTextDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_shardminvalue,
										   tupleDescriptor, &minValueNull);
	Datum maxValueTextDatum = heap_getattr(heapTuple, Anum_pg_dist_shard_shardmaxvalue,
										   tupleDescriptor, &maxValueNull);

	Oid relationId = DatumGetObjectId(relationIdDatum);
	int64 shardId = DatumGetInt64(shardIdDatum);
	char storageType = DatumGetChar(storageTypeDatum);
	Datum minValue = 0;
	Datum maxValue = 0;
	bool minValueExists = false;
	bool maxValueExists = false;
	int16 intervalTypeLen = 0;
	bool intervalByVal = false;
	char intervalAlign = '0';
	char intervalDelim = '0';

	if (!minValueNull && !maxValueNull)
	{
		char *minValueString = TextDatumGetCString(minValueTextDatum);
		char *maxValueString = TextDatumGetCString(maxValueTextDatum);

		/* TODO: move this up the call stack to avoid per-tuple invocation? */
		get_type_io_data(intervalTypeId, IOFunc_input, &intervalTypeLen, &intervalByVal,
						 &intervalAlign, &intervalDelim, &typeIoParam, &inputFunctionId);

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
 * CachedRelationLookup performs a cached lookup for the relation
 * relationName, with the result cached in *cachedOid.
 */
static void
CachedRelationLookup(const char *relationName, Oid *cachedOid)
{
	if (*cachedOid == InvalidOid)
	{
		*cachedOid = get_relname_relid(relationName, PG_CATALOG_NAMESPACE);

		if (*cachedOid == InvalidOid)
		{
			ereport(ERROR, (errmsg("cache lookup failed for %s, called to early?",
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
		ereport(DEBUG5, (errmsg("could not find distributed relation to invalidate for "
								"shard "INT64_FORMAT, shardId)));
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, NoLock);

	/* bump command counter, to force invalidation to take effect */
	CommandCounterIncrement();
}
