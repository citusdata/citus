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
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
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


/* state which should be cleared upon DROP EXTENSION */
static bool extensionLoaded = false;
static Oid distShardRelationId = InvalidOid;
static Oid distShardPlacementRelationId = InvalidOid;
static Oid distNodeRelationId = InvalidOid;
static Oid distPartitionRelationId = InvalidOid;
static Oid distPartitionLogicalRelidIndexId = InvalidOid;
static Oid distShardLogicalRelidIndexId = InvalidOid;
static Oid distShardShardidIndexId = InvalidOid;
static Oid distShardPlacementShardidIndexId = InvalidOid;
static Oid distShardPlacementNodeidIndexId = InvalidOid;
static Oid extraDataContainerFuncId = InvalidOid;

/* Hash table for informations about each partition */
static HTAB *DistTableCacheHash = NULL;

/* cached list of active workers */
static List *CachedNodeList = NULL;

/* built first time through in InitializePartitionCache */
static ScanKeyData DistPartitionScanKey[1];
static ScanKeyData DistShardScanKey[1];


/* local function forward declarations */
static DistTableCacheEntry * LookupDistTableCacheEntry(Oid relationId);
static FmgrInfo * ShardIntervalCompareFunction(ShardInterval **shardIntervalArray,
											   char partitionMethod);
static ShardInterval ** SortShardIntervalArray(ShardInterval **shardIntervalArray,
											   int shardCount,
											   FmgrInfo *
											   shardIntervalSortCompareFunction);
static bool HasUniformHashDistribution(ShardInterval **shardIntervalArray,
									   int shardIntervalArrayLength);
static bool HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray,
										  int shardCount);
static void InitializeDistTableCache(void);
static void InitializeWorkerNodeCache(void);
static void ResetDistTableCacheEntry(DistTableCacheEntry *cacheEntry);
static void InvalidateDistRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId);
static HeapTuple LookupDistPartitionTuple(Relation pgDistPartition, Oid relationId);
static List * LookupDistShardTuples(Oid relationId);
static void GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
									  Oid *intervalTypeId, int32 *intervalTypeMod);
static ShardInterval * TupleToShardInterval(HeapTuple heapTuple,
											TupleDesc tupleDescriptor, Oid intervalTypeId,
											int32 intervalTypeMod);
static List * ReadWorkerNodes(void);
static WorkerNode * TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static void CachedRelationLookup(const char *relationName, Oid *cachedOid);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_dist_partition_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_shard_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_node_cache_invalidate);


/*
 * IsDistributedTable returns whether relationId is a distributed relation or
 * not.
 */
bool
IsDistributedTable(Oid relationId)
{
	DistTableCacheEntry *cacheEntry = NULL;

	/*
	 * Can't be a distributed relation if the extension hasn't been loaded
	 * yet. As we can't do lookups in nonexistent tables, directly return
	 * false.
	 */
	if (!CitusHasBeenLoaded())
	{
		return false;
	}

	cacheEntry = LookupDistTableCacheEntry(relationId);

	return cacheEntry->isDistributedTable;
}


/*
 * LoadShardInterval reads shard metadata for given shardId from pg_dist_shard,
 * and converts min/max values in these metadata to their properly typed datum
 * representations. The function then allocates a structure that stores the read
 * and converted values, and returns this structure.
 */
ShardInterval *
LoadShardInterval(uint64 shardId)
{
	ShardInterval *shardInterval;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	Form_pg_dist_shard shardForm = NULL;
	DistTableCacheEntry *partitionEntry;
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = -1;

	Relation pgDistShard = heap_open(DistShardRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistShard);

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
	partitionEntry = DistributedTableCacheEntry(shardForm->logicalrelid);

	GetPartitionTypeInputInfo(partitionEntry->partitionKeyString,
							  partitionEntry->partitionMethod, &intervalTypeId,
							  &intervalTypeMod);

	shardInterval = TupleToShardInterval(heapTuple, tupleDescriptor, intervalTypeId,
										 intervalTypeMod);

	systable_endscan(scanDescriptor);
	heap_close(pgDistShard, AccessShareLock);

	return shardInterval;
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

	/*
	 * Can't be a distributed relation if the extension hasn't been loaded
	 * yet. As we can't do lookups in nonexistent tables, directly return NULL
	 * here.
	 */
	if (!CitusHasBeenLoaded())
	{
		return NULL;
	}

	cacheEntry = LookupDistTableCacheEntry(distributedRelationId);

	if (cacheEntry->isDistributedTable)
	{
		return cacheEntry;
	}
	else
	{
		ereport(ERROR, (errmsg("relation %u is not distributed",
							   distributedRelationId)));
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
	HeapTuple distPartitionTuple = NULL;
	char *partitionKeyString = NULL;
	char partitionMethod = 0;
	List *distShardTupleList = NIL;
	int shardIntervalArrayLength = 0;
	ShardInterval **shardIntervalArray = NULL;
	ShardInterval **sortedShardIntervalArray = NULL;
	FmgrInfo *shardIntervalCompareFunction = NULL;
	FmgrInfo *hashFunction = NULL;
	bool hasUninitializedShardInterval = false;
	bool hasUniformHashDistribution = false;
	void *hashKey = (void *) &relationId;
	Relation pgDistPartition = NULL;

	if (DistTableCacheHash == NULL)
	{
		InitializeDistTableCache();
	}

	cacheEntry = hash_search(DistTableCacheHash, hashKey, HASH_FIND, &foundInCache);

	/* return valid matches */
	if ((cacheEntry != NULL) && (cacheEntry->isValid))
	{
		return cacheEntry;
	}

	/* free the content of old, invalid, entries */
	if (cacheEntry != NULL)
	{
		ResetDistTableCacheEntry(cacheEntry);
	}

	pgDistPartition = heap_open(DistPartitionRelationId(), AccessShareLock);
	distPartitionTuple = LookupDistPartitionTuple(pgDistPartition, relationId);
	if (distPartitionTuple != NULL)
	{
		Form_pg_dist_partition partitionForm =
			(Form_pg_dist_partition) GETSTRUCT(distPartitionTuple);
		Datum partitionKeyDatum = 0;
		MemoryContext oldContext = NULL;
		bool isNull = false;

		partitionKeyDatum = heap_getattr(distPartitionTuple,
										 Anum_pg_dist_partition_partkey,
										 RelationGetDescr(pgDistPartition),
										 &isNull);
		Assert(!isNull);

		oldContext = MemoryContextSwitchTo(CacheMemoryContext);
		partitionKeyString = TextDatumGetCString(partitionKeyDatum);
		partitionMethod = partitionForm->partmethod;

		MemoryContextSwitchTo(oldContext);

		heap_freetuple(distPartitionTuple);
	}

	heap_close(pgDistPartition, NoLock);

	distShardTupleList = LookupDistShardTuples(relationId);
	shardIntervalArrayLength = list_length(distShardTupleList);
	if (shardIntervalArrayLength > 0)
	{
		Relation distShardRelation = heap_open(DistShardRelationId(), AccessShareLock);
		TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);
		ListCell *distShardTupleCell = NULL;
		int arrayIndex = 0;
		Oid intervalTypeId = InvalidOid;
		int32 intervalTypeMod = -1;

		GetPartitionTypeInputInfo(partitionKeyString, partitionMethod, &intervalTypeId,
								  &intervalTypeMod);

		shardIntervalArray = MemoryContextAllocZero(CacheMemoryContext,
													shardIntervalArrayLength *
													sizeof(ShardInterval *));

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

	/* decide and allocate interval comparison function */
	if (shardIntervalArrayLength > 0)
	{
		MemoryContext oldContext = CurrentMemoryContext;

		/* allocate the comparison function in the cache context */
		oldContext = MemoryContextSwitchTo(CacheMemoryContext);

		shardIntervalCompareFunction = ShardIntervalCompareFunction(shardIntervalArray,
																	partitionMethod);

		MemoryContextSwitchTo(oldContext);
	}

	/* sort the interval array */
	sortedShardIntervalArray = SortShardIntervalArray(shardIntervalArray,
													  shardIntervalArrayLength,
													  shardIntervalCompareFunction);

	/* check if there exists any shard intervals with no min/max values */
	hasUninitializedShardInterval =
		HasUninitializedShardInterval(sortedShardIntervalArray, shardIntervalArrayLength);

	/* we only need hash functions for hash distributed tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		TypeCacheEntry *typeEntry = NULL;
		Node *partitionNode = stringToNode(partitionKeyString);
		Var *partitionColumn = (Var *) partitionNode;
		Assert(IsA(partitionNode, Var));
		typeEntry = lookup_type_cache(partitionColumn->vartype,
									  TYPECACHE_HASH_PROC_FINFO);

		hashFunction = MemoryContextAllocZero(CacheMemoryContext,
											  sizeof(FmgrInfo));

		fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo), CacheMemoryContext);

		/* check the shard distribution for hash partitioned tables */
		hasUniformHashDistribution =
			HasUniformHashDistribution(sortedShardIntervalArray,
									   shardIntervalArrayLength);
	}

	cacheEntry = hash_search(DistTableCacheHash, hashKey, HASH_ENTER, NULL);

	/* zero out entry, but not the key part */
	memset(((char *) cacheEntry) + sizeof(Oid), 0,
		   sizeof(DistTableCacheEntry) - sizeof(Oid));

	if (distPartitionTuple == NULL)
	{
		cacheEntry->isValid = true;
		cacheEntry->isDistributedTable = false;
	}
	else
	{
		cacheEntry->isValid = true;
		cacheEntry->isDistributedTable = true;
		cacheEntry->partitionKeyString = partitionKeyString;
		cacheEntry->partitionMethod = partitionMethod;
		cacheEntry->shardIntervalArrayLength = shardIntervalArrayLength;
		cacheEntry->sortedShardIntervalArray = sortedShardIntervalArray;
		cacheEntry->shardIntervalCompareFunction = shardIntervalCompareFunction;
		cacheEntry->hashFunction = hashFunction;
		cacheEntry->hasUninitializedShardInterval = hasUninitializedShardInterval;
		cacheEntry->hasUniformHashDistribution = hasUniformHashDistribution;
	}

	return cacheEntry;
}


/*
 * ShardIntervalCompareFunction returns the appropriate compare function for the
 * partition column type. In case of hash-partitioning, it always returns the compare
 * function for integers. Callers of this function has to ensure that shardIntervalArray
 * has at least one element.
 */
static FmgrInfo *
ShardIntervalCompareFunction(ShardInterval **shardIntervalArray, char partitionMethod)
{
	FmgrInfo *shardIntervalCompareFunction = NULL;
	Oid comparisonTypeId = InvalidOid;

	Assert(shardIntervalArray != NULL);

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		comparisonTypeId = INT4OID;
	}
	else
	{
		ShardInterval *shardInterval = shardIntervalArray[0];
		comparisonTypeId = shardInterval->valueTypeId;
	}

	shardIntervalCompareFunction = GetFunctionInfo(comparisonTypeId, BTREE_AM_OID,
												   BTORDER_PROC);

	return shardIntervalCompareFunction;
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
 * CitusHasBeenLoaded returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false. The result is cached as this is called very frequently.
 */
bool
CitusHasBeenLoaded(void)
{
	/* recheck presence until citus has been loaded */
	if (!extensionLoaded)
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

		extensionLoaded = extensionPresent && extensionScriptExecuted;

		if (extensionLoaded)
		{
			/*
			 * InvalidateDistRelationCacheCallback resets state such as extensionLoaded
			 * when it notices changes to pg_dist_partition (which usually indicate
			 * `DROP EXTENSION citus;` has been run)
			 *
			 * Ensure InvalidateDistRelationCacheCallback will notice those changes
			 * by caching pg_dist_partition's oid.
			 */
			DistPartitionRelationId();
		}
	}

	return extensionLoaded;
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

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

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

	/* initialize the hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(DistTableCacheEntry);
	info.hash = tag_hash;
	DistTableCacheHash =
		hash_create("Distributed Relation Cache", 32, &info,
					HASH_ELEM | HASH_FUNCTION);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateDistRelationCacheCallback,
								  (Datum) 0);
}


/*
 * GetWorkerNodeList is a wrapper around InitializeWorkerNodeCache(). It
 * triggers InitializeWorkerNodeCache when the workerHash is NULL. Otherwise,
 * it returns the hash.
 */
List *
GetWorkerNodeList(void)
{
	List *resultList = NULL;
	ListCell *workerNodeCell;

	if (CachedNodeList == NULL)
	{
		InitializeWorkerNodeCache();
	}

	/* deep copy the list so invalidations, which free the list, don't cause crashes */
	foreach(workerNodeCell, CachedNodeList)
	{
		WorkerNode *oldNode = (WorkerNode *) lfirst(workerNodeCell);
		WorkerNode *newNode = palloc(sizeof(WorkerNode));

		memcpy(newNode, oldNode, sizeof(WorkerNode));

		resultList = lappend(resultList, newNode);
	}

	return resultList;
}


/*
 * Initialize the infrastructure for the worker node cache. The functions
 * reads the worker nodes from the metadata table, adds them to the hash and
 * finally registers an invalidation callaback.
 */
static void
InitializeWorkerNodeCache(void)
{
	MemoryContext oldContext;
	static bool invalidationRegistered = false;
	List *workerList = NULL;
	ListCell *workerListCell;

	/* make sure we've initialized CacheMemoryContext */
	if (CacheMemoryContext == NULL)
	{
		CreateCacheMemoryContext();
	}

	Assert(CachedNodeList == NULL);

	/* read the list from the pg_dist_node */
	workerList = ReadWorkerNodes();

	oldContext = MemoryContextSwitchTo(CacheMemoryContext);

	foreach(workerListCell, workerList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerListCell);
		WorkerNode *newWorkerNode = palloc(sizeof(WorkerNode));

		memcpy(newWorkerNode, workerNode, sizeof(WorkerNode));
		CachedNodeList = lappend(CachedNodeList, newWorkerNode);
	}

	MemoryContextSwitchTo(oldContext);

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
 * ResetDistTableCacheEntry frees any out-of-band memory used by a cache entry,
 * but does not free the entry itself.
 */
void
ResetDistTableCacheEntry(DistTableCacheEntry *cacheEntry)
{
	if (cacheEntry->partitionKeyString != NULL)
	{
		pfree(cacheEntry->partitionKeyString);
		cacheEntry->partitionKeyString = NULL;
	}

	if (cacheEntry->shardIntervalArrayLength > 0)
	{
		int i = 0;

		for (i = 0; i < cacheEntry->shardIntervalArrayLength; i++)
		{
			ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[i];
			bool valueByVal = shardInterval->valueByVal;

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

			pfree(shardInterval);
		}

		pfree(cacheEntry->sortedShardIntervalArray);
		cacheEntry->sortedShardIntervalArray = NULL;
		cacheEntry->shardIntervalArrayLength = 0;

		cacheEntry->hasUninitializedShardInterval = false;
		cacheEntry->hasUniformHashDistribution = false;

		pfree(cacheEntry->shardIntervalCompareFunction);
		cacheEntry->shardIntervalCompareFunction = NULL;

		/* we only allocated hash function for hash distributed tables */
		if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
		{
			pfree(cacheEntry->hashFunction);
			cacheEntry->hashFunction = NULL;
		}
	}
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
		distPartitionRelationId = InvalidOid;
		distPartitionLogicalRelidIndexId = InvalidOid;
		distShardLogicalRelidIndexId = InvalidOid;
		distShardShardidIndexId = InvalidOid;
		distShardPlacementShardidIndexId = InvalidOid;
		distNodeRelationId = InvalidOid;
		extraDataContainerFuncId = InvalidOid;
	}
}


/*
 * InvalidateNodeRelationCacheCallback destroys the CachedNodeList when
 * any change happens on pg_dist_node table. It also set CachedNodeList to
 * NULL, which allows consequent accesses to the hash read from the
 * pg_dist_node from scratch.
 */
static void
InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId)
{
	if (CachedNodeList != NULL && relationId == DistNodeRelationId())
	{
		list_free_deep(CachedNodeList);
		CachedNodeList = NULL;
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
		Assert(!HeapTupleHasNulls(currentPartitionTuple));

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
 * GetPartitionTypeInputInfo populates output parameters with the interval type
 * identifier and modifier for the specified partition key/method combination.
 */
static void
GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
						  Oid *intervalTypeId, int32 *intervalTypeMod)
{
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
			break;
		}

		case DISTRIBUTE_BY_HASH:
		{
			*intervalTypeId = INT4OID;
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
 * ReadWorkerNodes iterates over pg_dist_node table, converts each row
 * into it's memory representation (i.e., WorkerNode) and adds them into
 * a list. Lastly, the list is returned to the caller.
 */
static List *
ReadWorkerNodes()
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	List *workerNodeList = NIL;
	TupleDesc tupleDescriptor = NULL;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	scanDescriptor = systable_beginscan(pgDistNode,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistNode);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		WorkerNode *workerNode = TupleToWorkerNode(tupleDescriptor, heapTuple);
		workerNodeList = lappend(workerNodeList, workerNode);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, AccessExclusiveLock);

	return workerNodeList;
}


/*
 * InsertNodedRow opens the node system catalog, and inserts a new row with the
 * given values into that system catalog.
 */
void
InsertNodeRow(int nodeid, char *nodeName, int32 nodePort, uint32 groupId)
{
	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = UInt32GetDatum(nodeid);
	values[Anum_pg_dist_node_groupid - 1] = UInt32GetDatum(groupId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(nodeName);
	values[Anum_pg_dist_node_nodeport - 1] = UInt32GetDatum(nodePort);

	/* open shard relation and insert new tuple */
	pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistNode);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistNode, heapTuple);
	CatalogUpdateIndexes(pgDistNode, heapTuple);

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistNode, AccessExclusiveLock);

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();
}


/*
 * DeleteNodeRow removes the requested row if it exists
 */
void
DeleteNodeRow(char *nodeName, int32 nodePort)
{
	const int scanKeyCount = 2;
	bool indexOK = false;

	HeapTuple heapTuple = NULL;
	SysScanDesc heapScan;
	ScanKeyData scanKey[scanKeyCount];

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_nodename,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(nodeName));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_node_nodeport,
				BTEqualStrategyNumber, F_INT8EQ, Int32GetDatum(nodePort));

	heapScan = systable_beginscan(pgDistNode, InvalidOid, indexOK,
								  NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(heapScan);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for node \"%s:%d\"",
							   nodeName, nodePort)));
	}

	simple_heap_delete(pgDistNode, &(heapTuple->t_self));

	systable_endscan(heapScan);
	heap_close(pgDistNode, AccessExclusiveLock);

	/* ensure future commands don't use the node we just removed */
	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	/* increment the counter so that next command won't see the row */
	CommandCounterIncrement();
}


/*
 * TupleToWorkerNode takes in a heap tuple from pg_dist_node, and
 * converts this tuple to an equivalent struct in memory. The function assumes
 * the caller already has locks on the tuple, and doesn't perform any locking.
 */
static WorkerNode *
TupleToWorkerNode(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	WorkerNode *workerNode = NULL;
	bool isNull = false;

	Datum nodeId = heap_getattr(heapTuple, Anum_pg_dist_node_nodeid,
								tupleDescriptor, &isNull);
	Datum groupId = heap_getattr(heapTuple, Anum_pg_dist_node_groupid,
								 tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_pg_dist_node_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_pg_dist_node_nodeport,
								  tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(heapTuple));

	workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	workerNode->nodeId = DatumGetUInt32(nodeId);
	workerNode->workerPort = DatumGetUInt32(nodePort);
	workerNode->groupId = DatumGetUInt32(groupId);
	strlcpy(workerNode->workerName, TextDatumGetCString(nodeName), WORKER_LENGTH);

	return workerNode;
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
