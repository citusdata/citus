/*-------------------------------------------------------------------------
 *
 * metadata_cache.c
 *	  Distributed table metadata cache
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"
#include "miscadmin.h"
#include "stdint.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relmapper.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "citus_version.h"
#include "pg_version_compat.h"
#include "pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/citus_depended_object.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/function_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_local_group.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_node_metadata.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_placement.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/remote_commands.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shared_library_init.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/function.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"

#if PG_VERSION_NUM < PG_VERSION_16
#include "utils/relfilenodemap.h"
#endif


/* user configuration */
int ReadFromSecondaries = USE_SECONDARY_NODES_NEVER;


/*
 * CitusTableCacheEntrySlot is entry type for DistTableCacheHash,
 * entry data outlives slot on invalidation, so requires indirection.
 */
typedef struct CitusTableCacheEntrySlot
{
	/* lookup key - must be first. A pg_class.oid oid. */
	Oid relationId;

	/* Citus table metadata (NULL for local tables) */
	CitusTableCacheEntry *citusTableMetadata;

	/*
	 * If isValid is false, we need to recheck whether the relation ID
	 * belongs to a Citus or not.
	 */
	bool isValid;
} CitusTableCacheEntrySlot;


/*
 * ShardIdCacheEntry is the entry type for ShardIdCacheHash.
 *
 * This should never be used outside of this file. Use ShardInterval instead.
 */
typedef struct ShardIdCacheEntry
{
	/* hash key, needs to be first */
	uint64 shardId;

	/* pointer to the table entry to which this shard currently belongs */
	CitusTableCacheEntry *tableEntry;

	/* index of the shard interval in the sortedShardIntervalArray of the table entry */
	int shardIndex;
} ShardIdCacheEntry;

/*
 * ExtensionCreatedState is used to track if citus extension has been created
 * using CREATE EXTENSION command.
 *  UNKNOWN     : MetadataCache is invalid. State is UNKNOWN.
 *  CREATED     : Citus is created.
 *  NOTCREATED	: Citus is not created.
 */
typedef enum ExtensionCreatedState
{
	UNKNOWN = 0,
	CREATED = 1,
	NOTCREATED = 2,
} ExtensionCreatedState;

/*
 * State which should be cleared upon DROP EXTENSION. When the configuration
 * changes, e.g. because extension is dropped, these summarily get set to 0.
 */
typedef struct MetadataCacheData
{
	ExtensionCreatedState extensionCreatedState;
	Oid distShardRelationId;
	Oid distPlacementRelationId;
	Oid distBackgroundJobRelationId;
	Oid distBackgroundJobPKeyIndexId;
	Oid distBackgroundJobJobIdSequenceId;
	Oid distBackgroundTaskRelationId;
	Oid distBackgroundTaskPKeyIndexId;
	Oid distBackgroundTaskJobIdTaskIdIndexId;
	Oid distBackgroundTaskStatusTaskIdIndexId;
	Oid distBackgroundTaskTaskIdSequenceId;
	Oid distBackgroundTaskDependRelationId;
	Oid distBackgroundTaskDependTaskIdIndexId;
	Oid distBackgroundTaskDependDependsOnIndexId;
	Oid citusJobStatusScheduledId;
	Oid citusJobStatusRunningId;
	Oid citusJobStatusCancellingId;
	Oid citusJobStatusFinishedId;
	Oid citusJobStatusCancelledId;
	Oid citusJobStatusFailedId;
	Oid citusJobStatusFailingId;
	Oid citusTaskStatusBlockedId;
	Oid citusTaskStatusRunnableId;
	Oid citusTaskStatusRunningId;
	Oid citusTaskStatusDoneId;
	Oid citusTaskStatusErrorId;
	Oid citusTaskStatusUnscheduledId;
	Oid citusTaskStatusCancelledId;
	Oid citusTaskStatusCancellingId;
	Oid distRebalanceStrategyRelationId;
	Oid distNodeRelationId;
	Oid distNodeNodeIdIndexId;
	Oid distLocalGroupRelationId;
	Oid distObjectRelationId;
	Oid distObjectPrimaryKeyIndexId;
	Oid distCleanupRelationId;
	Oid distCleanupPrimaryKeyIndexId;
	Oid distColocationRelationId;
	Oid distColocationConfigurationIndexId;
	Oid distPartitionRelationId;
	Oid distTenantSchemaRelationId;
	Oid distPartitionLogicalRelidIndexId;
	Oid distPartitionColocationidIndexId;
	Oid distShardLogicalRelidIndexId;
	Oid distShardShardidIndexId;
	Oid distPlacementShardidIndexId;
	Oid distPlacementPlacementidIndexId;
	Oid distColocationidIndexId;
	Oid distPlacementGroupidIndexId;
	Oid distTransactionRelationId;
	Oid distTransactionGroupIndexId;
	Oid distTenantSchemaPrimaryKeyIndexId;
	Oid distTenantSchemaUniqueColocationIdIndexId;
	Oid citusCatalogNamespaceId;
	Oid copyFormatTypeId;
	Oid readIntermediateResultFuncId;
	Oid readIntermediateResultArrayFuncId;
	Oid extraDataContainerFuncId;
	Oid workerHashFunctionId;
	Oid anyValueFunctionId;
	Oid textSendAsJsonbFunctionId;
	Oid textoutFunctionId;
	Oid extensionOwner;
	Oid binaryCopyFormatId;
	Oid textCopyFormatId;
	Oid primaryNodeRoleId;
	Oid secondaryNodeRoleId;
	Oid pgTableIsVisibleFuncId;
	Oid citusTableIsVisibleFuncId;
	Oid distAuthinfoRelationId;
	Oid distAuthinfoIndexId;
	Oid distPoolinfoRelationId;
	Oid distPoolinfoIndexId;
	Oid relationIsAKnownShardFuncId;
	Oid jsonbExtractPathFuncId;
	Oid jsonbExtractPathTextFuncId;
	Oid CitusDependentObjectFuncId;
	Oid distClockLogicalSequenceId;
	bool databaseNameValid;
	char databaseName[NAMEDATALEN];
} MetadataCacheData;


static MetadataCacheData MetadataCache;

/* Citus extension version variables */
bool EnableVersionChecks = true; /* version checks are enabled */

static bool citusVersionKnownCompatible = false;

/* Variable to determine if we are in the process of creating citus */
static int CreateCitusTransactionLevel = 0;

/* Hash table for informations about each partition */
static HTAB *DistTableCacheHash = NULL;
static List *DistTableCacheExpired = NIL;

/* Hash table for informations about each shard */
static HTAB *ShardIdCacheHash = NULL;

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

/* default value is -1, increases with every node starting from 1 */
static int32 LocalNodeId = -1;

/* built first time through in InitializeDistCache */
static ScanKeyData DistPartitionScanKey[1];
static ScanKeyData DistShardScanKey[1];
static ScanKeyData DistObjectScanKey[3];


/* local function forward declarations */
static HeapTuple PgDistPartitionTupleViaCatalog(Oid relationId);
static ShardIdCacheEntry * LookupShardIdCacheEntry(int64 shardId, bool missingOk);
static CitusTableCacheEntry * BuildCitusTableCacheEntry(Oid relationId);
static void BuildCachedShardList(CitusTableCacheEntry *cacheEntry);
static void PrepareWorkerNodeCache(void);
static bool CheckInstalledVersion(int elevel);
static char * AvailableExtensionVersion(void);
static char * InstalledExtensionVersion(void);
static bool CitusHasBeenLoadedInternal(void);
static void InitializeCaches(void);
static void InitializeDistCache(void);
static void InitializeDistObjectCache(void);
static void InitializeWorkerNodeCache(void);
static void RegisterForeignKeyGraphCacheCallbacks(void);
static void RegisterWorkerNodeCacheCallbacks(void);
static void RegisterLocalGroupIdCacheCallbacks(void);
static void RegisterAuthinfoCacheCallbacks(void);
static void RegisterCitusTableCacheEntryReleaseCallbacks(void);
static void ResetCitusTableCacheEntry(CitusTableCacheEntry *cacheEntry);
static void RemoveStaleShardIdCacheEntries(CitusTableCacheEntry *tableEntry);
static void CreateDistTableCache(void);
static void CreateShardIdCache(void);
static void CreateDistObjectCache(void);
static void InvalidateForeignRelationGraphCacheCallback(Datum argument, Oid relationId);
static void InvalidateNodeRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateLocalGroupIdRelationCacheCallback(Datum argument, Oid relationId);
static void InvalidateConnParamsCacheCallback(Datum argument, Oid relationId);
static void CitusTableCacheEntryReleaseCallback(ResourceReleasePhase phase, bool isCommit,
												bool isTopLevel, void *arg);
static HeapTuple LookupDistPartitionTuple(Relation pgDistPartition, Oid relationId);
static void GetPartitionTypeInputInfo(char *partitionKeyString, char partitionMethod,
									  Oid *columnTypeId, int32 *columnTypeMod,
									  Oid *intervalTypeId, int32 *intervalTypeMod);
static void CachedNamespaceLookup(const char *nspname, Oid *cachedOid);
static void CachedRelationLookup(const char *relationName, Oid *cachedOid);
static void CachedRelationLookupExtended(const char *relationName, Oid *cachedOid,
										 bool missing_ok);
static void CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace,
										  Oid *cachedOid);
static void CachedRelationNamespaceLookupExtended(const char *relationName,
												  Oid renamespace, Oid *cachedOid,
												  bool missing_ok);
static ShardPlacement * ResolveGroupShardPlacement(
	GroupShardPlacement *groupShardPlacement, CitusTableCacheEntry *tableEntry,
	int shardIndex);
static Oid LookupEnumValueId(Oid typeId, char *valueName);
static void InvalidateCitusTableCacheEntrySlot(CitusTableCacheEntrySlot *cacheSlot);
static void InvalidateDistTableCache(void);
static void InvalidateDistObjectCache(void);
static bool InitializeTableCacheEntry(int64 shardId, bool missingOk);
static bool IsCitusTableTypeInternal(char partitionMethod, char replicationModel,
									 uint32 colocationId, CitusTableType tableType);
static bool RefreshTableCacheEntryIfInvalid(ShardIdCacheEntry *shardEntry, bool
											missingOk);

static Oid DistAuthinfoRelationId(void);
static Oid DistAuthinfoIndexId(void);
static Oid DistPoolinfoRelationId(void);
static Oid DistPoolinfoIndexId(void);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_dist_partition_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_partition_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_dist_shard_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_shard_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_dist_placement_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_placement_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_dist_node_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_node_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_dist_local_group_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_local_group_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_conninfo_cache_invalidate);
PG_FUNCTION_INFO_V1(master_dist_authinfo_cache_invalidate);
PG_FUNCTION_INFO_V1(citus_dist_object_cache_invalidate);
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
		ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
						errmsg("writing to worker nodes is not currently allowed"),
						errdetail("the database is read-only")));
	}

	if (ReadFromSecondaries == USE_SECONDARY_NODES_ALWAYS)
	{
		ereport(ERROR, (errmsg("writing to worker nodes is not currently allowed"),
						errdetail("citus.use_secondary_nodes is set to 'always'")));
	}
}


/*
 * EnsureModificationsCanRunOnRelation first calls into EnsureModificationsCanRun() and
 * then does one more additional check. The additional check is to give a proper error
 * message if any relation that is modified is replicated, as replicated tables use
 * 2PC and 2PC cannot happen when recovery is in progress.
 */
void
EnsureModificationsCanRunOnRelation(Oid relationId)
{
	EnsureModificationsCanRun();

	if (!OidIsValid(relationId) || !IsCitusTable(relationId))
	{
		/* we are not interested in PG tables */
		return;
	}

	bool modifiedTableReplicated =
		IsCitusTableType(relationId, REFERENCE_TABLE) ||
		!SingleReplicatedTable(relationId);

	if (!IsCoordinator() && !AllowModificationsFromWorkersToReplicatedTables &&
		modifiedTableReplicated)
	{
		ereport(ERROR, (errmsg("modifications via the worker nodes are not "
							   "allowed for replicated tables such as reference "
							   "tables or hash distributed tables with replication "
							   "factor greater than 1."),
						errhint("All modifications to replicated tables should "
								"happen via the coordinator unless "
								"citus.allow_modifications_from_workers_to_replicated_tables "
								" = true."),
						errdetail("Allowing modifications from the worker nodes "
								  "requires extra locking which might decrease "
								  "the throughput.")));
	}

	/*
	 * Even if user allows writes from standby, we should not allow for
	 * replicated tables as they require 2PC. And, 2PC needs to write a log
	 * record on the coordinator.
	 */
	if (!(RecoveryInProgress() && WritableStandbyCoordinator))
	{
		return;
	}

	if (modifiedTableReplicated)
	{
		ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
						errmsg("writing to worker nodes is not currently "
							   "allowed for replicated tables such as reference "
							   "tables or hash distributed tables with replication "
							   "factor greater than 1."),
						errhint("All modifications to replicated tables "
								"happen via 2PC, and 2PC requires the "
								"database to be in a writable state."),
						errdetail("the database is read-only")));
	}
}


/*
 * IsCitusTableType returns true if the given table with relationId
 * belongs to a citus table that matches the given table type. If cache
 * entry already exists, prefer using IsCitusTableTypeCacheEntry to avoid
 * an extra lookup.
 */
bool
IsCitusTableType(Oid relationId, CitusTableType tableType)
{
	CitusTableCacheEntry *tableEntry = LookupCitusTableCacheEntry(relationId);

	/* we are not interested in postgres tables */
	if (tableEntry == NULL)
	{
		return false;
	}
	return IsCitusTableTypeCacheEntry(tableEntry, tableType);
}


/*
 * GetCitusTableType is a helper function that returns the CitusTableType
 * for the given relationId.
 * Note that a single table can be qualified as multiple CitusTableType, such
 * as hash distributed tables are both HASH_DISTRIBUTED and DISTRIBUTED_TABLE.
 * This function returns the base type for a given table.
 *
 * If the table is not a Citus table, ANY_CITUS_TABLE_TYPE is returned.
 */
CitusTableType
GetCitusTableType(CitusTableCacheEntry *tableEntry)
{
	/* we do not expect local tables here */
	Assert(tableEntry != NULL);

	if (IsCitusTableTypeCacheEntry(tableEntry, HASH_DISTRIBUTED))
	{
		return HASH_DISTRIBUTED;
	}
	else if (IsCitusTableTypeCacheEntry(tableEntry, SINGLE_SHARD_DISTRIBUTED))
	{
		return SINGLE_SHARD_DISTRIBUTED;
	}
	else if (IsCitusTableTypeCacheEntry(tableEntry, REFERENCE_TABLE))
	{
		return REFERENCE_TABLE;
	}
	else if (IsCitusTableTypeCacheEntry(tableEntry, CITUS_LOCAL_TABLE))
	{
		return CITUS_LOCAL_TABLE;
	}
	else if (IsCitusTableTypeCacheEntry(tableEntry, APPEND_DISTRIBUTED))
	{
		return APPEND_DISTRIBUTED;
	}
	else if (IsCitusTableTypeCacheEntry(tableEntry, RANGE_DISTRIBUTED))
	{
		return RANGE_DISTRIBUTED;
	}
	else
	{
		return ANY_CITUS_TABLE_TYPE;
	}
}


/*
 * IsCitusTableTypeCacheEntry returns true if the given table cache entry
 * belongs to a citus table that matches the given table type.
 */
bool
IsCitusTableTypeCacheEntry(CitusTableCacheEntry *tableEntry, CitusTableType tableType)
{
	return IsCitusTableTypeInternal(tableEntry->partitionMethod,
									tableEntry->replicationModel,
									tableEntry->colocationId, tableType);
}

/*
* IsFirstShard returns true if the given shardId is the first shard.
*/
bool
IsFirstShard(CitusTableCacheEntry *tableEntry, uint64 shardId)
{
        if (tableEntry == NULL || tableEntry->sortedShardIntervalArray == NULL)
        {
                return false;
        }
        if (tableEntry->sortedShardIntervalArray[0]->shardId == INVALID_SHARD_ID)
        {
                return false;
        }

        if (shardId == tableEntry->sortedShardIntervalArray[0]->shardId)
        {
                return true;
        }
        else
        {
                return false;
        }
}

/*
 * HasDistributionKey returns true if given Citus table has a distribution key.
 */
bool
HasDistributionKey(Oid relationId)
{
	CitusTableCacheEntry *tableEntry = LookupCitusTableCacheEntry(relationId);
	if (tableEntry == NULL)
	{
		ereport(ERROR, (errmsg("relation with oid %u is not a Citus table", relationId)));
	}

	return HasDistributionKeyCacheEntry(tableEntry);
}


/*
 * HasDistributionKeyCacheEntry returns true if given cache entry identifies a
 * Citus table that has a distribution key.
 */
bool
HasDistributionKeyCacheEntry(CitusTableCacheEntry *tableEntry)
{
	return tableEntry->partitionMethod != DISTRIBUTE_BY_NONE;
}


/*
 * IsCitusTableTypeInternal returns true if the given table entry belongs to
 * the given table type group. For definition of table types, see CitusTableType.
 */
static bool
IsCitusTableTypeInternal(char partitionMethod, char replicationModel,
						 uint32 colocationId, CitusTableType tableType)
{
	switch (tableType)
	{
		case HASH_DISTRIBUTED:
		{
			return partitionMethod == DISTRIBUTE_BY_HASH;
		}

		case APPEND_DISTRIBUTED:
		{
			return partitionMethod == DISTRIBUTE_BY_APPEND;
		}

		case RANGE_DISTRIBUTED:
		{
			return partitionMethod == DISTRIBUTE_BY_RANGE;
		}

		case SINGLE_SHARD_DISTRIBUTED:
		{
			return partitionMethod == DISTRIBUTE_BY_NONE &&
				   replicationModel != REPLICATION_MODEL_2PC &&
				   colocationId != INVALID_COLOCATION_ID;
		}

		case DISTRIBUTED_TABLE:
		{
			return partitionMethod == DISTRIBUTE_BY_HASH ||
				   partitionMethod == DISTRIBUTE_BY_RANGE ||
				   partitionMethod == DISTRIBUTE_BY_APPEND ||
				   (partitionMethod == DISTRIBUTE_BY_NONE &&
					replicationModel != REPLICATION_MODEL_2PC &&
					colocationId != INVALID_COLOCATION_ID);
		}

		case STRICTLY_PARTITIONED_DISTRIBUTED_TABLE:
		{
			return partitionMethod == DISTRIBUTE_BY_HASH ||
				   partitionMethod == DISTRIBUTE_BY_RANGE;
		}

		case REFERENCE_TABLE:
		{
			return partitionMethod == DISTRIBUTE_BY_NONE &&
				   replicationModel == REPLICATION_MODEL_2PC;
		}

		case CITUS_LOCAL_TABLE:
		{
			return partitionMethod == DISTRIBUTE_BY_NONE &&
				   replicationModel != REPLICATION_MODEL_2PC &&
				   colocationId == INVALID_COLOCATION_ID;
		}

		case ANY_CITUS_TABLE_TYPE:
		{
			return true;
		}

		default:
		{
			ereport(ERROR, (errmsg("Unknown table type %d", tableType)));
		}
	}
	return false;
}


/*
 * GetTableTypeName returns string representation of the table type.
 */
char *
GetTableTypeName(Oid tableId)
{
	if (!IsCitusTable(tableId))
	{
		return "regular table";
	}

	CitusTableCacheEntry *tableCacheEntry = GetCitusTableCacheEntry(tableId);
	if (IsCitusTableTypeCacheEntry(tableCacheEntry, HASH_DISTRIBUTED))
	{
		return "distributed table";
	}
	else if (IsCitusTableTypeCacheEntry(tableCacheEntry, REFERENCE_TABLE))
	{
		return "reference table";
	}
	else if (IsCitusTableTypeCacheEntry(tableCacheEntry, CITUS_LOCAL_TABLE))
	{
		return "citus local table";
	}
	else
	{
		return "unknown table";
	}
}


/*
 * IsCitusTable returns whether relationId is a distributed relation or
 * not.
 */
bool
IsCitusTable(Oid relationId)
{
	/*
	 * PostgreSQL's OID generator assigns user operation OIDs starting
	 * from FirstNormalObjectId. This means no user object can have
	 * an OID lower than FirstNormalObjectId. Therefore, if the
	 * relationId is less than FirstNormalObjectId
	 * (i.e. in PostgreSQL's reserved range), we can immediately
	 * return false, since such objects cannot be Citus tables.
	 */
	if (relationId < FirstNormalObjectId)
	{
		return false;
	}
	return LookupCitusTableCacheEntry(relationId) != NULL;
}


/*
 * IsCitusTableRangeVar returns whether the table named in the given
 * rangeVar is a Citus table.
 */
bool
IsCitusTableRangeVar(RangeVar *rangeVar, LOCKMODE lockMode, bool missingOK)
{
	Oid relationId = RangeVarGetRelid(rangeVar, lockMode, missingOK);
	return IsCitusTable(relationId);
}


/*
 * IsCitusTableViaCatalog returns whether the given relation is a
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
bool
IsCitusTableViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = PgDistPartitionTupleViaCatalog(relationId);

	bool heapTupleIsValid = HeapTupleIsValid(partitionTuple);

	if (heapTupleIsValid)
	{
		heap_freetuple(partitionTuple);
	}
	return heapTupleIsValid;
}


/*
 * PartitionMethodViaCatalog gets a relationId and returns the partition
 * method column from pg_dist_partition via reading from catalog.
 */
char
PartitionMethodViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = PgDistPartitionTupleViaCatalog(relationId);
	if (!HeapTupleIsValid(partitionTuple))
	{
		return DISTRIBUTE_BY_INVALID;
	}

	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(partitionTuple, tupleDescriptor, datumArray, isNullArray);

	if (isNullArray[Anum_pg_dist_partition_partmethod - 1])
	{
		/* partition method cannot be NULL, still let's make sure */
		heap_freetuple(partitionTuple);
		table_close(pgDistPartition, NoLock);
		return DISTRIBUTE_BY_INVALID;
	}

	Datum partitionMethodDatum = datumArray[Anum_pg_dist_partition_partmethod - 1];
	char partitionMethodChar = DatumGetChar(partitionMethodDatum);

	heap_freetuple(partitionTuple);
	table_close(pgDistPartition, NoLock);

	return partitionMethodChar;
}


/*
 * PartitionColumnViaCatalog gets a relationId and returns the partition
 * key column from pg_dist_partition via reading from catalog.
 */
Var *
PartitionColumnViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = PgDistPartitionTupleViaCatalog(relationId);
	if (!HeapTupleIsValid(partitionTuple))
	{
		return NULL;
	}

	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(partitionTuple, tupleDescriptor, datumArray, isNullArray);

	if (isNullArray[Anum_pg_dist_partition_partkey - 1])
	{
		/* partition key cannot be NULL, still let's make sure */
		heap_freetuple(partitionTuple);
		table_close(pgDistPartition, NoLock);
		return NULL;
	}

	Datum partitionKeyDatum = datumArray[Anum_pg_dist_partition_partkey - 1];
	char *partitionKeyString = TextDatumGetCString(partitionKeyDatum);

	/* convert the string to a Node and ensure it is a Var */
	Node *partitionNode = stringToNode(partitionKeyString);
	Assert(IsA(partitionNode, Var));

	Var *partitionColumn = (Var *) partitionNode;

	heap_freetuple(partitionTuple);
	table_close(pgDistPartition, NoLock);

	return partitionColumn;
}


/*
 * ColocationIdViaCatalog gets a relationId and returns the colocation
 * id column from pg_dist_partition via reading from catalog.
 */
uint32
ColocationIdViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = PgDistPartitionTupleViaCatalog(relationId);
	if (!HeapTupleIsValid(partitionTuple))
	{
		return INVALID_COLOCATION_ID;
	}

	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(partitionTuple, tupleDescriptor, datumArray, isNullArray);

	if (isNullArray[Anum_pg_dist_partition_colocationid - 1])
	{
		/* colocation id cannot be NULL, still let's make sure */
		heap_freetuple(partitionTuple);
		table_close(pgDistPartition, NoLock);
		return INVALID_COLOCATION_ID;
	}

	Datum colocationIdDatum = datumArray[Anum_pg_dist_partition_colocationid - 1];
	uint32 colocationId = DatumGetUInt32(colocationIdDatum);

	heap_freetuple(partitionTuple);
	table_close(pgDistPartition, NoLock);

	return colocationId;
}


/*
 * PgDistPartitionTupleViaCatalog is a helper function that searches
 * pg_dist_partition for the given relationId. The caller is responsible
 * for ensuring that the returned heap tuple is valid before accessing
 * its fields.
 */
static HeapTuple
PgDistPartitionTupleViaCatalog(Oid relationId)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK, NULL, scanKeyCount, scanKey);

	HeapTuple partitionTuple = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(partitionTuple))
	{
		/* callers should have the tuple in their memory contexts */
		partitionTuple = heap_copytuple(partitionTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, AccessShareLock);

	return partitionTuple;
}


/*
 * IsReferenceTableByDistParams returns true if given partitionMethod and
 * replicationModel would identify a reference table.
 */
bool
IsReferenceTableByDistParams(char partitionMethod, char replicationModel)
{
	return partitionMethod == DISTRIBUTE_BY_NONE &&
		   replicationModel == REPLICATION_MODEL_2PC;
}


/*
 * IsCitusLocalTableByDistParams returns true if given partitionMethod,
 * replicationModel and colocationId would identify a citus local table.
 */
bool
IsCitusLocalTableByDistParams(char partitionMethod, char replicationModel,
							  uint32 colocationId)
{
	return partitionMethod == DISTRIBUTE_BY_NONE &&
		   replicationModel != REPLICATION_MODEL_2PC &&
		   colocationId == INVALID_COLOCATION_ID;
}


/*
 * IsSingleShardTableByDistParams returns true if given partitionMethod,
 * replicationModel and colocationId would identify a single-shard distributed
 * table that has a null shard key.
 */
bool
IsSingleShardTableByDistParams(char partitionMethod, char replicationModel,
							   uint32 colocationId)
{
	return partitionMethod == DISTRIBUTE_BY_NONE &&
		   replicationModel != REPLICATION_MODEL_2PC &&
		   colocationId != INVALID_COLOCATION_ID;
}


/*
 * CitusTableList returns a list that includes all the valid distributed table
 * cache entries.
 */
List *
CitusTableList(void)
{
	List *distributedTableList = NIL;

	Assert(CitusHasBeenLoaded() && CheckCitusVersion(WARNING));

	/* first, we need to iterate over pg_dist_partition */
	List *citusTableIdList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);

	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, citusTableIdList)
	{
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

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
	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	int shardIndex = shardIdEntry->shardIndex;

	/* the offset better be in a valid range */
	Assert(shardIndex < tableEntry->shardIntervalArrayLength);

	ShardInterval *sourceShardInterval =
		tableEntry->sortedShardIntervalArray[shardIndex];

	/* copy value to return */
	ShardInterval *shardInterval = CopyShardInterval(sourceShardInterval);

	return shardInterval;
}


/*
 * ShardExists returns whether given shard exists or not. It fails if missingOk is false
 * and shard is not found.
 */
bool
ShardExists(uint64 shardId)
{
	bool missingOk = true;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);

	if (!shardIdEntry)
	{
		return false;
	}

	return true;
}


/*
 * RelationIdOfShard returns the relationId of the given shardId.
 */
Oid
RelationIdForShard(uint64 shardId)
{
	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	return tableEntry->relationId;
}


/*
 * ReferenceTableShardId returns true if the given shardId belongs to
 * a reference table.
 */
bool
ReferenceTableShardId(uint64 shardId)
{
	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	return IsCitusTableTypeCacheEntry(tableEntry, REFERENCE_TABLE);
}


/*
 * DistributedTableShardId returns true if the given shardId belongs to
 * a distributed table.
 */
bool
DistributedTableShardId(uint64 shardId)
{
	if (shardId == INVALID_SHARD_ID)
	{
		return false;
	}

	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	return IsCitusTableTypeCacheEntry(tableEntry, DISTRIBUTED_TABLE);
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
	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	int shardIndex = shardIdEntry->shardIndex;

	/* the offset better be in a valid range */
	Assert(shardIndex < tableEntry->shardIntervalArrayLength);

	GroupShardPlacement *placementArray =
		tableEntry->arrayOfPlacementArrays[shardIndex];
	int numberOfPlacements =
		tableEntry->arrayOfPlacementArrayLengths[shardIndex];

	for (int i = 0; i < numberOfPlacements; i++)
	{
		if (placementArray[i].placementId == placementId)
		{
			GroupShardPlacement *shardPlacement = CitusMakeNode(GroupShardPlacement);

			*shardPlacement = placementArray[i];

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
	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	int shardIndex = shardIdEntry->shardIndex;
	GroupShardPlacement *groupPlacement = LoadGroupShardPlacement(shardId, placementId);
	ShardPlacement *nodePlacement = ResolveGroupShardPlacement(groupPlacement,
															   tableEntry, shardIndex);

	return nodePlacement;
}


/*
 * ShardPlacementOnGroupIncludingOrphanedPlacements returns the shard placement
 * for the given shard on the given group, or returns NULL if no placement for
 * the shard exists on the group.
 *
 * NOTE: This can return inactive or orphaned placements.
 */
ShardPlacement *
ShardPlacementOnGroupIncludingOrphanedPlacements(int32 groupId, uint64 shardId)
{
	ShardPlacement *placementOnNode = NULL;

	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	int shardIndex = shardIdEntry->shardIndex;
	GroupShardPlacement *placementArray =
		tableEntry->arrayOfPlacementArrays[shardIndex];
	int numberOfPlacements =
		tableEntry->arrayOfPlacementArrayLengths[shardIndex];

	for (int placementIndex = 0; placementIndex < numberOfPlacements; placementIndex++)
	{
		GroupShardPlacement *placement = &placementArray[placementIndex];
		if (placement->groupId == groupId)
		{
			placementOnNode = ResolveGroupShardPlacement(placement, tableEntry,
														 shardIndex);
			break;
		}
	}

	return placementOnNode;
}


/*
 * ActiveShardPlacementOnGroup returns the active shard placement for the
 * given shard on the given group, or returns NULL if no active placement for
 * the shard exists on the group.
 */
ShardPlacement *
ActiveShardPlacementOnGroup(int32 groupId, uint64 shardId)
{
	ShardPlacement *placement =
		ShardPlacementOnGroupIncludingOrphanedPlacements(groupId, shardId);
	if (placement == NULL)
	{
		return NULL;
	}
	return placement;
}


/*
 * ResolveGroupShardPlacement takes a GroupShardPlacement and adds additional data to it,
 * such as the node we should consider it to be on.
 */
static ShardPlacement *
ResolveGroupShardPlacement(GroupShardPlacement *groupShardPlacement,
						   CitusTableCacheEntry *tableEntry,
						   int shardIndex)
{
	ShardInterval *shardInterval = tableEntry->sortedShardIntervalArray[shardIndex];

	ShardPlacement *shardPlacement = CitusMakeNode(ShardPlacement);
	int32 groupId = groupShardPlacement->groupId;
	WorkerNode *workerNode = LookupNodeForGroup(groupId);

	/* copy everything into shardPlacement but preserve the header */
	CitusNode header = shardPlacement->type;
	GroupShardPlacement *shardPlacementAsGroupPlacement =
		(GroupShardPlacement *) shardPlacement;
	*shardPlacementAsGroupPlacement = *groupShardPlacement;
	shardPlacement->type = header;

	SetPlacementNodeMetadata(shardPlacement, workerNode);

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
 * HasAnyNodes returns whether there are any nodes in pg_dist_node.
 */
bool
HasAnyNodes(void)
{
	PrepareWorkerNodeCache();

	return WorkerNodeCount > 0;
}


/*
 * LookupNodeByNodeId returns a worker node by nodeId or NULL if the node
 * cannot be found.
 */
WorkerNode *
LookupNodeByNodeId(uint32 nodeId)
{
	PrepareWorkerNodeCache();

	for (int workerNodeIndex = 0; workerNodeIndex < WorkerNodeCount; workerNodeIndex++)
	{
		WorkerNode *workerNode = WorkerNodeArray[workerNodeIndex];
		if (workerNode->nodeId == nodeId)
		{
			WorkerNode *workerNodeCopy = palloc0(sizeof(WorkerNode));
			*workerNodeCopy = *workerNode;

			return workerNodeCopy;
		}
	}

	return NULL;
}


/*
 * LookupNodeByNodeIdOrError returns a worker node by nodeId or errors out if the
 * node cannot be found.
 */
WorkerNode *
LookupNodeByNodeIdOrError(uint32 nodeId)
{
	WorkerNode *node = LookupNodeByNodeId(nodeId);
	if (node == NULL)
	{
		ereport(ERROR, (errmsg("node %d could not be found", nodeId)));
	}
	return node;
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

	PrepareWorkerNodeCache();

	for (int workerNodeIndex = 0; workerNodeIndex < WorkerNodeCount; workerNodeIndex++)
	{
		WorkerNode *workerNode = WorkerNodeArray[workerNodeIndex];
		int32 workerNodeGroupId = workerNode->groupId;
		if (workerNodeGroupId != groupId)
		{
			continue;
		}

		foundAnyNodes = true;

		if (NodeIsReadable(workerNode))
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
			break;
		}

		case USE_SECONDARY_NODES_ALWAYS:
		{
			ereport(ERROR, (errmsg("node group %d does not have a secondary node",
								   groupId)));
			break;
		}

		default:
		{
			ereport(FATAL, (errmsg("unrecognized value for use_secondary_nodes")));
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
	List *placementList = NIL;

	bool missingOk = false;
	ShardIdCacheEntry *shardIdEntry = LookupShardIdCacheEntry(shardId, missingOk);
	CitusTableCacheEntry *tableEntry = shardIdEntry->tableEntry;
	int shardIndex = shardIdEntry->shardIndex;

	/* the offset better be in a valid range */
	Assert(shardIndex < tableEntry->shardIntervalArrayLength);

	GroupShardPlacement *placementArray =
		tableEntry->arrayOfPlacementArrays[shardIndex];
	int numberOfPlacements =
		tableEntry->arrayOfPlacementArrayLengths[shardIndex];

	for (int i = 0; i < numberOfPlacements; i++)
	{
		GroupShardPlacement *groupShardPlacement = &placementArray[i];
		ShardPlacement *shardPlacement = ResolveGroupShardPlacement(groupShardPlacement,
																	tableEntry,
																	shardIndex);

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
 * InitializeTableCacheEntry initializes a shard in cache.  A possible reason
 * for not finding an entry in the cache is that the distributed table's cache
 * entry hasn't been accessed yet. Thus look up the distributed table, and
 * build the cache entry. Afterwards we know that the shard has to be in the
 * cache if it exists. If the shard does *not* exist, this function errors
 * (because LookupShardRelationFromCatalog errors out).
 *
 * If missingOk is true and the shard cannot be found, the function returns false.
 */
static bool
InitializeTableCacheEntry(int64 shardId, bool missingOk)
{
	Oid relationId = LookupShardRelationFromCatalog(shardId, missingOk);

	if (!OidIsValid(relationId))
	{
		Assert(missingOk);
		return false;
	}

	/* trigger building the cache for the shard id */
	GetCitusTableCacheEntry(relationId); /* lgtm[cpp/return-value-ignored] */

	return true;
}


/*
 * RefreshTableCacheEntryIfInvalid checks if the cache entry is still valid and
 * refreshes it in cache when it's not. It returns true if it refreshed the
 * entry in the cache and false if it didn't.
 */
static bool
RefreshTableCacheEntryIfInvalid(ShardIdCacheEntry *shardEntry, bool missingOk)
{
	/*
	 * We might have some concurrent metadata changes. In order to get the changes,
	 * we first need to accept the cache invalidation messages.
	 */
	AcceptInvalidationMessages();
	if (shardEntry->tableEntry->isValid)
	{
		return false;
	}
	Oid oldRelationId = shardEntry->tableEntry->relationId;
	Oid currentRelationId = LookupShardRelationFromCatalog(shardEntry->shardId,
														   missingOk);

	/*
	 * The relation OID to which the shard belongs could have changed,
	 * most notably when the extension is dropped and a shard ID is
	 * reused. Reload the cache entries for both old and new relation
	 * ID and then look up the shard entry again.
	 */
	LookupCitusTableCacheEntry(oldRelationId);
	LookupCitusTableCacheEntry(currentRelationId);
	return true;
}


/*
 * LookupShardCacheEntry returns the cache entry belonging to a shard.
 * It errors out if that shard is unknown and missingOk is false. Else,
 * it will return a NULL cache entry.
 */
static ShardIdCacheEntry *
LookupShardIdCacheEntry(int64 shardId, bool missingOk)
{
	bool foundInCache = false;
	bool recheck = false;

	Assert(CitusHasBeenLoaded() && CheckCitusVersion(WARNING));

	InitializeCaches();

	ShardIdCacheEntry *shardEntry =
		hash_search(ShardIdCacheHash, &shardId, HASH_FIND, &foundInCache);

	if (!foundInCache)
	{
		if (!InitializeTableCacheEntry(shardId, missingOk))
		{
			return NULL;
		}

		recheck = true;
	}
	else
	{
		recheck = RefreshTableCacheEntryIfInvalid(shardEntry, missingOk);
	}

	/*
	 * If we (re-)loaded the table cache, re-search the shard cache - the
	 * shard index might have changed.  If we still can't find the entry, it
	 * can't exist.
	 */
	if (recheck)
	{
		shardEntry = hash_search(ShardIdCacheHash, &shardId, HASH_FIND, &foundInCache);

		if (!foundInCache)
		{
			int eflag = (missingOk) ? DEBUG1 : ERROR;
			ereport(eflag, (errmsg("could not find valid entry for shard "
								   UINT64_FORMAT, shardId)));
		}
	}

	return shardEntry;
}


/*
 * GetCitusTableCacheEntry looks up a pg_dist_partition entry for a
 * relation.
 *
 * Errors out if no relation matching the criteria could be found.
 */
CitusTableCacheEntry *
GetCitusTableCacheEntry(Oid distributedRelationId)
{
	CitusTableCacheEntry *cacheEntry =
		LookupCitusTableCacheEntry(distributedRelationId);

	if (cacheEntry)
	{
		return cacheEntry;
	}
	else
	{
		char *relationName = get_rel_name(distributedRelationId);

		if (relationName == NULL)
		{
			ereport(ERROR, (errmsg("relation with OID %u does not exist",
								   distributedRelationId)));
		}
		else
		{
			ereport(ERROR, (errmsg("relation %s is not distributed", relationName)));
		}
	}
}


/*
 * GetCitusTableCacheEntry returns the distributed table metadata for the
 * passed relationId. For efficiency it caches lookups. This function returns
 * NULL if the relation isn't a distributed table.
 */
CitusTableCacheEntry *
LookupCitusTableCacheEntry(Oid relationId)
{
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
		bool isCitusTable = IsCitusTableViaCatalog(relationId);
		int reportLevel = DEBUG1;

		/*
		 * If there's a version-mismatch, and we're dealing with a distributed
		 * table, we have to error out as we can't return a valid entry.  We
		 * want to check compatibility in the non-distributed case as well, so
		 * future lookups can use the cache if compatible.
		 */
		if (isCitusTable)
		{
			reportLevel = ERROR;
		}

		if (!CheckCitusVersion(reportLevel))
		{
			/* incompatible, can't access cache, so return before doing so */
			return NULL;
		}
	}

	/*
	 * We might have some concurrent metadata changes. In order to get the changes,
	 * we first need to accept the cache invalidation messages.
	 */
	AcceptInvalidationMessages();
	CitusTableCacheEntrySlot *cacheSlot =
		hash_search(DistTableCacheHash, hashKey, HASH_ENTER, &foundInCache);

	/* return valid matches */
	if (foundInCache)
	{
		if (cacheSlot->isValid)
		{
			return cacheSlot->citusTableMetadata;
		}
		else
		{
			/*
			 * An invalidation was received or we encountered an OOM while building
			 * the cache entry. We need to rebuild it.
			 */

			if (cacheSlot->citusTableMetadata)
			{
				/*
				 * The CitusTableCacheEntry might still be in use. We therefore do
				 * not reset it until the end of the transaction.
				 */
				MemoryContext oldContext =
					MemoryContextSwitchTo(MetadataCacheMemoryContext);

				DistTableCacheExpired = lappend(DistTableCacheExpired,
												cacheSlot->citusTableMetadata);

				MemoryContextSwitchTo(oldContext);
			}
		}
	}

	/* zero out entry, but not the key part */
	memset(((char *) cacheSlot) + sizeof(Oid), 0,
		   sizeof(CitusTableCacheEntrySlot) - sizeof(Oid));

	/*
	 * We disable interrupts while creating the cache entry because loading
	 * shard metadata can take a while, and if statement_timeout is too low,
	 * this will get canceled on each call and we won't be able to run any
	 * queries on the table.
	 */
	HOLD_INTERRUPTS();

	cacheSlot->citusTableMetadata = BuildCitusTableCacheEntry(relationId);

	/*
	 * Mark it as valid only after building the full entry, such that any
	 * error that happened during the build would trigger a rebuild.
	 */
	cacheSlot->isValid = true;

	RESUME_INTERRUPTS();

	return cacheSlot->citusTableMetadata;
}


/*
 * LookupDistObjectCacheEntry returns the distributed table metadata for the
 * passed relationId. For efficiency it caches lookups.
 */
DistObjectCacheEntry *
LookupDistObjectCacheEntry(Oid classid, Oid objid, int32 objsubid)
{
	bool foundInCache = false;
	DistObjectCacheEntryKey hashKey;
	ScanKeyData pgDistObjectKey[3];

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

	DistObjectCacheEntry *cacheEntry = hash_search(DistObjectCacheHash, &hashKey,
												   HASH_ENTER, &foundInCache);

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

	Relation pgDistObjectRel = table_open(DistObjectRelationId(), AccessShareLock);
	TupleDesc pgDistObjectTupleDesc = RelationGetDescr(pgDistObjectRel);

	ScanKeyInit(&pgDistObjectKey[0], Anum_pg_dist_object_classid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
	ScanKeyInit(&pgDistObjectKey[1], Anum_pg_dist_object_objid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
	ScanKeyInit(&pgDistObjectKey[2], Anum_pg_dist_object_objsubid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

	SysScanDesc pgDistObjectScan = systable_beginscan(pgDistObjectRel,
													  DistObjectPrimaryKeyIndexId(),
													  true, NULL, 3, pgDistObjectKey);
	HeapTuple pgDistObjectTup = systable_getnext(pgDistObjectScan);

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

		cacheEntry->forceDelegation =
			DatumGetBool(datumArray[Anum_pg_dist_object_force_delegation - 1]);
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
 * BuildCitusTableCacheEntry is a helper routine for
 * LookupCitusTableCacheEntry() for building the cache contents.
 * This function returns NULL if the relation isn't a distributed table.
 */
static CitusTableCacheEntry *
BuildCitusTableCacheEntry(Oid relationId)
{
	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);
	HeapTuple distPartitionTuple =
		LookupDistPartitionTuple(pgDistPartition, relationId);

	if (distPartitionTuple == NULL)
	{
		/* not a distributed table, done */
		table_close(pgDistPartition, NoLock);
		return NULL;
	}

	MemoryContext oldContext = NULL;
	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(distPartitionTuple, tupleDescriptor, datumArray, isNullArray);

	CitusTableCacheEntry *cacheEntry =
		MemoryContextAllocZero(MetadataCacheMemoryContext, sizeof(CitusTableCacheEntry));

	cacheEntry->relationId = relationId;

	cacheEntry->partitionMethod = datumArray[Anum_pg_dist_partition_partmethod - 1];
	Datum partitionKeyDatum = datumArray[Anum_pg_dist_partition_partkey - 1];
	bool partitionKeyIsNull = isNullArray[Anum_pg_dist_partition_partkey - 1];

	/* note that for reference tables partitionKeyisNull is true */
	if (!partitionKeyIsNull)
	{
		oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

		/* get the string representation of the partition column Var */
		cacheEntry->partitionKeyString = TextDatumGetCString(partitionKeyDatum);

		/* convert the string to a Node and ensure it is a Var */
		Node *partitionNode = stringToNode(cacheEntry->partitionKeyString);
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

	Datum replicationModelDatum = datumArray[Anum_pg_dist_partition_repmodel - 1];
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

	if (isNullArray[Anum_pg_dist_partition_autoconverted - 1])
	{
		/*
		 * We don't expect this to happen, but set it to false (the default value)
		 * to not break if anything goes wrong.
		 */
		cacheEntry->autoConverted = false;
	}
	else
	{
		cacheEntry->autoConverted = DatumGetBool(
			datumArray[Anum_pg_dist_partition_autoconverted - 1]);
	}

	heap_freetuple(distPartitionTuple);

	BuildCachedShardList(cacheEntry);

	/* we only need hash functions for hash distributed tables */
	if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
	{
		Var *partitionColumn = cacheEntry->partitionColumn;

		TypeCacheEntry *typeEntry = lookup_type_cache(partitionColumn->vartype,
													  TYPECACHE_HASH_PROC_FINFO);

		FmgrInfo *hashFunction = MemoryContextAllocZero(MetadataCacheMemoryContext,
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

	table_close(pgDistPartition, NoLock);

	cacheEntry->isValid = true;

	return cacheEntry;
}


/*
 * BuildCachedShardList() is a helper routine for BuildCitusTableCacheEntry()
 * building up the list of shards in a distributed relation.
 */
static void
BuildCachedShardList(CitusTableCacheEntry *cacheEntry)
{
	ShardInterval **shardIntervalArray = NULL;
	ShardInterval **sortedShardIntervalArray = NULL;
	FmgrInfo *shardIntervalCompareFunction = NULL;
	FmgrInfo *shardColumnCompareFunction = NULL;
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

	List *distShardTupleList = LookupDistShardTuples(cacheEntry->relationId);
	int shardIntervalArrayLength = list_length(distShardTupleList);
	if (shardIntervalArrayLength > 0)
	{
		Relation distShardRelation = table_open(DistShardRelationId(), AccessShareLock);
		TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);
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

		HeapTuple shardTuple = NULL;
		foreach_declared_ptr(shardTuple, distShardTupleList)
		{
			ShardInterval *shardInterval = TupleToShardInterval(shardTuple,
																distShardTupleDesc,
																intervalTypeId,
																intervalTypeMod);
			MemoryContext oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);

			shardIntervalArray[arrayIndex] = CopyShardInterval(shardInterval);

			MemoryContextSwitchTo(oldContext);

			heap_freetuple(shardTuple);

			arrayIndex++;
		}

		table_close(distShardRelation, AccessShareLock);
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
														  cacheEntry->partitionColumn->
														  varcollid,
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
											cacheEntry->partitionColumn->varcollid,
											shardIntervalCompareFunction);
		}
		else
		{
			cacheEntry->hasOverlappingShardInterval = true;
		}

		ErrorIfInconsistentShardIntervals(cacheEntry);
	}

	cacheEntry->sortedShardIntervalArray = sortedShardIntervalArray;
	cacheEntry->shardIntervalArrayLength = 0;

	/* maintain shardId->(table,ShardInterval) cache */
	for (int shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *shardInterval = sortedShardIntervalArray[shardIndex];
		int64 shardId = shardInterval->shardId;
		int placementOffset = 0;

		/*
		 * Enable quick lookups of this shard ID by adding it to ShardIdCacheHash
		 * or overwriting the previous values.
		 */
		ShardIdCacheEntry *shardIdCacheEntry =
			hash_search(ShardIdCacheHash, &shardId, HASH_ENTER, NULL);

		shardIdCacheEntry->tableEntry = cacheEntry;
		shardIdCacheEntry->shardIndex = shardIndex;

		/*
		 * We should increment this only after we are sure this hasn't already
		 * been assigned to any other relations. ResetCitusTableCacheEntry()
		 * depends on this.
		 */
		cacheEntry->shardIntervalArrayLength++;

		/* build list of shard placements */
		List *placementList = BuildShardPlacementList(shardId);
		int numberOfPlacements = list_length(placementList);

		/* and copy that list into the cache entry */
		MemoryContext oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);
		GroupShardPlacement *placementArray = palloc0(numberOfPlacements *
													  sizeof(GroupShardPlacement));
		GroupShardPlacement *srcPlacement = NULL;
		foreach_declared_ptr(srcPlacement, placementList)
		{
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
 * ErrorIfInconsistentShardIntervals checks if shard intervals are consistent with
 * our expectations.
 */
void
ErrorIfInconsistentShardIntervals(CitusTableCacheEntry *cacheEntry)
{
	/*
	 * If table is hash-partitioned and has shards, there never should be any
	 * uninitalized shards.  Historically we've not prevented that for range
	 * partitioned tables, but it might be a good idea to start doing so.
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
 * HasUniformHashDistribution determines whether the given list of sorted shards
 * has a uniform hash distribution, as produced by master_create_worker_shards for
 * hash partitioned tables.
 */
bool
HasUniformHashDistribution(ShardInterval **shardIntervalArray,
						   int shardIntervalArrayLength)
{
	/* if there are no shards, there is no uniform distribution */
	if (shardIntervalArrayLength == 0)
	{
		return false;
	}

	/* calculate the hash token increment */
	uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardIntervalArrayLength;

	for (int shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *shardInterval = shardIntervalArray[shardIndex];
		int32 shardMinHashToken = PG_INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);

		if (shardIndex == (shardIntervalArrayLength - 1))
		{
			shardMaxHashToken = PG_INT32_MAX;
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
bool
HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray, int shardCount)
{
	bool hasUninitializedShardInterval = false;

	if (shardCount == 0)
	{
		return hasUninitializedShardInterval;
	}

	Assert(sortedShardIntervalArray != NULL);

	/*
	 * Since the shard interval array is sorted, and uninitialized ones stored
	 * in the end of the array, checking the last element is enough.
	 */
	ShardInterval *lastShardInterval = sortedShardIntervalArray[shardCount - 1];
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
bool
HasOverlappingShardInterval(ShardInterval **shardIntervalArray,
							int shardIntervalArrayLength,
							Oid shardIntervalCollation,
							FmgrInfo *shardIntervalSortCompareFunction)
{
	Datum comparisonDatum = 0;
	int comparisonResult = 0;

	/* zero/a single shard can't overlap */
	if (shardIntervalArrayLength < 2)
	{
		return false;
	}

	ShardInterval *lastShardInterval = shardIntervalArray[0];
	for (int shardIndex = 1; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *curShardInterval = shardIntervalArray[shardIndex];

		/* only called if !hasUninitializedShardInterval */
		Assert(lastShardInterval->minValueExists && lastShardInterval->maxValueExists);
		Assert(curShardInterval->minValueExists && curShardInterval->maxValueExists);

		comparisonDatum = FunctionCall2Coll(shardIntervalSortCompareFunction,
											shardIntervalCollation,
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
	/*
	 * We do not use Citus hooks during CREATE/ALTER EXTENSION citus
	 * since the objects used by the C code might be not be there yet.
	 */
	if (creating_extension)
	{
		Oid citusExtensionOid = get_extension_oid("citus", true);

		if (CurrentExtensionObject == citusExtensionOid)
		{
			return false;
		}
	}

	/*
	 * If extensionCreatedState is UNKNOWN, query pg_extension for Citus
	 * and cache the result. Otherwise return the value extensionCreatedState
	 * indicates.
	 */
	if (MetadataCache.extensionCreatedState == UNKNOWN)
	{
		bool extensionCreated = CitusHasBeenLoadedInternal();

		if (extensionCreated)
		{
			/*
			 * Loaded Citus for the first time in this session, or first time after
			 * CREATE/ALTER EXTENSION citus. Do some initialisation.
			 */

			/*
			 * Make sure the maintenance daemon is running if it was not already.
			 */
			StartupCitusBackend();

			/*
			 * This needs to be initialized so we can receive foreign relation graph
			 * invalidation messages in InvalidateForeignRelationGraphCacheCallback().
			 * See the comments of InvalidateForeignKeyGraph for more context.
			 */
			DistColocationRelationId();

			MetadataCache.extensionCreatedState = CREATED;
		}
		else
		{
			MetadataCache.extensionCreatedState = NOTCREATED;
		}
	}

	return (MetadataCache.extensionCreatedState == CREATED) ? true : false;
}


/*
 * CitusHasBeenLoadedInternal returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false.
 */
static bool
CitusHasBeenLoadedInternal(void)
{
	if (IsBinaryUpgrade)
	{
		/* never use Citus logic during pg_upgrade */
		return false;
	}

	Oid citusExtensionOid = get_extension_oid("citus", true);
	if (citusExtensionOid == InvalidOid)
	{
		/* Citus extension does not exist yet */
		return false;
	}

	/* citus extension exists and has been created */
	return true;
}


/*
 * GetCitusCreationLevel returns the level of the transaction creating citus
 */
int
GetCitusCreationLevel(void)
{
	return CreateCitusTransactionLevel;
}


/*
 * Sets the value of CreateCitusTransactionLevel based on int received which represents the
 * nesting level of the transaction that created the Citus extension
 */
void
SetCreateCitusTransactionLevel(int val)
{
	CreateCitusTransactionLevel = val;
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
	if (!EnableVersionChecks)
	{
		return true;
	}

	char *availableVersion = AvailableExtensionVersion();

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
 * CheckInstalledVersion compares CITUS_EXTENSIONVERSION and the
 * extension's current version from the pg_extension catalog table. If they
 * are not compatible, this function logs an error with the specified elevel,
 * otherwise it returns true.
 */
static bool
CheckInstalledVersion(int elevel)
{
	Assert(CitusHasBeenLoaded());
	Assert(EnableVersionChecks);

	char *installedVersion = InstalledExtensionVersion();

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
 * InstalledAndAvailableVersionsSame compares extension's available version and
 * its current version from the pg_extension catalog table. If they are not same
 * returns false, otherwise returns true.
 */
bool
InstalledAndAvailableVersionsSame()
{
	char *installedVersion = InstalledExtensionVersion();
	char *availableVersion = AvailableExtensionVersion();

	if (strncmp(installedVersion, availableVersion, NAMEDATALEN) == 0)
	{
		return true;
	}

	return false;
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
	LOCAL_FCINFO(fcinfo, 0);
	FmgrInfo flinfo;

	bool goForward = true;
	bool doCopy = false;
	char *availableExtensionVersion;

	InitializeCaches();

	EState *estate = CreateExecutorState();
	ReturnSetInfo *extensionsResultSet = makeNode(ReturnSetInfo);
	extensionsResultSet->econtext = GetPerTupleExprContext(estate);
	extensionsResultSet->allowedModes = SFRM_Materialize;

	fmgr_info(F_PG_AVAILABLE_EXTENSIONS, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 0, InvalidOid, NULL,
							 (Node *) extensionsResultSet);

	/* pg_available_extensions returns result set containing all available extensions */
	(*pg_available_extensions)(fcinfo);

	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(
		extensionsResultSet->setDesc,
		&TTSOpsMinimalTuple);
	bool hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
											doCopy,
											tupleTableSlot);
	while (hasTuple)
	{
		bool isNull = false;

		Datum extensionNameDatum = slot_getattr(tupleTableSlot, 1, &isNull);
		char *extensionName = NameStr(*DatumGetName(extensionNameDatum));
		if (strcmp(extensionName, "citus") == 0)
		{
			Datum availableVersion = slot_getattr(tupleTableSlot, 2, &isNull);

			/* we will cache the result of citus version to prevent catalog access */
			MemoryContext oldMemoryContext = MemoryContextSwitchTo(
				MetadataCacheMemoryContext);

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

	return NULL; /* keep compiler happy */
}


/*
 * InstalledExtensionVersion returns the Citus version in PostgreSQL pg_extension table.
 */
static char *
InstalledExtensionVersion(void)
{
	ScanKeyData entry[1];
	char *installedExtensionVersion = NULL;

	InitializeCaches();

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
											  NULL, 1, entry);

	HeapTuple extensionTuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(extensionTuple))
	{
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
		MemoryContext oldMemoryContext = MemoryContextSwitchTo(
			MetadataCacheMemoryContext);

		installedExtensionVersion = text_to_cstring(DatumGetTextPP(installedVersion));

		MemoryContextSwitchTo(oldMemoryContext);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension is not loaded")));
	}

	systable_endscan(scandesc);

	table_close(relation, AccessShareLock);

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


Oid
DistBackgroundJobRelationId(void)
{
	CachedRelationLookup("pg_dist_background_job",
						 &MetadataCache.distBackgroundJobRelationId);

	return MetadataCache.distBackgroundJobRelationId;
}


Oid
DistBackgroundJobPKeyIndexId(void)
{
	CachedRelationLookup("pg_dist_background_job_pkey",
						 &MetadataCache.distBackgroundJobPKeyIndexId);

	return MetadataCache.distBackgroundJobPKeyIndexId;
}


Oid
DistBackgroundJobJobIdSequenceId(void)
{
	CachedRelationLookup("pg_dist_background_job_job_id_seq",
						 &MetadataCache.distBackgroundJobJobIdSequenceId);

	return MetadataCache.distBackgroundJobJobIdSequenceId;
}


Oid
DistBackgroundTaskRelationId(void)
{
	CachedRelationLookup("pg_dist_background_task",
						 &MetadataCache.distBackgroundTaskRelationId);

	return MetadataCache.distBackgroundTaskRelationId;
}


Oid
DistBackgroundTaskPKeyIndexId(void)
{
	CachedRelationLookup("pg_dist_background_task_pkey",
						 &MetadataCache.distBackgroundTaskPKeyIndexId);

	return MetadataCache.distBackgroundTaskPKeyIndexId;
}


Oid
DistBackgroundTaskJobIdTaskIdIndexId(void)
{
	CachedRelationLookup("pg_dist_background_task_job_id_task_id",
						 &MetadataCache.distBackgroundTaskJobIdTaskIdIndexId);

	return MetadataCache.distBackgroundTaskJobIdTaskIdIndexId;
}


Oid
DistBackgroundTaskStatusTaskIdIndexId(void)
{
	CachedRelationLookup("pg_dist_background_task_status_task_id_index",
						 &MetadataCache.distBackgroundTaskStatusTaskIdIndexId);

	return MetadataCache.distBackgroundTaskStatusTaskIdIndexId;
}


Oid
DistBackgroundTaskTaskIdSequenceId(void)
{
	CachedRelationLookup("pg_dist_background_task_task_id_seq",
						 &MetadataCache.distBackgroundTaskTaskIdSequenceId);

	return MetadataCache.distBackgroundTaskTaskIdSequenceId;
}


Oid
DistClockLogicalSequenceId(void)
{
	CachedRelationLookup("pg_dist_clock_logical_seq",
						 &MetadataCache.distClockLogicalSequenceId);

	return MetadataCache.distClockLogicalSequenceId;
}


Oid
DistBackgroundTaskDependRelationId(void)
{
	CachedRelationLookup("pg_dist_background_task_depend",
						 &MetadataCache.distBackgroundTaskDependRelationId);

	return MetadataCache.distBackgroundTaskDependRelationId;
}


Oid
DistBackgroundTaskDependTaskIdIndexId(void)
{
	CachedRelationLookup("pg_dist_background_task_depend_task_id",
						 &MetadataCache.distBackgroundTaskDependTaskIdIndexId);

	return MetadataCache.distBackgroundTaskDependTaskIdIndexId;
}


Oid
DistBackgroundTaskDependDependsOnIndexId(void)
{
	CachedRelationLookup("pg_dist_background_task_depend_depends_on",
						 &MetadataCache.distBackgroundTaskDependDependsOnIndexId);

	return MetadataCache.distBackgroundTaskDependDependsOnIndexId;
}


/* return oid of pg_dist_rebalance_strategy relation */
Oid
DistRebalanceStrategyRelationId(void)
{
	CachedRelationLookup("pg_dist_rebalance_strategy",
						 &MetadataCache.distRebalanceStrategyRelationId);

	return MetadataCache.distRebalanceStrategyRelationId;
}


/* return the oid of citus namespace */
Oid
CitusCatalogNamespaceId(void)
{
	CachedNamespaceLookup("citus", &MetadataCache.citusCatalogNamespaceId);
	return MetadataCache.citusCatalogNamespaceId;
}


/* return oid of pg_dist_object relation */
Oid
DistObjectRelationId(void)
{
	/*
	 * In older versions pg_dist_object was living in the `citus` namespace, With Citus 11
	 * this has been moved to pg_dist_catalog.
	 *
	 * During upgrades it could therefore be that we simply need to look in the old
	 * catalog. Since we expect to find it most of the time in the pg_catalog schema from
	 * now on we will start there.
	 *
	 * even after the table has been moved, the oid's stay the same, so we don't have to
	 * invalidate the cache after a move
	 *
	 * Note: during testing we also up/downgrade the extension, and sometimes interact
	 * with the database when the schema and the binary are not in sync. Hance we always
	 * allow the catalog to be missing on our first lookup. The error message might
	 * therefore become misleading as it will complain about citus.pg_dist_object not
	 * being found when called too early.
	 */
	CachedRelationLookupExtended("pg_dist_object",
								 &MetadataCache.distObjectRelationId,
								 true);
	if (!OidIsValid(MetadataCache.distObjectRelationId))
	{
		/*
		 * We can only ever reach here while we are creating/altering our extension before
		 * the table is moved to pg_catalog.
		 */
		CachedRelationNamespaceLookupExtended("pg_dist_object",
											  CitusCatalogNamespaceId(),
											  &MetadataCache.distObjectRelationId,
											  false);
	}

	return MetadataCache.distObjectRelationId;
}


/* return oid of pg_dist_object_pkey */
Oid
DistObjectPrimaryKeyIndexId(void)
{
	/*
	 * In older versions pg_dist_object was living in the `citus` namespace, With Citus 11
	 * this has been moved to pg_dist_catalog.
	 *
	 * During upgrades it could therefore be that we simply need to look in the old
	 * catalog. Since we expect to find it most of the time in the pg_catalog schema from
	 * now on we will start there.
	 *
	 * even after the table has been moved, the oid's stay the same, so we don't have to
	 * invalidate the cache after a move
	 *
	 * Note: during testing we also up/downgrade the extension, and sometimes interact
	 * with the database when the schema and the binary are not in sync. Hance we always
	 * allow the catalog to be missing on our first lookup. The error message might
	 * therefore become misleading as it will complain about citus.pg_dist_object not
	 * being found when called too early.
	 */
	CachedRelationLookupExtended("pg_dist_object_pkey",
								 &MetadataCache.distObjectPrimaryKeyIndexId,
								 true);

	if (!OidIsValid(MetadataCache.distObjectPrimaryKeyIndexId))
	{
		/*
		 * We can only ever reach here while we are creating/altering our extension before
		 * the table is moved to pg_catalog.
		 */
		CachedRelationNamespaceLookupExtended("pg_dist_object_pkey",
											  CitusCatalogNamespaceId(),
											  &MetadataCache.distObjectPrimaryKeyIndexId,
											  false);
	}

	return MetadataCache.distObjectPrimaryKeyIndexId;
}


/* return oid of pg_dist_cleanup relation */
Oid
DistCleanupRelationId(void)
{
	CachedRelationLookup("pg_dist_cleanup",
						 &MetadataCache.distCleanupRelationId);

	return MetadataCache.distCleanupRelationId;
}


/* return oid of pg_dist_cleanup primary key index */
Oid
DistCleanupPrimaryKeyIndexId(void)
{
	CachedRelationLookup("pg_dist_cleanup_pkey",
						 &MetadataCache.distCleanupPrimaryKeyIndexId);

	return MetadataCache.distCleanupPrimaryKeyIndexId;
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


/* return oid of pg_dist_schema relation */
Oid
DistTenantSchemaRelationId(void)
{
	CachedRelationLookup("pg_dist_schema",
						 &MetadataCache.distTenantSchemaRelationId);

	return MetadataCache.distTenantSchemaRelationId;
}


/* return oid of pg_dist_schema_pkey index */
Oid
DistTenantSchemaPrimaryKeyIndexId(void)
{
	CachedRelationLookup("pg_dist_schema_pkey",
						 &MetadataCache.distTenantSchemaPrimaryKeyIndexId);

	return MetadataCache.distTenantSchemaPrimaryKeyIndexId;
}


/* return oid of pg_dist_schema_unique_colocationid_index index */
Oid
DistTenantSchemaUniqueColocationIdIndexId(void)
{
	CachedRelationLookup("pg_dist_schema_unique_colocationid_index",
						 &MetadataCache.distTenantSchemaUniqueColocationIdIndexId);

	return MetadataCache.distTenantSchemaUniqueColocationIdIndexId;
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


/* return oid of pg_dist_colocation_pkey */
Oid
DistColocationIndexId(void)
{
	CachedRelationLookup("pg_dist_colocation_pkey",
						 &MetadataCache.distColocationidIndexId);

	return MetadataCache.distColocationidIndexId;
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


/* return oid of pg_dist_placement_groupid_index */
Oid
DistPlacementGroupidIndexId(void)
{
	CachedRelationLookup("pg_dist_placement_groupid_index",
						 &MetadataCache.distPlacementGroupidIndexId);

	return MetadataCache.distPlacementGroupidIndexId;
}


/* return oid of pg_dist_authinfo relation */
static Oid
DistAuthinfoRelationId(void)
{
	CachedRelationLookup("pg_dist_authinfo",
						 &MetadataCache.distAuthinfoRelationId);

	return MetadataCache.distAuthinfoRelationId;
}


/* return oid of pg_dist_authinfo identification index */
static Oid
DistAuthinfoIndexId(void)
{
	CachedRelationLookup("pg_dist_authinfo_identification_index",
						 &MetadataCache.distAuthinfoIndexId);

	return MetadataCache.distAuthinfoIndexId;
}


/* return oid of pg_dist_poolinfo relation */
static Oid
DistPoolinfoRelationId(void)
{
	CachedRelationLookup("pg_dist_poolinfo",
						 &MetadataCache.distPoolinfoRelationId);

	return MetadataCache.distPoolinfoRelationId;
}


/* return oid of pg_dist_poolinfo primary key index */
static Oid
DistPoolinfoIndexId(void)
{
	CachedRelationLookup("pg_dist_poolinfo_pkey",
						 &MetadataCache.distPoolinfoIndexId);

	return MetadataCache.distPoolinfoIndexId;
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


/* return oid of the read_intermediate_results(text[],citus_copy_format) function */
Oid
CitusReadIntermediateResultArrayFuncId(void)
{
	if (MetadataCache.readIntermediateResultArrayFuncId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("read_intermediate_results"));
		Oid copyFormatTypeOid = CitusCopyFormatTypeId();
		Oid paramOids[2] = { TEXTARRAYOID, copyFormatTypeOid };
		bool missingOK = false;

		MetadataCache.readIntermediateResultArrayFuncId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return MetadataCache.readIntermediateResultArrayFuncId;
}


/* return oid of the citus.copy_format enum type */
Oid
CitusCopyFormatTypeId(void)
{
	if (MetadataCache.copyFormatTypeId == InvalidOid)
	{
		char *typeName = "citus_copy_format";
		MetadataCache.copyFormatTypeId = GetSysCacheOid2(TYPENAMENSP,
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


/* return oid of the textout(text) function */
Oid
TextOutFunctionId(void)
{
	if (MetadataCache.textoutFunctionId == InvalidOid)
	{
		List *nameList = list_make2(makeString("pg_catalog"),
									makeString("textout"));
		Oid paramOids[1] = { TEXTOID };

		MetadataCache.textoutFunctionId =
			LookupFuncName(nameList, 1, paramOids, false);
	}

	return MetadataCache.textoutFunctionId;
}


/*
 * RelationIsAKnownShardFuncId returns oid of the relation_is_a_known_shard function.
 */
Oid
RelationIsAKnownShardFuncId(void)
{
	if (MetadataCache.relationIsAKnownShardFuncId == InvalidOid)
	{
		const int argCount = 1;

		MetadataCache.relationIsAKnownShardFuncId =
			FunctionOid("pg_catalog", "relation_is_a_known_shard", argCount);
	}

	return MetadataCache.relationIsAKnownShardFuncId;
}


/*
 * JsonbExtractPathFuncId returns oid of the jsonb_extract_path function.
 */
Oid
JsonbExtractPathFuncId(void)
{
	if (MetadataCache.jsonbExtractPathFuncId == InvalidOid)
	{
		const int argCount = 2;

		MetadataCache.jsonbExtractPathFuncId =
			FunctionOid("pg_catalog", "jsonb_extract_path", argCount);
	}

	return MetadataCache.jsonbExtractPathFuncId;
}


/*
 * JsonbExtractPathTextFuncId returns oid of the jsonb_extract_path_text function.
 */
Oid
JsonbExtractPathTextFuncId(void)
{
	if (MetadataCache.jsonbExtractPathTextFuncId == InvalidOid)
	{
		const int argCount = 2;

		MetadataCache.jsonbExtractPathTextFuncId =
			FunctionOid("pg_catalog", "jsonb_extract_path_text", argCount);
	}

	return MetadataCache.jsonbExtractPathTextFuncId;
}


/*
 * CitusDependentObjectFuncId returns oid of the is_citus_depended_object function.
 */
Oid
CitusDependentObjectFuncId(void)
{
	if (!HideCitusDependentObjects)
	{
		ereport(ERROR, (errmsg(
							"is_citus_depended_object can only be used while running the regression tests")));
	}

	if (MetadataCache.CitusDependentObjectFuncId == InvalidOid)
	{
		const int argCount = 2;

		MetadataCache.CitusDependentObjectFuncId =
			FunctionOid("pg_catalog", "is_citus_depended_object", argCount);
	}

	return MetadataCache.CitusDependentObjectFuncId;
}


/*
 * CurrentDatabaseName gets the name of the current database and caches
 * the result.
 *
 * Given that the database name cannot be changed when there is at least
 * one session connected to it, we do not need to implement any invalidation
 * mechanism.
 */
const char *
CurrentDatabaseName(void)
{
	if (!MetadataCache.databaseNameValid)
	{
		char *databaseName = get_database_name(MyDatabaseId);
		if (databaseName == NULL)
		{
			ereport(ERROR, (errmsg("database that is connected to does not exist")));
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
	ScanKeyData entry[1];
	Form_pg_extension extensionForm = NULL;

	if (MetadataCache.extensionOwner != InvalidOid)
	{
		return MetadataCache.extensionOwner;
	}

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
											  NULL, 1, entry);

	HeapTuple extensionTuple = systable_getnext(scandesc);

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

	table_close(relation, AccessShareLock);

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
 * LookupTypeOid returns the Oid of the "{schemaNameSting}.{typeNameString}" type, or
 * InvalidOid if it does not exist.
 */
Oid
LookupTypeOid(char *schemaNameSting, char *typeNameString)
{
	String *schemaName = makeString(schemaNameSting);
	String *typeName = makeString(typeNameString);
	List *qualifiedName = list_make2(schemaName, typeName);
	TypeName *enumTypeName = makeTypeNameFromNameList(qualifiedName);


	/* typenameTypeId but instead of raising an error return InvalidOid */
	Type tup = LookupTypeName(NULL, enumTypeName, NULL, false);
	if (tup == NULL)
	{
		return InvalidOid;
	}

	Oid nodeRoleTypId = ((Form_pg_type) GETSTRUCT(tup))->oid;
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
	Oid enumTypeId = LookupTypeOid("pg_catalog", enumName);

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


Oid
CitusJobStatusScheduledId(void)
{
	if (!MetadataCache.citusJobStatusScheduledId)
	{
		MetadataCache.citusJobStatusScheduledId =
			LookupStringEnumValueId("citus_job_status", "scheduled");
	}

	return MetadataCache.citusJobStatusScheduledId;
}


Oid
CitusJobStatusRunningId(void)
{
	if (!MetadataCache.citusJobStatusRunningId)
	{
		MetadataCache.citusJobStatusRunningId =
			LookupStringEnumValueId("citus_job_status", "running");
	}

	return MetadataCache.citusJobStatusRunningId;
}


Oid
CitusJobStatusCancellingId(void)
{
	if (!MetadataCache.citusJobStatusCancellingId)
	{
		MetadataCache.citusJobStatusCancellingId =
			LookupStringEnumValueId("citus_job_status", "cancelling");
	}

	return MetadataCache.citusJobStatusCancellingId;
}


Oid
CitusJobStatusFinishedId(void)
{
	if (!MetadataCache.citusJobStatusFinishedId)
	{
		MetadataCache.citusJobStatusFinishedId =
			LookupStringEnumValueId("citus_job_status", "finished");
	}

	return MetadataCache.citusJobStatusFinishedId;
}


Oid
CitusJobStatusCancelledId(void)
{
	if (!MetadataCache.citusJobStatusCancelledId)
	{
		MetadataCache.citusJobStatusCancelledId =
			LookupStringEnumValueId("citus_job_status", "cancelled");
	}

	return MetadataCache.citusJobStatusCancelledId;
}


Oid
CitusJobStatusFailedId(void)
{
	if (!MetadataCache.citusJobStatusFailedId)
	{
		MetadataCache.citusJobStatusFailedId =
			LookupStringEnumValueId("citus_job_status", "failed");
	}

	return MetadataCache.citusJobStatusFailedId;
}


Oid
CitusJobStatusFailingId(void)
{
	if (!MetadataCache.citusJobStatusFailingId)
	{
		MetadataCache.citusJobStatusFailingId =
			LookupStringEnumValueId("citus_job_status", "failing");
	}

	return MetadataCache.citusJobStatusFailingId;
}


Oid
CitusTaskStatusBlockedId(void)
{
	if (!MetadataCache.citusTaskStatusBlockedId)
	{
		MetadataCache.citusTaskStatusBlockedId =
			LookupStringEnumValueId("citus_task_status", "blocked");
	}

	return MetadataCache.citusTaskStatusBlockedId;
}


Oid
CitusTaskStatusCancelledId(void)
{
	if (!MetadataCache.citusTaskStatusCancelledId)
	{
		MetadataCache.citusTaskStatusCancelledId =
			LookupStringEnumValueId("citus_task_status", "cancelled");
	}

	return MetadataCache.citusTaskStatusCancelledId;
}


Oid
CitusTaskStatusCancellingId(void)
{
	if (!MetadataCache.citusTaskStatusCancellingId)
	{
		MetadataCache.citusTaskStatusCancellingId =
			LookupStringEnumValueId("citus_task_status", "cancelling");
	}

	return MetadataCache.citusTaskStatusCancellingId;
}


Oid
CitusTaskStatusRunnableId(void)
{
	if (!MetadataCache.citusTaskStatusRunnableId)
	{
		MetadataCache.citusTaskStatusRunnableId =
			LookupStringEnumValueId("citus_task_status", "runnable");
	}

	return MetadataCache.citusTaskStatusRunnableId;
}


Oid
CitusTaskStatusRunningId(void)
{
	if (!MetadataCache.citusTaskStatusRunningId)
	{
		MetadataCache.citusTaskStatusRunningId =
			LookupStringEnumValueId("citus_task_status", "running");
	}

	return MetadataCache.citusTaskStatusRunningId;
}


Oid
CitusTaskStatusDoneId(void)
{
	if (!MetadataCache.citusTaskStatusDoneId)
	{
		MetadataCache.citusTaskStatusDoneId =
			LookupStringEnumValueId("citus_task_status", "done");
	}

	return MetadataCache.citusTaskStatusDoneId;
}


Oid
CitusTaskStatusErrorId(void)
{
	if (!MetadataCache.citusTaskStatusErrorId)
	{
		MetadataCache.citusTaskStatusErrorId =
			LookupStringEnumValueId("citus_task_status", "error");
	}

	return MetadataCache.citusTaskStatusErrorId;
}


Oid
CitusTaskStatusUnscheduledId(void)
{
	if (!MetadataCache.citusTaskStatusUnscheduledId)
	{
		MetadataCache.citusTaskStatusUnscheduledId =
			LookupStringEnumValueId("citus_task_status", "unscheduled");
	}

	return MetadataCache.citusTaskStatusUnscheduledId;
}


/*
 * citus_dist_partition_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_partition are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_partition_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Oid oldLogicalRelationId = InvalidOid;
	Oid newLogicalRelationId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	if (RelationGetRelid(triggerData->tg_relation) != DistPartitionRelationId())
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("triggered on incorrect relation")));
	}

	HeapTuple newTuple = triggerData->tg_newtuple;
	HeapTuple oldTuple = triggerData->tg_trigtuple;

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
 * master_dist_partition_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_partition_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_partition_cache_invalidate(fcinfo);
}


/*
 * citus_dist_shard_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_shard are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_shard_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Oid oldLogicalRelationId = InvalidOid;
	Oid newLogicalRelationId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	if (RelationGetRelid(triggerData->tg_relation) != DistShardRelationId())
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("triggered on incorrect relation")));
	}

	HeapTuple newTuple = triggerData->tg_newtuple;
	HeapTuple oldTuple = triggerData->tg_trigtuple;

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
 * master_dist_shard_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_shard_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_shard_cache_invalidate(fcinfo);
}


/*
 * citus_dist_placement_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_placement are
 * changed on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_placement_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Oid oldShardId = InvalidOid;
	Oid newShardId = InvalidOid;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	/*
	 * Before 7.0-2 this trigger is on pg_dist_shard_placement,
	 * ignore trigger in this scenario.
	 */
	Oid pgDistShardPlacementId = get_relname_relid("pg_dist_shard_placement",
												   PG_CATALOG_NAMESPACE);
	if (RelationGetRelid(triggerData->tg_relation) == pgDistShardPlacementId)
	{
		PG_RETURN_DATUM(PointerGetDatum(NULL));
	}

	if (RelationGetRelid(triggerData->tg_relation) != DistPlacementRelationId())
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("triggered on incorrect relation")));
	}

	HeapTuple newTuple = triggerData->tg_newtuple;
	HeapTuple oldTuple = triggerData->tg_trigtuple;

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
 * master_dist_placement_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_placement_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_placement_cache_invalidate(fcinfo);
}


/*
 * citus_dist_node_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_node are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_node_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_node_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_node_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_node_cache_invalidate(fcinfo);
}


/*
 * citus_conninfo_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_authinfo are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_conninfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CitusInvalidateRelcacheByRelid(DistAuthinfoRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_authinfo_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_authinfo_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_conninfo_cache_invalidate(fcinfo);
}


/*
 * citus_dist_local_group_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_dist_local_group are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_local_group_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CitusInvalidateRelcacheByRelid(DistLocalGroupIdRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_local_group_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_local_group_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_local_group_cache_invalidate(fcinfo);
}


/*
 * citus_dist_object_cache_invalidate is a trigger function that performs relcache
 * invalidation when the contents of pg_dist_object are changed on the SQL
 * level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
citus_dist_object_cache_invalidate(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	CitusInvalidateRelcacheByRelid(DistObjectRelationId());

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * master_dist_object_cache_invalidate is a wrapper function for old UDF name.
 */
Datum
master_dist_object_cache_invalidate(PG_FUNCTION_ARGS)
{
	return citus_dist_object_cache_invalidate(fcinfo);
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
			RegisterAuthinfoCacheCallbacks();
			RegisterCitusTableCacheEntryReleaseCallbacks();
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
			DistTableCacheExpired = NIL;
			ShardIdCacheHash = NULL;

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}


/* initialize the infrastructure for the metadata cache */
static void
InitializeDistCache(void)
{
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
	CreateShardIdCache();

	InitializeDistObjectCache();
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
	HASHCTL info;
	long maxTableSize = (long) MaxWorkerNodesTracked;
	bool includeNodesFromOtherClusters = false;
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
	int hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE;

	HTAB *newWorkerNodeHash = hash_create("Worker Node Hash", maxTableSize, &info,
										  hashFlags);

	/* read the list from pg_dist_node */
	List *workerNodeList = ReadDistNode(includeNodesFromOtherClusters);

	int newWorkerNodeCount = list_length(workerNodeList);
	WorkerNode **newWorkerNodeArray = MemoryContextAlloc(MetadataCacheMemoryContext,
														 sizeof(WorkerNode *) *
														 newWorkerNodeCount);

	/* iterate over the worker node list */
	WorkerNode *currentNode = NULL;
	foreach_declared_ptr(currentNode, workerNodeList)
	{
		bool handleFound = false;

		/* search for the worker node in the hash, and then insert the values */
		void *hashKey = (void *) currentNode;
		WorkerNode *workerNode = (WorkerNode *) hash_search(newWorkerNodeHash, hashKey,
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
 * RegisterCitusTableCacheEntryReleaseCallbacks registers callbacks to release
 * cache entries. Data should be locked by callers to avoid staleness.
 */
static void
RegisterCitusTableCacheEntryReleaseCallbacks(void)
{
	RegisterResourceReleaseCallback(CitusTableCacheEntryReleaseCallback, NULL);
}


/*
 * GetLocalGroupId returns the group identifier of the local node. The function
 * assumes that pg_dist_local_group has exactly one row and has at least one
 * column. Otherwise, the function errors out.
 */
int32
GetLocalGroupId(void)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	int32 groupId = 0;

	InitializeCaches();

	/*
	 * Already set the group id, no need to read the heap again.
	 */
	if (LocalGroupId != -1)
	{
		return LocalGroupId;
	}

	Oid localGroupTableOid = DistLocalGroupIdRelationId();
	if (localGroupTableOid == InvalidOid)
	{
		return 0;
	}

	Relation pgDistLocalGroupId = table_open(localGroupTableOid, AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistLocalGroupId,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistLocalGroupId);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

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
	table_close(pgDistLocalGroupId, AccessShareLock);

	return groupId;
}


/*
 * GetNodeId returns the node identifier of the local node.
 */
int32
GetLocalNodeId(void)
{
	InitializeCaches();

	/*
	 * Already set the node id, no need to read the heap again.
	 */
	if (LocalNodeId != -1)
	{
		return LocalNodeId;
	}

	uint32 nodeId = -1;

	int32 localGroupId = GetLocalGroupId();

	bool includeNodesFromOtherClusters = false;
	List *workerNodeList = ReadDistNode(includeNodesFromOtherClusters);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		if (workerNode->groupId == localGroupId &&
			workerNode->isActive)
		{
			nodeId = workerNode->nodeId;
			break;
		}
	}

	/*
	 * nodeId is -1 if we cannot find an active node whose group id is
	 * localGroupId in pg_dist_node.
	 */
	if (nodeId == -1)
	{
		elog(DEBUG4, "there is no active node with group id '%d' on pg_dist_node",
			 localGroupId);

		/*
		 * This is expected if the coordinator is not added to the metadata.
		 * We'll return GLOBAL_PID_NODE_ID_FOR_NODES_NOT_IN_METADATA for this case and
		 * for all cases so views can function almost normally
		 */
		nodeId = GLOBAL_PID_NODE_ID_FOR_NODES_NOT_IN_METADATA;
	}

	LocalNodeId = nodeId;

	return nodeId;
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
 * RegisterAuthinfoCacheCallbacks registers the callbacks required to
 * maintain cached connection parameters at fresh values.
 */
static void
RegisterAuthinfoCacheCallbacks(void)
{
	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(InvalidateConnParamsCacheCallback, (Datum) 0);
}


/*
 * ResetCitusTableCacheEntry frees any out-of-band memory used by a cache entry,
 * but does not free the entry itself.
 */
static void
ResetCitusTableCacheEntry(CitusTableCacheEntry *cacheEntry)
{
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

	/* clean up ShardIdCacheHash */
	RemoveStaleShardIdCacheEntries(cacheEntry);

	for (int shardIndex = 0; shardIndex < cacheEntry->shardIntervalArrayLength;
		 shardIndex++)
	{
		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
		GroupShardPlacement *placementArray =
			cacheEntry->arrayOfPlacementArrays[shardIndex];
		bool valueByVal = shardInterval->valueByVal;

		/* delete the shard's placements */
		if (placementArray != NULL)
		{
			pfree(placementArray);
		}

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
	cacheEntry->autoConverted = false;

	pfree(cacheEntry);
}


/*
 * RemoveStaleShardIdCacheEntries removes all shard ID cache entries belonging to the
 * given table entry. If the shard ID belongs to a different (newer) table entry,
 * we leave it in place.
 */
static void
RemoveStaleShardIdCacheEntries(CitusTableCacheEntry *invalidatedTableEntry)
{
	int shardIndex = 0;
	int shardCount = invalidatedTableEntry->shardIntervalArrayLength;

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval =
			invalidatedTableEntry->sortedShardIntervalArray[shardIndex];
		int64 shardId = shardInterval->shardId;
		bool foundInCache = false;

		ShardIdCacheEntry *shardIdCacheEntry =
			hash_search(ShardIdCacheHash, &shardId, HASH_FIND, &foundInCache);

		if (foundInCache && shardIdCacheEntry->tableEntry == invalidatedTableEntry)
		{
			hash_search(ShardIdCacheHash, &shardId, HASH_REMOVE, &foundInCache);
		}
	}
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
 * We acknowledge that it is not a very intuitive way of implementing this cache
 * invalidation, but, seems acceptable for now. If this becomes problematic, we
 * could try using a magic oid where we're sure that no relation would ever use
 * that oid.
 */
void
InvalidateForeignKeyGraph(void)
{
	if (!CitusHasBeenLoaded())
	{
		/*
		 * We should not try to invalidate foreign key graph
		 * if citus is not loaded.
		 */
		return;
	}

	CitusInvalidateRelcacheByRelid(DistColocationRelationId());

	/* bump command counter to force invalidation to take effect */
	CommandCounterIncrement();
}


/*
 * InvalidateDistRelationCacheCallback flushes cache entries when a relation
 * is updated (or flushes the entire cache).
 */
void
InvalidateDistRelationCacheCallback(Datum argument, Oid relationId)
{
	/* invalidate either entire cache or a specific entry */
	if (relationId == InvalidOid)
	{
		InvalidateDistTableCache();
		InvalidateDistObjectCache();
		InvalidateMetadataSystemCache();
	}
	else
	{
		void *hashKey = (void *) &relationId;
		bool foundInCache = false;

		if (DistTableCacheHash == NULL)
		{
			return;
		}

		CitusTableCacheEntrySlot *cacheSlot =
			hash_search(DistTableCacheHash, hashKey, HASH_FIND, &foundInCache);
		if (foundInCache)
		{
			InvalidateCitusTableCacheEntrySlot(cacheSlot);
		}

		/*
		 * if pg_dist_partition relcache is invalidated for some reason,
		 * invalidate the MetadataCache. It is likely an overkill to invalidate
		 * the entire cache here. But until a better fix, we keep it this way
		 * for postgres regression tests that includes
		 *   REINDEX SCHEMA CONCURRENTLY pg_catalog
		 * command.
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
 * InvalidateCitusTableCacheEntrySlot marks a CitusTableCacheEntrySlot as invalid,
 * meaning it needs to be rebuilt and the citusTableMetadata (if any) should be
 * released.
 */
static void
InvalidateCitusTableCacheEntrySlot(CitusTableCacheEntrySlot *cacheSlot)
{
	/* recheck whether this is a distributed table */
	cacheSlot->isValid = false;

	if (cacheSlot->citusTableMetadata != NULL)
	{
		/* reload the metadata */
		cacheSlot->citusTableMetadata->isValid = false;

		/* clean up ShardIdCacheHash */
		RemoveStaleShardIdCacheEntries(cacheSlot->citusTableMetadata);
	}
}


/*
 * InvalidateDistTableCache marks all DistTableCacheHash entries invalid.
 */
static void
InvalidateDistTableCache(void)
{
	CitusTableCacheEntrySlot *cacheSlot = NULL;
	HASH_SEQ_STATUS status;

	if (DistTableCacheHash == NULL)
	{
		return;
	}

	hash_seq_init(&status, DistTableCacheHash);

	while ((cacheSlot = (CitusTableCacheEntrySlot *) hash_seq_search(&status)) != NULL)
	{
		InvalidateCitusTableCacheEntrySlot(cacheSlot);
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

	if (DistObjectCacheHash == NULL)
	{
		return;
	}

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
	CitusTableCacheEntrySlot *cacheSlot = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, DistTableCacheHash);

	while ((cacheSlot = (CitusTableCacheEntrySlot *) hash_seq_search(&status)) != NULL)
	{
		ResetCitusTableCacheEntry(cacheSlot->citusTableMetadata);
	}

	hash_destroy(DistTableCacheHash);
	hash_destroy(ShardIdCacheHash);
	CreateDistTableCache();
	CreateShardIdCache();
}


/* CreateDistTableCache initializes the per-table hash table */
static void
CreateDistTableCache(void)
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(CitusTableCacheEntrySlot);
	info.hash = tag_hash;
	info.hcxt = MetadataCacheMemoryContext;
	DistTableCacheHash =
		hash_create("Distributed Relation Cache", 32, &info,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}


/* CreateShardIdCache initializes the shard ID mapping */
static void
CreateShardIdCache(void)
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardIdCacheEntry);
	info.hash = tag_hash;
	info.hcxt = MetadataCacheMemoryContext;
	ShardIdCacheHash =
		hash_create("Shard Id Cache", 128, &info,
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
 * InvalidateMetadataSystemCache resets all the cached OIDs and the extensionCreatedState
 * flag and invalidates the worker node, ConnParams, and local group ID caches.
 */
void
InvalidateMetadataSystemCache(void)
{
	InvalidateConnParamsHashEntries();

	memset(&MetadataCache, 0, sizeof(MetadataCache));
	workerNodeHashValid = false;
	LocalGroupId = -1;
	LocalNodeId = -1;
}


/*
 * AllCitusTableIds returns all citus table ids.
 */
List *
AllCitusTableIds(void)
{
	return CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
}


/*
 * CitusTableTypeIdList function scans pg_dist_partition and returns a
 * list of OID's for the tables matching given citusTableType.
 * To create the list, it performs sequential scan. Since it is not expected
 * that this function will be called frequently, it is OK not to use index
 * scan. If this function becomes performance bottleneck, it is possible to
 * modify this function to perform index scan.
 */
List *
CitusTableTypeIdList(CitusTableType citusTableType)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	List *relationIdList = NIL;

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNullArray[Natts_pg_dist_partition];
		Datum datumArray[Natts_pg_dist_partition];
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		Datum partMethodDatum = datumArray[Anum_pg_dist_partition_partmethod - 1];
		Datum replicationModelDatum = datumArray[Anum_pg_dist_partition_repmodel - 1];
		Datum colocationIdDatum = datumArray[Anum_pg_dist_partition_colocationid - 1];

		char partitionMethod = DatumGetChar(partMethodDatum);
		char replicationModel = DatumGetChar(replicationModelDatum);
		uint32 colocationId = DatumGetUInt32(colocationIdDatum);

		if (IsCitusTableTypeInternal(partitionMethod, replicationModel, colocationId,
									 citusTableType))
		{
			Datum relationIdDatum = datumArray[Anum_pg_dist_partition_logicalrelid - 1];

			Oid relationId = DatumGetObjectId(relationIdDatum);

			relationIdList = lappend_oid(relationIdList, relationId);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, AccessShareLock);

	return relationIdList;
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
		LocalNodeId = -1;
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
 * InvalidateConnParamsCacheCallback sets isValid flag to false for all entries
 * in ConnParamsHash, a cache used during connection establishment.
 */
static void
InvalidateConnParamsCacheCallback(Datum argument, Oid relationId)
{
	if (relationId == MetadataCache.distAuthinfoRelationId ||
		relationId == MetadataCache.distPoolinfoRelationId ||
		relationId == InvalidOid)
	{
		ConnParamsHashEntry *entry = NULL;
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, ConnParamsHash);

		while ((entry = (ConnParamsHashEntry *) hash_seq_search(&status)) != NULL)
		{
			entry->isValid = false;
		}
	}
}


/*
 * CitusTableCacheFlushInvalidatedEntries frees invalidated cache entries.
 * Invalidated entries aren't freed immediately as callers expect their lifetime
 * to extend beyond that scope.
 */
void
CitusTableCacheFlushInvalidatedEntries()
{
	if (DistTableCacheHash != NULL && DistTableCacheExpired != NIL)
	{
		CitusTableCacheEntry *cacheEntry = NULL;
		foreach_declared_ptr(cacheEntry, DistTableCacheExpired)
		{
			ResetCitusTableCacheEntry(cacheEntry);
		}
		list_free(DistTableCacheExpired);
		DistTableCacheExpired = NIL;
	}
}


/*
 * CitusTableCacheEntryReleaseCallback frees invalidated cache entries.
 */
static void
CitusTableCacheEntryReleaseCallback(ResourceReleasePhase phase, bool isCommit,
									bool isTopLevel, void *arg)
{
	if (isTopLevel && phase == RESOURCE_RELEASE_LOCKS)
	{
		CitusTableCacheFlushInvalidatedEntries();
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
	ScanKeyData scanKey[1];

	/* copy scankey to local copy, it will be modified during the scan */
	scanKey[0] = DistPartitionScanKey[0];

	/* set scan arguments */
	scanKey[0].sk_argument = ObjectIdGetDatum(relationId);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													true, NULL, 1, scanKey);

	HeapTuple currentPartitionTuple = systable_getnext(scanDescriptor);
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
List *
LookupDistShardTuples(Oid relationId)
{
	List *distShardTupleList = NIL;
	ScanKeyData scanKey[1];

	Relation pgDistShard = table_open(DistShardRelationId(), AccessShareLock);

	/* copy scankey to local copy, it will be modified during the scan */
	scanKey[0] = DistShardScanKey[0];

	/* set scan arguments */
	scanKey[0].sk_argument = ObjectIdGetDatum(relationId);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistShard,
													DistShardLogicalRelidIndexId(), true,
													NULL, 1, scanKey);

	HeapTuple currentShardTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(currentShardTuple))
	{
		HeapTuple shardTupleCopy = heap_copytuple(currentShardTuple);
		distShardTupleList = lappend(distShardTupleList, shardTupleCopy);

		currentShardTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistShard, AccessShareLock);

	return distShardTupleList;
}


/*
 * LookupShardRelationFromCatalog returns the logical relation oid a shard belongs to.
 *
 * Errors out if the shardId does not exist and missingOk is false.
 * Returns InvalidOid if the shardId does not exist and missingOk is true.
 */
Oid
LookupShardRelationFromCatalog(int64 shardId, bool missingOk)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Form_pg_dist_shard shardForm = NULL;
	Relation pgDistShard = table_open(DistShardRelationId(), AccessShareLock);
	Oid relationId = InvalidOid;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistShard,
													DistShardShardidIndexId(), true,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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
	table_close(pgDistShard, NoLock);

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
		case DISTRIBUTE_BY_HASH:
		{
			Node *partitionNode = stringToNode(partitionKeyString);
			Var *partitionColumn = (Var *) partitionNode;
			Assert(IsA(partitionNode, Var));

			GetIntervalTypeInfo(partitionMethod, partitionColumn,
								intervalTypeId, intervalTypeMod);

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
 * GetIntervalTypeInfo gets type id and type mod of the min/max values
 * of shard intervals for a distributed table with given partition method
 * and partition column.
 */
void
GetIntervalTypeInfo(char partitionMethod, Var *partitionColumn,
					Oid *intervalTypeId, int32 *intervalTypeMod)
{
	*intervalTypeId = InvalidOid;
	*intervalTypeMod = -1;

	switch (partitionMethod)
	{
		case DISTRIBUTE_BY_APPEND:
		case DISTRIBUTE_BY_RANGE:
		{
			/* we need a valid partition column Var in this case */
			if (partitionColumn == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("unexpected partition column value: null"),
								errdetail("Please report this to the Citus core team.")));
			}
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
			break;
		}
	}
}


/*
 * TupleToShardInterval transforms the specified dist_shard tuple into a new
 * ShardInterval using the provided descriptor and partition type information.
 */
ShardInterval *
TupleToShardInterval(HeapTuple heapTuple, TupleDesc tupleDescriptor, Oid
					 intervalTypeId,
					 int32 intervalTypeMod)
{
	Datum datumArray[Natts_pg_dist_shard];
	bool isNullArray[Natts_pg_dist_shard];

	/*
	 * We use heap_deform_tuple() instead of heap_getattr() to expand tuple
	 * to contain missing values when ALTER TABLE ADD COLUMN happens.
	 */
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	ShardInterval *shardInterval =
		DeformedDistShardTupleToShardInterval(datumArray, isNullArray,
											  intervalTypeId, intervalTypeMod);

	return shardInterval;
}


/*
 * DeformedDistShardTupleToShardInterval transforms the specified deformed
 * pg_dist_shard tuple into a new ShardInterval.
 */
ShardInterval *
DeformedDistShardTupleToShardInterval(Datum *datumArray, bool *isNullArray,
									  Oid intervalTypeId, int32 intervalTypeMod)
{
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	Datum minValue = 0;
	Datum maxValue = 0;
	bool minValueExists = false;
	bool maxValueExists = false;
	int16 intervalTypeLen = 0;
	bool intervalByVal = false;
	char intervalAlign = '0';
	char intervalDelim = '0';

	Oid relationId =
		DatumGetObjectId(datumArray[Anum_pg_dist_shard_logicalrelid - 1]);
	int64 shardId = DatumGetInt64(datumArray[Anum_pg_dist_shard_shardid - 1]);
	char storageType = DatumGetChar(datumArray[Anum_pg_dist_shard_shardstorage - 1]);
	Datum minValueTextDatum = datumArray[Anum_pg_dist_shard_shardminvalue - 1];
	Datum maxValueTextDatum = datumArray[Anum_pg_dist_shard_shardmaxvalue - 1];

	bool minValueNull = isNullArray[Anum_pg_dist_shard_shardminvalue - 1];
	bool maxValueNull = isNullArray[Anum_pg_dist_shard_shardmaxvalue - 1];

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

	ShardInterval *shardInterval = CitusMakeNode(ShardInterval);
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


/*
 * CachedRelationLookupExtended performs a cached lookup for the relation
 * relationName, with the result cached in *cachedOid. Will _not_ throw an error when
 * missing_ok is set to true.
 */
static void
CachedRelationLookupExtended(const char *relationName, Oid *cachedOid, bool missing_ok)
{
	CachedRelationNamespaceLookupExtended(relationName, PG_CATALOG_NAMESPACE, cachedOid,
										  missing_ok);
}


static void
CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace,
							  Oid *cachedOid)
{
	CachedRelationNamespaceLookupExtended(relationName, relnamespace, cachedOid, false);
}


static void
CachedRelationNamespaceLookupExtended(const char *relationName, Oid relnamespace,
									  Oid *cachedOid, bool missing_ok)
{
	/* force callbacks to be registered, so we always get notified upon changes */
	InitializeCaches();

	if (*cachedOid == InvalidOid)
	{
		*cachedOid = get_relname_relid(relationName, relnamespace);

		if (*cachedOid == InvalidOid && !missing_ok)
		{
			ereport(ERROR, (errmsg(
								"cache lookup failed for %s, called too early?",
								relationName)));
		}
	}
}


/*
 * RelationExists returns whether a relation with the given OID exists.
 */
bool
RelationExists(Oid relationId)
{
	HeapTuple relTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));

	bool relationExists = HeapTupleIsValid(relTuple);
	if (relationExists)
	{
		ReleaseSysCache(relTuple);
	}

	return relationExists;
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
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Form_pg_dist_shard shardForm = NULL;
	Relation pgDistShard = table_open(DistShardRelationId(), AccessShareLock);

	/*
	 * Load shard, to find the associated relation id. Can't use
	 * LoadShardInterval directly because that'd fail if the shard doesn't
	 * exist anymore, which we can't have. Also lower overhead is desirable
	 * here.
	 */

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistShard,
													DistShardShardidIndexId(), true,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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
	table_close(pgDistShard, NoLock);

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
	ScanKeyData scanKey[1];
	const int scanKeyCount = 0;

	Oid metadataTableOid = get_relname_relid("pg_dist_node_metadata",
											 PG_CATALOG_NAMESPACE);
	if (metadataTableOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("pg_dist_node_metadata was not found")));
	}

	Relation pgDistNodeMetadata = table_open(metadataTableOid, AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan(pgDistNodeMetadata,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistNodeMetadata);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
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

	/*
	 * Copy the jsonb result before closing the table
	 * since that memory can be freed.
	 */
	metadata = JsonbPGetDatum(DatumGetJsonbPCopy(metadata));

	systable_endscan(scanDescriptor);
	table_close(pgDistNodeMetadata, AccessShareLock);

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
 * GetPoolinfoViaCatalog searches the pg_dist_poolinfo table for a row matching
 * the provided nodeId and returns the poolinfo field of this row if found.
 * Otherwise, this function returns NULL.
 */
char *
GetPoolinfoViaCatalog(int32 nodeId)
{
	ScanKeyData scanKey[1];
	const int scanKeyCount = 1;
	const AttrNumber nodeIdIdx = 1, poolinfoIdx = 2;
	Relation pgDistPoolinfo = table_open(DistPoolinfoRelationId(), AccessShareLock);
	bool indexOK = true;
	char *poolinfo = NULL;

	/* set scan arguments */
	ScanKeyInit(&scanKey[0], nodeIdIdx, BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPoolinfo, DistPoolinfoIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistPoolinfo);
		bool isNull = false;

		Datum poolinfoDatum = heap_getattr(heapTuple, poolinfoIdx, tupleDescriptor,
										   &isNull);

		Assert(!isNull);

		poolinfo = TextDatumGetCString(poolinfoDatum);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistPoolinfo, AccessShareLock);

	return poolinfo;
}


/*
 * GetAuthinfoViaCatalog searches pg_dist_authinfo for a row matching a pro-
 * vided role and node id. Three types of rules are currently permitted: those
 * matching a specific node (non-zero nodeid), those matching all nodes (a
 * nodeid of zero), and those denoting a loopback connection (nodeid of -1).
 * Rolename must always be specified. If both types of rules exist for a given
 * user/host, the more specific (host-specific) rule wins. This means that when
 * both a zero and non-zero row exist for a given rolename, the non-zero row
 * has precedence.
 *
 * In short, this function will return a rule matching nodeId, or if that's
 * absent the rule for 0, or if that's absent, an empty string. Callers can
 * just use the returned authinfo and know the precedence has been honored.
 */
char *
GetAuthinfoViaCatalog(const char *roleName, int64 nodeId)
{
	char *authinfo = "";
	Datum nodeIdDatumArray[2] = {
		Int32GetDatum(nodeId),
		Int32GetDatum(WILDCARD_NODE_ID)
	};
	ArrayType *nodeIdArrayType = DatumArrayToArrayType(nodeIdDatumArray,
													   lengthof(nodeIdDatumArray),
													   INT4OID);
	ScanKeyData scanKey[2];
	const AttrNumber nodeIdIdx = 1, roleIdx = 2, authinfoIdx = 3;

	/*
	 * Our index's definition ensures correct precedence for positive nodeIds,
	 * but when handling a negative value we need to traverse backwards to keep
	 * the invariant that the zero rule has lowest precedence.
	 */
	ScanDirection direction = (nodeId < 0) ? BackwardScanDirection : ForwardScanDirection;

	if (ReindexIsProcessingIndex(DistAuthinfoIndexId()))
	{
		ereport(ERROR, (errmsg("authinfo is being reindexed; try again")));
	}

	memset(&scanKey, 0, sizeof(scanKey));

	/* first column in index is rolename, need exact match there ... */
	ScanKeyInit(&scanKey[0], roleIdx, BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(roleName));

	/* second column is nodeId, match against array of nodeid and zero (any node) ... */
	ScanKeyInit(&scanKey[1], nodeIdIdx, BTEqualStrategyNumber,
				F_INT4EQ, PointerGetDatum(nodeIdArrayType));
	scanKey[1].sk_flags |= SK_SEARCHARRAY;

	/*
	 * It's important that we traverse the index in order: we need to ensure
	 * that rules with nodeid 0 are encountered last. We'll use the first tuple
	 * we find. This ordering defines the precedence order of authinfo rules.
	 */
	Relation pgDistAuthinfo = table_open(DistAuthinfoRelationId(), AccessShareLock);
	Relation pgDistAuthinfoIdx = index_open(DistAuthinfoIndexId(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(pgDistAuthinfo,
															pgDistAuthinfoIdx,
															NULL, lengthof(scanKey),
															scanKey);

	/* first tuple represents highest-precedence rule for this node */
	HeapTuple authinfoTuple = systable_getnext_ordered(scanDescriptor, direction);
	if (HeapTupleIsValid(authinfoTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistAuthinfo);
		bool isNull = false;

		Datum authinfoDatum = heap_getattr(authinfoTuple, authinfoIdx,
										   tupleDescriptor, &isNull);

		Assert(!isNull);

		authinfo = TextDatumGetCString(authinfoDatum);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(pgDistAuthinfoIdx, AccessShareLock);
	table_close(pgDistAuthinfo, AccessShareLock);

	return authinfo;
}


/*
 * authinfo_valid is a check constraint to verify that an inserted authinfo row
 * uses only permitted libpq parameters.
 */
Datum
authinfo_valid(PG_FUNCTION_ARGS)
{
	char *authinfo = TextDatumGetCString(PG_GETARG_DATUM(0));

	/* this array _must_ be kept in an order usable by bsearch */
	const char *allowList[] = { "password", "sslcert", "sslkey" };
	bool authinfoValid = CheckConninfo(authinfo, allowList, lengthof(allowList), NULL);

	PG_RETURN_BOOL(authinfoValid);
}


/*
 * poolinfo_valid is a check constraint to verify that an inserted poolinfo row
 * uses only permitted libpq parameters.
 */
Datum
poolinfo_valid(PG_FUNCTION_ARGS)
{
	char *poolinfo = TextDatumGetCString(PG_GETARG_DATUM(0));

	/* this array _must_ be kept in an order usable by bsearch */
	const char *allowList[] = { "dbname", "host", "port" };
	bool poolinfoValid = CheckConninfo(poolinfo, allowList, lengthof(allowList), NULL);

	PG_RETURN_BOOL(poolinfoValid);
}
