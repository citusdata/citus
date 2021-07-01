/*-------------------------------------------------------------------------
 *
 * metadata_cache.h
 *	  Executor support for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_CACHE_H
#define METADATA_CACHE_H

#include "postgres.h"

#include "fmgr.h"
#include "distributed/metadata_utility.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_manager.h"
#include "utils/hsearch.h"

extern bool EnableVersionChecks;

/* managed via guc.c */
typedef enum
{
	USE_SECONDARY_NODES_NEVER = 0,
	USE_SECONDARY_NODES_ALWAYS = 1
} ReadFromSecondariesType;
extern int ReadFromSecondaries;


/*
 * While upgrading pg_dist_local_group can be empty temporarily, in that
 * case we use GROUP_ID_UPGRADING as the local group id to communicate
 * this to other functions.
 */
#define GROUP_ID_UPGRADING -2


/*
 * Representation of a table's metadata that is frequently used for
 * distributed execution. Cached.
 */
typedef struct
{
	/* lookup key - must be first. A pg_class.oid oid. */
	Oid relationId;

	/*
	 * Has an invalidation been received for this entry, requiring a rebuild
	 * of the cache entry?
	 */
	bool isValid;

	bool hasUninitializedShardInterval;
	bool hasUniformHashDistribution; /* valid for hash partitioned tables */
	bool hasOverlappingShardInterval;

	/* pg_dist_partition metadata for this table */
	char *partitionKeyString;
	Var *partitionColumn;
	char partitionMethod;
	uint32 colocationId;
	char replicationModel;

	/* pg_dist_shard metadata (variable-length ShardInterval array) for this table */
	int shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray;

	/* comparator for partition column's type, NULL if DISTRIBUTE_BY_NONE */
	FmgrInfo *shardColumnCompareFunction;

	/*
	 * Comparator for partition interval type (different from
	 * shardColumnCompareFunction if hash-partitioned), NULL if
	 * DISTRIBUTE_BY_NONE.
	 */
	FmgrInfo *shardIntervalCompareFunction;
	FmgrInfo *hashFunction; /* NULL if table is not distributed by hash */

	/*
	 * The following two lists consists of relationIds that this distributed
	 * relation has a foreign key to (e.g., referencedRelationsViaForeignKey) or
	 * other relations has a foreign key to this relation (e.g.,
	 * referencingRelationsViaForeignKey).
	 *
	 * Note that we're keeping all transitive foreign key references as well
	 * such that if relation A refers to B, and B refers to C, we keep A and B
	 * in C's referencingRelationsViaForeignKey.
	 */
	List *referencedRelationsViaForeignKey;
	List *referencingRelationsViaForeignKey;

	/* pg_dist_placement metadata */
	GroupShardPlacement **arrayOfPlacementArrays;
	int *arrayOfPlacementArrayLengths;
} CitusTableCacheEntry;

typedef struct DistObjectCacheEntryKey
{
	Oid classid;
	Oid objid;
	int32 objsubid;
} DistObjectCacheEntryKey;

typedef struct DistObjectCacheEntry
{
	/* lookup key - must be first. */
	DistObjectCacheEntryKey key;

	bool isValid;
	bool isDistributed;

	int distributionArgIndex;
	int colocationId;
} DistObjectCacheEntry;

typedef enum
{
	HASH_DISTRIBUTED,
	APPEND_DISTRIBUTED,
	RANGE_DISTRIBUTED,

	/* hash, range or append distributed table */
	DISTRIBUTED_TABLE,

	/* hash- or range-distributed table */
	STRICTLY_PARTITIONED_DISTRIBUTED_TABLE,

	REFERENCE_TABLE,
	CITUS_LOCAL_TABLE,

	/* table without a dist key such as reference table */
	CITUS_TABLE_WITH_NO_DIST_KEY,

	ANY_CITUS_TABLE_TYPE
} CitusTableType;

extern List * AllCitusTableIds(void);
extern bool IsCitusTableType(Oid relationId, CitusTableType tableType);
extern bool IsCitusTableTypeCacheEntry(CitusTableCacheEntry *tableEtnry,
									   CitusTableType tableType);

extern bool IsCitusTable(Oid relationId);
extern char PgDistPartitionViaCatalog(Oid relationId);
extern List * LookupDistShardTuples(Oid relationId);
extern char PartitionMethodViaCatalog(Oid relationId);
extern bool IsCitusLocalTableByDistParams(char partitionMethod, char replicationModel);
extern List * CitusTableList(void);
extern ShardInterval * LoadShardInterval(uint64 shardId);
extern Oid RelationIdForShard(uint64 shardId);
extern bool ReferenceTableShardId(uint64 shardId);
extern ShardPlacement * ShardPlacementOnGroupIncludingOrphanedPlacements(int32 groupId,
																		 uint64 shardId);
extern ShardPlacement * ActiveShardPlacementOnGroup(int32 groupId, uint64 shardId);
extern GroupShardPlacement * LoadGroupShardPlacement(uint64 shardId, uint64 placementId);
extern ShardPlacement * LoadShardPlacement(uint64 shardId, uint64 placementId);
extern CitusTableCacheEntry * GetCitusTableCacheEntry(Oid distributedRelationId);
extern CitusTableCacheEntry * LookupCitusTableCacheEntry(Oid relationId);
extern DistObjectCacheEntry * LookupDistObjectCacheEntry(Oid classid, Oid objid, int32
														 objsubid);
extern int32 GetLocalGroupId(void);
extern void CitusTableCacheFlushInvalidatedEntries(void);
extern Oid LookupShardRelationFromCatalog(int64 shardId, bool missing_ok);
extern List * ShardPlacementListIncludingOrphanedPlacements(uint64 shardId);
extern bool ShardExists(int64 shardId);
extern void CitusInvalidateRelcacheByRelid(Oid relationId);
extern void CitusInvalidateRelcacheByShardId(int64 shardId);
extern void InvalidateForeignKeyGraph(void);
extern void FlushDistTableCache(void);
extern void InvalidateMetadataSystemCache(void);
extern List * CitusTableTypeIdList(CitusTableType citusTableType);
extern Datum DistNodeMetadata(void);
extern bool ClusterHasReferenceTable(void);
extern bool HasUniformHashDistribution(ShardInterval **shardIntervalArray,
									   int shardIntervalArrayLength);
extern bool HasUninitializedShardInterval(ShardInterval **sortedShardIntervalArray,
										  int shardCount);
extern bool HasOverlappingShardInterval(ShardInterval **shardIntervalArray,
										int shardIntervalArrayLength,
										Oid shardIntervalCollation,
										FmgrInfo *shardIntervalSortCompareFunction);

extern ShardPlacement * ShardPlacementForFunctionColocatedWithReferenceTable(
	CitusTableCacheEntry *cacheEntry);
extern ShardPlacement * ShardPlacementForFunctionColocatedWithDistTable(
	DistObjectCacheEntry *procedure, FuncExpr *funcExpr, Var *partitionColumn,
	CitusTableCacheEntry
	*cacheEntry,
	PlannedStmt *plan);

extern bool CitusHasBeenLoaded(void);
extern bool CheckCitusVersion(int elevel);
extern bool CheckAvailableVersion(int elevel);
extern bool InstalledAndAvailableVersionsSame(void);
extern bool MajorVersionsCompatible(char *leftVersion, char *rightVersion);
extern void ErrorIfInconsistentShardIntervals(CitusTableCacheEntry *cacheEntry);
extern void EnsureModificationsCanRun(void);
extern char LookupDistributionMethod(Oid distributionMethodOid);
extern bool RelationExists(Oid relationId);
extern ShardInterval * TupleToShardInterval(HeapTuple heapTuple,
											TupleDesc tupleDescriptor, Oid intervalTypeId,
											int32 intervalTypeMod);

/* access WorkerNodeHash */
extern bool HasAnyNodes(void);
extern HTAB * GetWorkerNodeHash(void);
extern WorkerNode * LookupNodeByNodeId(uint32 nodeId);
extern WorkerNode * LookupNodeByNodeIdOrError(uint32 nodeId);
extern WorkerNode * LookupNodeForGroup(int32 groupId);

/* namespace oids */
extern Oid CitusCatalogNamespaceId(void);

/* relation oids */
extern Oid DistColocationRelationId(void);
extern Oid DistColocationConfigurationIndexId(void);
extern Oid DistPartitionRelationId(void);
extern Oid DistShardRelationId(void);
extern Oid DistPlacementRelationId(void);
extern Oid DistNodeRelationId(void);
extern Oid DistRebalanceStrategyRelationId(void);
extern Oid DistLocalGroupIdRelationId(void);
extern Oid DistObjectRelationId(void);
extern Oid DistEnabledCustomAggregatesId(void);

/* index oids */
extern Oid DistNodeNodeIdIndexId(void);
extern Oid DistPartitionLogicalRelidIndexId(void);
extern Oid DistPartitionColocationidIndexId(void);
extern Oid DistShardLogicalRelidIndexId(void);
extern Oid DistShardShardidIndexId(void);
extern Oid DistPlacementShardidIndexId(void);
extern Oid DistPlacementPlacementidIndexId(void);
extern Oid DistTransactionRelationId(void);
extern Oid DistTransactionGroupIndexId(void);
extern Oid DistPlacementGroupidIndexId(void);
extern Oid DistObjectPrimaryKeyIndexId(void);

/* type oids */
extern Oid LookupTypeOid(char *schemaNameSting, char *typeNameString);
extern Oid CitusCopyFormatTypeId(void);

/* function oids */
extern Oid CitusReadIntermediateResultFuncId(void);
Oid CitusReadIntermediateResultArrayFuncId(void);
extern Oid CitusExtraDataContainerFuncId(void);
extern Oid CitusAnyValueFunctionId(void);
extern Oid PgTableVisibleFuncId(void);
extern Oid CitusTableVisibleFuncId(void);
extern Oid JsonbExtractPathFuncId(void);

/* enum oids */
extern Oid PrimaryNodeRoleId(void);
extern Oid SecondaryNodeRoleId(void);
extern Oid CitusCopyFormatTypeId(void);
extern Oid TextCopyFormatId(void);
extern Oid BinaryCopyFormatId(void);

/* user related functions */
extern Oid CitusExtensionOwner(void);
extern char * CitusExtensionOwnerName(void);
extern char * CurrentUserName(void);
extern const char * CurrentDatabaseName(void);

#endif /* METADATA_CACHE_H */
