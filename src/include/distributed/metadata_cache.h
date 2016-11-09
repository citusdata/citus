/*-------------------------------------------------------------------------
 *
 * metadata_cache.h
 *	  Executor support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_CACHE_H
#define METADATA_CACHE_H

#include "fmgr.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_manager.h"
#include "utils/hsearch.h"


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

	bool isDistributedTable;
	bool hasUninitializedShardInterval;
	bool hasUniformHashDistribution; /* valid for hash partitioned tables */

	/* pg_dist_partition metadata for this table */
	char *partitionKeyString;
	char partitionMethod;
	uint32 colocationId;
	char replicationModel;

	/* pg_dist_shard metadata (variable-length ShardInterval array) for this table */
	int shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray;

	FmgrInfo *shardIntervalCompareFunction; /* NULL if no shard intervals exist */
	FmgrInfo *hashFunction; /* NULL if table is not distributed by hash */
} DistTableCacheEntry;


extern bool IsDistributedTable(Oid relationId);
extern List * DistributedTableList(void);
extern ShardInterval * LoadShardInterval(uint64 shardId);
extern DistTableCacheEntry * DistributedTableCacheEntry(Oid distributedRelationId);
extern int GetLocalGroupId(void);
extern void CitusInvalidateRelcacheByRelid(Oid relationId);
extern void CitusInvalidateRelcacheByShardId(int64 shardId);

extern bool CitusHasBeenLoaded(void);

/* access WorkerNodeHash */
extern HTAB * GetWorkerNodeHash(void);

/* relation oids */
extern Oid DistColocationRelationId(void);
extern Oid DistColocationConfigurationIndexId(void);
extern Oid DistPartitionRelationId(void);
extern Oid DistShardRelationId(void);
extern Oid DistShardPlacementRelationId(void);
extern Oid DistNodeRelationId(void);
extern Oid DistLocalGroupIdRelationId(void);

/* index oids */
extern Oid DistPartitionLogicalRelidIndexId(void);
extern Oid DistPartitionColocationidIndexId(void);
extern Oid DistShardLogicalRelidIndexId(void);
extern Oid DistShardShardidIndexId(void);
extern Oid DistShardPlacementShardidIndexId(void);
extern Oid DistShardPlacementPlacementidIndexId(void);
extern Oid DistTransactionRelationId(void);
extern Oid DistTransactionGroupIndexId(void);
extern Oid DistShardPlacementNodeidIndexId(void);

/* function oids */
extern Oid CitusExtraDataContainerFuncId(void);

/* user related functions */
extern Oid CitusExtensionOwner(void);
extern char * CitusExtensionOwnerName(void);
extern char * CurrentUserName(void);
#endif /* METADATA_CACHE_H */
