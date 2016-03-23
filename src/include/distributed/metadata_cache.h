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

#include "distributed/master_metadata_utility.h"
#include "distributed/pg_dist_partition.h"


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

	/* pg_dist_partition metadata for this table */
	char *partitionKeyString;
	char partitionMethod;

	/* pg_dist_shard metadata (variable-length ShardInterval array) for this table */
	int shardIntervalArrayLength;
	ShardInterval *shardIntervalArray;
} DistTableCacheEntry;


extern bool IsDistributedTable(Oid relationId);
extern ShardInterval * LoadShardInterval(uint64 shardId);
extern DistTableCacheEntry * DistributedTableCacheEntry(Oid distributedRelationId);
extern void CitusInvalidateRelcacheByRelid(Oid relationId);

extern bool CitusHasBeenLoaded(void);

/* relation oids */
extern Oid DistPartitionRelationId(void);
extern Oid DistShardRelationId(void);
extern Oid DistShardPlacementRelationId(void);

/* index oids */
extern Oid DistPartitionLogicalRelidIndexId(void);
extern Oid DistShardLogicalRelidIndexId(void);
extern Oid DistShardShardidIndexId(void);
extern Oid DistShardPlacementShardidIndexId(void);

/* function oids */
extern Oid CitusExtraDataContainerFuncId(void);

#endif /* METADATA_CACHE_H */
