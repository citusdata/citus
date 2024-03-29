/*-------------------------------------------------------------------------
 *
 * shardinterval_utils.h
 *
 * Declarations for public utility functions related to shard intervals.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDINTERVAL_UTILS_H_
#define SHARDINTERVAL_UTILS_H_

#include "nodes/primnodes.h"

#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"

#define INVALID_SHARD_INDEX -1

/* OperatorCacheEntry contains information for each element in OperatorCache */
typedef struct ShardIntervalCompareFunctionCacheEntry
{
	Var *partitionColumn;
	char partitionMethod;
	FmgrInfo *functionInfo;
} ShardIntervalCompareFunctionCacheEntry;

/*
 * SortShardIntervalContext is the context parameter in SortShardIntervalArray
 */
typedef struct SortShardIntervalContext
{
	FmgrInfo *comparisonFunction;
	Oid collation;
} SortShardIntervalContext;

extern ShardInterval ** SortShardIntervalArray(ShardInterval **shardIntervalArray, int
											   shardCount, Oid collation,
											   FmgrInfo *shardIntervalSortCompareFunction);
extern int CompareShardIntervals(const void *leftElement, const void *rightElement,
								 SortShardIntervalContext *sortContext);
extern int CompareShardIntervalsById(const void *leftElement, const void *rightElement);
extern int CompareShardPlacementsByShardId(const void *leftElement,
										   const void *rightElement);
extern int CompareRelationShards(const void *leftElement,
								 const void *rightElement);
extern int ShardIndex(ShardInterval *shardInterval);
extern int CalculateUniformHashRangeIndex(int hashedValue, int shardCount);
extern ShardInterval * FindShardInterval(Datum partitionColumnValue,
										 CitusTableCacheEntry *cacheEntry);
extern int FindShardIntervalIndex(Datum searchedValue, CitusTableCacheEntry *cacheEntry);
extern int SearchCachedShardInterval(Datum partitionColumnValue,
									 ShardInterval **shardIntervalCache,
									 int shardCount, Oid shardIntervalCollation,
									 FmgrInfo *compareFunction);
extern bool SingleReplicatedTable(Oid relationId);


#endif /* SHARDINTERVAL_UTILS_H_ */
