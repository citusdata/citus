/*-------------------------------------------------------------------------
 *
 * shardinterval_utils.h
 *
 * Declarations for public utility functions related to shard intervals.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDINTERVAL_UTILS_H_
#define SHARDINTERVAL_UTILS_H_

#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "nodes/primnodes.h"

#define INVALID_SHARD_INDEX -1

/* OperatorCacheEntry contains information for each element in OperatorCache */
typedef struct ShardIntervalCompareFunctionCacheEntry
{
	Var *partitionColumn;
	char partitionMethod;
	FmgrInfo *functionInfo;
} ShardIntervalCompareFunctionCacheEntry;

extern ShardInterval * LowestShardIntervalById(List *shardIntervalList);
extern int CompareShardIntervals(const void *leftElement, const void *rightElement,
								 FmgrInfo *typeCompareFunction);
extern int CompareShardIntervalsById(const void *leftElement, const void *rightElement);
extern int CompareRelationShards(const void *leftElement,
								 const void *rightElement);
extern int ShardIndex(ShardInterval *shardInterval);
extern ShardInterval * FindShardInterval(Datum partitionColumnValue,
										 DistTableCacheEntry *cacheEntry);
extern int FindShardIntervalIndex(Datum searchedValue, DistTableCacheEntry *cacheEntry);
extern bool SingleReplicatedTable(Oid relationId);

#endif /* SHARDINTERVAL_UTILS_H_ */
