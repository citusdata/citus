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
#include "nodes/primnodes.h"

/* OperatorCacheEntry contains information for each element in OperatorCache */
typedef struct ShardIntervalCompareFunctionCacheEntry
{
	Var *partitionColumn;
	char partitionMethod;
	FmgrInfo *functionInfo;
} ShardIntervalCompareFunctionCacheEntry;

extern int CompareShardIntervals(const void *leftElement, const void *rightElement,
								 FmgrInfo *typeCompareFunction);
extern int CompareShardIntervalsById(const void *leftElement, const void *rightElement);
extern ShardInterval * FindShardInterval(Datum partitionColumnValue,
										 ShardInterval **shardIntervalCache,
										 int shardCount, char partitionMethod,
										 FmgrInfo *compareFunction,
										 FmgrInfo *hashFunction, bool useBinarySearch);

#endif /* SHARDINTERVAL_UTILS_H_ */
