/*-------------------------------------------------------------------------
 *
 * shardinterval_utils.c
 *
 * This file contains functions to perform useful operations on shard intervals.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_planner.h"
#include "distributed/shard_pruning.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_protocol.h"
#include "utils/catcache.h"
#include "utils/memutils.h"


static int SearchCachedShardInterval(Datum partitionColumnValue,
									 ShardInterval **shardIntervalCache,
									 int shardCount, FmgrInfo *compareFunction);


/*
 * LowestShardIntervalById returns the shard interval with the lowest shard
 * ID from a list of shard intervals.
 */
ShardInterval *
LowestShardIntervalById(List *shardIntervalList)
{
	ShardInterval *lowestShardInterval = NULL;
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);

		if (lowestShardInterval == NULL ||
			lowestShardInterval->shardId > shardInterval->shardId)
		{
			lowestShardInterval = shardInterval;
		}
	}

	return lowestShardInterval;
}


/*
 * CompareShardIntervals acts as a helper function to compare two shard intervals
 * by their minimum values, using the value's type comparison function.
 *
 * If a shard interval does not have min/max value, it's treated as being greater
 * than the other.
 */
int
CompareShardIntervals(const void *leftElement, const void *rightElement,
					  FmgrInfo *typeCompareFunction)
{
	ShardInterval *leftShardInterval = *((ShardInterval **) leftElement);
	ShardInterval *rightShardInterval = *((ShardInterval **) rightElement);
	Datum leftDatum = 0;
	Datum rightDatum = 0;
	Datum comparisonDatum = 0;
	int comparisonResult = 0;

	Assert(typeCompareFunction != NULL);

	/*
	 * Left element should be treated as the greater element in case it doesn't
	 * have min or max values.
	 */
	if (!leftShardInterval->minValueExists || !leftShardInterval->maxValueExists)
	{
		comparisonResult = 1;
		return comparisonResult;
	}

	/*
	 * Right element should be treated as the greater element in case it doesn't
	 * have min or max values.
	 */
	if (!rightShardInterval->minValueExists || !rightShardInterval->maxValueExists)
	{
		comparisonResult = -1;
		return comparisonResult;
	}

	/* if both shard interval have min/max values, calculate the comparison result */
	leftDatum = leftShardInterval->minValue;
	rightDatum = rightShardInterval->minValue;

	comparisonDatum = CompareCall2(typeCompareFunction, leftDatum, rightDatum);
	comparisonResult = DatumGetInt32(comparisonDatum);

	return comparisonResult;
}


/*
 * CompareShardIntervalsById is a comparison function for sort shard
 * intervals by their shard ID.
 */
int
CompareShardIntervalsById(const void *leftElement, const void *rightElement)
{
	ShardInterval *leftInterval = *((ShardInterval **) leftElement);
	ShardInterval *rightInterval = *((ShardInterval **) rightElement);
	int64 leftShardId = leftInterval->shardId;
	int64 rightShardId = rightInterval->shardId;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * CompareRelationShards is a comparison function for sorting relation
 * to shard mappings by their relation ID and then shard ID.
 */
int
CompareRelationShards(const void *leftElement, const void *rightElement)
{
	RelationShard *leftRelationShard = *((RelationShard **) leftElement);
	RelationShard *rightRelationShard = *((RelationShard **) rightElement);
	Oid leftRelationId = leftRelationShard->relationId;
	Oid rightRelationId = rightRelationShard->relationId;
	int64 leftShardId = leftRelationShard->shardId;
	int64 rightShardId = rightRelationShard->shardId;

	if (leftRelationId > rightRelationId)
	{
		return 1;
	}
	else if (leftRelationId < rightRelationId)
	{
		return -1;
	}
	else if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * ShardIndex finds the index of given shard in sorted shard interval array.
 *
 * For hash partitioned tables, it calculates hash value of a number in its
 * range (e.g. min value) and finds which shard should contain the hashed
 * value. For reference tables, it simply returns 0. For distribution methods
 * other than hash and reference, the function errors out.
 */
int
ShardIndex(ShardInterval *shardInterval)
{
	int shardIndex = INVALID_SHARD_INDEX;
	Oid distributedTableId = shardInterval->relationId;
	Datum shardMinValue = shardInterval->minValue;

	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;

	/*
	 * Note that, we can also support append and range distributed tables, but
	 * currently it is not required.
	 */
	if (partitionMethod != DISTRIBUTE_BY_HASH && partitionMethod != DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("finding index of a given shard is only supported for "
							   "hash distributed and reference tables")));
	}

	/* short-circuit for reference tables */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		/* reference tables has only a single shard, so the index is fixed to 0 */
		shardIndex = 0;

		return shardIndex;
	}

	shardIndex = FindShardIntervalIndex(shardMinValue, cacheEntry);

	return shardIndex;
}


/*
 * FindShardInterval finds a single shard interval in the cache for the
 * given partition column value. Note that reference tables do not have
 * partition columns, thus, pass partitionColumnValue and compareFunction
 * as NULL for them.
 */
ShardInterval *
FindShardInterval(Datum partitionColumnValue, DistTableCacheEntry *cacheEntry)
{
	Datum searchedValue = partitionColumnValue;
	int shardIndex = INVALID_SHARD_INDEX;

	if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
	{
		searchedValue = FunctionCall1(cacheEntry->hashFunction, partitionColumnValue);
	}

	shardIndex = FindShardIntervalIndex(searchedValue, cacheEntry);

	if (shardIndex == INVALID_SHARD_INDEX)
	{
		return NULL;
	}

	return cacheEntry->sortedShardIntervalArray[shardIndex];
}


/*
 * FindShardIntervalIndex finds the index of the shard interval which covers
 * the searched value. Note that the searched value must be the hashed value
 * of the original value if the distribution method is hash.
 *
 * Note that, if the searched value can not be found for hash partitioned
 * tables, we error out (unless there are no shards, in which case
 * INVALID_SHARD_INDEX is returned). This should only happen if something is
 * terribly wrong, either metadata tables are corrupted or we have a bug
 * somewhere. Such as a hash function which returns a value not in the range
 * of [INT32_MIN, INT32_MAX] can fire this.
 */
int
FindShardIntervalIndex(Datum searchedValue, DistTableCacheEntry *cacheEntry)
{
	ShardInterval **shardIntervalCache = cacheEntry->sortedShardIntervalArray;
	int shardCount = cacheEntry->shardIntervalArrayLength;
	char partitionMethod = cacheEntry->partitionMethod;
	FmgrInfo *compareFunction = cacheEntry->shardIntervalCompareFunction;
	bool useBinarySearch = (partitionMethod != DISTRIBUTE_BY_HASH ||
							!cacheEntry->hasUniformHashDistribution);
	int shardIndex = INVALID_SHARD_INDEX;

	if (shardCount == 0)
	{
		return INVALID_SHARD_INDEX;
	}

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		if (useBinarySearch)
		{
			Assert(compareFunction != NULL);

			shardIndex = SearchCachedShardInterval(searchedValue, shardIntervalCache,
												   shardCount, compareFunction);

			/* we should always return a valid shard index for hash partitioned tables */
			if (shardIndex == INVALID_SHARD_INDEX)
			{
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
								errmsg("cannot find shard interval"),
								errdetail("Hash of the partition column value "
										  "does not fall into any shards.")));
			}
		}
		else
		{
			int hashedValue = DatumGetInt32(searchedValue);
			uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

			shardIndex = (uint32) (hashedValue - INT32_MIN) / hashTokenIncrement;
			Assert(shardIndex <= shardCount);

			/*
			 * If the shard count is not power of 2, the range of the last
			 * shard becomes larger than others. For that extra piece of range,
			 * we still need to use the last shard.
			 */
			if (shardIndex == shardCount)
			{
				shardIndex = shardCount - 1;
			}
		}
	}
	else if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		/* reference tables has a single shard, all values mapped to that shard */
		Assert(shardCount == 1);

		shardIndex = 0;
	}
	else
	{
		Assert(compareFunction != NULL);

		shardIndex = SearchCachedShardInterval(searchedValue, shardIntervalCache,
											   shardCount, compareFunction);
	}

	return shardIndex;
}


/*
 * SearchCachedShardInterval performs a binary search for a shard interval
 * matching a given partition column value and returns it's index in the cached
 * array. If it can not find any shard interval with the given value, it returns
 * INVALID_SHARD_INDEX.
 */
static int
SearchCachedShardInterval(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
						  int shardCount, FmgrInfo *compareFunction)
{
	int lowerBoundIndex = 0;
	int upperBoundIndex = shardCount;

	while (lowerBoundIndex < upperBoundIndex)
	{
		int middleIndex = (lowerBoundIndex + upperBoundIndex) / 2;
		int maxValueComparison = 0;
		int minValueComparison = 0;

		minValueComparison = FunctionCall2Coll(compareFunction,
											   DEFAULT_COLLATION_OID,
											   partitionColumnValue,
											   shardIntervalCache[middleIndex]->minValue);

		if (DatumGetInt32(minValueComparison) < 0)
		{
			upperBoundIndex = middleIndex;
			continue;
		}

		maxValueComparison = FunctionCall2Coll(compareFunction,
											   DEFAULT_COLLATION_OID,
											   partitionColumnValue,
											   shardIntervalCache[middleIndex]->maxValue);

		if (DatumGetInt32(maxValueComparison) <= 0)
		{
			return middleIndex;
		}

		lowerBoundIndex = middleIndex + 1;
	}

	return INVALID_SHARD_INDEX;
}


/*
 * SingleReplicatedTable checks whether all shards of a distributed table, do not have
 * more than one replica. If even one shard has more than one replica, this function
 * returns false, otherwise it returns true.
 */
bool
SingleReplicatedTable(Oid relationId)
{
	List *shardIntervalList = LoadShardList(relationId);
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		uint64 *shardIdPointer = (uint64 *) lfirst(shardIntervalCell);
		uint64 shardId = (*shardIdPointer);
		List *shardPlacementList = ShardPlacementList(shardId);

		if (shardPlacementList->length > 1)
		{
			return false;
		}
	}

	return true;
}
