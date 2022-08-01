/*-------------------------------------------------------------------------
 *
 * shard_split.h
 *
 * API for shard splits.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_H_
#define SHARDSPLIT_H_

/* Split Modes supported by Shard Split API */
typedef enum SplitMode
{
	BLOCKING_SPLIT = 0,
	NON_BLOCKING_SPLIT = 1
} SplitMode;

/*
 * User Scenario calling Split Shard API.
 * The 'SplitOperation' type is used to customize info/error messages based on user scenario.
 */
typedef enum SplitOperation
{
	SHARD_SPLIT_API = 0,
	ISOLATE_TENANT_TO_NEW_SHARD
} SplitOperation;


/*
 * SplitShard API to split a given shard (or shard group) using split mode and
 * specified split points to a set of destination nodes.
 */
extern void SplitShard(SplitMode splitMode,
					   SplitOperation splitOperation,
					   uint64 shardIdToSplit,
					   List *shardSplitPointsList,
					   List *nodeIdsForPlacementList);

/* TODO(niupre): Make all these APIs private when all consumers (Example : ISOLATE_TENANT_TO_NEW_SHARD) directly call 'SplitShard' API. */
extern void ErrorIfCannotSplitShard(SplitOperation splitOperation,
									ShardInterval *sourceShard);
extern void DropShardList(List *shardIntervalList);

#endif /* SHARDSPLIT_H_ */
