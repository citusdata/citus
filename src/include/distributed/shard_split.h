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

#ifndef SHARDSPLIT_UTILS_H_
#define SHARDSPLIT_UTILS_H_

/* Split Modes supported by Shard Split API */
typedef enum SplitMode
{
	BLOCKING_SPLIT = 0,
} SplitMode;

/*
 * User Scenario calling Split Shard API.
 * The 'SplitOperation' type is used to customize info/error messages based on user scenario.
 */
typedef enum SplitOperation
{
	SHARD_SPLIT_API = 0
} SplitOperation;

/*
 * SplitShard API to split a given shard (or shard group) in blocking / non-blocking fashion
 * based on specified split points to a set of destination nodes.
 */
extern void SplitShard(SplitMode splitMode,
					   SplitOperation splitOperation,
					   uint64 shardIdToSplit,
					   List *shardSplitPointsList,
					   List *nodeIdsForPlacementList);

#endif /* SHARDSPLIT_UTILS_H_ */
