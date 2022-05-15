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
	BLOCKING_SPLIT = 0
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
 * In-memory representation of a split child shard.
 */
typedef struct ShardSplitInfo
{
	Oid distributedTableOid;     /* citus distributed table Oid */
	int partitionColumnIndex;
	Oid sourceShardOid;          /* parent shard Oid */
	Oid splitChildShardOid;      /* child shard Oid */
	int32 shardMinValue;
	int32 shardMaxValue;
	uint64 nodeId;               /* node where child shard is to be placed */
	char slotName[NAMEDATALEN];  /* replication slot name belonging to this node */
} ShardSplitInfo;

/*
 * SplitShard API to split a given shard (or shard group) in blocking / non-blocking fashion
 * based on specified split points to a set of destination nodes.
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
