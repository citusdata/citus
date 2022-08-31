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

#include "distributed/utils/distribution_column_map.h"

/* Split Modes supported by Shard Split API */
typedef enum SplitMode
{
	BLOCKING_SPLIT = 0,
	NON_BLOCKING_SPLIT = 1,
	AUTO_SPLIT = 2
} SplitMode;


/*
 * User Scenario calling Split Shard API.
 * The 'SplitOperation' type is used to customize info/error messages based on user scenario.
 */
typedef enum SplitOperation
{
	SHARD_SPLIT_API = 0,
	ISOLATE_TENANT_TO_NEW_SHARD,
	CREATE_DISTRIBUTED_TABLE
} SplitOperation;

/*
 * SplitShard API to split a given shard (or shard group) using split mode and
 * specified split points to a set of destination nodes.
 */
extern void SplitShard(SplitMode splitMode,
					   SplitOperation splitOperation,
					   uint64 shardIdToSplit,
					   List *shardSplitPointsList,
					   List *nodeIdsForPlacementList,
					   DistributionColumnMap *distributionColumnOverrides,
					   List *colocatedShardIntervalList,
					   uint32 targetColocationId);

extern SplitMode LookupSplitMode(Oid shardTransferModeOid);

extern void ErrorIfMultipleNonblockingMoveSplitInTheSameTransaction(void);

#endif /* SHARDSPLIT_H_ */
