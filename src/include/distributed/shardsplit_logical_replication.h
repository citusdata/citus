/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.h
 *
 * Function declarations for logically replicating shard to split children.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_LOGICAL_REPLICATION_H
#define SHARDSPLIT_LOGICAL_REPLICATION_H

#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/worker_manager.h"

/*
 * GroupedShardSplitInfos groups all ShardSplitInfos belonging to the same node
 * and table owner together. This data structure its only purpose is creating a
 * hashmap that allows us to search ShardSplitInfos by node and owner.
 */
typedef struct GroupedShardSplitInfos
{
	NodeAndOwner key;
	List *shardSplitInfoList;
} GroupedShardSplitInfos;


/* Functions for subscriber metadata management */
extern List * PopulateShardSplitSubscriptionsMetadataList(HTAB *shardSplitInfoHashMap,
														  List *replicationSlotInfoList,
														  List *
														  shardGroupSplitIntervalListList,
														  List *workersForPlacementList);
extern HTAB *  CreateShardSplitInfoMapForPublication(
	List *sourceColocatedShardIntervalList,
	List *shardGroupSplitIntervalListList,
	List *destinationWorkerNodesList);

/* Functions to drop publisher-subscriber resources */
extern void DropAllShardSplitLeftOvers(WorkerNode *sourceNode,
									   HTAB *shardSplitMapOfPublications);
extern void DropShardSplitPublications(MultiConnection *sourceConnection,
									   HTAB *shardInfoHashMapForPublication);
extern void DropShardSplitSubsriptions(List *shardSplitSubscribersMetadataList);
extern void DropShardSplitReplicationSlots(MultiConnection *sourceConnection,
										   List *replicationSlotInfoList);

#endif /* SHARDSPLIT_LOGICAL_REPLICATION_H */
