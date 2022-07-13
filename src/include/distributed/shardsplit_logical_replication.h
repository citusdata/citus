/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_LOGICAL_REPLICATION_H
#define SHARDSPLIT_LOGICAL_REPLICATION_H

typedef struct ReplicationSlotInfo
{
    uint32 targetNodeId;
    char * tableOwnerName;
    char * slotName;
} ReplicationSlotInfo;

typedef struct ShardSplitPubSubMetadata
{
	List * shardIntervalListForSubscription;
	Oid tableOwnerId;
    ReplicationSlotInfo *slotInfo;
} ShardSplitPubSubMetadata;

/* key for NodeShardMappingEntry */
typedef struct NodeShardMappingKey
{
	uint32_t nodeId;
	Oid tableOwnerId;
} NodeShardMappingKey;

/* Entry for hash map */
typedef struct NodeShardMappingEntry
{
	NodeShardMappingKey key;
	List *shardSplitInfoList;
} NodeShardMappingEntry;

extern uint32 NodeShardMappingHash(const void *key, Size keysize);
extern int NodeShardMappingHashCompare(const void *left, const void *right, Size keysize);
HTAB * SetupHashMapForShardInfo(void);

List * ParseReplicationSlotInfoFromResult(PGresult * result);

extern StringInfo CreateSplitShardReplicationSetupUDF(List *sourceColocatedShardIntervalList,
									   List *shardGroupSplitIntervalListList,
									   List *destinationWorkerNodesList);

extern List *  CreateShardSplitPubSubMetadataList(List *sourceColocatedShardIntervalList,
									   List *shardGroupSplitIntervalListList,
									   List *destinationWorkerNodesList,
									   List *replicationSlotInfoList);

extern void LogicallReplicateSplitShards(WorkerNode *sourceWorkerNode, List* shardSplitPubSubMetadataList);
#endif /* SHARDSPLIT_LOGICAL_REPLICATION_H */