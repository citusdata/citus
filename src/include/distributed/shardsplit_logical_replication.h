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

#include "distributed/multi_logical_replication.h"

typedef struct ReplicationSlotInfo
{
	uint32 targetNodeId;
	char *tableOwnerName;
	char *slotName;
} ReplicationSlotInfo;

typedef struct ShardSplitSubscriberMetadata
{
	Oid tableOwnerId;
	ReplicationSlotInfo *slotInfo;

	/*
	 * Exclusively claimed connection for subscription.The target node of subscription
	 * is pointed by ReplicationSlotInfo.
	 */
	MultiConnection *targetNodeConnection;
} ShardSplitSubscriberMetadata;

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

extern List * ParseReplicationSlotInfoFromResult(PGresult *result);


extern HTAB *  CreateShardSplitInfoMapForPublication(
	List *sourceColocatedShardIntervalList,
	List *shardGroupSplitIntervalListList,
	List *destinationWorkerNodesList);

extern void LogicallyReplicateSplitShards(WorkerNode *sourceWorkerNode,
										  List *shardSplitPubSubMetadataList,
										  List *sourceColocatedShardIntervalList,
										  List *shardGroupSplitIntervalListList,
										  List *destinationWorkerNodesList);
extern void CreateShardSplitPublications(MultiConnection *sourceConnection,
										 HTAB *shardInfoHashMapForPublication);
extern void DropAllShardSplitLeftOvers(WorkerNode *sourceNode,
									   HTAB *shardSplitMapOfPublications);

extern List * PopulateShardSplitSubscriptionsMetadataList(HTAB *shardSplitInfoHashMap,
														  List *replicationSlotInfoList);

extern char *  DropExistingIfAnyAndCreateTemplateReplicationSlot(
	ShardInterval *shardIntervalToSplit,
	MultiConnection *
	sourceConnection);

extern void CreateShardSplitSubscriptions(List *targetNodeConnectionList,
										  List *shardSplitPubSubMetadataList,
										  WorkerNode *sourceWorkerNode, char *superUser,
										  char *databaseName);
extern void WaitForShardSplitRelationSubscriptionsBecomeReady(
	List *shardSplitPubSubMetadataList);
extern void WaitForShardSplitRelationSubscriptionsToBeCaughtUp(XLogRecPtr sourcePosition,
															   List *
															   shardSplitPubSubMetadataList);

List * CreateTargetNodeConnectionsForShardSplit(List *shardSplitSubscribersMetadataList,
												int
												connectionFlags, char *user,
												char *databaseName);

/*used for debuggin. Remove later*/
extern void PrintShardSplitPubSubMetadata(
	ShardSplitSubscriberMetadata *shardSplitMetadata);
#endif /* SHARDSPLIT_LOGICAL_REPLICATION_H */
