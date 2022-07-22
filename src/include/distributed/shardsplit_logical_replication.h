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

/*
 * Invocation of 'worker_split_shard_replication_setup' UDF returns set of records
 * of custom datatype 'replication_slot_info'. This information is parsed and stored in
 * the below data structure. The information is used to create a subscriber on target node
 * with corresponding slot name.
 */
typedef struct ReplicationSlotInfo
{
	uint32 targetNodeId;
	char *tableOwnerName;
	char *slotName;
} ReplicationSlotInfo;

/*
 * Stores information necesary for creating a subscriber on target node.
 * Based on how a shard is split and mapped to target nodes, for each unique combination of
 * <tableOwner, targetNodeId> there is a 'ShardSplitSubscriberMetadata'.
 */
typedef struct ShardSplitSubscriberMetadata
{
	Oid tableOwnerId;
	ReplicationSlotInfo *slotInfo;

	/*
	 * Exclusively claimed connection for a subscription.The target node of subscription
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
extern HTAB * SetupHashMapForShardInfo(void);

/* Functions for subscriber metadata management */
extern List * ParseReplicationSlotInfoFromResult(PGresult *result);
extern List * PopulateShardSplitSubscriptionsMetadataList(HTAB *shardSplitInfoHashMap,
														  List *replicationSlotInfoList);
extern HTAB *  CreateShardSplitInfoMapForPublication(
	List *sourceColocatedShardIntervalList,
	List *shardGroupSplitIntervalListList,
	List *destinationWorkerNodesList);

/* Functions for creating publications and subscriptions*/
extern void CreateShardSplitPublications(MultiConnection *sourceConnection,
										 HTAB *shardInfoHashMapForPublication);
extern void CreateShardSplitSubscriptions(List *targetNodeConnectionList,
										  List *shardSplitPubSubMetadataList,
										  WorkerNode *sourceWorkerNode, char *superUser,
										  char *databaseName);
extern void CreateReplicationSlots(MultiConnection *sourceNodeConnection,
								   char *templateSlotName,
								   List *shardSplitSubscriberMetadataList);
extern List * CreateTargetNodeConnectionsForShardSplit(
	List *shardSplitSubscribersMetadataList,
	int
	connectionFlags, char *user,
	char *databaseName);

/* Functions to drop publisher-subscriber resources */
extern void DropAllShardSplitLeftOvers(WorkerNode *sourceNode,
									   HTAB *shardSplitMapOfPublications);
extern void DropShardSplitPublications(MultiConnection *sourceConnection,
									   HTAB *shardInfoHashMapForPublication);
extern void DropShardSplitSubsriptions(List *shardSplitSubscribersMetadataList);
extern char *  DropExistingIfAnyAndCreateTemplateReplicationSlot(
	ShardInterval *shardIntervalToSplit,
	MultiConnection *
	sourceConnection);

/* Wrapper functions which wait for a subscriber to be ready and catchup */
extern void WaitForShardSplitRelationSubscriptionsBecomeReady(
	List *shardSplitPubSubMetadataList);
extern void WaitForShardSplitRelationSubscriptionsToBeCaughtUp(XLogRecPtr sourcePosition,
															   List *
															   shardSplitPubSubMetadataList);

extern char * ShardSplitTemplateReplicationSlotName(uint64 shardId);

extern void CloseShardSplitSubscriberConnections(List *shardSplitSubscriberMetadataList);
#endif /* SHARDSPLIT_LOGICAL_REPLICATION_H */
