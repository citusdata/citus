/*-------------------------------------------------------------------------
 *
 * multi_logical_replication.h
 *
 *    Declarations for public functions and variables used in logical replication
 *    on the distributed tables while moving shards.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTI_LOGICAL_REPLICATION_H_
#define MULTI_LOGICAL_REPLICATION_H_

#include "c.h"

#include "nodes/pg_list.h"

#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/shard_cleaner.h"


/* Config variables managed via guc.c */
extern int LogicalReplicationTimeout;

extern bool PlacementMovedUsingLogicalReplicationInTX;

/*
 * NodeAndOwner should be used as a key for structs that should be hashed by a
 * combination of node and owner.
 */
typedef struct NodeAndOwner
{
	uint32_t nodeId;
	Oid tableOwnerId;
} NodeAndOwner;
assert_valid_hash_key2(NodeAndOwner, nodeId, tableOwnerId);


/*
 * ReplicationSlotInfo stores the info that defines a replication slot. For
 * shard splits this information is built by parsing the result of the
 * 'worker_split_shard_replication_setup' UDF.
 */
typedef struct ReplicationSlotInfo
{
	uint32 targetNodeId;
	Oid tableOwnerId;
	char *name;
} ReplicationSlotInfo;

/*
 * PublicationInfo stores the information that defines a publication.
 */
typedef struct PublicationInfo
{
	NodeAndOwner key;
	char *name;
	List *shardIntervals;
	struct LogicalRepTarget *target;
} PublicationInfo;

/*
 * Stores information necesary to create all the th
 */
typedef struct LogicalRepTarget
{
	/*
	 * The Oid of the user that owns the shards in newShards. This Oid is the
	 * Oid of the user on the coordinator, this Oid is likely different than
	 * the Oid of the user on the logical replication source or target.
	 */
	Oid tableOwnerId;
	char *subscriptionName;

	/*
	 * The name of the user that's used as the owner of the subscription. This
	 * is not the same as the name of the user that matches tableOwnerId.
	 * Instead we create a temporary user with the same permissions as that
	 * user, with its only purpose being owning the subscription.
	 */
	char *subscriptionOwnerName;
	ReplicationSlotInfo *replicationSlot;
	PublicationInfo *publication;

	/*
	 * The shardIntervals that we want to create on this logical replication
	 * target. This can be different from the shard intervals that are part of
	 * the publication for two reasons:
	 * 1. The publication does not contain partitioned tables, only their
	 *    children. The partition parent tables ARE part of newShards.
	 * 2. For shard splits the publication also contains dummy shards, these
	 *    ARE NOT part of newShards.
	 */
	List *newShards;

	/*
	 * The superuserConnection is shared between all LogicalRepTargets that have
	 * the same node. This can be initialized easily by using
	 * CreateGroupedLogicalRepTargetsConnections.
	 */
	MultiConnection *superuserConnection;
} LogicalRepTarget;

/*
 * GroupedLogicalRepTargets groups LogicalRepTargets by node. This allows to
 * create a hashmap where we can filter by search by nodeId. Which is useful
 * because these targets can all use the same superuserConection for
 * management, which allows us to batch certain operations such as getting
 * state of the subscriptions.
 */
typedef struct GroupedLogicalRepTargets
{
	uint32 nodeId;
	List *logicalRepTargetList;
	MultiConnection *superuserConnection;
} GroupedLogicalRepTargets;


/*
 * LogicalRepType is used for various functions to do something different for
 * shard moves than for shard splits. Such as using a different prefix for a
 * subscription name.
 */
typedef enum LogicalRepType
{
	SHARD_MOVE,
	SHARD_SPLIT,
} LogicalRepType;

extern void LogicallyReplicateShards(List *shardList, char *sourceNodeName,
									 int sourceNodePort, char *targetNodeName,
									 int targetNodePort);

extern void ConflictWithIsolationTestingBeforeCopy(void);
extern void ConflictWithIsolationTestingAfterCopy(void);
extern void CreateReplicaIdentities(List *subscriptionInfoList);
extern void CreateReplicaIdentitiesOnNode(List *shardList,
										  char *nodeName,
										  int32 nodePort);
extern XLogRecPtr GetRemoteLogPosition(MultiConnection *connection);
extern List * GetQueryResultStringList(MultiConnection *connection, char *query);

extern MultiConnection * GetReplicationConnection(char *nodeName, int nodePort);
extern void CreatePublications(MultiConnection *sourceConnection,
							   HTAB *publicationInfoHash);
extern void CreateSubscriptions(MultiConnection *sourceConnection,
								char *databaseName, List *subscriptionInfoList);
extern char * CreateReplicationSlots(MultiConnection *sourceConnection,
									 MultiConnection *sourceReplicationConnection,
									 List *subscriptionInfoList,
									 char *outputPlugin);
extern void EnableSubscriptions(List *subscriptionInfoList);

extern char * PublicationName(LogicalRepType type, uint32_t nodeId, Oid ownerId);
extern char * ReplicationSlotNameForNodeAndOwnerForOperation(LogicalRepType type,
															 uint32_t nodeId,
															 Oid ownerId,
															 OperationId operationId);
extern char * SubscriptionName(LogicalRepType type, Oid ownerId);
extern char * SubscriptionRoleName(LogicalRepType type, Oid ownerId);

extern void WaitForAllSubscriptionsToCatchUp(MultiConnection *sourceConnection,
											 HTAB *groupedLogicalRepTargetsHash);
extern void WaitForShardSubscriptionToCatchUp(MultiConnection *targetConnection,
											  XLogRecPtr sourcePosition,
											  Bitmapset *tableOwnerIds,
											  char *operationPrefix);
extern HTAB * CreateGroupedLogicalRepTargetsHash(List *subscriptionInfoList);
extern void CreateGroupedLogicalRepTargetsConnections(HTAB *groupedLogicalRepTargetsHash,
													  char *user,
													  char *databaseName);
extern void CloseGroupedLogicalRepTargetsConnections(HTAB *groupedLogicalRepTargetsHash);
extern void CompleteNonBlockingShardTransfer(List *shardList,
											 MultiConnection *sourceConnection,
											 HTAB *publicationInfoHash,
											 List *logicalRepTargetList,
											 HTAB *groupedLogicalRepTargetsHash,
											 LogicalRepType type);
extern void CreateUncheckedForeignKeyConstraints(List *logicalRepTargetList);
extern void CreatePartitioningHierarchy(List *logicalRepTargetList);

#endif /* MULTI_LOGICAL_REPLICATION_H_ */
