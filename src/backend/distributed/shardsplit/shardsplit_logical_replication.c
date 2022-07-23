/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/listutils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/resource_lock.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"


static HTAB *ShardInfoHashMapForPublications = NULL;

/* function declarations */
static void AddPublishableShardEntryInMap(uint32 targetNodeId,
										  ShardInterval *shardInterval, bool
										  isChildShardInterval);
ShardSplitSubscriberMetadata * CreateShardSplitSubscriberMetadata(Oid tableOwnerId, uint32
																  nodeId,
																  List *
																  replicationSlotInfoList);
static void CreateShardSplitPublicationForNode(MultiConnection *connection,
											   List *shardList,
											   uint32_t publicationForTargetNodeId, Oid
											   tableOwner);
static char * ShardSplitPublicationName(uint32_t nodeId, Oid ownerId);
static void DropAllShardSplitSubscriptions(MultiConnection *cleanupConnection);
static void DropAllShardSplitPublications(MultiConnection *cleanupConnection);
static void DropAllShardSplitUsers(MultiConnection *cleanupConnection);
static void DropAllShardSplitReplicationSlots(MultiConnection *cleanupConnection);


/*
 * CreateShardSplitInfoMapForPublication creates a hashmap that groups
 * shards for creating publications and subscriptions.
 *
 * While creating publications and subscriptions, apart from table owners,
 * placement of child shard matters too. To further understand this, please see
 * the following example:
 *
 * Shard1(on Worker1) is to be split in Shard2 and Shard3 on Worker2 and Worker3 respectively.
 * Lets assume the owner to be 'A'. The hashmap groups shard list in the following way.
 *
 * Map key
 * =======	               ------     ------
 * <Worker2, 'A'> ------> |Shard2|-->|Shard1|
 *                         ------     ------
 *
 *                         ------     ------
 * <Worker3, 'A'> ------> |Shard3|-->|Shard1|
 *                         ------     ------
 * Shard1 is a dummy table that is to be created on Worker2 and Worker3.
 * Based on the above placement, we would need to create two publications on the source node.
 */
HTAB *
CreateShardSplitInfoMapForPublication(List *sourceColocatedShardIntervalList,
									  List *shardGroupSplitIntervalListList,
									  List *destinationWorkerNodesList)
{
	ShardInfoHashMapForPublications = SetupHashMapForShardInfo();
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
					destinationWorkerNode, destinationWorkerNodesList)
		{
			uint32 destinationWorkerNodeId = destinationWorkerNode->nodeId;

			/* Add split child shard interval */
			AddPublishableShardEntryInMap(destinationWorkerNodeId,
										  splitChildShardInterval,
										  true /*isChildShardInterval*/);

			/* Add parent shard interval if not already added */
			AddPublishableShardEntryInMap(destinationWorkerNodeId,
										  sourceShardIntervalToCopy,
										  false /*isChildShardInterval*/);
		}
	}

	return ShardInfoHashMapForPublications;
}


/*
 * AddPublishableShardEntryInMap adds a shard interval in the list
 * of shards to be published.
 */
static void
AddPublishableShardEntryInMap(uint32 targetNodeId, ShardInterval *shardInterval, bool
							  isChildShardInterval)
{
	NodeShardMappingKey key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(ShardInfoHashMapForPublications, &key,
											  HASH_ENTER,
											  &found);

	/* Create a new list for <nodeId, owner> pair */
	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NIL;
	}

	/* Add child shard interval */
	if (isChildShardInterval)
	{
		nodeMappingEntry->shardSplitInfoList =
			lappend(nodeMappingEntry->shardSplitInfoList,
					(ShardInterval *) shardInterval);

		/* We return from here as the child interval is only added once in the list */
		return;
	}

	/* Check if parent is already added */
	ShardInterval *existingShardInterval = NULL;
	foreach_ptr(existingShardInterval, nodeMappingEntry->shardSplitInfoList)
	{
		if (existingShardInterval->shardId == shardInterval->shardId)
		{
			/* parent shard interval is already added hence return */
			return;
		}
	}

	/* Add parent shard Interval */
	nodeMappingEntry->shardSplitInfoList =
		lappend(nodeMappingEntry->shardSplitInfoList, (ShardInterval *) shardInterval);
}


/*
 * CreateShardSplitPublications creates publications on the source node.
 *
 * sourceConnection - Connection of source node.
 *
 * shardInfoHashMapForPublication - ShardIntervals are grouped by <owner, nodeId> key.
 *                                  A publication is created for list of
 *                                  ShardIntervals mapped by key.
 */
void
CreateShardSplitPublications(MultiConnection *sourceConnection,
							 HTAB *shardInfoHashMapForPublication)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMapForPublication);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		uint32 tableOwnerId = entry->key.tableOwnerId;
		List *shardListForPublication = entry->shardSplitInfoList;

		/* Create publication on shard list */
		CreateShardSplitPublicationForNode(sourceConnection,
										   shardListForPublication,
										   nodeId,
										   tableOwnerId);
	}
}


/*
 * CreateShardSplitPublicationForNode creates a publication on source node
 * for given shard list.
 * We follow the 'SHARD_SPLIT_X_PREFIX' naming scheme for creating publications
 * related to split operations.
 */
static void
CreateShardSplitPublicationForNode(MultiConnection *connection, List *shardList,
								   uint32_t publicationForTargetNodeId, Oid ownerId)
{
	StringInfo createPublicationCommand = makeStringInfo();
	bool prefixWithComma = false;

	appendStringInfo(createPublicationCommand, "CREATE PUBLICATION %s FOR TABLE ",
					 ShardSplitPublicationName(publicationForTargetNodeId, ownerId));

	ShardInterval *shard = NULL;
	foreach_ptr(shard, shardList)
	{
		char *shardName = ConstructQualifiedShardName(shard);

		if (prefixWithComma)
		{
			appendStringInfoString(createPublicationCommand, ",");
		}

		appendStringInfoString(createPublicationCommand, shardName);
		prefixWithComma = true;
	}

	ExecuteCriticalRemoteCommand(connection, createPublicationCommand->data);
	pfree(createPublicationCommand->data);
	pfree(createPublicationCommand);
}


/*
 * ShardSplitPublicationName returns publication name for Shard Split operations.
 */
static char *
ShardSplitPublicationName(uint32_t nodeId, Oid ownerId)
{
	return psprintf("%s%u_%u", SHARD_SPLIT_PUBLICATION_PREFIX, nodeId, ownerId);
}


List *
CreateTargetNodeConnectionsForShardSplit(List *shardSplitSubscribersMetadataList, int
										 connectionFlags, char *user, char *databaseName)
{
	List *targetNodeConnectionList = NIL;
	ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata = NULL;
	foreach_ptr(shardSplitSubscriberMetadata, shardSplitSubscribersMetadataList)
	{
		/*TODO(saawasek):For slot equals not null */
		uint32 targetWorkerNodeId = shardSplitSubscriberMetadata->slotInfo->targetNodeId;
		WorkerNode *targetWorkerNode = FindNodeWithNodeId(targetWorkerNodeId, false);

		MultiConnection *targetConnection =
			GetNodeUserDatabaseConnection(connectionFlags, targetWorkerNode->workerName,
										  targetWorkerNode->workerPort,
										  user,
										  databaseName);
		ClaimConnectionExclusively(targetConnection);

		targetNodeConnectionList = lappend(targetNodeConnectionList, targetConnection);

		shardSplitSubscriberMetadata->targetNodeConnection = targetConnection;
	}

	return targetNodeConnectionList;
}


/*
 * PopulateShardSplitSubscriptionsMetadataList returns a list of 'ShardSplitSubscriberMetadata'
 * structure.
 *
 * shardSplitInfoHashMap - Shards are grouped by <owner, node id> key.
 *                         For each key, we create a metadata structure. This facilitates easy
 *                         publication-subscription management.
 *
 * replicationSlotInfoList - List of replication slot info.
 */
List *
PopulateShardSplitSubscriptionsMetadataList(HTAB *shardSplitInfoHashMap,
											List *replicationSlotInfoList)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardSplitInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	List *shardSplitSubscriptionMetadataList = NIL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		uint32 tableOwnerId = entry->key.tableOwnerId;
		ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata =
			CreateShardSplitSubscriberMetadata(tableOwnerId, nodeId,
											   replicationSlotInfoList);

		shardSplitSubscriptionMetadataList = lappend(shardSplitSubscriptionMetadataList,
													 shardSplitSubscriberMetadata);
	}

	return shardSplitSubscriptionMetadataList;
}


/*
 * Creates a 'ShardSplitSubscriberMetadata' structure for given table owner, node id.
 * It scans the list of 'ReplicationSlotInfo' to identify the corresponding slot
 * to be used for given tableOwnerId and nodeId.
 */
ShardSplitSubscriberMetadata *
CreateShardSplitSubscriberMetadata(Oid tableOwnerId, uint32 nodeId,
								   List *replicationSlotInfoList)
{
	ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata = palloc0(
		sizeof(ShardSplitSubscriberMetadata));
	shardSplitSubscriberMetadata->tableOwnerId = tableOwnerId;

	/*
	 * Each 'ReplicationSlotInfo' belongs to a unique combination of node id and owner.
	 * Traverse the slot list to identify the corresponding slot for given
	 * table owner and node.
	 */
	char *tableOwnerName = GetUserNameFromId(tableOwnerId, false);
	ReplicationSlotInfo *replicationSlotInfo = NULL;
	foreach_ptr(replicationSlotInfo, replicationSlotInfoList)
	{
		if (nodeId == replicationSlotInfo->targetNodeId &&
			strcmp(tableOwnerName, replicationSlotInfo->tableOwnerName) == 0)
		{
			shardSplitSubscriberMetadata->slotInfo = replicationSlotInfo;
			break;
		}
	}

	return shardSplitSubscriberMetadata;
}


/*
 * CreateShardSplitSubscriptions creates subscriptions for Shard Split operation.
 * We follow Shard Split naming scheme for Publication-Subscription management.
 *
 * targetNodeConnectionList - List of connections to target nodes on which
 *                            subscriptions have to be created.
 *
 * shardSplitSubscriberMetadataList - List of subscriber metadata.
 *
 * sourceWorkerNode - Source node.
 */
void
CreateShardSplitSubscriptions(List *targetNodeConnectionList,
							  List *shardSplitSubscriberMetadataList,
							  WorkerNode *sourceWorkerNode,
							  char *superUser,
							  char *databaseName)
{
	MultiConnection *targetConnection = NULL;
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	forboth_ptr(targetConnection, targetNodeConnectionList,
				shardSplitPubSubMetadata, shardSplitSubscriberMetadataList)
	{
		uint32 publicationForNodeId = shardSplitPubSubMetadata->slotInfo->targetNodeId;
		Oid ownerId = shardSplitPubSubMetadata->tableOwnerId;
		CreateShardSubscription(targetConnection,
								sourceWorkerNode->workerName,
								sourceWorkerNode->workerPort,
								superUser,
								databaseName,
								ShardSplitPublicationName(publicationForNodeId, ownerId),
								shardSplitPubSubMetadata->slotInfo->slotName,
								ownerId);
	}
}


/*
 * WaitForShardSplitRelationSubscriptionsBecomeReady waits for a list of subscriptions
 * to be come ready. This method invokes 'WaitForRelationSubscriptionsBecomeReady' for each
 * subscription.
 */
void
WaitForShardSplitRelationSubscriptionsBecomeReady(List *shardSplitPubSubMetadataList)
{
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
	{
		Bitmapset *tableOwnerIds = NULL;
		tableOwnerIds = bms_add_member(tableOwnerIds,
									   shardSplitPubSubMetadata->tableOwnerId);
		WaitForRelationSubscriptionsBecomeReady(
			shardSplitPubSubMetadata->targetNodeConnection, tableOwnerIds,
			SHARD_SPLIT_SUBSCRIPTION_PREFIX);
	}
}


/*
 * WaitForShardSplitRelationSubscriptionsToBeCaughtUp waits until subscriptions are caught up till
 * the source LSN. This method invokes 'WaitForShardSubscriptionToCatchUp' for each subscription.
 */
void
WaitForShardSplitRelationSubscriptionsToBeCaughtUp(XLogRecPtr sourcePosition,
												   List *shardSplitPubSubMetadataList)
{
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
	{
		Bitmapset *tableOwnerIds = NULL;
		tableOwnerIds = bms_add_member(tableOwnerIds,
									   shardSplitPubSubMetadata->tableOwnerId);

		WaitForShardSubscriptionToCatchUp(shardSplitPubSubMetadata->targetNodeConnection,
										  sourcePosition,
										  tableOwnerIds,
										  SHARD_SPLIT_SUBSCRIPTION_PREFIX);
	}
}


char *
DropExistingIfAnyAndCreateTemplateReplicationSlot(ShardInterval *shardIntervalToSplit,
												  MultiConnection *sourceConnection)
{
	/*
	 * To ensure SPLIT is idempotent drop any existing slot from
	 * previous failed operation.
	 */
	StringInfo dropReplicationSlotCommand = makeStringInfo();
	appendStringInfo(dropReplicationSlotCommand, "SELECT pg_drop_replication_slot('%s')",
					 ShardSplitTemplateReplicationSlotName(
						 shardIntervalToSplit->shardId));

	/* The Drop command can fail so ignore the response / result and proceed anyways */
	PGresult *result = NULL;
	int response = ExecuteOptionalRemoteCommand(sourceConnection,
												dropReplicationSlotCommand->data,
												&result);

	PQclear(result);
	ForgetResults(sourceConnection);

	/*
	 * Note: Temporary slot are only live during the session's lifetime causing them to be dropped when the session ends.
	 */
	StringInfo createReplicationSlotCommand = makeStringInfo();

	appendStringInfo(createReplicationSlotCommand,
					 "CREATE_REPLICATION_SLOT %s LOGICAL citus EXPORT_SNAPSHOT;",
					 ShardSplitTemplateReplicationSlotName(
						 shardIntervalToSplit->shardId));

	response = ExecuteOptionalRemoteCommand(sourceConnection,
											createReplicationSlotCommand->data, &result);

	if (response != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
	{
		ReportResultError(sourceConnection, result, ERROR);
	}

	/*'snapshot_name' is second column where index starts from zero.
	 * We're using the pstrdup to copy the data into the current memory context */
	char *snapShotName = pstrdup(PQgetvalue(result, 0, 2 /* columIndex */));
	return snapShotName;
}


/*
 * ShardSplitTemplateReplicationSlotName returns name of template replication slot
 * following the shard split naming scheme.
 */
char *
ShardSplitTemplateReplicationSlotName(uint64 shardId)
{
	return psprintf("%s%lu", SHARD_SPLIT_TEMPLATE_REPLICATION_SLOT_PREFIX, shardId);
}


/*
 * CreateReplicationSlots creates copies of template replication slot
 * on the source node.
 *
 * sourceNodeConnection - Source node connection.
 *
 * templateSlotName - Template replication slot name whose copies have to be created.
 *                    This slot holds a LSN from which the logical replication
 *                    begins.
 *
 * shardSplitSubscriberMetadataList - List of 'ShardSplitSubscriberMetadata. '
 *
 * 'ShardSplitSubscriberMetadata' contains replication slot name that is used
 *  to create copies of template replication slot on source node. These slot names are returned by
 * 'worker_split_shard_replication_setup' UDF and each slot is responsible for a specific
 * split range. We try multiple attemtps to clean up these replicaton slot copies in the
 * below order to be on safer side.
 * 1. Clean up before starting shard split workflow.
 * 2. Implicitly dropping slots while dropping subscriptions.
 * 3. Explicitly dropping slots which would have skipped over from 2.
 */
void
CreateReplicationSlots(MultiConnection *sourceNodeConnection, char *templateSlotName,
					   List *shardSplitSubscriberMetadataList)
{
	ShardSplitSubscriberMetadata *subscriberMetadata = NULL;
	foreach_ptr(subscriberMetadata, shardSplitSubscriberMetadataList)
	{
		char *slotName = subscriberMetadata->slotInfo->slotName;

		StringInfo createReplicationSlotCommand = makeStringInfo();

		appendStringInfo(createReplicationSlotCommand,
						 "SELECT * FROM  pg_copy_logical_replication_slot ('%s','%s')",
						 templateSlotName, slotName);

		PGresult *result = NULL;
		int response = ExecuteOptionalRemoteCommand(sourceNodeConnection,
													createReplicationSlotCommand->data,
													&result);
		if (response != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
		{
			ReportResultError(sourceNodeConnection, result, ERROR);
		}

		PQclear(result);
		ForgetResults(sourceNodeConnection);
	}
}


/*
 * DropAllShardSplitLeftOvers drops shard split subscriptions, publications, roles
 * and replication slots. These might have been left there after
 * the coordinator crashed during a shard split. It's important to delete them
 * for two reasons:
 * 1. Starting new shard split might fail when they exist, because it cannot
 *    create them.
 * 2. Leftover replication slots that are not consumed from anymore make it
 *    impossible for WAL to be dropped. This can cause out-of-disk issues.
 */
void
DropAllShardSplitLeftOvers(WorkerNode *sourceNode, HTAB *shardSplitHashMapForPubSub)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	/*
	 * We open new connections to all nodes. The reason for this is that
	 * operations on subscriptions and publications cannot be run in a
	 * transaction. By forcing a new connection we make sure no transaction is
	 * active on the connection.
	 */
	int connectionFlags = FORCE_NEW_CONNECTION;

	HASH_SEQ_STATUS statusForSubscription;
	hash_seq_init(&statusForSubscription, shardSplitHashMapForPubSub);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&statusForSubscription)) !=
		   NULL)
	{
		uint32_t nodeId = entry->key.nodeId;
		WorkerNode *workerNode = FindNodeWithNodeId(nodeId, false /*missingOk*/);

		MultiConnection *cleanupConnection = GetNodeUserDatabaseConnection(
			connectionFlags, workerNode->workerName, workerNode->workerPort,
			superUser, databaseName);

		/* We need to claim the connection exclusively while dropping the subscription */
		ClaimConnectionExclusively(cleanupConnection);

		DropAllShardSplitSubscriptions(cleanupConnection);
		DropAllShardSplitUsers(cleanupConnection);

		/* Close connection after cleanup */
		CloseConnection(cleanupConnection);
	}

	/*Drop all shard split publications at the source*/
	MultiConnection *sourceNodeConnection = GetNodeUserDatabaseConnection(
		connectionFlags, sourceNode->workerName, sourceNode->workerPort,
		superUser, databaseName);

	ClaimConnectionExclusively(sourceNodeConnection);

	/*
	 * If replication slot could not be dropped while dropping the
	 * subscriber, drop it here.
	 */
	DropAllShardSplitReplicationSlots(sourceNodeConnection);
	DropAllShardSplitPublications(sourceNodeConnection);

	CloseConnection(sourceNodeConnection);
}


/*
 * DropAllShardSplitSubscriptions drops all the existing subscriptions that
 * match our shard split naming scheme on the node that the connection points
 * to.
 */
void
DropAllShardSplitSubscriptions(MultiConnection *cleanupConnection)
{
	char *query = psprintf(
		"SELECT subname FROM pg_subscription "
		"WHERE subname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_SUBSCRIPTION_PREFIX));
	List *subscriptionNameList = GetQueryResultStringList(cleanupConnection, query);
	char *subscriptionName = NULL;
	foreach_ptr(subscriptionName, subscriptionNameList)
	{
		DropShardSubscription(cleanupConnection, subscriptionName);
	}
}


/*
 * DropAllShardSplitPublications drops all the existing publications that
 * match our shard split naming scheme on the node that the connection points
 * to.
 */
static void
DropAllShardSplitPublications(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT pubname FROM pg_publication "
		"WHERE pubname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_PUBLICATION_PREFIX));
	List *publicationNameList = GetQueryResultStringList(connection, query);
	char *publicationName;
	foreach_ptr(publicationName, publicationNameList)
	{
		DropShardPublication(connection, publicationName);
	}
}


/*
 * DropAllShardSplitUsers drops all the users that match our shard split naming
 * scheme. The users are temporary created for shard splits.
 */
static void
DropAllShardSplitUsers(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT rolname FROM pg_roles "
		"WHERE rolname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_SUBSCRIPTION_ROLE_PREFIX));
	List *usernameList = GetQueryResultStringList(connection, query);
	char *username;
	foreach_ptr(username, usernameList)
	{
		DropShardUser(connection, username);
	}
}


/*
 * DropAllShardSplitReplicationSlots drops all the existing replication slots
 * that match shard split naming scheme on the node that the connection
 * points to.
 */
static void
DropAllShardSplitReplicationSlots(MultiConnection *cleanupConnection)
{
	char *query = psprintf(
		"SELECT slot_name FROM pg_replication_slots "
		"WHERE slot_name LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_REPLICATION_SLOT_PREFIX));
	List *slotNameList = GetQueryResultStringList(cleanupConnection, query);
	char *slotName;
	foreach_ptr(slotName, slotNameList)
	{
		DropShardMoveReplicationSlot(cleanupConnection, slotName);
	}
}


/*
 * DropShardSplitPublications drops the publication used for shard splits over the given
 * connection, if it exists.
 */
void
DropShardSplitPublications(MultiConnection *sourceConnection,
						   HTAB *shardInfoHashMapForPublication)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMapForPublication);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		uint32 tableOwnerId = entry->key.tableOwnerId;
		DropShardPublication(sourceConnection, ShardSplitPublicationName(nodeId,
																		 tableOwnerId));
	}
}


/*
 * DropShardSplitSubsriptions drops subscriptions from the subscriber node that
 * are used to split shards for the given table owners. Note that, it drops the
 * replication slots on the publisher node if it can drop the slots as well
 * with the DROP SUBSCRIPTION command. Otherwise, only the subscriptions will
 * be deleted with DROP SUBSCRIPTION via the connection. In the latter case,
 * replication slots will be dropped separately by calling DropShardSplitReplicationSlots.
 */
void
DropShardSplitSubsriptions(List *shardSplitSubscribersMetadataList)
{
	ShardSplitSubscriberMetadata *subscriberMetadata = NULL;
	foreach_ptr(subscriberMetadata, shardSplitSubscribersMetadataList)
	{
		uint32 tableOwnerId = subscriberMetadata->tableOwnerId;
		MultiConnection *targetNodeConnection = subscriberMetadata->targetNodeConnection;

		DropShardSubscription(targetNodeConnection, ShardSubscriptionName(tableOwnerId,
																		  SHARD_SPLIT_SUBSCRIPTION_PREFIX));

		DropShardUser(targetNodeConnection, ShardSubscriptionRole(tableOwnerId,
																  SHARD_SPLIT_SUBSCRIPTION_ROLE_PREFIX));
	}
}


/*
 * CloseShardSplitSubscriberConnections closes connection of  subscriber nodes.
 * 'ShardSplitSubscriberMetadata' holds connection for a subscriber node. The method
 * traverses the list and closes each connection.
 */
void
CloseShardSplitSubscriberConnections(List *shardSplitSubscriberMetadataList)
{
	ShardSplitSubscriberMetadata *subscriberMetadata = NULL;
	foreach_ptr(subscriberMetadata, shardSplitSubscriberMetadataList)
	{
		CloseConnection(subscriberMetadata->targetNodeConnection);
	}
}
