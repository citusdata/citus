/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.c
 *
 * Function definitions for logically replicating shard to split children.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"

#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/priority.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_split.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/shared_library_init.h"


static HTAB *ShardInfoHashMapForPublications = NULL;

/* function declarations */
static void AddPublishableShardEntryInMap(uint32 targetNodeId,
										  ShardInterval *shardInterval, bool
										  isChildShardInterval);
static LogicalRepTarget * CreateLogicalRepTarget(Oid tableOwnerId,
												 uint32 nodeId,
												 List *replicationSlotInfoList);

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
	ShardInfoHashMapForPublications = CreateSimpleHash(NodeAndOwner, PublicationInfo);
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		/*
		 * Skipping partitioned table for logical replication.
		 * Since PG13, logical replication is supported for partitioned tables.
		 * However, we want to keep the behaviour consistent with shard moves.
		 */
		if (PartitionedTable(sourceShardIntervalToCopy->relationId))
		{
			continue;
		}

		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
					destinationWorkerNode, destinationWorkerNodesList)
		{
			uint32 destinationWorkerNodeId = destinationWorkerNode->nodeId;

			/* Add child shard for publication.
			 * If a columnar shard is a part of publications, then writes on the shard fail.
			 * In the case of local split, adding child shards to the publication
			 * would prevent copying the initial data done through 'DoSplitCopy'.
			 * Hence we avoid adding columnar child shards to publication.
			 */
			if (!extern_IsColumnarTableAmTable(splitChildShardInterval->relationId))
			{
				AddPublishableShardEntryInMap(destinationWorkerNodeId,
											  splitChildShardInterval,
											  true /*isChildShardInterval*/);
			}

			/* Add parent shard if not already added */
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
	NodeAndOwner key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	PublicationInfo *publicationInfo =
		(PublicationInfo *) hash_search(ShardInfoHashMapForPublications, &key,
										HASH_ENTER,
										&found);

	/* Create a new list for <nodeId, owner> pair */
	if (!found)
	{
		publicationInfo->shardIntervals = NIL;
		publicationInfo->name = PublicationName(SHARD_SPLIT, key.nodeId,
												key.tableOwnerId);
	}

	/* Add child shard interval */
	if (isChildShardInterval)
	{
		publicationInfo->shardIntervals =
			lappend(publicationInfo->shardIntervals, shardInterval);

		/* We return from here as the child interval is only added once in the list */
		return;
	}

	/* Check if parent is already added */
	ShardInterval *existingShardInterval = NULL;
	foreach_declared_ptr(existingShardInterval, publicationInfo->shardIntervals)
	{
		if (existingShardInterval->shardId == shardInterval->shardId)
		{
			/* parent shard interval is already added hence return */
			return;
		}
	}

	/* Add parent shard Interval */
	publicationInfo->shardIntervals =
		lappend(publicationInfo->shardIntervals, shardInterval);
}


/*
 * PopulateShardSplitSubscriptionsMetadataList returns a list of 'LogicalRepTarget'
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
											List *replicationSlotInfoList,
											List *shardGroupSplitIntervalListList,
											List *workersForPlacementList)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardSplitInfoHashMap);

	PublicationInfo *publication = NULL;
	List *logicalRepTargetList = NIL;
	while ((publication = (PublicationInfo *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = publication->key.nodeId;
		uint32 tableOwnerId = publication->key.tableOwnerId;
		LogicalRepTarget *target =
			CreateLogicalRepTarget(tableOwnerId, nodeId,
								   replicationSlotInfoList);
		target->publication = publication;
		publication->target = target;

		logicalRepTargetList = lappend(logicalRepTargetList, target);
	}

	List *shardIntervalList = NIL;
	foreach_declared_ptr(shardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *shardInterval = NULL;
		WorkerNode *workerPlacementNode = NULL;
		forboth_ptr(shardInterval, shardIntervalList, workerPlacementNode,
					workersForPlacementList)
		{
			NodeAndOwner key;
			key.nodeId = workerPlacementNode->nodeId;
			key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

			bool found = false;
			publication = (PublicationInfo *) hash_search(
				ShardInfoHashMapForPublications,
				&key,
				HASH_FIND,
				&found);
			if (!found)
			{
				ereport(ERROR, errmsg("Could not find publication matching a split"));
			}
			publication->target->newShards = lappend(
				publication->target->newShards, shardInterval);
		}
	}

	return logicalRepTargetList;
}


/*
 * Creates a 'LogicalRepTarget' structure for given table owner, node id.
 * It scans the list of 'ReplicationSlotInfo' to identify the corresponding slot
 * to be used for given tableOwnerId and nodeId.
 */
static LogicalRepTarget *
CreateLogicalRepTarget(Oid tableOwnerId, uint32 nodeId,
					   List *replicationSlotInfoList)
{
	LogicalRepTarget *target = palloc0(sizeof(LogicalRepTarget));
	target->subscriptionName = SubscriptionName(SHARD_SPLIT, tableOwnerId);
	target->tableOwnerId = tableOwnerId;
	target->subscriptionOwnerName =
		SubscriptionRoleName(SHARD_SPLIT, tableOwnerId);
	target->superuserConnection = NULL;

	/*
	 * Each 'ReplicationSlotInfo' belongs to a unique combination of node id and owner.
	 * Traverse the slot list to identify the corresponding slot for given
	 * table owner and node.
	 */
	ReplicationSlotInfo *replicationSlot = NULL;
	foreach_declared_ptr(replicationSlot, replicationSlotInfoList)
	{
		if (nodeId == replicationSlot->targetNodeId &&
			tableOwnerId == replicationSlot->tableOwnerId)
		{
			target->replicationSlot = replicationSlot;

			break;
		}
	}

	if (!target->replicationSlot)
	{
		ereport(ERROR, errmsg(
					"Could not find replication slot matching a subscription %s",
					target->subscriptionName));
	}

	return target;
}
