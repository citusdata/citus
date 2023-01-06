/*-------------------------------------------------------------------------
 *
 * multi_logical_replication.c
 *
 * This file contains functions to use logical replication on the distributed
 * tables for moving/replicating shards.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "pgstat.h"
#include "libpq-fe.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"

#include "postmaster/interrupt.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "distributed/adaptive_executor.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/priority.h"
#include "distributed/distributed_planner.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_transfer.h"
#include "distributed/version_compat.h"
#include "nodes/bitmapset.h"
#include "parser/scansup.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#define STR_ERRCODE_UNDEFINED_OBJECT "42704"
#define STR_ERRCODE_OBJECT_IN_USE "55006"


#define REPLICATION_SLOT_CATALOG_TABLE_NAME "pg_replication_slots"
#define CURRENT_LOG_POSITION_COMMAND "SELECT pg_current_wal_lsn()"

/* decimal representation of Adler-16 hash value of citus_shard_move_publication */
#define SHARD_MOVE_ADVISORY_LOCK_FIRST_KEY 44000

/* decimal representation of Adler-16 hash value of citus_shard_move_subscription */
#define SHARD_MOVE_ADVISORY_LOCK_SECOND_KEY 55152

static const char *publicationPrefix[] = {
	[SHARD_MOVE] = "citus_shard_move_publication_",
	[SHARD_SPLIT] = "citus_shard_split_publication_",
};

static const char *replicationSlotPrefix[] = {
	[SHARD_MOVE] = "citus_shard_move_slot_",
	[SHARD_SPLIT] = "citus_shard_split_slot_",
};

/*
 * IMPORTANT: All the subscription names should start with "citus_". Otherwise
 * our utility hook does not defend against non-superusers altering or dropping
 * them, which is important for security purposes.
 */
static const char *subscriptionPrefix[] = {
	[SHARD_MOVE] = "citus_shard_move_subscription_",
	[SHARD_SPLIT] = "citus_shard_split_subscription_",
};

static const char *subscriptionRolePrefix[] = {
	[SHARD_MOVE] = "citus_shard_move_subscription_role_",
	[SHARD_SPLIT] = "citus_shard_split_subscription_role_",
};


/* GUC variable, defaults to 2 hours */
int LogicalReplicationTimeout = 2 * 60 * 60 * 1000;


/* see the comment in master_move_shard_placement */
bool PlacementMovedUsingLogicalReplicationInTX = false;

/* report in every 10 seconds */
static int logicalReplicationProgressReportTimeout = 10 * 1000;


static List * PrepareReplicationSubscriptionList(List *shardList);
static List * GetReplicaIdentityCommandListForShard(Oid relationId, uint64 shardId);
static List * GetIndexCommandListForShardBackingReplicaIdentity(Oid relationId,
																uint64 shardId);
static void CreatePostLogicalReplicationDataLoadObjects(List *logicalRepTargetList,
														LogicalRepType type);
static void ExecuteCreateIndexCommands(List *logicalRepTargetList);
static void ExecuteCreateConstraintsBackedByIndexCommands(List *logicalRepTargetList);
static List * ConvertNonExistingPlacementDDLCommandsToTasks(List *shardCommandList,
															char *targetNodeName,
															int targetNodePort);
static void ExecuteClusterOnCommands(List *logicalRepTargetList);
static void ExecuteCreateIndexStatisticsCommands(List *logicalRepTargetList);
static void ExecuteRemainingPostLoadTableCommands(List *logicalRepTargetList);
static char * escape_param_str(const char *str);
static XLogRecPtr GetRemoteLSN(MultiConnection *connection, char *command);
static bool RelationSubscriptionsAreReady(
	GroupedLogicalRepTargets *groupedLogicalRepTargets);
static void WaitForMiliseconds(long timeout);
static XLogRecPtr GetSubscriptionPosition(
	GroupedLogicalRepTargets *groupedLogicalRepTargets);
static void AcquireLogicalReplicationLock(void);
static void DropSubscription(MultiConnection *connection,
							 char *subscriptionName);
static void DropPublication(MultiConnection *connection, char *publicationName);

static void DropUser(MultiConnection *connection, char *username);
static void DropReplicationSlot(MultiConnection *connection,
								char *publicationName);
static void DropAllSubscriptions(MultiConnection *connection, LogicalRepType type);
static void DropAllReplicationSlots(MultiConnection *connection, LogicalRepType type);
static void DropAllPublications(MultiConnection *connection, LogicalRepType type);
static void DropAllUsers(MultiConnection *connection, LogicalRepType type);
static HTAB * CreateShardMovePublicationInfoHash(WorkerNode *targetNode,
												 List *shardIntervals);
static List * CreateShardMoveLogicalRepTargetList(HTAB *publicationInfoHash,
												  List *shardList);
static void WaitForGroupedLogicalRepTargetsToBecomeReady(
	GroupedLogicalRepTargets *groupedLogicalRepTargets);
static void WaitForGroupedLogicalRepTargetsToCatchUp(XLogRecPtr sourcePosition,
													 GroupedLogicalRepTargets *
													 groupedLogicalRepTargets);
static void RecreateGroupedLogicalRepTargetsConnections(
	HTAB *groupedLogicalRepTargetsHash,
	char *user,
	char *databaseName);

/*
 * LogicallyReplicateShards replicates a list of shards from one node to another
 * using logical replication. Once replication is reasonably caught up, writes
 * are blocked and then the publication and subscription are dropped.
 *
 * The caller of the function should ensure that logical replication is applicable
 * for the given shards, source and target nodes. Also, the caller is responsible
 * for ensuring that the input shard list consists of co-located distributed tables
 * or a single shard.
 */
void
LogicallyReplicateShards(List *shardList, char *sourceNodeName, int sourceNodePort,
						 char *targetNodeName, int targetNodePort)
{
	AcquireLogicalReplicationLock();
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);
	int connectionFlags = FORCE_NEW_CONNECTION;
	List *replicationSubscriptionList = PrepareReplicationSubscriptionList(shardList);

	/* no shards to move */
	if (list_length(replicationSubscriptionList) == 0)
	{
		return;
	}

	DropAllLogicalReplicationLeftovers(SHARD_MOVE);

	MultiConnection *sourceConnection =
		GetNodeUserDatabaseConnection(connectionFlags, sourceNodeName, sourceNodePort,
									  superUser, databaseName);

	/*
	 * Operations on publications and replication slots cannot run in a
	 * transaction block. We claim the connections exclusively to ensure they
	 * do not get used for metadata syncing, which does open a transaction
	 * block.
	 */
	ClaimConnectionExclusively(sourceConnection);

	WorkerNode *sourceNode = FindWorkerNode(sourceNodeName, sourceNodePort);
	WorkerNode *targetNode = FindWorkerNode(targetNodeName, targetNodePort);

	HTAB *publicationInfoHash = CreateShardMovePublicationInfoHash(
		targetNode, replicationSubscriptionList);

	List *logicalRepTargetList = CreateShardMoveLogicalRepTargetList(publicationInfoHash,
																	 shardList);

	HTAB *groupedLogicalRepTargetsHash = CreateGroupedLogicalRepTargetsHash(
		logicalRepTargetList);

	CreateGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash, superUser,
											  databaseName);

	PG_TRY();
	{
		MultiConnection *sourceReplicationConnection =
			GetReplicationConnection(sourceConnection->hostname, sourceConnection->port);

		/* set up the publication on the source and subscription on the target */
		CreatePublications(sourceConnection, publicationInfoHash);
		char *snapshot = CreateReplicationSlots(
			sourceConnection,
			sourceReplicationConnection,
			logicalRepTargetList,
			"pgoutput");

		CreateSubscriptions(
			sourceConnection,
			sourceConnection->database,
			logicalRepTargetList);

		/* only useful for isolation testing, see the function comment for the details */
		ConflictOnlyWithIsolationTesting();

		/*
		 * We have to create the primary key (or any other replica identity)
		 * before the update/delete operations that are queued will be
		 * replicated. Because if the replica identity does not exist on the
		 * target, the replication would fail.
		 *
		 * So the latest possible moment we could do this is right after the
		 * initial data COPY, but before enabling the susbcriptions. It might
		 * seem like a good idea to it after the initial data COPY, since
		 * it's generally the rule that it's cheaper to build an index at once
		 * than to create it incrementally. This general rule, is why we create
		 * all the regular indexes as late during the move as possible.
		 *
		 * But as it turns out in practice it's not as clear cut, and we saw a
		 * speed degradation in the time it takes to move shards when doing the
		 * replica identity creation after the initial COPY. So, instead we
		 * keep it before the COPY.
		 */
		CreateReplicaIdentities(logicalRepTargetList);

		CopyShardsToNode(sourceNode, targetNode, shardList, snapshot);

		/*
		 * We can close this connection now, because we're done copying the
		 * data and thus don't need access to the snapshot anymore. The
		 * replication slot will still be at the same LSN, because the
		 * subscriptions have not been enabled yet.
		 */
		CloseConnection(sourceReplicationConnection);

		/*
		 * Start the replication and copy all data
		 */
		CompleteNonBlockingShardTransfer(shardList,
										 sourceConnection,
										 publicationInfoHash,
										 logicalRepTargetList,
										 groupedLogicalRepTargetsHash,
										 SHARD_MOVE);

		/*
		 * We use these connections exclusively for subscription management,
		 * because otherwise subsequent metadata changes may inadvertedly use
		 * these connections instead of the connections that were used to
		 * grab locks in BlockWritesToShardList.
		 */
		CloseGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash);
		CloseConnection(sourceConnection);
	}
	PG_CATCH();
	{
		/*
		 * Try our best not to leave any left-over subscription or publication.
		 *
		 * Although it is not very advisable to use code-paths that could throw
		 * new errors, we prefer to do it here since we expect the cost of leaving
		 * left-overs not be very low.
		 */

		/* reconnect if the connection failed or is waiting for a command */
		RecreateGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash,
													superUser, databaseName);

		DropSubscriptions(logicalRepTargetList);

		/* reconnect if the connection failed or is waiting for a command */
		if (PQstatus(sourceConnection->pgConn) != CONNECTION_OK ||
			PQisBusy(sourceConnection->pgConn))
		{
			sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
															 sourceNodeName,
															 sourceNodePort, superUser,
															 databaseName);
		}
		DropReplicationSlots(sourceConnection, logicalRepTargetList);
		DropPublications(sourceConnection, publicationInfoHash);

		/* We don't need to UnclaimConnections since we're already erroring out */

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * CreateGroupedLogicalRepTargetsHash creates a hashmap that groups the subscriptions
 * logicalRepTargetList by node. This is useful for cases where we want to
 * iterate the subscriptions by node, so we can batch certain operations, such
 * as checking subscription readiness.
 */
HTAB *
CreateGroupedLogicalRepTargetsHash(List *logicalRepTargetList)
{
	HTAB *logicalRepTargetsHash = CreateSimpleHash(uint32, GroupedLogicalRepTargets);
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		bool found = false;
		GroupedLogicalRepTargets *groupedLogicalRepTargets =
			(GroupedLogicalRepTargets *) hash_search(
				logicalRepTargetsHash,
				&target->replicationSlot->targetNodeId,
				HASH_ENTER,
				&found);
		if (!found)
		{
			groupedLogicalRepTargets->logicalRepTargetList = NIL;
			groupedLogicalRepTargets->superuserConnection = NULL;
		}
		groupedLogicalRepTargets->logicalRepTargetList =
			lappend(groupedLogicalRepTargets->logicalRepTargetList, target);
	}
	return logicalRepTargetsHash;
}


/*
 * CompleteNonBlockingShardTransfer uses logical replication to apply the changes
 * made on the source to the target. It also runs all DDL on the target shards
 * that need to be run after the data copy.
 *
 * For shard splits it skips the partition hierarchy and foreign key creation
 * though, since those need to happen after the metadata is updated.
 */
void
CompleteNonBlockingShardTransfer(List *shardList,
								 MultiConnection *sourceConnection,
								 HTAB *publicationInfoHash,
								 List *logicalRepTargetList,
								 HTAB *groupedLogicalRepTargetsHash,
								 LogicalRepType type)
{
	/* Start applying the changes from the replication slots to catch up. */
	EnableSubscriptions(logicalRepTargetList);

	/*
	 * The following check is a leftover from when used subscriptions with
	 * copy_data=true. It's probably not really necessary anymore, but it
	 * seemed like a nice check to keep. At least for debugging issues it
	 * seems nice to report differences between the subscription never
	 * becoming ready and the subscriber not applying WAL. It's also not
	 * entirely clear if the catchup check handles the case correctly where
	 * the subscription is not in the ready state yet, because so far it
	 * never had to.
	 */
	WaitForAllSubscriptionsToBecomeReady(groupedLogicalRepTargetsHash);

	/*
	 * Wait until all the subscriptions are caught up to changes that
	 * happened after the initial COPY on the shards.
	 */
	WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

	/*
	 * Now lets create the post-load objects, such as the indexes, constraints
	 * and partitioning hierarchy. Once they are done, wait until the replication
	 * catches up again. So we don't block writes too long.
	 */
	CreatePostLogicalReplicationDataLoadObjects(logicalRepTargetList, type);
	WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);


	/* only useful for isolation testing, see the function comment for the details */
	ConflictOnlyWithIsolationTesting();

	/*
	 * We're almost done, we'll block the writes to the shards that we're
	 * replicating and expect all the subscription to catch up quickly
	 * afterwards.
	 *
	 * Notice that although shards in partitioned relation are excluded from
	 * logical replication, they are still locked against modification, and
	 * foreign constraints are created on them too.
	 */
	BlockWritesToShardList(shardList);

	WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

	if (type != SHARD_SPLIT)
	{
		/*
		 * We're creating the foreign constraints to reference tables after the
		 * data is already replicated and all the necessary locks are acquired.
		 *
		 * We prefer to do it here because the placements of reference tables
		 * are always valid, and any modification during the shard move would
		 * cascade to the hash distributed tables' shards if we had created
		 * the constraints earlier. The same is true for foreign keys between
		 * tables owned by different users.
		 */
		CreateUncheckedForeignKeyConstraints(logicalRepTargetList);
	}

	/* we're done, cleanup the publication and subscription */
	DropSubscriptions(logicalRepTargetList);
	DropReplicationSlots(sourceConnection, logicalRepTargetList);
	DropPublications(sourceConnection, publicationInfoHash);
}


/*
 * CreateShardMovePublicationInfoHash creates hashmap of PublicationInfos for a
 * shard move. Even though we only support moving a shard to a single target
 * node, the resulting hashmap can have multiple PublicationInfos in it.
 * The reason for that is that we need a separate publication for each
 * distributed table owning user in the shard group.
 */
static HTAB *
CreateShardMovePublicationInfoHash(WorkerNode *targetNode, List *shardIntervals)
{
	HTAB *publicationInfoHash = CreateSimpleHash(NodeAndOwner, PublicationInfo);
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervals)
	{
		NodeAndOwner key;
		key.nodeId = targetNode->nodeId;
		key.tableOwnerId = TableOwnerOid(shardInterval->relationId);
		bool found = false;
		PublicationInfo *publicationInfo =
			(PublicationInfo *) hash_search(publicationInfoHash, &key,
											HASH_ENTER,
											&found);
		if (!found)
		{
			publicationInfo->name = PublicationName(SHARD_MOVE, key.nodeId,
													key.tableOwnerId);
			publicationInfo->shardIntervals = NIL;
		}
		publicationInfo->shardIntervals =
			lappend(publicationInfo->shardIntervals, shardInterval);
	}
	return publicationInfoHash;
}


/*
 * CreateShardMoveLogicalRepTargetList creates the list containing all the
 * subscriptions that should be connected to the publications in the given
 * publicationHash.
 */
static List *
CreateShardMoveLogicalRepTargetList(HTAB *publicationInfoHash, List *shardList)
{
	List *logicalRepTargetList = NIL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, publicationInfoHash);
	Oid nodeId = InvalidOid;

	PublicationInfo *publication = NULL;
	while ((publication = (PublicationInfo *) hash_seq_search(&status)) != NULL)
	{
		Oid ownerId = publication->key.tableOwnerId;
		nodeId = publication->key.nodeId;
		LogicalRepTarget *target = palloc0(sizeof(LogicalRepTarget));
		target->subscriptionName = SubscriptionName(SHARD_MOVE, ownerId);
		target->tableOwnerId = ownerId;
		target->publication = publication;
		publication->target = target;
		target->newShards = NIL;
		target->subscriptionOwnerName = SubscriptionRoleName(SHARD_MOVE, ownerId);
		target->replicationSlot = palloc0(sizeof(ReplicationSlotInfo));
		target->replicationSlot->name = ReplicationSlotNameForNodeAndOwner(SHARD_MOVE,
																		   nodeId,
																		   ownerId);
		target->replicationSlot->targetNodeId = nodeId;
		target->replicationSlot->tableOwnerId = ownerId;
		logicalRepTargetList = lappend(logicalRepTargetList, target);
	}

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardList)
	{
		NodeAndOwner key;
		key.nodeId = nodeId;
		key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

		bool found = false;
		publication = (PublicationInfo *) hash_search(
			publicationInfoHash,
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
	return logicalRepTargetList;
}


/*
 * AcquireLogicalReplicationLock tries to acquire a lock for logical
 * replication. We need this lock, because at the start of logical replication
 * we clean up old subscriptions and publications. Because of this cleanup it's
 * not safe to run multiple logical replication based shard moves at the same
 * time. If multiple logical replication moves would run at the same time, the
 * second move might clean up subscriptions and publications that are in use by
 * another move.
 */
static void
AcquireLogicalReplicationLock(void)
{
	LOCKTAG tag;
	SET_LOCKTAG_LOGICAL_REPLICATION(tag);

	LockAcquire(&tag, ExclusiveLock, false, false);
}


/*
 * DropAllLogicalReplicationLeftovers drops all subscriptions, publications,
 * roles and replication slots on all nodes that were related to this
 * LogicalRepType. These might have been left there after the coordinator
 * crashed during a shard move/split. It's important to delete them for two
 * reasons:
 * 1. Starting new shard moves will fail when they exist, because it cannot
 *    create them.
 * 2. Leftover replication slots that are not consumed from anymore make it
 *    impossible for WAL to be dropped. This can cause out-of-disk issues.
 */
void
DropAllLogicalReplicationLeftovers(LogicalRepType type)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	/*
	 * We need connections that are not currently inside a transaction. The
	 * reason for this is that operations on subscriptions, publications and
	 * replication slots cannot be run in a transaction. By forcing a new
	 * connection we make sure no transaction is active on the connection.
	 */
	int connectionFlags = FORCE_NEW_CONNECTION;

	List *workerNodeList = ActivePrimaryNodeList(AccessShareLock);
	List *cleanupConnectionList = NIL;
	WorkerNode *workerNode = NULL;

	/*
	 * First we try to remove the subscription, everywhere and only after
	 * having done that we try to remove the publication everywhere. This is
	 * needed, because the publication can only be removed if there's no active
	 * subscription on it.
	 */
	foreach_ptr(workerNode, workerNodeList)
	{
		MultiConnection *cleanupConnection = GetNodeUserDatabaseConnection(
			connectionFlags, workerNode->workerName, workerNode->workerPort,
			superUser, databaseName);
		cleanupConnectionList = lappend(cleanupConnectionList, cleanupConnection);

		DropAllSubscriptions(cleanupConnection, type);
		DropAllUsers(cleanupConnection, type);
	}

	MultiConnection *cleanupConnection = NULL;
	foreach_ptr(cleanupConnection, cleanupConnectionList)
	{
		/*
		 * If replication slot could not be dropped while dropping the
		 * subscriber, drop it here.
		 */
		DropAllReplicationSlots(cleanupConnection, type);
		DropAllPublications(cleanupConnection, type);

		/*
		 * We close all connections that we opened for the dropping here. That
		 * way we don't keep these connections open unnecessarily during the
		 * 'LogicalRepType' operation (which can take a long time). We might
		 * need to reopen a few later on, but that seems better than keeping
		 * many open for no reason for a long time.
		 */
		CloseConnection(cleanupConnection);
	}
}


/*
 * PrepareReplicationSubscriptionList returns list of shards to be logically
 * replicated from given shard list. This is needed because Postgres does not
 * allow logical replication on partitioned tables, therefore shards belonging
 * to a partitioned tables should be exluded from logical replication
 * subscription list.
 */
static List *
PrepareReplicationSubscriptionList(List *shardList)
{
	List *replicationSubscriptionList = NIL;
	ListCell *shardCell = NULL;

	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
		if (!PartitionedTable(shardInterval->relationId))
		{
			/* only add regular and child tables to subscription */
			replicationSubscriptionList = lappend(replicationSubscriptionList,
												  shardInterval);
		}
	}

	return replicationSubscriptionList;
}


/*
 * CreateReplicaIdentities creates replica identities for all the shards that
 * are part of the given subscriptions.
 */
void
CreateReplicaIdentities(List *logicalRepTargetList)
{
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		MultiConnection *superuserConnection = target->superuserConnection;
		CreateReplicaIdentitiesOnNode(
			target->newShards,
			superuserConnection->hostname,
			superuserConnection->port);
	}
}


/*
 * CreateReplicaIdentitiesOnNode gets a shardList and creates all the replica
 * identities on the shards in the given node.
 */
void
CreateReplicaIdentitiesOnNode(List *shardList, char *nodeName, int32 nodePort)
{
	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateReplicaIdentitiesOnNode",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ShardInterval *shardInterval;
	foreach_ptr(shardInterval, shardList)
	{
		uint64 shardId = shardInterval->shardId;
		Oid relationId = shardInterval->relationId;

		List *backingIndexCommandList =
			GetIndexCommandListForShardBackingReplicaIdentity(relationId, shardId);

		List *replicaIdentityShardCommandList =
			GetReplicaIdentityCommandListForShard(relationId, shardId);

		List *commandList =
			list_concat(backingIndexCommandList, replicaIdentityShardCommandList);

		if (commandList != NIL)
		{
			ereport(DEBUG1, (errmsg("Creating replica identity for shard %ld on"
									"target node %s:%d", shardId, nodeName, nodePort)));

			SendCommandListToWorkerOutsideTransaction(nodeName, nodePort,
													  TableOwner(relationId),
													  commandList);
		}

		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * GetIndexCommandListForShardBackingReplicaIdentity returns all the create index
 * commands that are needed to create replica identity. If the table doesn't have
 * a replica identity, the function returns NIL.
 */
static List *
GetIndexCommandListForShardBackingReplicaIdentity(Oid relationId, uint64 shardId)
{
	List *commandList = NIL;
	Relation relation = table_open(relationId, AccessShareLock);
	Oid replicaIdentityIndex = GetRelationIdentityOrPK(relation);
	table_close(relation, NoLock);

	if (OidIsValid(replicaIdentityIndex))
	{
		/*
		 * The replica identity is backed by an index or primary key,
		 * so get the index/pkey definition first.
		 */
		HeapTuple indexTuple =
			SearchSysCache1(INDEXRELID, ObjectIdGetDatum(replicaIdentityIndex));
		if (!HeapTupleIsValid(indexTuple))
		{
			/* should not happen */
			elog(ERROR, "cache lookup failed for index %u", replicaIdentityIndex);
		}

		Form_pg_index indexForm = ((Form_pg_index) GETSTRUCT(indexTuple));
		List *indexCommandTableDDLList = NIL;
		int indexFlags = INCLUDE_INDEX_ALL_STATEMENTS;
		GatherIndexAndConstraintDefinitionList(indexForm, &indexCommandTableDDLList,
											   indexFlags);

		List *indexCommandShardDDLList =
			WorkerApplyShardDDLCommandList(indexCommandTableDDLList, shardId);

		commandList = list_concat(commandList, indexCommandShardDDLList);

		ReleaseSysCache(indexTuple);
	}

	return commandList;
}


/*
 * GetReplicaIdentityCommandListForShard returns the create replica identity
 * command that are needed to create replica identity. If the table doesn't have
 * a replica identity, the function returns NIL.
 */
static List *
GetReplicaIdentityCommandListForShard(Oid relationId, uint64 shardId)
{
	List *replicaIdentityTableDDLCommand =
		GetTableReplicaIdentityCommand(relationId);
	List *replicaIdentityShardCommandList =
		WorkerApplyShardDDLCommandList(replicaIdentityTableDDLCommand, shardId);

	return replicaIdentityShardCommandList;
}


/*
 * CreatePostLogicalReplicationDataLoadObjects gets a shardList and creates all
 * the objects that can be created after the data is moved with logical replication.
 */
static void
CreatePostLogicalReplicationDataLoadObjects(List *logicalRepTargetList,
											LogicalRepType type)
{
	/*
	 * We create indexes in 4 steps.
	 *  - CREATE INDEX statements
	 *  - CREATE CONSTRAINT statements that are backed by
	 *    indexes (unique and exclude constraints)
	 *  - ALTER TABLE %s CLUSTER ON %s
	 *  - ALTER INDEX %s ALTER COLUMN %d SET STATISTICS %d
	 *
	 *  On each step, we execute can execute commands in parallel. For example,
	 *  multiple indexes on the shard table or indexes for the colocated shards
	 *  can be created in parallel. However, the latter two steps, clustering the
	 *  table and setting the statistics of indexes, depends on the indexes being
	 *  created. That's why the execution is divided into four distinct stages.
	 */
	ExecuteCreateIndexCommands(logicalRepTargetList);
	ExecuteCreateConstraintsBackedByIndexCommands(logicalRepTargetList);
	ExecuteClusterOnCommands(logicalRepTargetList);
	ExecuteCreateIndexStatisticsCommands(logicalRepTargetList);

	/*
	 * Once the indexes are created, there are few more objects like triggers and table
	 * statistics that should be created after the data move.
	 */
	ExecuteRemainingPostLoadTableCommands(logicalRepTargetList);

	/*
	 * Creating the partitioning hierarchy errors out in shard splits when
	 */
	if (type != SHARD_SPLIT)
	{
		/* create partitioning hierarchy, if any */
		CreatePartitioningHierarchy(logicalRepTargetList);
	}
}


/*
 * ExecuteCreateIndexCommands gets a shardList and creates all the indexes
 * for the given shardList in the given target node.
 *
 * The execution is done in parallel, and throws an error if any of the
 * commands fail.
 */
static void
ExecuteCreateIndexCommands(List *logicalRepTargetList)
{
	List *taskList = NIL;
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			List *tableCreateIndexCommandList =
				GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																		   INCLUDE_CREATE_INDEX_STATEMENTS);

			List *shardCreateIndexCommandList =
				WorkerApplyShardDDLCommandList(tableCreateIndexCommandList,
											   shardInterval->shardId);
			List *taskListForShard =
				ConvertNonExistingPlacementDDLCommandsToTasks(
					shardCreateIndexCommandList,
					target->superuserConnection->hostname,
					target->superuserConnection->port);
			taskList = list_concat(taskList, taskListForShard);
		}
	}

	/*
	 * We are going to create indexes and constraints using the current user. That is
	 * alright because an index/constraint always belongs to the owner of the table,
	 * and Citus already ensures that the current user owns all the tables that are
	 * moved.
	 *
	 * CREATE INDEX commands acquire ShareLock on a relation. So, it is
	 * allowed to run multiple CREATE INDEX commands concurrently on a table
	 * and across different tables (e.g., shards).
	 */

	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(indexes)")));

	ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, taskList,
									  MaxAdaptiveExecutorPoolSize,
									  NIL);
}


/*
 * ExecuteCreateConstraintsBackedByIndexCommands gets a shardList and creates all the constraints
 * that are backed by indexes for the given shardList in the given target node.
 *
 * The execution is done in sequential mode, and throws an error if any of the
 * commands fail.
 */
static void
ExecuteCreateConstraintsBackedByIndexCommands(List *logicalRepTargetList)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(constraints backed by indexes)")));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateConstraintsBackedByIndexContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			List *tableCreateConstraintCommandList =
				GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																		   INCLUDE_CREATE_CONSTRAINT_STATEMENTS);

			if (tableCreateConstraintCommandList == NIL)
			{
				/* no constraints backed by indexes, skip */
				MemoryContextReset(localContext);
				continue;
			}

			List *shardCreateConstraintCommandList =
				WorkerApplyShardDDLCommandList(tableCreateConstraintCommandList,
											   shardInterval->shardId);

			char *tableOwner = TableOwner(shardInterval->relationId);
			SendCommandListToWorkerOutsideTransaction(
				target->superuserConnection->hostname,
				target->superuserConnection->port,
				tableOwner,
				shardCreateConstraintCommandList);
			MemoryContextReset(localContext);
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * ConvertNonExistingShardDDLCommandsToTasks generates one task per input
 * element in shardCommandList.
 *
 * The generated tasks' placements do not exist (yet). We are generating
 * fake placements for the tasks.
 */
static List *
ConvertNonExistingPlacementDDLCommandsToTasks(List *shardCommandList,
											  char *targetNodeName,
											  int targetNodePort)
{
	WorkerNode *workerNode = FindWorkerNodeOrError(targetNodeName, targetNodePort);

	List *taskList = NIL;
	uint64 jobId = INVALID_JOB_ID;

	ListCell *commandCell = NULL;
	int taskId = 1;
	foreach(commandCell, shardCommandList)
	{
		char *command = (char *) lfirst(commandCell);
		Task *task = CreateBasicTask(jobId, taskId, DDL_TASK, command);

		/* this placement currently does not exist */
		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, workerNode);

		task->taskPlacementList = list_make1(taskPlacement);

		taskList = lappend(taskList, task);
		taskId++;
	}

	return taskList;
}


/*
 * ExecuteClusterOnCommands gets a shardList and creates all the CLUSTER ON commands
 * for the given shardList in the given target node.
 *
 * The execution is done in parallel, and in case of any failure, the transaction
 * is aborted.
 */
static void
ExecuteClusterOnCommands(List *logicalRepTargetList)
{
	List *taskList = NIL;
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			List *tableAlterTableClusterOnCommandList =
				GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																		   INCLUDE_INDEX_CLUSTERED_STATEMENTS);

			List *shardAlterTableClusterOnCommandList =
				WorkerApplyShardDDLCommandList(tableAlterTableClusterOnCommandList,
											   shardInterval->shardId);

			List *taskListForShard =
				ConvertNonExistingPlacementDDLCommandsToTasks(
					shardAlterTableClusterOnCommandList,
					target->superuserConnection->hostname,
					target->superuserConnection->port);
			taskList = list_concat(taskList, taskListForShard);
		}
	}

	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(CLUSTER ON)")));

	ExecuteTaskListOutsideTransaction(ROW_MODIFY_NONE, taskList,
									  MaxAdaptiveExecutorPoolSize,
									  NIL);
}


/*
 * ExecuteCreateIndexStatisticsCommands gets a shardList and creates
 * all the statistics objects for the indexes in the given target node.
 *
 * The execution is done in sequentially, and in case of any failure, the transaction
 * is aborted.
 */
static void
ExecuteCreateIndexStatisticsCommands(List *logicalRepTargetList)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(index statistics)")));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateIndexStatisticsContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			List *tableAlterIndexSetStatisticsCommandList =
				GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																		   INCLUDE_INDEX_STATISTICS_STATEMENTTS);
			List *shardAlterIndexSetStatisticsCommandList =
				WorkerApplyShardDDLCommandList(tableAlterIndexSetStatisticsCommandList,
											   shardInterval->shardId);

			if (shardAlterIndexSetStatisticsCommandList == NIL)
			{
				/* no index statistics exists, skip */
				MemoryContextReset(localContext);
				continue;
			}

			/*
			 * These remaining operations do not require significant resources, so no
			 * need to create them in parallel.
			 */
			char *tableOwner = TableOwner(shardInterval->relationId);
			SendCommandListToWorkerOutsideTransaction(
				target->superuserConnection->hostname,
				target->superuserConnection->port,
				tableOwner,
				shardAlterIndexSetStatisticsCommandList);

			MemoryContextReset(localContext);
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * ExecuteRemainingPostLoadTableCommands gets a shardList and creates
 * all the remaining post load objects other than the indexes
 * in the given target node.
 */
static void
ExecuteRemainingPostLoadTableCommands(List *logicalRepTargetList)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(triggers and table statistics)"
							)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateTableStatisticsContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			bool includeIndexes = false;
			bool includeReplicaIdentity = false;

			List *tablePostLoadTableCommandList =
				GetPostLoadTableCreationCommands(relationId, includeIndexes,
												 includeReplicaIdentity);

			List *shardPostLoadTableCommandList =
				WorkerApplyShardDDLCommandList(tablePostLoadTableCommandList,
											   shardInterval->shardId);

			if (shardPostLoadTableCommandList == NIL)
			{
				/* no index statistics exists, skip */
				continue;
			}

			/*
			 * These remaining operations do not require significant resources, so no
			 * need to create them in parallel.
			 */
			char *tableOwner = TableOwner(shardInterval->relationId);
			SendCommandListToWorkerOutsideTransaction(
				target->superuserConnection->hostname,
				target->superuserConnection->port,
				tableOwner,
				shardPostLoadTableCommandList);

			MemoryContextReset(localContext);
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CreatePartitioningHierarchy gets a shardList and creates the partitioning
 * hierarchy between the shardList, if any,
 */
void
CreatePartitioningHierarchy(List *logicalRepTargetList)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(partitioning hierarchy)")));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreatePartitioningHierarchy",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_ptr(shardInterval, target->newShards)
		{
			if (PartitionTable(shardInterval->relationId))
			{
				char *attachPartitionCommand =
					GenerateAttachShardPartitionCommand(shardInterval);

				char *tableOwner = TableOwner(shardInterval->relationId);

				/*
				 * Attaching partition may acquire conflicting locks when created in
				 * parallel, so create them sequentially. Also attaching partition
				 * is a quick operation, so it is fine to execute sequentially.
				 */

				MultiConnection *connection =
					GetNodeUserDatabaseConnection(OUTSIDE_TRANSACTION,
												  target->superuserConnection->hostname,
												  target->superuserConnection->port,
												  tableOwner, NULL);
				ExecuteCriticalRemoteCommand(connection, attachPartitionCommand);

				MemoryContextReset(localContext);
			}
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CreateUncheckedForeignKeyConstraints is used to create the foreign
 * constraints on the logical replication target without checking that they are
 * actually valid.
 *
 * We skip the validation phase of foreign keys to after a shard
 * move/copy/split because the validation is pretty costly and given that the
 * source placements are already valid, the validation in the target nodes is
 * useless.
 */
void
CreateUncheckedForeignKeyConstraints(List *logicalRepTargetList)
{
	MemoryContext localContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "CreateKeyForeignConstraints",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);


	/*
	 * Iterate over all the shards in the shard group.
	 */
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;

		/*
		 * Iterate on split shards list for a given shard and create constraints.
		 */
		foreach_ptr(shardInterval, target->newShards)
		{
			List *commandList = CopyShardForeignConstraintCommandList(
				shardInterval);
			commandList = list_concat(
				list_make1("SET LOCAL citus.skip_constraint_validation TO ON;"),
				commandList);

			SendCommandListToWorkerOutsideTransactionWithConnection(
				target->superuserConnection,
				commandList);

			MemoryContextReset(localContext);
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * ConflictOnlyWithIsolationTesting is only useful for testing and should
 * not be called by any code-path except for LogicallyReplicateShards().
 *
 * Since logically replicating shards does eventually block modifications,
 * it becomes tricky to use isolation tester to show concurrent behaviour
 * of online shard rebalancing and modification queries.
 *
 * Note that since the cost of calling this function is pretty low, we prefer
 * to use it in non-assert builds as well not to diverge in the behaviour.
 */
extern void
ConflictOnlyWithIsolationTesting()
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	if (RunningUnderIsolationTest)
	{
		/* we've picked random keys */
		SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, SHARD_MOVE_ADVISORY_LOCK_FIRST_KEY,
							 SHARD_MOVE_ADVISORY_LOCK_SECOND_KEY, 2);

		(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
	}
}


/*
 * DropReplicationSlots drops the replication slots used for shard moves/splits
 * over the given connection (if they exist).
 */
void
DropReplicationSlots(MultiConnection *sourceConnection, List *logicalRepTargetList)
{
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		DropReplicationSlot(sourceConnection, target->replicationSlot->name);
	}
}


/*
 * DropPublications drops the publications used for shard moves/splits over the
 * given connection (if they exist).
 */
void
DropPublications(MultiConnection *sourceConnection, HTAB *publicationInfoHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, publicationInfoHash);

	PublicationInfo *entry = NULL;
	while ((entry = (PublicationInfo *) hash_seq_search(&status)) != NULL)
	{
		DropPublication(sourceConnection, entry->name);
	}
}


/*
 * DropReplicationSlot drops the replication slot with the given name
 * if it exists. It retries if the command fails with an OBJECT_IN_USE error.
 */
static void
DropReplicationSlot(MultiConnection *connection, char *replicationSlotName)
{
	int maxSecondsToTryDropping = 20;
	bool raiseInterrupts = true;
	PGresult *result = NULL;

	/* we'll retry in case of an OBJECT_IN_USE error */
	while (maxSecondsToTryDropping >= 0)
	{
		int querySent = SendRemoteCommand(
			connection,
			psprintf(
				"select pg_drop_replication_slot(slot_name) from "
				REPLICATION_SLOT_CATALOG_TABLE_NAME
				" where slot_name = %s",
				quote_literal_cstr(replicationSlotName))
			);

		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}

		result = GetRemoteCommandResult(connection, raiseInterrupts);

		if (IsResponseOK(result))
		{
			/* no error, we are good to go */
			break;
		}

		char *errorcode = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		if (errorcode != NULL && strcmp(errorcode, STR_ERRCODE_OBJECT_IN_USE) == 0 &&
			maxSecondsToTryDropping > 0)
		{
			/* retry dropping the replication slot after sleeping for one sec */
			maxSecondsToTryDropping--;
			pg_usleep(1000);
		}
		else
		{
			/*
			 * Report error if:
			 * - Error code is not 55006 (Object In Use)
			 * - Or, we have made enough number of retries (currently 20), but didn't work
			 */
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * DropPublication drops the publication with the given name if it
 * exists.
 */
static void
DropPublication(MultiConnection *connection, char *publicationName)
{
	ExecuteCriticalRemoteCommand(connection, psprintf(
									 "DROP PUBLICATION IF EXISTS %s",
									 quote_identifier(publicationName)));
}


/*
 * PublicationName returns the name of the publication for the given node and
 * table owner.
 */
char *
PublicationName(LogicalRepType type, uint32_t nodeId, Oid ownerId)
{
	return psprintf("%s%u_%u", publicationPrefix[type], nodeId, ownerId);
}


/*
 * ReplicationSlotNameForNodeAndOwner returns the name of the replication slot for the
 * given node and table owner.
 *
 * Note that PG15 introduced a new ReplicationSlotName function that caused name conflicts
 * and we renamed this function.
 */
char *
ReplicationSlotNameForNodeAndOwner(LogicalRepType type, uint32_t nodeId, Oid ownerId)
{
	StringInfo slotName = makeStringInfo();
	appendStringInfo(slotName, "%s%u_%u", replicationSlotPrefix[type], nodeId,
					 ownerId);

	if (slotName->len > NAMEDATALEN)
	{
		ereport(ERROR,
				(errmsg(
					 "Replication Slot name:%s having length:%d is greater than maximum allowed length:%d",
					 slotName->data, slotName->len, NAMEDATALEN)));
	}
	return slotName->data;
}


/*
 * SubscriptionName returns the name of the subscription for the given owner.
 */
char *
SubscriptionName(LogicalRepType type, Oid ownerId)
{
	return psprintf("%s%u", subscriptionPrefix[type], ownerId);
}


/*
 * SubscriptionRoleName returns the name of the role used by the
 * subscription that subscribes to the tables of the given owner.
 */
char *
SubscriptionRoleName(LogicalRepType type, Oid ownerId)
{
	return psprintf("%s%u", subscriptionRolePrefix[type], ownerId);
}


/*
 * GetQueryResultStringList expects a query that returns a single column of
 * strings. This query is executed on the connection and the function then
 * returns the results of the query in a List.
 */
List *
GetQueryResultStringList(MultiConnection *connection, char *query)
{
	bool raiseInterrupts = true;

	int querySent = SendRemoteCommand(connection, query);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	int rowCount = PQntuples(result);
	int columnCount = PQnfields(result);

	if (columnCount != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of columns returned while reading ")));
	}

	List *resultList = NIL;
	for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		int columnIndex = 0;
		StringInfo resultStringInfo = makeStringInfo();

		char *resultString = PQgetvalue(result, rowIndex, columnIndex);

		/* we're using the stringinfo to copy the data into the current memory context */
		appendStringInfoString(resultStringInfo, resultString);

		resultList = lappend(resultList, resultStringInfo->data);
	}

	PQclear(result);
	ForgetResults(connection);
	return resultList;
}


/*
 * DropAllSubscriptions drops all the existing subscriptions that
 * match our shard move naming scheme on the node that the connection points
 * to.
 */
static void
DropAllSubscriptions(MultiConnection *connection, LogicalRepType type)
{
	char *query = psprintf(
		"SELECT subname FROM pg_subscription "
		"WHERE subname LIKE %s || '%%'",
		quote_literal_cstr(subscriptionPrefix[type]));
	List *subscriptionNameList = GetQueryResultStringList(connection, query);
	char *subscriptionName;
	foreach_ptr(subscriptionName, subscriptionNameList)
	{
		DropSubscription(connection, subscriptionName);
	}
}


/*
 * DropAllUsers drops all the users that match our shard move naming
 * scheme for temporary shard move users on the node that the connection points
 * to.
 */
static void
DropAllUsers(MultiConnection *connection, LogicalRepType type)
{
	char *query = psprintf(
		"SELECT rolname FROM pg_roles "
		"WHERE rolname LIKE %s || '%%'",
		quote_literal_cstr(subscriptionRolePrefix[type]));
	List *usernameList = GetQueryResultStringList(connection, query);
	char *username;
	foreach_ptr(username, usernameList)
	{
		DropUser(connection, username);
	}
}


/*
 * DropAllReplicationSlots drops all the existing replication slots
 * that match our shard move naming scheme on the node that the connection
 * points to.
 */
static void
DropAllReplicationSlots(MultiConnection *connection, LogicalRepType type)
{
	char *query = psprintf(
		"SELECT slot_name FROM pg_replication_slots "
		"WHERE slot_name LIKE %s || '%%'",
		quote_literal_cstr(replicationSlotPrefix[type]));
	List *slotNameList = GetQueryResultStringList(connection, query);
	char *slotName;
	foreach_ptr(slotName, slotNameList)
	{
		DropReplicationSlot(connection, slotName);
	}
}


/*
 * DropAllPublications drops all the existing publications that
 * match our shard move naming scheme on the node that the connection points
 * to.
 */
static void
DropAllPublications(MultiConnection *connection, LogicalRepType type)
{
	char *query = psprintf(
		"SELECT pubname FROM pg_publication "
		"WHERE pubname LIKE %s || '%%'",
		quote_literal_cstr(publicationPrefix[type]));
	List *publicationNameList = GetQueryResultStringList(connection, query);
	char *publicationName;
	foreach_ptr(publicationName, publicationNameList)
	{
		DropPublication(connection, publicationName);
	}
}


/*
 * DropSubscriptions drops all the subscriptions in the logicalRepTargetList
 * from the subscriber node. It also drops the temporary users that are used as
 * owners for of the subscription. This doesn't drop the replication slots on
 * the publisher, these should be dropped using DropReplicationSlots.
 */
void
DropSubscriptions(List *logicalRepTargetList)
{
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		DropSubscription(target->superuserConnection,
						 target->subscriptionName);
		DropUser(target->superuserConnection, target->subscriptionOwnerName);
	}
}


/*
 * DropSubscription drops subscription with the given name on the subscriber
 * node if it exists. Note that this doesn't drop the replication slot on the
 * publisher node. The reason is that sometimes this is not possible. To known
 * cases where this is not possible are:
 * 1. Due to the node with the replication slot being down.
 * 2. Due to a deadlock when the replication is on the same node as the
 *    subscription, which is the case for shard splits to the local node.
 *
 * So instead of directly dropping the subscription, including the attached
 * replication slot, the subscription is first disconnected from the
 * replication slot before dropping it. The replication slot itself should be
 * dropped using DropReplicationSlot on the source connection.
 */
static void
DropSubscription(MultiConnection *connection, char *subscriptionName)
{
	int querySent = SendRemoteCommand(
		connection,
		psprintf("ALTER SUBSCRIPTION %s DISABLE", quote_identifier(subscriptionName)));
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	bool raiseInterrupts = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		char *errorcode = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		if (errorcode != NULL && strcmp(errorcode, STR_ERRCODE_UNDEFINED_OBJECT) == 0)
		{
			/*
			 * The subscription doesn't exist, so we can return right away.
			 * This DropSubscription call is effectively a no-op.
			 */
			return;
		}
		else
		{
			ReportResultError(connection, result, ERROR);
			PQclear(result);
			ForgetResults(connection);
		}
	}

	PQclear(result);
	ForgetResults(connection);

	ExecuteCriticalRemoteCommand(connection, psprintf(
									 "ALTER SUBSCRIPTION %s SET (slot_name = NONE)",
									 quote_identifier(subscriptionName)));

	ExecuteCriticalRemoteCommand(connection, psprintf(
									 "DROP SUBSCRIPTION %s",
									 quote_identifier(subscriptionName)));
}


/*
 * DropUser drops the user with the given name if it exists.
 */
static void
DropUser(MultiConnection *connection, char *username)
{
	/*
	 * The DROP USER command should not propagate, so we temporarily disable
	 * DDL propagation.
	 */
	SendCommandListToWorkerOutsideTransactionWithConnection(
		connection,
		list_make2(
			"SET LOCAL citus.enable_ddl_propagation TO OFF;",
			psprintf("DROP USER IF EXISTS %s;",
					 quote_identifier(username))));
}


/*
 * CreatePublications creates a the publications defined in the
 * publicationInfoHash over the given connection.
 */
void
CreatePublications(MultiConnection *connection,
				   HTAB *publicationInfoHash)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, publicationInfoHash);
	PublicationInfo *entry = NULL;
	while ((entry = (PublicationInfo *) hash_seq_search(&status)) != NULL)
	{
		StringInfo createPublicationCommand = makeStringInfo();
		bool prefixWithComma = false;

		appendStringInfo(createPublicationCommand, "CREATE PUBLICATION %s FOR TABLE ",
						 quote_identifier(entry->name));

		ShardInterval *shard = NULL;
		foreach_ptr(shard, entry->shardIntervals)
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
}


/*
 * GetReplicationConnection opens a new replication connection to this node.
 * This connection can be used to send replication commands, such as
 * CREATE_REPLICATION_SLOT.
 */
MultiConnection *
GetReplicationConnection(char *nodeName, int nodePort)
{
	int connectionFlags = FORCE_NEW_CONNECTION;
	connectionFlags |= REQUIRE_REPLICATION_CONNECTION_PARAM;

	MultiConnection *connection = GetNodeUserDatabaseConnection(
		connectionFlags,
		nodeName,
		nodePort,
		CitusExtensionOwnerName(),
		get_database_name(MyDatabaseId));

	/*
	 * Replication connections are special and don't support all of SQL, so we
	 * don't want it to be used for other purposes what we create it for.
	 */
	ClaimConnectionExclusively(connection);
	return connection;
}


/*
 * CreateReplicationSlot creates a replication slot with the given slot name
 * over the given connection. The given connection should be a replication
 * connection. This function returns the name of the snapshot that is used for
 * this replication slot. When using this snapshot name for other transactions
 * you need to keep the given replication connection open until you have used
 * the snapshot name.
 */
static char *
CreateReplicationSlot(MultiConnection *connection, char *slotname, char *outputPlugin)
{
	StringInfo createReplicationSlotCommand = makeStringInfo();
	appendStringInfo(createReplicationSlotCommand,
					 "CREATE_REPLICATION_SLOT %s LOGICAL %s EXPORT_SNAPSHOT;",
					 quote_identifier(slotname), quote_identifier(outputPlugin));

	PGresult *result = NULL;
	int response = ExecuteOptionalRemoteCommand(connection,
												createReplicationSlotCommand->data,
												&result);

	if (response != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
	{
		ReportResultError(connection, result, ERROR);
	}

	/*'snapshot_name' is second column where index starts from zero.
	 * We're using the pstrdup to copy the data into the current memory context */
	char *snapShotName = pstrdup(PQgetvalue(result, 0, 2 /* columIndex */));
	PQclear(result);
	ForgetResults(connection);
	return snapShotName;
}


/*
 * CreateReplicationSlots creates the replication slots that the subscriptions
 * in the logicalRepTargetList can use.
 *
 * This function returns the snapshot name of the replication slots that are
 * used by the subscription. When using this snapshot name for other
 * transactions you need to keep the given replication connection open until
 * you are finished using the snapshot.
 */
char *
CreateReplicationSlots(MultiConnection *sourceConnection,
					   MultiConnection *sourceReplicationConnection,
					   List *logicalRepTargetList,
					   char *outputPlugin)
{
	ReplicationSlotInfo *firstReplicationSlot = NULL;
	char *snapshot = NULL;
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ReplicationSlotInfo *replicationSlot = target->replicationSlot;

		if (!firstReplicationSlot)
		{
			firstReplicationSlot = replicationSlot;
			snapshot = CreateReplicationSlot(
				sourceReplicationConnection,
				replicationSlot->name,
				outputPlugin
				);
		}
		else
		{
			ExecuteCriticalRemoteCommand(
				sourceConnection,
				psprintf("SELECT pg_catalog.pg_copy_logical_replication_slot(%s, %s)",
						 quote_literal_cstr(firstReplicationSlot->name),
						 quote_literal_cstr(replicationSlot->name)));
		}
	}
	return snapshot;
}


/*
 * CreateSubscriptions creates the subscriptions according to their definition
 * in the logicalRepTargetList. The remote node(s) needs to have appropriate
 * pg_dist_authinfo rows for the superuser such that the apply process can
 * connect. Because the generated CREATE SUBSCRIPTION statements use the host
 * and port names directly (rather than looking up any relevant
 * pg_dist_poolinfo rows), all such connections remain direct and will not
 * route through any configured poolers.
 *
 * The subscriptions created by this function are created in the disabled
 * state. This is done so a data copy can be done manually afterwards. To
 * enable the subscriptions you can use EnableSubscriptions().
 */
void
CreateSubscriptions(MultiConnection *sourceConnection,
					char *databaseName,
					List *logicalRepTargetList)
{
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		int ownerId = target->tableOwnerId;

		/*
		 * The CREATE USER command should not propagate, so we temporarily
		 * disable DDL propagation.
		 *
		 * Subscription workers have SUPERUSER permissions. Hence we temporarily
		 * create a user with SUPERUSER permissions and then alter it to NOSUPERUSER.
		 * This prevents permission escalations.
		 */
		SendCommandListToWorkerOutsideTransactionWithConnection(
			target->superuserConnection,
			list_make2(
				"SET LOCAL citus.enable_ddl_propagation TO OFF;",
				psprintf(
					"CREATE USER %s SUPERUSER IN ROLE %s;",
					quote_identifier(target->subscriptionOwnerName),
					quote_identifier(GetUserNameFromId(ownerId, false))
					)));

		StringInfo conninfo = makeStringInfo();
		appendStringInfo(conninfo, "host='%s' port=%d user='%s' dbname='%s' "
								   "connect_timeout=20",
						 escape_param_str(sourceConnection->hostname),
						 sourceConnection->port,
						 escape_param_str(sourceConnection->user), escape_param_str(
							 databaseName));
		if (CpuPriorityLogicalRepSender != CPU_PRIORITY_INHERIT &&
			list_length(logicalRepTargetList) <= MaxHighPriorityBackgroundProcesess)
		{
			appendStringInfo(conninfo,
							 " options='-c citus.cpu_priority=%d'",
							 CpuPriorityLogicalRepSender);
		}

		StringInfo createSubscriptionCommand = makeStringInfo();
		appendStringInfo(createSubscriptionCommand,
						 "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
						 "WITH (citus_use_authinfo=true, create_slot=false, "
						 "copy_data=false, enabled=false, slot_name=%s",
						 quote_identifier(target->subscriptionName),
						 quote_literal_cstr(conninfo->data),
						 quote_identifier(target->publication->name),
						 quote_identifier(target->replicationSlot->name));

		if (EnableBinaryProtocol && PG_VERSION_NUM >= PG_VERSION_14)
		{
			appendStringInfoString(createSubscriptionCommand, ", binary=true)");
		}
		else
		{
			appendStringInfoString(createSubscriptionCommand, ")");
		}


		ExecuteCriticalRemoteCommand(target->superuserConnection,
									 createSubscriptionCommand->data);
		pfree(createSubscriptionCommand->data);
		pfree(createSubscriptionCommand);
		ExecuteCriticalRemoteCommand(target->superuserConnection, psprintf(
										 "ALTER SUBSCRIPTION %s OWNER TO %s",
										 quote_identifier(target->subscriptionName),
										 quote_identifier(target->subscriptionOwnerName)
										 ));

		/*
		 * The ALTER ROLE command should not propagate, so we temporarily
		 * disable DDL propagation.
		 */
		SendCommandListToWorkerOutsideTransactionWithConnection(
			target->superuserConnection,
			list_make2(
				"SET LOCAL citus.enable_ddl_propagation TO OFF;",
				psprintf(
					"ALTER ROLE %s NOSUPERUSER;",
					quote_identifier(target->subscriptionOwnerName)
					)));
	}
}


/*
 * EnableSubscriptions enables all the the subscriptions in the
 * logicalRepTargetList. This means the replication slot will start to be read
 * and the catchup phase begins.
 */
void
EnableSubscriptions(List *logicalRepTargetList)
{
	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		ExecuteCriticalRemoteCommand(target->superuserConnection, psprintf(
										 "ALTER SUBSCRIPTION %s ENABLE",
										 target->subscriptionName
										 ));
	}
}


/* *INDENT-OFF* */
/*
 * Escaping libpq connect parameter strings.
 *
 * Replaces "'" with "\'" and "\" with "\\".
 *
 * Copied from dblink.c to escape libpq params
 */
static char *
escape_param_str(const char *str)
{
	StringInfoData buf;

	initStringInfo(&buf);

	for (const char *cp = str; *cp; cp++)
	{
		if (*cp == '\\' || *cp == '\'')
			appendStringInfoChar(&buf, '\\');
		appendStringInfoChar(&buf, *cp);
	}

	return buf.data;
}

/* *INDENT-ON* */


/*
 * GetRemoteLogPosition gets the current WAL log position over the given connection.
 */
XLogRecPtr
GetRemoteLogPosition(MultiConnection *connection)
{
	return GetRemoteLSN(connection, CURRENT_LOG_POSITION_COMMAND);
}


/*
 * GetRemoteLSN executes a command that returns a single LSN over the given connection
 * and returns it as an XLogRecPtr (uint64).
 */
static XLogRecPtr
GetRemoteLSN(MultiConnection *connection, char *command)
{
	bool raiseInterrupts = false;
	XLogRecPtr remoteLogPosition = InvalidXLogRecPtr;

	int querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	int rowCount = PQntuples(result);
	if (rowCount != 1)
	{
		PQclear(result);
		ForgetResults(connection);
		return InvalidXLogRecPtr;
	}

	int colCount = PQnfields(result);
	if (colCount != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of columns returned by: %s",
							   command)));
	}

	if (!PQgetisnull(result, 0, 0))
	{
		char *resultString = PQgetvalue(result, 0, 0);
		Datum remoteLogPositionDatum = DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
															   CStringGetDatum(
																   resultString));
		remoteLogPosition = DatumGetLSN(remoteLogPositionDatum);
	}

	PQclear(result);
	ForgetResults(connection);

	return remoteLogPosition;
}


/*
 * CreateGroupedLogicalRepTargetsConnections creates connections for all of the nodes
 * in the groupedLogicalRepTargetsHash.
 */
void
CreateGroupedLogicalRepTargetsConnections(HTAB *groupedLogicalRepTargetsHash,
										  char *user,
										  char *databaseName)
{
	int connectionFlags = FORCE_NEW_CONNECTION;
	HASH_SEQ_STATUS status;
	GroupedLogicalRepTargets *groupedLogicalRepTargets = NULL;
	foreach_htab(groupedLogicalRepTargets, &status, groupedLogicalRepTargetsHash)
	{
		WorkerNode *targetWorkerNode = FindNodeWithNodeId(
			groupedLogicalRepTargets->nodeId,
			false);
		MultiConnection *superuserConnection =
			GetNodeUserDatabaseConnection(connectionFlags, targetWorkerNode->workerName,
										  targetWorkerNode->workerPort,
										  user,
										  databaseName);

		/*
		 * Operations on subscriptions cannot run in a transaction block. We
		 * claim the connections exclusively to ensure they do not get used for
		 * metadata syncing, which does open a transaction block.
		 */
		ClaimConnectionExclusively(superuserConnection);

		groupedLogicalRepTargets->superuserConnection = superuserConnection;

		LogicalRepTarget *target = NULL;
		foreach_ptr(target, groupedLogicalRepTargets->logicalRepTargetList)
		{
			target->superuserConnection = superuserConnection;
		}
	}
}


/*
 * RecreateGroupedLogicalRepTargetsConnections recreates connections for all of the
 * nodes in the groupedLogicalRepTargetsHash where the old connection is broken or
 * currently running a query.
 *
 * IMPORTANT: When it recreates the connection, it doesn't close the existing
 * connection. This means that this function should only be called when we know
 * we'll throw an error afterwards, otherwise we would leak these connections.
 */
static void
RecreateGroupedLogicalRepTargetsConnections(HTAB *groupedLogicalRepTargetsHash,
											char *user,
											char *databaseName)
{
	int connectionFlags = FORCE_NEW_CONNECTION;
	HASH_SEQ_STATUS status;
	GroupedLogicalRepTargets *groupedLogicalRepTargets = NULL;
	foreach_htab(groupedLogicalRepTargets, &status, groupedLogicalRepTargetsHash)
	{
		MultiConnection *superuserConnection =
			groupedLogicalRepTargets->superuserConnection;
		if (superuserConnection &&
			PQstatus(superuserConnection->pgConn) == CONNECTION_OK &&
			!PQisBusy(superuserConnection->pgConn)
			)
		{
			continue;
		}
		WorkerNode *targetWorkerNode = FindNodeWithNodeId(
			groupedLogicalRepTargets->nodeId,
			false);
		superuserConnection = GetNodeUserDatabaseConnection(
			connectionFlags,
			targetWorkerNode->workerName,
			targetWorkerNode->workerPort,
			user,
			databaseName);

		/*
		 * Operations on subscriptions cannot run in a transaction block. We
		 * claim the connections exclusively to ensure they do not get used for
		 * metadata syncing, which does open a transaction block.
		 */
		ClaimConnectionExclusively(superuserConnection);

		groupedLogicalRepTargets->superuserConnection = superuserConnection;

		LogicalRepTarget *target = NULL;
		foreach_ptr(target, groupedLogicalRepTargets->logicalRepTargetList)
		{
			target->superuserConnection = superuserConnection;
		}
	}
}


/*
 * CreateGroupedLogicalRepTargetsConnections closes the  connections for all of the
 * nodes in the groupedLogicalRepTargetsHash.
 */
void
CloseGroupedLogicalRepTargetsConnections(HTAB *groupedLogicalRepTargetsHash)
{
	HASH_SEQ_STATUS status;
	GroupedLogicalRepTargets *groupedLogicalRepTargets = NULL;
	foreach_htab(groupedLogicalRepTargets, &status, groupedLogicalRepTargetsHash)
	{
		CloseConnection(groupedLogicalRepTargets->superuserConnection);
	}
}


/*
 * WaitForRelationSubscriptionsBecomeReady waits until the states of the
 * subsriptions in the groupedLogicalRepTargetsHash becomes ready. This should happen
 * very quickly, because we don't use the COPY logic from the subscriptions. So
 * all that's needed is to start reading from the replication slot.
 *
 * The function errors if the subscriptions on one of the nodes don't become
 * ready within LogicalReplicationErrorTimeout. The function also reports its
 * progress every logicalReplicationProgressReportTimeout.
 */
void
WaitForAllSubscriptionsToBecomeReady(HTAB *groupedLogicalRepTargetsHash)
{
	HASH_SEQ_STATUS status;
	GroupedLogicalRepTargets *groupedLogicalRepTargets = NULL;
	foreach_htab(groupedLogicalRepTargets, &status, groupedLogicalRepTargetsHash)
	{
		WaitForGroupedLogicalRepTargetsToBecomeReady(groupedLogicalRepTargets);
	}
	elog(LOG, "The states of all subscriptions have become READY");
}


/*
 * WaitForRelationSubscriptionsBecomeReady waits until the states of the
 * subsriptions for each shard becomes ready. This should happen very quickly,
 * because we don't use the COPY logic from the subscriptions. So all that's
 * needed is to start reading from the replication slot.
 *
 * The function errors if the subscriptions don't become ready within
 * LogicalReplicationErrorTimeout. The function also reports its progress in
 * every logicalReplicationProgressReportTimeout.
 */
static void
WaitForGroupedLogicalRepTargetsToBecomeReady(
	GroupedLogicalRepTargets *groupedLogicalRepTargets)
{
	TimestampTz previousSizeChangeTime = GetCurrentTimestamp();
	TimestampTz previousReportTime = GetCurrentTimestamp();

	MultiConnection *superuserConnection = groupedLogicalRepTargets->superuserConnection;

	/*
	 * We might be in the loop for a while. Since we don't need to preserve
	 * any memory beyond this function, we can simply switch to a child context
	 * and reset it on every iteration to make sure we don't slowly build up
	 * a lot of memory.
	 */
	MemoryContext loopContext = AllocSetContextCreateInternal(CurrentMemoryContext,
															  "WaitForRelationSubscriptionsBecomeReady",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(loopContext);
	while (true)
	{
		/* we're done, all relations are ready */
		if (RelationSubscriptionsAreReady(groupedLogicalRepTargets))
		{
			ereport(LOG, (errmsg("The states of the relations belonging to the "
								 "subscriptions became READY on the "
								 "target node %s:%d",
								 superuserConnection->hostname,
								 superuserConnection->port)));

			break;
		}

		/* log the progress if necessary */
		if (TimestampDifferenceExceeds(previousReportTime,
									   GetCurrentTimestamp(),
									   logicalReplicationProgressReportTimeout))
		{
			ereport(LOG, (errmsg("Not all subscriptions on target node %s:%d "
								 "are READY yet",
								 superuserConnection->hostname,
								 superuserConnection->port)));

			previousReportTime = GetCurrentTimestamp();
		}

		/* Error out if the size does not change within the given time threshold */
		if (TimestampDifferenceExceeds(previousSizeChangeTime,
									   GetCurrentTimestamp(),
									   LogicalReplicationTimeout))
		{
			ereport(ERROR, (errmsg("The logical replication waiting timeout "
								   "of %d msec is exceeded",
								   LogicalReplicationTimeout),
							errdetail("The subscribed relations haven't become "
									  "ready on the target node %s:%d",
									  superuserConnection->hostname,
									  superuserConnection->port),
							errhint(
								"Logical replication has failed to initialize "
								"on the target node. If not, consider using "
								"higher values for "
								"citus.logical_replication_timeout")));
		}

		/* wait for 1 second (1000 miliseconds) and try again */
		WaitForMiliseconds(1000);
		MemoryContextReset(loopContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * SubscriptionNamesValueList returns a SQL value list containing the
 * subscription names from the logicalRepTargetList. This value list can
 * be used in a query by using the IN operator.
 */
static char *
SubscriptionNamesValueList(List *logicalRepTargetList)
{
	StringInfo subscriptionValueList = makeStringInfo();
	appendStringInfoString(subscriptionValueList, "(");
	bool first = true;

	LogicalRepTarget *target = NULL;
	foreach_ptr(target, logicalRepTargetList)
	{
		if (!first)
		{
			appendStringInfoString(subscriptionValueList, ",");
		}
		else
		{
			first = false;
		}
		appendStringInfoString(subscriptionValueList, quote_literal_cstr(
								   target->subscriptionName));
	}
	appendStringInfoString(subscriptionValueList, ")");
	return subscriptionValueList->data;
}


/*
 * RelationSubscriptionsAreReady gets the subscription status for each
 * subscriptions and returns false if at least one of them is not ready.
 */
static bool
RelationSubscriptionsAreReady(GroupedLogicalRepTargets *groupedLogicalRepTargets)
{
	bool raiseInterrupts = false;

	List *logicalRepTargetList = groupedLogicalRepTargets->logicalRepTargetList;
	MultiConnection *superuserConnection = groupedLogicalRepTargets->superuserConnection;

	char *subscriptionValueList = SubscriptionNamesValueList(logicalRepTargetList);
	char *query = psprintf(
		"SELECT count(*) FROM pg_subscription_rel, pg_stat_subscription "
		"WHERE srsubid = subid AND srsubstate != 'r' AND subname IN %s",
		subscriptionValueList);
	int querySent = SendRemoteCommand(superuserConnection, query);
	if (querySent == 0)
	{
		ReportConnectionError(superuserConnection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(superuserConnection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(superuserConnection, result, ERROR);
	}

	int rowCount = PQntuples(result);
	int columnCount = PQnfields(result);

	if (columnCount != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of columns returned while reading ")));
	}
	if (rowCount != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of rows returned while reading ")));
	}

	int columnIndex = 0;
	int rowIndex = 0;

	/* we're using the pstrdup to copy the data into the current memory context */
	char *resultString = pstrdup(PQgetvalue(result, rowIndex, columnIndex));

	PQclear(result);
	ForgetResults(superuserConnection);

	int64 resultInt = SafeStringToInt64(resultString);

	return resultInt == 0;
}


/*
 * WaitForAllSubscriptionToCatchUp waits until the last LSN reported by the
 * subscription.
 *
 * The function errors if the target LSN doesn't increase within
 * LogicalReplicationErrorTimeout. The function also reports its progress in
 * every logicalReplicationProgressReportTimeout.
 */
void
WaitForAllSubscriptionsToCatchUp(MultiConnection *sourceConnection,
								 HTAB *groupedLogicalRepTargetsHash)
{
	XLogRecPtr sourcePosition = GetRemoteLogPosition(sourceConnection);
	HASH_SEQ_STATUS status;
	GroupedLogicalRepTargets *groupedLogicalRepTargets = NULL;
	foreach_htab(groupedLogicalRepTargets, &status, groupedLogicalRepTargetsHash)
	{
		WaitForGroupedLogicalRepTargetsToCatchUp(sourcePosition,
												 groupedLogicalRepTargets);
	}
}


/*
 * WaitForNodeSubscriptionToCatchUp waits until the last LSN reported by the
 * subscription.
 *
 * The function errors if the target LSN doesn't increase within
 * LogicalReplicationErrorTimeout. The function also reports its progress in
 * every logicalReplicationProgressReportTimeout.
 */
static void
WaitForGroupedLogicalRepTargetsToCatchUp(XLogRecPtr sourcePosition,
										 GroupedLogicalRepTargets *
										 groupedLogicalRepTargets)
{
	XLogRecPtr previousTargetPosition = 0;
	TimestampTz previousLSNIncrementTime = GetCurrentTimestamp();

	/* report in the first iteration as well */
	TimestampTz previousReportTime = 0;
	MultiConnection *superuserConnection = groupedLogicalRepTargets->superuserConnection;


	/*
	 * We might be in the loop for a while. Since we don't need to preserve
	 * any memory beyond this function, we can simply switch to a child context
	 * and reset it on every iteration to make sure we don't slowly build up
	 * a lot of memory.
	 */
	MemoryContext loopContext = AllocSetContextCreateInternal(CurrentMemoryContext,
															  "WaitForShardSubscriptionToCatchUp",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(loopContext);

	while (true)
	{
		XLogRecPtr targetPosition = GetSubscriptionPosition(groupedLogicalRepTargets);
		if (targetPosition >= sourcePosition)
		{
			ereport(LOG, (errmsg(
							  "The LSN of the target subscriptions on node %s:%d have "
							  "caught up with the source LSN ",
							  superuserConnection->hostname,
							  superuserConnection->port)));

			break;
		}

		/*
		 * The following logic ensures that the subsription continues to grow withing
		 * LogicalReplicationErrorTimeout duration. Otherwise, we error out since we
		 * suspect that there is a problem on the target. It also handles the progess
		 * reporting.
		 */
		if (targetPosition > previousTargetPosition)
		{
			/* variable is only used for the log message */
			uint64 previousTargetBeforeThisLoop = previousTargetPosition;

			previousTargetPosition = targetPosition;
			previousLSNIncrementTime = GetCurrentTimestamp();

			if (TimestampDifferenceExceeds(previousReportTime,
										   GetCurrentTimestamp(),
										   logicalReplicationProgressReportTimeout))
			{
				ereport(LOG, (errmsg(
								  "The LSN of the target subscriptions on node %s:%d have "
								  "increased from %ld to %ld at %s where the source LSN is %ld  ",
								  superuserConnection->hostname,
								  superuserConnection->port, previousTargetBeforeThisLoop,
								  targetPosition,
								  timestamptz_to_str(previousLSNIncrementTime),
								  sourcePosition)));

				previousReportTime = GetCurrentTimestamp();
			}
		}
		else
		{
			if (TimestampDifferenceExceeds(previousLSNIncrementTime,
										   GetCurrentTimestamp(),
										   LogicalReplicationTimeout))
			{
				ereport(ERROR, (errmsg("The logical replication waiting timeout "
									   "of %d msec is exceeded",
									   LogicalReplicationTimeout),
								errdetail("The LSN on the target subscription hasn't "
										  "caught up ready on the target node %s:%d",
										  superuserConnection->hostname,
										  superuserConnection->port),
								errhint(
									"There might have occurred problems on the target "
									"node. If not consider using higher values for "
									"citus.logical_replication_error_timeout")));
			}
		}

		/* sleep for 1 seconds (1000 miliseconds) and try again */
		WaitForMiliseconds(1000);

		MemoryContextReset(loopContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * WaitForMiliseconds waits for given timeout and then checks for some
 * interrupts.
 */
static void
WaitForMiliseconds(long timeout)
{
	int latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;

	/* wait until timeout, or until somebody wakes us up */
	int rc = WaitLatch(MyLatch, latchFlags, timeout, PG_WAIT_EXTENSION);

	/* emergency bailout if postmaster has died */
	if (rc & WL_POSTMASTER_DEATH)
	{
		proc_exit(1);
	}

	if (rc & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
}


/*
 * GetSubscriptionPosition gets the minimum WAL log position of the
 * subscription given subscriptions: That is the WAL log position on the source
 * node up to which the subscription completed replication.
 */
static XLogRecPtr
GetSubscriptionPosition(GroupedLogicalRepTargets *groupedLogicalRepTargets)
{
	char *subscriptionValueList = SubscriptionNamesValueList(
		groupedLogicalRepTargets->logicalRepTargetList);
	return GetRemoteLSN(groupedLogicalRepTargets->superuserConnection, psprintf(
							"SELECT min(latest_end_lsn) FROM pg_stat_subscription "
							"WHERE subname IN %s", subscriptionValueList));
}
