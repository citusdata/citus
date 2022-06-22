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

#if PG_VERSION_NUM >= PG_VERSION_12
#include "access/genam.h"
#endif

#if PG_VERSION_NUM >= PG_VERSION_13
#include "postmaster/interrupt.h"
#endif

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/dbcommands.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "distributed/adaptive_executor.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_rebalancer.h"
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


#define REPLICATION_SLOT_CATALOG_TABLE_NAME "pg_replication_slots"
#define CURRENT_LOG_POSITION_COMMAND "SELECT pg_current_wal_lsn()"

/* decimal representation of Adler-16 hash value of citus_shard_move_publication */
#define SHARD_MOVE_ADVISORY_LOCK_FIRST_KEY 44000

/* decimal representation of Adler-16 hash value of citus_shard_move_subscription */
#define SHARD_MOVE_ADVISORY_LOCK_SECOND_KEY 55152


/* GUC variable, defaults to 2 hours */
int LogicalReplicationTimeout = 2 * 60 * 60 * 1000;


/* see the comment in master_move_shard_placement */
bool PlacementMovedUsingLogicalReplicationInTX = false;


/* report in every 10 seconds */
static int logicalReplicationProgressReportTimeout = 10 * 1000;


static void CreateForeignConstraintsToReferenceTable(List *shardList,
													 MultiConnection *targetConnection);
static List * PrepareReplicationSubscriptionList(List *shardList);
static Bitmapset * TableOwnerIds(List *shardList);
static void CreateReplicaIdentity(List *shardList, char *nodeName, int32
								  nodePort);
static List * GetReplicaIdentityCommandListForShard(Oid relationId, uint64 shardId);
static List * GetIndexCommandListForShardBackingReplicaIdentity(Oid relationId,
																uint64 shardId);
static void CreatePostLogicalReplicationDataLoadObjects(List *shardList,
														char *targetNodeName,
														int32 targetNodePort);
static void ExecuteCreateIndexCommands(List *shardList, char *targetNodeName,
									   int targetNodePort);
static void ExecuteCreateConstraintsBackedByIndexCommands(List *shardList,
														  char *targetNodeName,
														  int targetNodePort);
static List * ConvertNonExistingPlacementDDLCommandsToTasks(List *shardCommandList,
															uint64 shardId,
															char *targetNodeName,
															int targetNodePort);
static void ExecuteClusterOnCommands(List *shardList, char *targetNodeName,
									 int targetNodePort);
static void ExecuteCreateIndexStatisticsCommands(List *shardList, char *targetNodeName,
												 int targetNodePort);
static void ExecuteRemainingPostLoadTableCommands(List *shardList, char *targetNodeName,
												  int targetNodePort);
static void CreatePartitioningHierarchy(List *shardList, char *targetNodeName,
										int targetNodePort);
static void CreateColocatedForeignKeys(List *shardList, char *targetNodeName,
									   int targetNodePort);
static void ConflictOnlyWithIsolationTesting(void);
static void DropShardMovePublications(MultiConnection *connection,
									  Bitmapset *tableOwnerIds);
static void DropShardMoveSubscriptions(MultiConnection *connection,
									   Bitmapset *tableOwnerIds);
static void CreateShardMovePublications(MultiConnection *connection, List *shardList,
										Bitmapset *tableOwnerIds);
static void CreateShardMoveSubscriptions(MultiConnection *connection,
										 char *sourceNodeName,
										 int sourceNodePort, char *userName,
										 char *databaseName,
										 Bitmapset *tableOwnerIds);
static char * escape_param_str(const char *str);
static XLogRecPtr GetRemoteLogPosition(MultiConnection *connection);
static XLogRecPtr GetRemoteLSN(MultiConnection *connection, char *command);
static void WaitForRelationSubscriptionsBecomeReady(MultiConnection *targetConnection,
													Bitmapset *tableOwnerIds);
static uint64 TotalRelationSizeForSubscription(MultiConnection *connection,
											   char *command);
static bool RelationSubscriptionsAreReady(MultiConnection *targetConnection,
										  Bitmapset *tableOwnerIds);
static void WaitForShardMoveSubscription(MultiConnection *targetConnection,
										 XLogRecPtr sourcePosition,
										 Bitmapset *tableOwnerIds);
static void WaitForMiliseconds(long timeout);
static XLogRecPtr GetSubscriptionPosition(MultiConnection *connection,
										  Bitmapset *tableOwnerIds);
static char * ShardMovePublicationName(Oid ownerId);
static char * ShardMoveSubscriptionName(Oid ownerId);
static void AcquireLogicalReplicationLock(void);
static void DropAllShardMoveLeftovers(void);
static void DropAllShardMoveSubscriptions(MultiConnection *connection);
static void DropAllShardMoveReplicationSlots(MultiConnection *connection);
static void DropAllShardMovePublications(MultiConnection *connection);
static void DropAllShardMoveUsers(MultiConnection *connection);
static char * ShardMoveSubscriptionNamesValueList(Bitmapset *tableOwnerIds);
static void DropShardMoveSubscription(MultiConnection *connection,
									  char *subscriptionName);
static void DropShardMoveReplicationSlot(MultiConnection *connection,
										 char *publicationName);
static void DropShardMovePublication(MultiConnection *connection, char *publicationName);
static void DropShardMoveUser(MultiConnection *connection, char *username);

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

	Bitmapset *tableOwnerIds = TableOwnerIds(replicationSubscriptionList);

	DropAllShardMoveLeftovers();

	MultiConnection *sourceConnection =
		GetNodeUserDatabaseConnection(connectionFlags, sourceNodeName, sourceNodePort,
									  superUser, databaseName);
	MultiConnection *targetConnection =
		GetNodeUserDatabaseConnection(connectionFlags, targetNodeName, targetNodePort,
									  superUser, databaseName);

	/*
	 * Operations on publications and subscriptions cannot run in a transaction
	 * block. Claim the connections exclusively to ensure they do not get used
	 * for metadata syncing, which does open a transaction block.
	 */
	ClaimConnectionExclusively(sourceConnection);
	ClaimConnectionExclusively(targetConnection);

	PG_TRY();
	{
		/*
		 * We have to create the primary key (or any other replica identity)
		 * before the initial COPY is done. This is necessary because as soon
		 * as the COPY command finishes, the update/delete operations that
		 * are queued will be replicated. And, if the replica identity does not
		 * exist on the target, the replication would fail.
		 */
		CreateReplicaIdentity(shardList, targetNodeName, targetNodePort);

		/* set up the publication on the source and subscription on the target */
		CreateShardMovePublications(sourceConnection, replicationSubscriptionList,
									tableOwnerIds);
		CreateShardMoveSubscriptions(targetConnection, sourceNodeName, sourceNodePort,
									 superUser, databaseName, tableOwnerIds);

		/* only useful for isolation testing, see the function comment for the details */
		ConflictOnlyWithIsolationTesting();

		/*
		 * Logical replication starts with copying the existing data for each table in
		 * the publication. During the copy operation the state of the associated relation
		 * subscription is not ready. There is no point of locking the shards before the
		 * subscriptions for each relation becomes ready, so wait for it.
		 */
		WaitForRelationSubscriptionsBecomeReady(targetConnection, tableOwnerIds);

		/*
		 * Wait until the subscription is caught up to changes that has happened
		 * after the initial COPY on the shards.
		 */
		XLogRecPtr sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardMoveSubscription(targetConnection, sourcePosition, tableOwnerIds);

		/*
		 * Now lets create the post-load objects, such as the indexes, constraints
		 * and partitioning hierarchy. Once they are done, wait until the replication
		 * catches up again. So we don't block writes too long.
		 */
		CreatePostLogicalReplicationDataLoadObjects(shardList, targetNodeName,
													targetNodePort);
		sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardMoveSubscription(targetConnection, sourcePosition, tableOwnerIds);

		/*
		 * We're almost done, we'll block the writes to the shards that we're
		 * replicating and expect the subscription to catch up quickly afterwards.
		 *
		 * Notice that although shards in partitioned relation are excluded from
		 * logical replication, they are still locked against modification, and
		 * foreign constraints are created on them too.
		 */
		BlockWritesToShardList(shardList);

		sourcePosition = GetRemoteLogPosition(sourceConnection);
		WaitForShardMoveSubscription(targetConnection, sourcePosition, tableOwnerIds);

		/*
		 * We're creating the foreign constraints to reference tables after the
		 * data is already replicated and all the necessary locks are acquired.
		 *
		 * We prefer to do it here because the placements of reference tables
		 * are always valid, and any modification during the shard move would
		 * cascade to the hash distributed tables' shards if we had created
		 * the constraints earlier.
		 */
		CreateForeignConstraintsToReferenceTable(shardList, targetConnection);

		/* we're done, cleanup the publication and subscription */
		DropShardMoveSubscriptions(targetConnection, tableOwnerIds);
		DropShardMovePublications(sourceConnection, tableOwnerIds);

		/*
		 * We use these connections exclusively for subscription management,
		 * because otherwise subsequent metadata changes may inadvertedly use
		 * these connections instead of the connections that were used to
		 * grab locks in BlockWritesToShardList.
		 */
		CloseConnection(targetConnection);
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
		if (PQstatus(targetConnection->pgConn) != CONNECTION_OK ||
			PQisBusy(targetConnection->pgConn))
		{
			targetConnection = GetNodeUserDatabaseConnection(connectionFlags,
															 targetNodeName,
															 targetNodePort,
															 superUser, databaseName);
		}
		DropShardMoveSubscriptions(targetConnection, tableOwnerIds);

		/* reconnect if the connection failed or is waiting for a command */
		if (PQstatus(sourceConnection->pgConn) != CONNECTION_OK ||
			PQisBusy(sourceConnection->pgConn))
		{
			sourceConnection = GetNodeUserDatabaseConnection(connectionFlags,
															 sourceNodeName,
															 sourceNodePort, superUser,
															 databaseName);
		}
		DropShardMovePublications(sourceConnection, tableOwnerIds);

		/* We don't need to UnclaimConnections since we're already erroring out */

		PG_RE_THROW();
	}
	PG_END_TRY();
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
 * DropAllShardMoveLeftovers drops shard move subscriptions, publications, roles
 * and replication slots on all nodes. These might have been left there after
 * the coordinator crashed during a shard move. It's important to delete them
 * for two reasons:
 * 1. Starting new shard moves will fail when they exist, because it cannot
 *    create them.
 * 2. Leftover replication slots that are not consumed from anymore make it
 *    impossible for WAL to be dropped. This can cause out-of-disk issues.
 */
static void
DropAllShardMoveLeftovers(void)
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

		DropAllShardMoveSubscriptions(cleanupConnection);
		DropAllShardMoveUsers(cleanupConnection);
	}

	MultiConnection *cleanupConnection = NULL;
	foreach_ptr(cleanupConnection, cleanupConnectionList)
	{
		/*
		 * If replication slot could not be dropped while dropping the
		 * subscriber, drop it here.
		 */
		DropAllShardMoveReplicationSlots(cleanupConnection);
		DropAllShardMovePublications(cleanupConnection);

		/*
		 * We close all connections that we opened for the dropping here. That
		 * way we don't keep these connections open unnecessarily during the
		 * shard move (which can take a long time).
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
 * TableOwnerIds returns a bitmapset containing all the owners of the tables
 * that the given shards belong to.
 */
static Bitmapset *
TableOwnerIds(List *shardList)
{
	ShardInterval *shardInterval = NULL;
	Bitmapset *tableOwnerIds = NULL;

	foreach_ptr(shardInterval, shardList)
	{
		tableOwnerIds = bms_add_member(tableOwnerIds, TableOwnerOid(
										   shardInterval->relationId));
	}

	return tableOwnerIds;
}


/*
 * CreateReplicaIdentity gets a shardList and creates all the replica identities
 * on the shards in the given node.
 */
static void
CreateReplicaIdentity(List *shardList, char *nodeName, int32 nodePort)
{
	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateReplicaIdentity",
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
CreatePostLogicalReplicationDataLoadObjects(List *shardList, char *targetNodeName,
											int32 targetNodePort)
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
	ExecuteCreateIndexCommands(shardList, targetNodeName, targetNodePort);
	ExecuteCreateConstraintsBackedByIndexCommands(shardList, targetNodeName,
												  targetNodePort);
	ExecuteClusterOnCommands(shardList, targetNodeName, targetNodePort);
	ExecuteCreateIndexStatisticsCommands(shardList, targetNodeName, targetNodePort);

	/*
	 * Once the indexes are created, there are few more objects like triggers and table
	 * statistics that should be created after the data move.
	 */
	ExecuteRemainingPostLoadTableCommands(shardList, targetNodeName, targetNodePort);

	/* create partitioning hierarchy, if any */
	CreatePartitioningHierarchy(shardList, targetNodeName, targetNodePort);

	/* create colocated foreign keys, if any */
	CreateColocatedForeignKeys(shardList, targetNodeName, targetNodePort);
}


/*
 * ExecuteCreateIndexCommands gets a shardList and creates all the indexes
 * for the given shardList in the given target node.
 *
 * The execution is done in parallel, and throws an error if any of the
 * commands fail.
 */
static void
ExecuteCreateIndexCommands(List *shardList, char *targetNodeName, int targetNodePort)
{
	List *taskList = NIL;
	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
		Oid relationId = shardInterval->relationId;

		List *tableCreateIndexCommandList =
			GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																	   INCLUDE_CREATE_INDEX_STATEMENTS);

		List *shardCreateIndexCommandList =
			WorkerApplyShardDDLCommandList(tableCreateIndexCommandList,
										   shardInterval->shardId);
		List *taskListForShard =
			ConvertNonExistingPlacementDDLCommandsToTasks(shardCreateIndexCommandList,
														  shardInterval->shardId,
														  targetNodeName, targetNodePort);
		taskList = list_concat(taskList, taskListForShard);
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
							"(indexes) on node %s:%d", targetNodeName,
							targetNodePort)));

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
ExecuteCreateConstraintsBackedByIndexCommands(List *shardList, char *targetNodeName,
											  int targetNodePort)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(constraints backed by indexes) on node %s:%d",
							targetNodeName,
							targetNodePort)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateConstraintsBackedByIndexContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
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
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  shardCreateConstraintCommandList);
		MemoryContextReset(localContext);
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
											  uint64 shardId,
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
		task->anchorShardId = shardId;

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
ExecuteClusterOnCommands(List *shardList, char *targetNodeName, int targetNodePort)
{
	List *taskList = NIL;
	ListCell *shardCell;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
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
				shardInterval->shardId,
				targetNodeName, targetNodePort);
		taskList = list_concat(taskList, taskListForShard);
	}

	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(CLUSTER ON) on node %s:%d", targetNodeName,
							targetNodePort)));

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
ExecuteCreateIndexStatisticsCommands(List *shardList, char *targetNodeName, int
									 targetNodePort)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(index statistics) on node %s:%d", targetNodeName,
							targetNodePort)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateIndexStatisticsContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *shardCell;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
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
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  shardAlterIndexSetStatisticsCommandList);

		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * ExecuteRemainingPostLoadTableCommands gets a shardList and creates
 * all the remaining post load objects other than the indexes
 * in the given target node.
 */
static void
ExecuteRemainingPostLoadTableCommands(List *shardList, char *targetNodeName, int
									  targetNodePort)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(triggers and table statistics) on node %s:%d",
							targetNodeName,
							targetNodePort)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateTableStatisticsContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
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
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  shardPostLoadTableCommandList);

		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CreatePartitioningHierarchy gets a shardList and creates the partitioning
 * hierarchy between the shardList, if any,
 */
static void
CreatePartitioningHierarchy(List *shardList, char *targetNodeName, int targetNodePort)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(partitioning hierarchy) on node %s:%d", targetNodeName,
							targetNodePort)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreatePartitioningHierarchy",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);

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
			SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
													  tableOwner,
													  list_make1(
														  attachPartitionCommand));
			MemoryContextReset(localContext);
		}
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CreateColocatedForeignKeys gets a shardList and creates the colocated foreign
 * keys between the shardList, if any,
 */
static void
CreateColocatedForeignKeys(List *shardList, char *targetNodeName, int targetNodePort)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(co-located foreign keys) on node %s:%d", targetNodeName,
							targetNodePort)));

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CreateColocatedForeignKeys",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);

		List *shardForeignConstraintCommandList = NIL;
		List *referenceTableForeignConstraintList = NIL;
		CopyShardForeignConstraintCommandListGrouped(shardInterval,
													 &shardForeignConstraintCommandList,
													 &referenceTableForeignConstraintList);

		if (shardForeignConstraintCommandList == NIL)
		{
			/* no colocated foreign keys, skip */
			continue;
		}

		/*
		 * Creating foreign keys may acquire conflicting locks when done in
		 * parallel. Hence we create foreign keys one at a time.
		 *
		 */
		char *tableOwner = TableOwner(shardInterval->relationId);
		SendCommandListToWorkerOutsideTransaction(targetNodeName, targetNodePort,
												  tableOwner,
												  shardForeignConstraintCommandList);
		MemoryContextReset(localContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CreateForeignConstraintsToReferenceTable is used to create the foreign constraints
 * from distributed to reference tables in the newly created shard replicas.
 */
static void
CreateForeignConstraintsToReferenceTable(List *shardList,
										 MultiConnection *targetConnection)
{
	ereport(DEBUG1, (errmsg("Creating post logical replication objects "
							"(foreign keys to reference tables) on node "
							"%s:%d", targetConnection->hostname,
							targetConnection->port)));

	MemoryContext localContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "CreateForeignConstraintsToReferenceTable",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);


	ListCell *shardCell = NULL;
	foreach(shardCell, shardList)
	{
		ListCell *commandCell = NULL;
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardCell);
		List *commandList = GetForeignConstraintCommandsToReferenceTable(shardInterval);

		/* iterate over the commands and execute them in the same connection */
		foreach(commandCell, commandList)
		{
			char *commandString = lfirst(commandCell);

			ExecuteCriticalRemoteCommand(targetConnection, commandString);
		}
		MemoryContextReset(localContext);
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
static void
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
 * DropShardMovePublication drops the publication used for shard moves over the given
 * connection, if it exists. It also drops the replication slot if that slot was not
 * dropped while dropping the subscription.
 */
static void
DropShardMovePublications(MultiConnection *connection, Bitmapset *tableOwnerIds)
{
	int ownerId = -1;

	while ((ownerId = bms_next_member(tableOwnerIds, ownerId)) >= 0)
	{
		/*
		 * If replication slot can not be dropped while dropping the subscriber, drop
		 * it here.
		 */
		DropShardMoveReplicationSlot(connection, ShardMoveSubscriptionName(ownerId));
		DropShardMovePublication(connection, ShardMovePublicationName(ownerId));
	}
}


/*
 * DropShardMoveReplicationSlot drops the replication slot with the given name
 * if it exists.
 */
static void
DropShardMoveReplicationSlot(MultiConnection *connection, char *replicationSlotName)
{
	ExecuteCriticalRemoteCommand(
		connection,
		psprintf(
			"select pg_drop_replication_slot(slot_name) from "
			REPLICATION_SLOT_CATALOG_TABLE_NAME
			" where slot_name = %s",
			quote_literal_cstr(replicationSlotName)));
}


/*
 * DropShardMovePublication drops the publication with the given name if it
 * exists.
 */
static void
DropShardMovePublication(MultiConnection *connection, char *publicationName)
{
	ExecuteCriticalRemoteCommand(connection, psprintf(
									 "DROP PUBLICATION IF EXISTS %s",
									 quote_identifier(publicationName)));
}


/*
 * ShardMovePublicationName returns the name of the publication for the given
 * table owner.
 */
static char *
ShardMovePublicationName(Oid ownerId)
{
	return psprintf("%s%i", SHARD_MOVE_PUBLICATION_PREFIX, ownerId);
}


/*
 * ShardMoveSubscriptionName returns the name of the subscription for the given
 * owner. If we're running the isolation tester the function also appends the
 * process id normal subscription name.
 *
 * When it contains the PID of the current process it is used for block detection
 * by the isolation test runner, since the replication process on the publishing
 * node uses the name of the subscription as the application_name of the SQL session.
 * This PID is then extracted from the application_name to find out which PID on the
 * coordinator is blocked by the blocked replication process.
 */
static char *
ShardMoveSubscriptionName(Oid ownerId)
{
	if (RunningUnderIsolationTest)
	{
		return psprintf("%s%i_%i", SHARD_MOVE_SUBSCRIPTION_PREFIX, ownerId, MyProcPid);
	}
	else
	{
		return psprintf("%s%i", SHARD_MOVE_SUBSCRIPTION_PREFIX, ownerId);
	}
}


/*
 * ShardMoveSubscriptionRole returns the name of the role used by the
 * subscription that subscribes to the tables of the given owner.
 */
static char *
ShardMoveSubscriptionRole(Oid ownerId)
{
	return psprintf("%s%i", SHARD_MOVE_SUBSCRIPTION_ROLE_PREFIX, ownerId);
}


/*
 * GetQueryResultStringList expects a query that returns a single column of
 * strings. This query is executed on the connection and the function then
 * returns the results of the query in a List.
 */
static List *
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
 * DropAllShardMoveSubscriptions drops all the existing subscriptions that
 * match our shard move naming scheme on the node that the connection points
 * to.
 */
static void
DropAllShardMoveSubscriptions(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT subname FROM pg_subscription "
		"WHERE subname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_MOVE_SUBSCRIPTION_PREFIX));
	List *subscriptionNameList = GetQueryResultStringList(connection, query);
	char *subscriptionName;
	foreach_ptr(subscriptionName, subscriptionNameList)
	{
		DropShardMoveSubscription(connection, subscriptionName);
	}
}


/*
 * DropAllShardMoveUsers drops all the users that match our shard move naming
 * scheme for temporary shard move users on the node that the connection points
 * to.
 */
static void
DropAllShardMoveUsers(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT rolname FROM pg_roles "
		"WHERE rolname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_MOVE_SUBSCRIPTION_ROLE_PREFIX));
	List *usernameList = GetQueryResultStringList(connection, query);
	char *username;
	foreach_ptr(username, usernameList)
	{
		DropShardMoveUser(connection, username);
	}
}


/*
 * DropAllShardMoveReplicationSlots drops all the existing replication slots
 * that match our shard move naming scheme on the node that the connection
 * points to.
 */
static void
DropAllShardMoveReplicationSlots(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT slot_name FROM pg_replication_slots "
		"WHERE slot_name LIKE %s || '%%'",
		quote_literal_cstr(SHARD_MOVE_SUBSCRIPTION_PREFIX));
	List *slotNameList = GetQueryResultStringList(connection, query);
	char *slotName;
	foreach_ptr(slotName, slotNameList)
	{
		DropShardMoveReplicationSlot(connection, slotName);
	}
}


/*
 * DropAllShardMovePublications drops all the existing publications that
 * match our shard move naming scheme on the node that the connection points
 * to.
 */
static void
DropAllShardMovePublications(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT pubname FROM pg_publication "
		"WHERE pubname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_MOVE_PUBLICATION_PREFIX));
	List *publicationNameList = GetQueryResultStringList(connection, query);
	char *publicationName;
	foreach_ptr(publicationName, publicationNameList)
	{
		DropShardMovePublication(connection, publicationName);
	}
}


/*
 * DropShardMoveSubscriptions drops subscriptions from the subscriber node that
 * are used to move shards for the given table owners. Note that, it drops the
 * replication slots on the publisher node if it can drop the slots as well
 * with the DROP SUBSCRIPTION command. Otherwise, only the subscriptions will
 * be deleted with DROP SUBSCRIPTION via the connection. In the latter case,
 * replication slots will be dropped while cleaning the publisher node when
 * calling DropShardMovePublications.
 */
static void
DropShardMoveSubscriptions(MultiConnection *connection, Bitmapset *tableOwnerIds)
{
	int ownerId = -1;
	while ((ownerId = bms_next_member(tableOwnerIds, ownerId)) >= 0)
	{
		DropShardMoveSubscription(connection, ShardMoveSubscriptionName(ownerId));
		DropShardMoveUser(connection, ShardMoveSubscriptionRole(ownerId));
	}
}


/*
 * DropShardMoveSubscription drops subscription with the given name on the
 * subscriber node. Note that, it also drops the replication slot on the
 * publisher node if it can drop the slot as well with the DROP SUBSCRIPTION
 * command. Otherwise, only the subscription will be deleted with DROP
 * SUBSCRIPTION via the connection.
 */
static void
DropShardMoveSubscription(MultiConnection *connection, char *subscriptionName)
{
	PGresult *result = NULL;

	/*
	 * Instead of ExecuteCriticalRemoteCommand, we use the
	 * ExecuteOptionalRemoteCommand to fall back into the logic inside the
	 * if block below in case of any error while sending the command.
	 */
	int dropCommandResult = ExecuteOptionalRemoteCommand(
		connection,
		psprintf(
			"DROP SUBSCRIPTION IF EXISTS %s",
			quote_identifier(subscriptionName)),
		&result);

	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(connection, ERROR);
	}

	PQclear(result);
	ForgetResults(connection);

	/*
	 * If we can not drop the replication slot using the DROP SUBSCRIPTION command
	 * then we need to alter the subscription to drop the subscriber only and drop
	 * the replication slot separately.
	 */
	if (dropCommandResult != 0)
	{
		StringInfo alterSubscriptionSlotCommand = makeStringInfo();
		StringInfo alterSubscriptionDisableCommand = makeStringInfo();

		appendStringInfo(alterSubscriptionDisableCommand,
						 "ALTER SUBSCRIPTION %s DISABLE",
						 quote_identifier(subscriptionName));
		ExecuteCriticalRemoteCommand(connection,
									 alterSubscriptionDisableCommand->data);

		appendStringInfo(alterSubscriptionSlotCommand,
						 "ALTER SUBSCRIPTION %s SET (slot_name = NONE)",
						 quote_identifier(subscriptionName));
		ExecuteCriticalRemoteCommand(connection, alterSubscriptionSlotCommand->data);

		ExecuteCriticalRemoteCommand(connection, psprintf(
										 "DROP SUBSCRIPTION %s",
										 quote_identifier(subscriptionName)));
	}
}


/*
 * DropShardMoveUser drops the user with the given name if it exists.
 */
static void
DropShardMoveUser(MultiConnection *connection, char *username)
{
	/*
	 * The DROP USER command should not propagate, so we temporarily disable
	 * DDL propagation.
	 */
	SendCommandListToWorkerOutsideTransaction(
		connection->hostname, connection->port, connection->user,
		list_make2(
			"SET LOCAL citus.enable_ddl_propagation TO OFF;",
			psprintf("DROP USER IF EXISTS %s",
					 quote_identifier(username))));
}


/*
 * CreateShardMovePublications creates a set of publications for moving a list
 * of shards over the given connection. One publication is created for each of
 * the table owners in tableOwnerIds. Each of those publications only contains
 * shards that the respective table owner owns.
 */
static void
CreateShardMovePublications(MultiConnection *connection, List *shardList,
							Bitmapset *tableOwnerIds)
{
	int ownerId = -1;

	while ((ownerId = bms_next_member(tableOwnerIds, ownerId)) >= 0)
	{
		StringInfo createPublicationCommand = makeStringInfo();
		bool prefixWithComma = false;

		appendStringInfo(createPublicationCommand, "CREATE PUBLICATION %s FOR TABLE ",
						 ShardMovePublicationName(ownerId));

		ShardInterval *shard = NULL;
		foreach_ptr(shard, shardList)
		{
			if (TableOwnerOid(shard->relationId) != ownerId)
			{
				continue;
			}

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
 * CreateShardMoveSubscriptions creates the subscriptions used for shard moves
 * over the given connection. One subscription is created for each of the table
 * owners in tableOwnerIds. The remote node needs to have appropriate
 * pg_dist_authinfo rows for the user such that the apply process can connect.
 * Because the generated CREATE SUBSCRIPTION statements uses the host and port
 * names directly (rather than looking up any relevant pg_dist_poolinfo rows),
 * all such connections remain direct and will not route through any configured
 * poolers.
 */
static void
CreateShardMoveSubscriptions(MultiConnection *connection, char *sourceNodeName,
							 int sourceNodePort, char *userName, char *databaseName,
							 Bitmapset *tableOwnerIds)
{
	int ownerId = -1;
	while ((ownerId = bms_next_member(tableOwnerIds, ownerId)) >= 0)
	{
		StringInfo createSubscriptionCommand = makeStringInfo();
		StringInfo conninfo = makeStringInfo();

		/*
		 * The CREATE USER command should not propagate, so we temporarily
		 * disable DDL propagation.
		 */
		SendCommandListToWorkerOutsideTransaction(
			connection->hostname, connection->port, connection->user,
			list_make2(
				"SET LOCAL citus.enable_ddl_propagation TO OFF;",
				psprintf(
					"CREATE USER %s SUPERUSER IN ROLE %s",
					ShardMoveSubscriptionRole(ownerId),
					GetUserNameFromId(ownerId, false)
					)));

		appendStringInfo(conninfo, "host='%s' port=%d user='%s' dbname='%s' "
								   "connect_timeout=20",
						 escape_param_str(sourceNodeName), sourceNodePort,
						 escape_param_str(userName), escape_param_str(databaseName));

		appendStringInfo(createSubscriptionCommand,
						 "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
						 "WITH (citus_use_authinfo=true, enabled=false)",
						 quote_identifier(ShardMoveSubscriptionName(ownerId)),
						 quote_literal_cstr(conninfo->data),
						 quote_identifier(ShardMovePublicationName(ownerId)));

		ExecuteCriticalRemoteCommand(connection, createSubscriptionCommand->data);
		pfree(createSubscriptionCommand->data);
		pfree(createSubscriptionCommand);
		ExecuteCriticalRemoteCommand(connection, psprintf(
										 "ALTER SUBSCRIPTION %s OWNER TO %s",
										 ShardMoveSubscriptionName(ownerId),
										 ShardMoveSubscriptionRole(ownerId)
										 ));

		/*
		 * The ALTER ROLE command should not propagate, so we temporarily
		 * disable DDL propagation.
		 */
		SendCommandListToWorkerOutsideTransaction(
			connection->hostname, connection->port, connection->user,
			list_make2(
				"SET LOCAL citus.enable_ddl_propagation TO OFF;",
				psprintf(
					"ALTER ROLE %s NOSUPERUSER",
					ShardMoveSubscriptionRole(ownerId)
					)));

		ExecuteCriticalRemoteCommand(connection, psprintf(
										 "ALTER SUBSCRIPTION %s ENABLE",
										 ShardMoveSubscriptionName(ownerId)
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
static XLogRecPtr
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
 * WaitForRelationSubscriptionsBecomeReady waits until the states of the subsriptions
 * for each shard becomes ready. This indicates that the initial COPY is finished
 * on the shards.
 *
 * The function errors if the total size of the relations that belong to the subscription
 * on the target node doesn't change within LogicalReplicationErrorTimeout. The
 * function also reports its progress in every logicalReplicationProgressReportTimeout.
 */
static void
WaitForRelationSubscriptionsBecomeReady(MultiConnection *targetConnection,
										Bitmapset *tableOwnerIds)
{
	uint64 previousTotalRelationSizeForSubscription = 0;
	TimestampTz previousSizeChangeTime = GetCurrentTimestamp();

	/* report in the first iteration as well */
	TimestampTz previousReportTime = 0;

	uint64 previousReportedTotalSize = 0;


	/*
	 * We might be in the loop for a while. Since we don't need to preserve
	 * any memory beyond this function, we can simply switch to a child context
	 * and reset it on every iteration to make sure we don't slowly build up
	 * a lot of memory.
	 */
	MemoryContext loopContext = AllocSetContextCreateExtended(CurrentMemoryContext,
															  "WaitForRelationSubscriptionsBecomeReady",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(loopContext);

	while (true)
	{
		/* we're done, all relations are ready */
		if (RelationSubscriptionsAreReady(targetConnection, tableOwnerIds))
		{
			ereport(LOG, (errmsg("The states of the relations belonging to the "
								 "subscriptions became READY on the "
								 "target node %s:%d",
								 targetConnection->hostname,
								 targetConnection->port)));

			break;
		}
		char *subscriptionValueList = ShardMoveSubscriptionNamesValueList(tableOwnerIds);

		/* Get the current total size of tables belonging to the subscriber */
		uint64 currentTotalRelationSize =
			TotalRelationSizeForSubscription(targetConnection, psprintf(
												 "SELECT sum(pg_total_relation_size(srrelid)) "
												 "FROM pg_subscription_rel, pg_stat_subscription "
												 "WHERE srsubid = subid AND subname IN %s",
												 subscriptionValueList
												 )
											 );

		/*
		 * The size has not been changed within the last iteration. If necessary
		 * log a messages. If size does not change over a given replication timeout
		 * error out.
		 */
		if (currentTotalRelationSize == previousTotalRelationSizeForSubscription)
		{
			/* log the progress if necessary */
			if (TimestampDifferenceExceeds(previousReportTime,
										   GetCurrentTimestamp(),
										   logicalReplicationProgressReportTimeout))
			{
				ereport(LOG, (errmsg("Subscription size has been staying same for the "
									 "last %d msec",
									 logicalReplicationProgressReportTimeout)));

				previousReportTime = GetCurrentTimestamp();
			}

			/* Error out if the size does not change within the given time threshold */
			if (TimestampDifferenceExceeds(previousSizeChangeTime,
										   GetCurrentTimestamp(),
										   LogicalReplicationTimeout))
			{
				ereport(ERROR, (errmsg("The logical replication waiting timeout "
									   "%d msec exceeded",
									   LogicalReplicationTimeout),
								errdetail("The subscribed relations haven't become "
										  "ready on the target node %s:%d",
										  targetConnection->hostname,
										  targetConnection->port),
								errhint(
									"There might have occurred problems on the target "
									"node. If not, consider using higher values for "
									"citus.logical_replication_timeout")));
			}
		}
		else
		{
			/* first, record that there is some change in the size */
			previousSizeChangeTime = GetCurrentTimestamp();

			/*
			 * Subscription size may decrease or increase.
			 *
			 * Subscription size may decrease in case of VACUUM operation, which
			 * may get fired with autovacuum, on it.
			 *
			 * Increase of the relation's size belonging to subscriber means a successful
			 * copy from publisher to subscriber.
			 */
			bool sizeIncreased = currentTotalRelationSize >
								 previousTotalRelationSizeForSubscription;

			if (TimestampDifferenceExceeds(previousReportTime,
										   GetCurrentTimestamp(),
										   logicalReplicationProgressReportTimeout))
			{
				ereport(LOG, ((errmsg("The total size of the relations belonging to "
									  "subscriptions %s from %ld to %ld at %s "
									  "on the target node %s:%d",
									  sizeIncreased ? "increased" : "decreased",
									  previousReportedTotalSize,
									  currentTotalRelationSize,
									  timestamptz_to_str(previousSizeChangeTime),
									  targetConnection->hostname,
									  targetConnection->port))));

				previousReportedTotalSize = currentTotalRelationSize;
				previousReportTime = GetCurrentTimestamp();
			}
		}

		previousTotalRelationSizeForSubscription = currentTotalRelationSize;

		/* wait for 1 second (1000 miliseconds) and try again */
		WaitForMiliseconds(1000);

		MemoryContextReset(loopContext);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * TotalRelationSizeForSubscription is a helper function which returns the total
 * size of the shards that are replicated via the subscription. Note that the
 * function returns the total size including indexes.
 */
static uint64
TotalRelationSizeForSubscription(MultiConnection *connection, char *command)
{
	bool raiseInterrupts = false;
	uint64 remoteTotalSize = 0;

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
		ereport(ERROR, (errmsg("unexpected number of rows returned by: %s",
							   command)));
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

		remoteTotalSize = strtou64(resultString, NULL, 10);
	}
	else
	{
		ereport(ERROR, (errmsg("unexpected value returned by: %s",
							   command)));
	}

	PQclear(result);
	ForgetResults(connection);

	return remoteTotalSize;
}


/*
 * ShardMoveSubscriptionNamesValueList returns a SQL value list containing the
 * subscription names for all of the given table owner ids. This value list can
 * be used in a query by using the IN operator.
 */
static char *
ShardMoveSubscriptionNamesValueList(Bitmapset *tableOwnerIds)
{
	StringInfo subscriptionValueList = makeStringInfo();
	appendStringInfoString(subscriptionValueList, "(");
	int ownerId = -1;
	bool first = true;

	while ((ownerId = bms_next_member(tableOwnerIds, ownerId)) >= 0)
	{
		if (!first)
		{
			appendStringInfoString(subscriptionValueList, ",");
		}
		else
		{
			first = false;
		}
		appendStringInfoString(subscriptionValueList,
							   quote_literal_cstr(ShardMoveSubscriptionName(ownerId)));
	}
	appendStringInfoString(subscriptionValueList, ")");
	return subscriptionValueList->data;
}


/*
 * RelationSubscriptionsAreReady gets the subscription status for each
 * shard and returns false if at least one of them is not ready.
 */
static bool
RelationSubscriptionsAreReady(MultiConnection *targetConnection,
							  Bitmapset *tableOwnerIds)
{
	bool raiseInterrupts = false;

	char *subscriptionValueList = ShardMoveSubscriptionNamesValueList(tableOwnerIds);
	char *query = psprintf(
		"SELECT count(*) FROM pg_subscription_rel, pg_stat_subscription "
		"WHERE srsubid = subid AND srsubstate != 'r' AND subname IN %s",
		subscriptionValueList);
	int querySent = SendRemoteCommand(targetConnection, query);
	if (querySent == 0)
	{
		ReportConnectionError(targetConnection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(targetConnection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(targetConnection, result, ERROR);
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
	ForgetResults(targetConnection);

	int64 resultInt = SafeStringToInt64(resultString);

	return resultInt == 0;
}


/*
 * WaitForShardMoveSubscription waits until the last LSN reported by the subscription.
 *
 * The function errors if the target LSN doesn't increase within LogicalReplicationErrorTimeout.
 * The function also reports its progress in every logicalReplicationProgressReportTimeout.
 */
static void
WaitForShardMoveSubscription(MultiConnection *targetConnection, XLogRecPtr sourcePosition,
							 Bitmapset *tableOwnerIds)
{
	XLogRecPtr previousTargetPosition = 0;
	TimestampTz previousLSNIncrementTime = GetCurrentTimestamp();

	/* report in the first iteration as well */
	TimestampTz previousReportTime = 0;


	/*
	 * We might be in the loop for a while. Since we don't need to preserve
	 * any memory beyond this function, we can simply switch to a child context
	 * and reset it on every iteration to make sure we don't slowly build up
	 * a lot of memory.
	 */
	MemoryContext loopContext = AllocSetContextCreateExtended(CurrentMemoryContext,
															  "WaitForShardMoveSubscription",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(loopContext);

	while (true)
	{
		XLogRecPtr targetPosition = GetSubscriptionPosition(targetConnection,
															tableOwnerIds);
		if (targetPosition >= sourcePosition)
		{
			ereport(LOG, (errmsg(
							  "The LSN of the target subscriptions on node %s:%d have "
							  "caught up with the source LSN ",
							  targetConnection->hostname,
							  targetConnection->port)));

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
								  targetConnection->hostname,
								  targetConnection->port, previousTargetBeforeThisLoop,
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
									   "%d msec exceeded",
									   LogicalReplicationTimeout),
								errdetail("The LSN on the target subscription hasn't "
										  "caught up ready on the target node %s:%d",
										  targetConnection->hostname,
										  targetConnection->port),
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

	#if PG_VERSION_NUM >= PG_VERSION_13
	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
	#endif
}


/*
 * GetSubscriptionPosition gets the current WAL log position of the subscription, that
 * is the WAL log position on the source node up to which the subscription completed
 * replication.
 */
static XLogRecPtr
GetSubscriptionPosition(MultiConnection *connection, Bitmapset *tableOwnerIds)
{
	char *subscriptionValueList = ShardMoveSubscriptionNamesValueList(tableOwnerIds);
	return GetRemoteLSN(connection, psprintf(
							"SELECT min(latest_end_lsn) FROM pg_stat_subscription "
							"WHERE subname IN %s", subscriptionValueList));
}
