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

#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "common/hashfn.h"
#include "nodes/bitmapset.h"
#include "parser/scansup.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#include "pg_version_constants.h"

#include "distributed/adaptive_executor.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/distributed_planner.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/priority.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_transfer.h"
#include "distributed/version_compat.h"

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
 *
 * We should also keep these in sync with IsCitusShardTransferBackend().
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
														LogicalRepType type,
														bool skipInterShardRelationships,
														bool
														useAutoIdentityLogicalReplication
														);
static void ExecuteCreateIndexCommands(List *logicalRepTargetList, LogicalRepType type,
									   bool useAutoIdentityLogicalReplication);
static void ExecuteCreateConstraintsBackedByIndexCommands(List *logicalRepTargetList);
static List * ConvertNonExistingPlacementDDLCommandsToTasks(List *shardCommandList,
															char *targetNodeName,
															int targetNodePort);
static void ExecuteClusterOnCommands(List *logicalRepTargetList);
static void ExecuteCreateIndexStatisticsCommands(List *logicalRepTargetList);
static void ExecuteRemainingPostLoadTableCommands(List *logicalRepTargetList);
static void PrepareReplicaIdentitiesForPublication(MultiConnection *connection,
												   HTAB *publicationInfoHash);
static char * escape_param_str(const char *str);
static XLogRecPtr GetRemoteLSN(MultiConnection *connection, char *command);
static void WaitForMiliseconds(long timeout);
static XLogRecPtr GetSubscriptionPosition(GroupedLogicalRepTargets *
										  groupedLogicalRepTargets);

static HTAB * CreateShardMovePublicationInfoHash(WorkerNode *targetNode,
												 List *shardIntervals);
static List * CreateShardMoveLogicalRepTargetList(HTAB *publicationInfoHash,
												  List *shardList);
static void WaitForGroupedLogicalRepTargetsToCatchUp(XLogRecPtr sourcePosition,
													 GroupedLogicalRepTargets *
													 groupedLogicalRepTargets);
static void CreateCatchupIndexesForLogicalReplication(List *shardList,
													  MultiConnection *sourceConnection,
													  WorkerNode *targetNode);
static char * ChooseHelperIndexColumn(MultiConnection *sourceConnection,
									  ShardInterval *shardInterval);

/*
 * LogicallyReplicateShards replicates a list of shards from one node to another
 * using logical replication. Once replication is reasonably caught up, writes
 * are blocked and then the publication and subscription are dropped.
 *
 * The caller of the function should ensure that logical replication is applicable
 * for the given shards (or can be forced via useAutoIdentityLogicalReplication),
 * source and target nodes. Also, the caller is responsible for ensuring that the
 * input shard list consists of co-located distributed tables or a single shard.
 */
void
LogicallyReplicateShards(List *shardList, char *sourceNodeName, int sourceNodePort,
						 char *targetNodeName, int targetNodePort,
						 bool skipInterShardRelationshipCreation,
						 bool useAutoIdentityLogicalReplication)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);
	int connectionFlags = FORCE_NEW_CONNECTION;
	List *replicationSubscriptionList = PrepareReplicationSubscriptionList(shardList);

	/* no shards to move */
	if (list_length(replicationSubscriptionList) == 0)
	{
		return;
	}

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

	MultiConnection *sourceReplicationConnection =
		GetReplicationConnection(sourceConnection->hostname, sourceConnection->port);

	if (useAutoIdentityLogicalReplication)
	{
		PrepareReplicaIdentitiesForPublication(sourceConnection, publicationInfoHash);
	}

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
	ConflictWithIsolationTestingBeforeCopy();

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

	UpdatePlacementUpdateStatusForShardIntervalList(
		shardList,
		sourceNodeName,
		sourceNodePort,
		PLACEMENT_UPDATE_STATUS_COPYING_DATA);

	CopyShardsToNode(sourceNode, targetNode, shardList, snapshot);

	/*
	 * We can close this connection now, because we're done copying the
	 * data and thus don't need access to the snapshot anymore. The
	 * replication slot will still be at the same LSN, because the
	 * subscriptions have not been enabled yet.
	 */
	CloseConnection(sourceReplicationConnection);

	/*
	 * Optionally build an index on each destination shard before the catch-up below
	 * so that the logical-replication subscriber can use an index scan (instead of a
	 * sequential scan) while it applies the changes that accumulate during catch-up.
	 * This only does something for tables that lack a replica identity / primary key
	 * index. It must run before the subscriptions are enabled (which happens in
	 * CompleteNonBlockingShardTransfer).
	 */
	if (useAutoIdentityLogicalReplication)
	{
		CreateCatchupIndexesForLogicalReplication(shardList, sourceConnection,
												  targetNode);
	}

	/*
	 * Start the replication and copy all data
	 */
	CompleteNonBlockingShardTransfer(shardList,
									 sourceConnection,
									 publicationInfoHash,
									 logicalRepTargetList,
									 groupedLogicalRepTargetsHash,
									 SHARD_MOVE,
									 skipInterShardRelationshipCreation,
									 useAutoIdentityLogicalReplication);

	/*
	 * We use these connections exclusively for subscription management,
	 * because otherwise subsequent metadata changes may inadvertedly use
	 * these connections instead of the connections that were used to
	 * grab locks in BlockWritesToShardList.
	 */
	CloseGroupedLogicalRepTargetsConnections(groupedLogicalRepTargetsHash);
	CloseConnection(sourceConnection);
}


/*
 * CreateCatchupIndexesForLogicalReplication builds, for each shard in the given
 * list, at most one index on the destination shard before the catch-up phase of the
 * logical-replication based move, so that the subscriber can use an index scan
 * (instead of a sequential scan) while it applies the changes that accumulate during
 * catch-up.
 *
 * The caller only invokes this in the force_logical_auto_identity shard
 * transfer mode. Per shard, it then does the first of the following that
 * applies, and nothing when none does (i.e. the subscriber sequential-scans
 * the destination during catch-up):
 *
 *  - Nothing for a partitioned table (it holds no data of its own) or for a table
 *    that already has a replica identity / primary key index, which Citus builds on
 *    the destination shard before the catch-up anyway (see CreateReplicaIdentities).
 *
 *  - When the table has a usable existing index eligible to be built early (chosen by
 *    ChooseReplicationHelperIndexToBuildEarly), build that index and keep it permanently:
 *    it is excluded from the late CREATE INDEX phase (see ExecuteCreateIndexCommands),
 *    so it is created exactly once.
 *
 *  - Otherwise, build a temporary single-column helper index (on the column chosen by
 *    ChooseHelperIndexColumn). Unlike the existing index above, this is a throwaway: it
 *    is registered with the resource cleanup framework (CLEANUP_OBJECT_INDEX,
 *    CLEANUP_ALWAYS) before it is created, so it is dropped once the transfer completes.
 *
 * Every index here is built on the destination shard, which is not yet visible to
 * users, so a plain (blocking) CREATE INDEX is safe and cheaper than CONCURRENTLY.
 */
static void
CreateCatchupIndexesForLogicalReplication(List *shardList,
										  MultiConnection *sourceConnection,
										  WorkerNode *targetNode)
{
	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardList)
	{
		Oid relationId = shardInterval->relationId;

		/* partitioned tables hold no data of their own */
		if (PartitionedTable(relationId))
		{
			continue;
		}

		/*
		 * A replica identity / primary key index is built on the destination shard
		 * before the catch-up (see CreateReplicaIdentities), so the subscriber can
		 * already locate rows efficiently and we build nothing more here.
		 */
		Relation relation = table_open(relationId, AccessShareLock);
		Oid replicaIdentityOrPKIndex = GetRelationIdentityOrPK(relation);
		table_close(relation, NoLock);

		if (OidIsValid(replicaIdentityOrPKIndex))
		{
			continue;
		}

		/*
		 * Prefer reusing one of the table's own existing indexes -- built early here
		 * and kept permanently -- over the throwaway helper below. This does nothing
		 * for a table whose only usable btree backs a UNIQUE or EXCLUDE constraint
		 * (ChooseReplicationHelperIndexToBuildEarly refuses those), so such a table
		 * falls through.
		 */
		List *tableCreateIndexCommandList =
			GetReplicationHelperIndexCommandList(relationId);
		if (tableCreateIndexCommandList != NIL)
		{
			List *shardCreateIndexCommandList =
				WorkerApplyShardDDLCommandList(tableCreateIndexCommandList,
											   shardInterval->shardId);

			ereport(DEBUG1, (errmsg("building existing index early on shard "
									UINT64_FORMAT " on node %s:%d",
									shardInterval->shardId,
									targetNode->workerName, targetNode->workerPort)));

			SendCommandListToWorkerOutsideTransaction(
				targetNode->workerName, targetNode->workerPort,
				TableOwner(relationId), shardCreateIndexCommandList);

			continue;
		}

		/*
		 * Otherwise fall back to a temporary single-column helper index, which is
		 * dropped once the transfer completes.
		 */
		char *columnName = ChooseHelperIndexColumn(sourceConnection, shardInterval);
		if (columnName == NULL)
		{
			/*
			 * No column is eligible for a btree index (e.g. no column type has a
			 * default btree operator class). Fall back to today's behavior, i.e. a
			 * sequential scan on the subscriber.
			 */
			ereport(WARNING, (errmsg("no suitable column found to build a temporary "
									 "replica identity helper index for shard "
									 UINT64_FORMAT "; falling back to a sequential "
									 "scan during catch-up", shardInterval->shardId)));
			continue;
		}

		char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);

		/*
		 * Include the operation id to avoid colliding with an unrelated index that
		 * happens to use the helper prefix and shard id. The maximum generated name
		 * is shorter than NAMEDATALEN.
		 */
		char *indexName = psprintf("citus_ri_helper_" UINT64_FORMAT "_" UINT64_FORMAT,
								   shardInterval->shardId, CurrentOperationId);
		char *schemaName = get_namespace_name(get_rel_namespace(relationId));
		char *qualifiedIndexName = quote_qualified_identifier(schemaName, indexName);

		/*
		 * Register the cleanup record before creating the index so that it is dropped
		 * even if we crash right after the CREATE INDEX below.
		 */
		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_INDEX,
											  qualifiedIndexName,
											  targetNode->groupId,
											  CLEANUP_ALWAYS);

		ereport(DEBUG1, (errmsg("building temporary replica identity helper index "
								"on column \"%s\" of shard " UINT64_FORMAT
								" on node %s:%d",
								columnName, shardInterval->shardId,
								targetNode->workerName, targetNode->workerPort)));

		/*
		 * The helper index only speeds up the catch-up phase, so a failure to
		 * build it (e.g. a column value larger than the btree entry limit) must
		 * not abort the whole move. Use an optional command and, on failure, fall
		 * back to a sequential scan on the subscriber. The CLEANUP_ALWAYS record
		 * registered above drops the (absent) index with DROP INDEX IF EXISTS at
		 * the end of the operation.
		 */
		bool helperIndexCreated = SendOptionalCommandListToWorkerOutsideTransaction(
			targetNode->workerName, targetNode->workerPort,
			TableOwner(relationId),
			list_make1(psprintf("CREATE INDEX %s ON %s USING btree (%s)",
								quote_identifier(indexName), qualifiedShardName,
								quote_identifier(columnName))));

		if (!helperIndexCreated)
		{
			ereport(WARNING, (errmsg("could not build temporary replica identity "
									 "helper index on shard " UINT64_FORMAT
									 "; falling back to a sequential scan during "
									 "catch-up", shardInterval->shardId)));
		}
	}
}


/*
 * ChooseHelperIndexColumn picks the column of the given shard's table that is most
 * useful to build a temporary replica-identity helper index on. It returns the column
 * name or NULL if the table has no column that can back a btree index.
 */
static char *
ChooseHelperIndexColumn(MultiConnection *sourceConnection, ShardInterval *shardInterval)
{
	Oid relationId = shardInterval->relationId;
	Relation relation = table_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	StringInfo eligibleColumnNames = makeStringInfo();
	char *firstEligibleColumn = NULL;
	bool foundEligible = false;

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++
		 )
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attisdropped || attributeForm->attnum <= 0)
		{
			continue;
		}

		/* the column type must have a default btree operator class to be indexable */
		Oid opclass = GetDefaultOpClass(attributeForm->atttypid, BTREE_AM_OID);
		if (!OidIsValid(opclass))
		{
			continue;
		}

		if (foundEligible)
		{
			appendStringInfoChar(eligibleColumnNames, ',');
		}
		else
		{
			firstEligibleColumn = pstrdup(NameStr(attributeForm->attname));
		}

		appendStringInfoString(eligibleColumnNames,
							   quote_literal_cstr(NameStr(attributeForm->attname)));
		foundEligible = true;
	}

	table_close(relation, NoLock);

	if (!foundEligible)
	{
		return NULL;
	}

	/*
	 * Ask the source shard which eligible column is the most selective according to
	 * its statistics. We normalize stadistinct (negative values are a fraction of the
	 * row count, positive values are an absolute count) and give unanalyzed columns a
	 * score of 0 so that, in the absence of statistics, we fall back to the first
	 * eligible column (lowest attnum). The query runs on the source connection because
	 * the source shard holds both the data and the user's ANALYZE statistics.
	 *
	 * If the query cannot be sent, or does not come back as a single non-null column
	 * name, we log a warning and keep the first-eligible-column fallback.
	 */
	char *qualifiedShardName = ConstructQualifiedShardName(shardInterval);
	char *statsQuery = psprintf(
		"SELECT a.attname FROM pg_catalog.pg_attribute a "
		"JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
		"LEFT JOIN pg_catalog.pg_statistic s "
		"ON s.starelid = a.attrelid AND s.staattnum = a.attnum "
		"WHERE a.attrelid = %s::regclass "
		"AND a.attname = ANY(ARRAY[%s]::name[]) AND NOT a.attisdropped "
		"ORDER BY (CASE WHEN s.stadistinct IS NULL THEN 0 "
		"WHEN s.stadistinct < 0 THEN -s.stadistinct "
		"WHEN c.reltuples > 0 THEN s.stadistinct / c.reltuples "
		"ELSE 0 END) DESC, a.attnum LIMIT 1",
		quote_literal_cstr(qualifiedShardName), eligibleColumnNames->data);

	char *chosenColumn = firstEligibleColumn;

	int querySent = SendRemoteCommand(sourceConnection, statsQuery);
	if (querySent == 0)
	{
		ereport(WARNING, (errmsg("could not send column statistics query to source "
								 "shard %s; using first eligible column \"%s\" for the "
								 "replica identity helper index",
								 qualifiedShardName, firstEligibleColumn)));
	}
	else
	{
		bool raiseInterrupts = false;
		PGresult *result = GetRemoteCommandResult(sourceConnection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ereport(WARNING, (errmsg("column statistics query on source shard %s did "
									 "not return a successful response; using first "
									 "eligible column \"%s\" for the replica identity "
									 "helper index",
									 qualifiedShardName, firstEligibleColumn)));
		}
		else if (PQntuples(result) != 1 || PQnfields(result) != 1)
		{
			ereport(WARNING, (errmsg("column statistics query on source shard %s "
									 "returned %d row(s) and %d column(s) instead of a "
									 "single value; using first eligible column \"%s\" "
									 "for the replica identity helper index",
									 qualifiedShardName, PQntuples(result),
									 PQnfields(result), firstEligibleColumn)));
		}
		else if (PQgetisnull(result, 0, 0))
		{
			ereport(WARNING, (errmsg("column statistics query on source shard %s "
									 "returned a null value; using first eligible column "
									 "\"%s\" for the replica identity helper index",
									 qualifiedShardName, firstEligibleColumn)));
		}
		else
		{
			chosenColumn = pstrdup(PQgetvalue(result, 0, 0));
		}

		PQclear(result);
		ForgetResults(sourceConnection);
	}

	return chosenColumn;
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
	foreach_declared_ptr(target, logicalRepTargetList)
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
								 LogicalRepType type,
								 bool skipInterShardRelationshipCreation,
								 bool useAutoIdentityLogicalReplication)
{
	/* Start applying the changes from the replication slots to catch up. */
	EnableSubscriptions(logicalRepTargetList);

	UpdatePlacementUpdateStatusForShardIntervalList(
		shardList,
		sourceConnection->hostname,
		sourceConnection->port,
		PLACEMENT_UPDATE_STATUS_CATCHING_UP);

	/*
	 * Wait until all the subscriptions are caught up to changes that
	 * happened after the initial COPY on the shards.
	 */
	WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);

	UpdatePlacementUpdateStatusForShardIntervalList(
		shardList,
		sourceConnection->hostname,
		sourceConnection->port,
		PLACEMENT_UPDATE_STATUS_CREATING_CONSTRAINTS);

	/*
	 * Now lets create the post-load objects, such as the indexes, constraints
	 * and partitioning hierarchy. Once they are done, wait until the replication
	 * catches up again. So we don't block writes too long.
	 */
	CreatePostLogicalReplicationDataLoadObjects(logicalRepTargetList, type,
												skipInterShardRelationshipCreation,
												useAutoIdentityLogicalReplication);

	UpdatePlacementUpdateStatusForShardIntervalList(
		shardList,
		sourceConnection->hostname,
		sourceConnection->port,
		PLACEMENT_UPDATE_STATUS_FINAL_CATCH_UP);

	WaitForAllSubscriptionsToCatchUp(sourceConnection, groupedLogicalRepTargetsHash);


	/* only useful for isolation testing, see the function comment for the details */
	ConflictWithIsolationTestingAfterCopy();

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

	if (type != SHARD_SPLIT && !skipInterShardRelationshipCreation)
	{
		UpdatePlacementUpdateStatusForShardIntervalList(
			shardList,
			sourceConnection->hostname,
			sourceConnection->port,
			PLACEMENT_UPDATE_STATUS_CREATING_FOREIGN_KEYS);

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

	UpdatePlacementUpdateStatusForShardIntervalList(
		shardList,
		sourceConnection->hostname,
		sourceConnection->port,
		PLACEMENT_UPDATE_STATUS_COMPLETING);
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
	foreach_declared_ptr(shardInterval, shardIntervals)
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
		target->replicationSlot->name =
			ReplicationSlotNameForNodeAndOwnerForOperation(SHARD_MOVE,
														   nodeId,
														   ownerId,
														   CurrentOperationId);
		target->replicationSlot->targetNodeId = nodeId;
		target->replicationSlot->tableOwnerId = ownerId;
		logicalRepTargetList = lappend(logicalRepTargetList, target);
	}

	ShardInterval *shardInterval = NULL;
	foreach_declared_ptr(shardInterval, shardList)
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
	foreach_declared_ptr(target, logicalRepTargetList)
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
	foreach_declared_ptr(shardInterval, shardList)
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
			ereport(DEBUG1, (errmsg("Creating replica identity for shard %ld on "
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
											LogicalRepType type,
											bool skipInterShardRelationships,
											bool useAutoIdentityLogicalReplication)
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
	ExecuteCreateIndexCommands(logicalRepTargetList, type,
							   useAutoIdentityLogicalReplication);
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
	if (type != SHARD_SPLIT && !skipInterShardRelationships)
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
ExecuteCreateIndexCommands(List *logicalRepTargetList, LogicalRepType type,
						   bool useAutoIdentityLogicalReplication)
{
	List *taskList = NIL;
	LogicalRepTarget *target = NULL;
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
		{
			Oid relationId = shardInterval->relationId;

			/*
			 * For a shard move in the force_logical_auto_identity mode
			 * (useAutoIdentityLogicalReplication = true), we may have built one of
			 * the table's existing indexes early on the destination shard (see
			 * CreateCatchupIndexesForLogicalReplication); if so, that index
			 * must be excluded here so it is not created a second time.
			 */
			List *tableCreateIndexCommandList = NIL;
			if (type == SHARD_MOVE && useAutoIdentityLogicalReplication)
			{
				tableCreateIndexCommandList =
					GetTableIndexAndConstraintCommandsExcludingReplicaIdentityAndReplicationHelperIndex
					(
						relationId, INCLUDE_CREATE_INDEX_STATEMENTS);
			}
			else
			{
				tableCreateIndexCommandList =
					GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(
						relationId, INCLUDE_CREATE_INDEX_STATEMENTS);
			}

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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;
		foreach_declared_ptr(shardInterval, target->newShards)
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ShardInterval *shardInterval = NULL;

		/*
		 * Iterate on split shards list for a given shard and create constraints.
		 */
		foreach_declared_ptr(shardInterval, target->newShards)
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
 * ConflictWithIsolationTestingBeforeCopy is only useful to test
 * get_rebalance_progress by pausing before doing the actual copy. This way we
 * can see the state of the tables at that point. This should not be called by
 * any code-path except for code paths to move and split shards().
 *
 * Note that since the cost of calling this function is pretty low, we prefer
 * to use it in non-assert builds as well not to diverge in the behaviour.
 */
extern void
ConflictWithIsolationTestingBeforeCopy(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	if (RunningUnderCitusTestSuite)
	{
		SET_LOCKTAG_ADVISORY(tag, MyDatabaseId,
							 SHARD_MOVE_ADVISORY_LOCK_SECOND_KEY,
							 SHARD_MOVE_ADVISORY_LOCK_FIRST_KEY, 2);

		/* uses sharelock so concurrent moves don't conflict with eachother */
		(void) LockAcquire(&tag, ShareLock, sessionLock, dontWait);
	}
}


/*
 * ConflictWithIsolationTestingAfterCopy is only useful for two types of tests.
 * 1. Testing the output of get_rebalance_progress after the copy is completed,
 *    but before the move is completely finished. Because finishing the move
 *    will clear the contents of get_rebalance_progress.
 * 2. To test that our non-blocking shard moves/splits actually don't block
 *    writes. Since logically replicating shards does eventually block
 *    modifications, it becomes tricky to use isolation tester to show
 *    concurrent behaviour of online shard rebalancing and modification
 *    queries. So, during logical replication we call this function at
 *    the end of the catchup, right before blocking writes.
 *
 * Note that since the cost of calling this function is pretty low, we prefer
 * to use it in non-assert builds as well not to diverge in the behaviour.
 */
extern void
ConflictWithIsolationTestingAfterCopy(void)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	if (RunningUnderCitusTestSuite)
	{
		SET_LOCKTAG_ADVISORY(tag, MyDatabaseId,
							 SHARD_MOVE_ADVISORY_LOCK_FIRST_KEY,
							 SHARD_MOVE_ADVISORY_LOCK_SECOND_KEY, 2);

		/* uses sharelock so concurrent moves don't conflict with eachother */
		(void) LockAcquire(&tag, ShareLock, sessionLock, dontWait);
	}
}


/*
 * PublicationName returns the name of the publication for the given node and
 * table owner.
 */
char *
PublicationName(LogicalRepType type, uint32_t nodeId, Oid ownerId)
{
	return psprintf("%s%u_%u_%lu", publicationPrefix[type],
					nodeId, ownerId, CurrentOperationId);
}


/*
 * ReplicationSlotNameForNodeAndOwnerForOperation returns the name of the
 * replication slot for the given node, table owner and operation id.
 */
char *
ReplicationSlotNameForNodeAndOwnerForOperation(LogicalRepType type, uint32_t nodeId,
											   Oid ownerId, OperationId operationId)
{
	StringInfo slotName = makeStringInfo();
	appendStringInfo(slotName, "%s%u_%u_%lu", replicationSlotPrefix[type], nodeId,
					 ownerId, operationId);

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
	return psprintf("%s%u_%lu", subscriptionPrefix[type],
					ownerId, CurrentOperationId);
}


/*
 * SubscriptionRoleName returns the name of the role used by the
 * subscription that subscribes to the tables of the given owner.
 */
char *
SubscriptionRoleName(LogicalRepType type, Oid ownerId)
{
	return psprintf("%s%u_%lu", subscriptionRolePrefix[type], ownerId,
					CurrentOperationId);
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
 * PrepareReplicaIdentitiesForPublication makes sure that every source shard in
 * publicationInfoHash can publish all of its modifications (INSERT/UPDATE/DELETE)
 * over logical replication.
 *
 * A table that has neither a REPLICA IDENTITY nor a PRIMARY KEY cannot publish
 * UPDATE/DELETE: the publisher errors out with "cannot update table ... because
 * it does not have a replica identity and publishes updates", which would break
 * the user's concurrent writes for the duration of the transfer.
 *
 * To avoid this, we temporarily set REPLICA IDENTITY FULL on such source
 * shards right before we create the publications, and the original replica
 * identity is restored via the resource cleanup framework later. The caller
 * only invokes this in the force_logical_auto_identity shard transfer mode.
 *
 * Also note that setting REPLICA IDENTITY FULL requires a brief AccessExclusiveLock
 * on the source shard, so we bound it with a short lock_timeout (see below) so that
 * a busy shard fails the transfer fast.
 */
static void
PrepareReplicaIdentitiesForPublication(MultiConnection *connection,
									   HTAB *publicationInfoHash)
{
	WorkerNode *worker = FindWorkerNode(connection->hostname, connection->port);

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, publicationInfoHash);

	PublicationInfo *entry = NULL;
	while ((entry = (PublicationInfo *) hash_seq_search(&status)) != NULL)
	{
		ShardInterval *shard = NULL;
		foreach_declared_ptr(shard, entry->shardIntervals)
		{
			/*
			 * If the table already has a replica identity (or is a partitioned
			 * parent, which holds no data), it can publish all modifications and
			 * we don't need to touch it.
			 */
			if (RelationCanPublishAllModifications(shard->relationId))
			{
				continue;
			}

			Relation relation = RelationIdGetRelation(shard->relationId);
			char originalReplicaIdentity = relation->rd_rel->relreplident;
			RelationClose(relation);

			char *shardName = ConstructQualifiedShardName(shard);

			/*
			 * Register the cleanup record before making the change so that the
			 * original replica identity is restored even if we crash right after
			 * the ALTER TABLE below. The object name encodes the original setting.
			 */
			char *cleanupObjectName = psprintf("%c:%s", originalReplicaIdentity,
											   shardName);
			InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_REPLICA_IDENTITY,
												  cleanupObjectName,
												  worker->groupId,
												  CLEANUP_ALWAYS);

			/*
			 * Setting REPLICA IDENTITY FULL requires an AccessExclusiveLock on the
			 * shard, which conflicts with any concurrent access to it. To avoid
			 * blocking indefinitely (and blocking others queued behind us for the
			 * lock) we set a short lock_timeout so that we fail fast if the shard is
			 * busy. The whole transfer then errors out and can be retried; the
			 * resource cleanup framework restores the original replica identity via
			 * the record we registered above.
			 *
			 * DDL propagation is disabled because the ALTER targets a shard, which is
			 * not a distributed object. Both settings use SET LOCAL and are therefore
			 * scoped to the transaction opened by
			 * SendCommandListToWorkerOutsideTransactionWithConnection, so they are
			 * reset automatically once it commits.
			 */
			SendCommandListToWorkerOutsideTransactionWithConnection(
				connection,
				list_make3(
					"SET LOCAL lock_timeout TO '1s'",
					"SET LOCAL citus.enable_ddl_propagation TO OFF",
					psprintf("ALTER TABLE %s REPLICA IDENTITY FULL", shardName)));
		}
	}
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
		foreach_declared_ptr(shard, entry->shardIntervals)
		{
			char *shardName = ConstructQualifiedShardName(shard);

			if (prefixWithComma)
			{
				appendStringInfoString(createPublicationCommand, ",");
			}

			appendStringInfoString(createPublicationCommand, shardName);
			prefixWithComma = true;
		}

		WorkerNode *worker = FindWorkerNode(connection->hostname,
											connection->port);
		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_PUBLICATION,
											  entry->name,
											  worker->groupId,
											  CLEANUP_ALWAYS);

		ExecuteCriticalRemoteCommand(connection, DISABLE_DDL_PROPAGATION);
		ExecuteCriticalRemoteCommand(connection, createPublicationCommand->data);
		ExecuteCriticalRemoteCommand(connection, ENABLE_DDL_PROPAGATION);
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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		ReplicationSlotInfo *replicationSlot = target->replicationSlot;

		WorkerNode *worker = FindWorkerNode(sourceConnection->hostname,
											sourceConnection->port);
		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_REPLICATION_SLOT,
											  replicationSlot->name,
											  worker->groupId,
											  CLEANUP_ALWAYS);

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
	foreach_declared_ptr(target, logicalRepTargetList)
	{
		int ownerId = target->tableOwnerId;

		WorkerNode *worker = FindWorkerNode(target->superuserConnection->hostname,
											target->superuserConnection->port);

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

		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_USER,
											  target->subscriptionOwnerName,
											  worker->groupId,
											  CLEANUP_ALWAYS);

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

		                 /*
		                  * password_required specifies whether connections to the publisher
		                  * made as a result of this subscription must use password authentication.
		                  * However, this setting is ignored when the subscription is owned
		                  * by a superuser.
		                  * Given that this command is executed below with superuser
		                  * ExecuteCriticalRemoteCommand(target->superuserConnection,
		                  *                              createSubscriptionCommand->data);
		                  * We are safe to pass password_required as false because
		                  * it will be ignored anyway
		                  */
						 "copy_data=false, enabled=false, slot_name=%s, password_required=false",
						 quote_identifier(target->subscriptionName),
						 quote_literal_cstr(conninfo->data),
						 quote_identifier(target->publication->name),
						 quote_identifier(target->replicationSlot->name));

		if (EnableBinaryProtocol)
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

		InsertCleanupRecordOutsideTransaction(CLEANUP_OBJECT_SUBSCRIPTION,
											  target->subscriptionName,
											  worker->groupId,
											  CLEANUP_ALWAYS);

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
	foreach_declared_ptr(target, logicalRepTargetList)
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
		foreach_declared_ptr(target, groupedLogicalRepTargets->logicalRepTargetList)
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
	foreach_declared_ptr(target, logicalRepTargetList)
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
				ereport(LOG, (errmsg("The LSN of the target subscriptions on node %s:%d "
									 "has increased from %X/%X to %X/%X at %s where the "
									 "source LSN is %X/%X ",
									 superuserConnection->hostname,
									 superuserConnection->port,
									 LSN_FORMAT_ARGS(previousTargetBeforeThisLoop),
									 LSN_FORMAT_ARGS(targetPosition),
									 timestamptz_to_str(previousLSNIncrementTime),
									 LSN_FORMAT_ARGS(sourcePosition))));

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
