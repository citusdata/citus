#include "postgres.h"

#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"

#include "distributed/argutils.h"
#include "distributed/clonenode_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_rebalancer.h"


static void BlockAllWritesToWorkerNode(WorkerNode *workerNode);
static bool GetNodeIsInRecoveryStatus(WorkerNode *workerNode);
static void PromoteCloneNode(WorkerNode *cloneWorkerNode);
static void EnsureSingleNodePromotion(WorkerNode *primaryNode);

PG_FUNCTION_INFO_V1(citus_promote_clone_and_rebalance);

/*
 * citus_promote_clone_and_rebalance promotes an inactive clone node to become
 * the new primary node, replacing its original primary node.
 *
 * This function performs the following steps:
 * 1. Validates that the clone node exists and is properly configured
 * 2. Ensures the clone is inactive and has a valid primary node reference
 * 3. Blocks all writes to the primary node to prevent data divergence
 * 4. Waits for the clone to catch up with the primary's WAL position
 * 5. Promotes the clone node to become a standalone primary
 * 6. Updates metadata to mark the clone as active and primary
 * 7. Rebalances shards between the old primary and new primary
 * 8. Returns information about the promotion and any shard movements
 *
 * Arguments:
 * - clone_nodeid: The node ID of the clone to promote
 * - catchUpTimeoutSeconds: Maximum time to wait for clone to catch up (default: 300)
 *
 * The function ensures data consistency by blocking writes during the promotion
 * process and verifying replication lag before proceeding.
 */
Datum
citus_promote_clone_and_rebalance(PG_FUNCTION_ARGS)
{
	/* Ensure superuser and coordinator */
	EnsureSuperUser();
	EnsureCoordinator();

	/* Get clone_nodeid argument */
	int32 cloneNodeIdArg = PG_GETARG_INT32(0);

	/* Get catchUpTimeoutSeconds argument with default value of 300 */
	int32 catchUpTimeoutSeconds = PG_ARGISNULL(2) ? 300 : PG_GETARG_INT32(2);

	/* Lock pg_dist_node to prevent concurrent modifications during this operation */
	LockRelationOid(DistNodeRelationId(), RowExclusiveLock);

	WorkerNode *cloneNode = FindNodeAnyClusterByNodeId(cloneNodeIdArg);
	if (cloneNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Clone node with ID %d not found.", cloneNodeIdArg)));
	}

	if (!cloneNode->nodeisclone || cloneNode->nodeprimarynodeid == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Node %s:%d (ID %d) is not a valid clone or its primary node ID is not set.",
							cloneNode->workerName, cloneNode->workerPort, cloneNode->
							nodeId)));
	}

	if (cloneNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Clone node %s:%d (ID %d) is already active and cannot be promoted.",
							cloneNode->workerName, cloneNode->workerPort, cloneNode->
							nodeId)));
	}

	WorkerNode *primaryNode = FindNodeAnyClusterByNodeId(cloneNode->nodeprimarynodeid);
	if (primaryNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node with ID %d (for clone %s:%d) not found.",
							   cloneNode->nodeprimarynodeid, cloneNode->workerName,
							   cloneNode->workerPort)));
	}

	if (primaryNode->nodeisclone)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is itself a clone.",
							   primaryNode->workerName, primaryNode->workerPort,
							   primaryNode->nodeId)));
	}

	if (!primaryNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is not active.",
							   primaryNode->workerName, primaryNode->workerPort,
							   primaryNode->nodeId)));
	}

	/* Ensure the primary node is related to the clone node */
	if (primaryNode->nodeId != cloneNode->nodeprimarynodeid)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Clone node %s:%d (ID %d) is not a clone of the primary node %s:%d (ID %d).",
							cloneNode->workerName, cloneNode->workerPort, cloneNode->
							nodeId,
							primaryNode->workerName, primaryNode->workerPort,
							primaryNode->nodeId)));
	}

	EnsureSingleNodePromotion(primaryNode);
	ereport(NOTICE, (errmsg(
						 "Starting promotion process for clone node %s:%d (ID %d), original primary %s:%d (ID %d)",
						 cloneNode->workerName, cloneNode->workerPort, cloneNode->
						 nodeId,
						 primaryNode->workerName, primaryNode->workerPort, primaryNode
						 ->nodeId)));

	/* Step 0: Check if clone is replica of provided primary node and is not synchronous */
	char *operation = "promote";
	EnsureValidCloneMode(primaryNode, cloneNode->workerName, cloneNode->workerPort,
						 operation);

	/* Step 1: Block Writes on Original Primary's Shards */
	ereport(NOTICE, (errmsg(
						 "Blocking writes on shards of original primary node %s:%d (group %d)",
						 primaryNode->workerName, primaryNode->workerPort, primaryNode
						 ->groupId)));

	BlockAllWritesToWorkerNode(primaryNode);

	/* Step 2: Wait for Clone to Catch Up */
	ereport(NOTICE, (errmsg(
						 "Waiting for clone %s:%d to catch up with primary %s:%d (timeout: %d seconds)",
						 cloneNode->workerName, cloneNode->workerPort,
						 primaryNode->workerName, primaryNode->workerPort,
						 catchUpTimeoutSeconds)));

	bool caughtUp = false;
	const int sleepIntervalSeconds = 5;
	int elapsedTimeSeconds = 0;

	while (elapsedTimeSeconds < catchUpTimeoutSeconds)
	{
		uint64 repLag = GetReplicationLag(primaryNode, cloneNode);
		if (repLag <= 0)
		{
			caughtUp = true;
			break;
		}
		pg_usleep(sleepIntervalSeconds * 1000000L);
		elapsedTimeSeconds += sleepIntervalSeconds;
	}

	if (!caughtUp)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Clone %s:%d failed to catch up with primary %s:%d within %d seconds.",
							cloneNode->workerName, cloneNode->workerPort,
							primaryNode->workerName, primaryNode->workerPort,
							catchUpTimeoutSeconds)));
	}

	ereport(NOTICE, (errmsg("Clone %s:%d is now caught up with primary %s:%d.",
							cloneNode->workerName, cloneNode->workerPort,
							primaryNode->workerName, primaryNode->workerPort)));


	/* Step 3: PostgreSQL Clone Promotion */
	ereport(NOTICE, (errmsg("Attempting to promote clone %s:%d via pg_promote().",
							cloneNode->workerName, cloneNode->workerPort)));

	PromoteCloneNode(cloneNode);

	/* Step 4: Update Clone Metadata in pg_dist_node on Coordinator */

	ereport(NOTICE, (errmsg("Updating metadata for promoted clone %s:%d (ID %d)",
							cloneNode->workerName, cloneNode->workerPort, cloneNode->
							nodeId)));
	ActivateCloneNodeAsPrimary(cloneNode);

	/* We need to sync metadata changes to all nodes before rebalancing shards
	 * since the rebalancing algorithm depends on the latest metadata.
	 */
	SyncNodeMetadataToNodes();

	/* Step 5: Split Shards Between Primary and Clone */
	SplitShardsBetweenPrimaryAndClone(primaryNode, cloneNode, PG_GETARG_NAME_OR_NULL(1))
	;


	TransactionModifiedNodeMetadata = true; /* Inform Citus about metadata change */
	TriggerNodeMetadataSyncOnCommit();      /* Ensure changes are propagated */


	ereport(NOTICE, (errmsg(
						 "Clone node %s:%d (ID %d) metadata updated. It is now a primary",
						 cloneNode->workerName, cloneNode->workerPort, cloneNode->
						 nodeId)));


	/* Step 6: Unblock Writes (should be handled by transaction commit) */
	ereport(NOTICE, (errmsg(
						 "Clone node %s:%d (ID %d) successfully registered as a worker node",
						 cloneNode->workerName, cloneNode->workerPort, cloneNode->
						 nodeId)));

	PG_RETURN_VOID();
}


/*
 * PromoteCloneNode promotes a clone node to a primary node using PostgreSQL's
 * pg_promote() function.
 *
 * This function performs the following steps:
 * 1. Connects to the clone node
 * 2. Executes pg_promote(wait := true) to promote the clone to primary
 * 3. Reconnects to verify the promotion was successful
 * 4. Checks if the node is still in recovery mode (which would indicate failure)
 *
 * The function throws an ERROR if:
 * - Connection to the clone node fails
 * - The pg_promote() command fails
 * - The clone is still in recovery mode after promotion attempt
 *
 * On success, it logs a NOTICE message confirming the promotion.
 *
 * Note: This function assumes the clone has already been validated for promotion
 * (e.g., replication lag is acceptable, clone is not synchronous, etc.)
 */
static void
PromoteCloneNode(WorkerNode *cloneWorkerNode)
{
	/* Step 1: Connect to the clone node */
	int connectionFlag = 0;
	MultiConnection *cloneConnection = GetNodeConnection(connectionFlag,
														 cloneWorkerNode->workerName,
														 cloneWorkerNode->workerPort);

	if (PQstatus(cloneConnection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(cloneConnection, ERROR);
	}

	/* Step 2: Execute pg_promote() to promote the clone to primary */
	const char *promoteQuery = "SELECT pg_promote(wait := true);";
	int resultCode = SendRemoteCommand(cloneConnection, promoteQuery);
	if (resultCode == 0)
	{
		ReportConnectionError(cloneConnection, ERROR);
	}
	ForgetResults(cloneConnection);
	CloseConnection(cloneConnection);

	/* Step 3: Reconnect and verify the promotion was successful */
	if (GetNodeIsInRecoveryStatus(cloneWorkerNode))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Failed to promote clone %s:%d (ID %d). It is still in recovery.",
							cloneWorkerNode->workerName, cloneWorkerNode->workerPort,
							cloneWorkerNode->nodeId)));
	}
	else
	{
		ereport(NOTICE, (errmsg(
							 "Clone node %s:%d (ID %d) has been successfully promoted.",
							 cloneWorkerNode->workerName, cloneWorkerNode->workerPort,
							 cloneWorkerNode->nodeId)));
	}
}


static void
BlockAllWritesToWorkerNode(WorkerNode *workerNode)
{
	ereport(NOTICE, (errmsg("Blocking all writes to worker node %s:%d (ID %d)",
							workerNode->workerName, workerNode->workerPort, workerNode->
							nodeId)));

	LockShardsInWorkerPlacementList(workerNode, AccessExclusiveLock);
}


/*
 * GetNodeIsInRecoveryStatus checks if a PostgreSQL node is currently in recovery mode.
 *
 * This function connects to the specified worker node and executes pg_is_in_recovery()
 * to determine if the node is still acting as a replica (in recovery) or has been
 * promoted to a primary (not in recovery).
 *
 * Arguments:
 * - workerNode: The WorkerNode to check recovery status for
 *
 * Returns:
 * - true if the node is in recovery mode (acting as a replica)
 * - false if the node is not in recovery mode (acting as a primary)
 *
 * The function will ERROR if:
 * - Cannot establish connection to the node
 * - The remote query fails
 * - The query result cannot be parsed
 *
 * This is used after promoting a clone node to verify that the
 * promotion was successful and the node is no longer in recovery mode.
 */
static bool
GetNodeIsInRecoveryStatus(WorkerNode *workerNode)
{
	int connectionFlag = 0;
	MultiConnection *nodeConnection = GetNodeConnection(connectionFlag,
														workerNode->workerName,
														workerNode->workerPort);

	if (PQstatus(nodeConnection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(nodeConnection, ERROR);
	}

	const char *recoveryQuery = "SELECT pg_is_in_recovery();";
	int resultCode = SendRemoteCommand(nodeConnection, recoveryQuery);
	if (resultCode == 0)
	{
		ReportConnectionError(nodeConnection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(nodeConnection, true);
	if (!IsResponseOK(result))
	{
		ReportResultError(nodeConnection, result, ERROR);
	}

	List *recoveryStatusList = ReadFirstColumnAsText(result);
	if (list_length(recoveryStatusList) != 1)
	{
		PQclear(result);
		ClearResults(nodeConnection, true);
		CloseConnection(nodeConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse recovery status result from %s:%d",
							   workerNode->workerName,
							   workerNode->workerPort)));
	}

	StringInfo recoveryStatusInfo = (StringInfo) linitial(recoveryStatusList);
	bool isInRecovery = (strcmp(recoveryStatusInfo->data, "t") == 0) || (strcmp(
																			 recoveryStatusInfo
																			 ->data,
																			 "true") == 0)
	;

	PQclear(result);
	ForgetResults(nodeConnection);
	CloseConnection(nodeConnection);

	return isInRecovery;
}


/*
 * EnsureSingleNodePromotion ensures that only one node promotion operation
 * can proceed at a time by acquiring necessary locks and checking for
 * conflicting operations.
 *
 * This function performs the following safety checks:
 * 1. Verifies no rebalance operations are currently running, as they would
 *    conflict with the shard redistribution that occurs during promotion
 * 2. Acquires exclusive placement colocation locks on all shards residing
 *    on the primary node's group to prevent concurrent shard operations
 *
 * The locks are acquired in shard ID order to prevent deadlocks when
 * multiple operations attempt to lock the same set of shards.
 *
 * Arguments:
 * - primaryNode: The primary node whose shards need to be locked
 *
 * Throws ERROR if:
 * - A rebalance operation is already running
 * - Unable to acquire necessary locks
 */
static void
EnsureSingleNodePromotion(WorkerNode *primaryNode)
{
	/* Error out if some rebalancer is running */
	int64 jobId = 0;
	if (HasNonTerminalJobOfType("rebalance", &jobId))
	{
		ereport(ERROR, (
					errmsg("A rebalance operation is already running as job %ld", jobId),
					errdetail("A rebalance was already scheduled as background job"),
					errhint("To monitor progress, run: SELECT * FROM "
							"citus_rebalance_status();")));
	}
	List *placementList = AllShardPlacementsOnNodeGroup(primaryNode->groupId);

	/* lock shards in order of shard id to prevent deadlock */
	placementList = SortList(placementList, CompareShardPlacementsByShardId);

	GroupShardPlacement *placement = NULL;
	foreach_declared_ptr(placement, placementList)
	{
		int64 shardId = placement->shardId;
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		Oid distributedTableId = shardInterval->relationId;

		AcquirePlacementColocationLock(distributedTableId, ExclusiveLock, "promote clone")
		;
	}
}
