#include "postgres.h"
#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"

#include "distributed/argutils.h"
#include "distributed/remote_commands.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/shard_rebalancer.h"


static int64 GetReplicationLag(WorkerNode *primaryWorkerNode, WorkerNode *replicaWorkerNode);
static void BlockAllWritesToWorkerNode(WorkerNode *workerNode);
static bool GetNodeIsInRecoveryStatus(WorkerNode *workerNode);
static void PromoteReplicaNode(WorkerNode *replicaWorkerNode);


PG_FUNCTION_INFO_V1(citus_promote_replica_and_rebalance);

Datum
citus_promote_replica_and_rebalance(PG_FUNCTION_ARGS)
{
	// Ensure superuser and coordinator
	EnsureSuperUser();
	EnsureCoordinator();

	// Get replica_nodeid argument
	int32 replicaNodeIdArg = PG_GETARG_INT32(0);

	WorkerNode *replicaNode = NULL;
	WorkerNode *primaryNode = NULL;

	// Lock pg_dist_node to prevent concurrent modifications during this operation
	LockRelationOid(DistNodeRelationId(), RowExclusiveLock);

	replicaNode = FindNodeAnyClusterByNodeId(replicaNodeIdArg);
	if (replicaNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica node with ID %d not found.", replicaNodeIdArg)));
	}

	if (!replicaNode->nodeisreplica || replicaNode->nodeprimarynodeid == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Node %s:%d (ID %d) is not a valid replica or its primary node ID is not set.",
							   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	}

	if (replicaNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica node %s:%d (ID %d) is already active and cannot be promoted.",
							   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	}

	primaryNode = FindNodeAnyClusterByNodeId(replicaNode->nodeprimarynodeid);
	if (primaryNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node with ID %d (for replica %s:%d) not found.",
							   replicaNode->nodeprimarynodeid, replicaNode->workerName, replicaNode->workerPort)));
	}

	if (primaryNode->nodeisreplica)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is itself a replica.",
							   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));
	}

	if (!primaryNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is not active.",
							   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));
	}
	/* Ensure the primary node is related to the replica node */
	if (primaryNode->nodeId != replicaNode->nodeprimarynodeid)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica node %s:%d (ID %d) is not replica of the primary node %s:%d (ID %d).",
							   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId,
							   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));
	}

	ereport(NOTICE, (errmsg("Starting promotion process for replica node %s:%d (ID %d), original primary %s:%d (ID %d)",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId,
						   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));

	/* Step 1: Block Writes on Original Primary's Shards */
	ereport(NOTICE, (errmsg("Blocking writes on shards of original primary node %s:%d (group %d)",
						   primaryNode->workerName, primaryNode->workerPort, primaryNode->groupId)));

	BlockAllWritesToWorkerNode(primaryNode);

	/* Step 2: Wait for Replica to Catch Up */
	ereport(NOTICE, (errmsg("Waiting for replica %s:%d to catch up with primary %s:%d",
						   replicaNode->workerName, replicaNode->workerPort,
						   primaryNode->workerName, primaryNode->workerPort)));

	bool caughtUp = false;
	const int catchUpTimeoutSeconds = 300; // 5 minutes, TODO: Make GUC
	const int sleepIntervalSeconds = 5;
	int elapsedTimeSeconds = 0;

	while (elapsedTimeSeconds < catchUpTimeoutSeconds)
	{
		uint64 repLag = GetReplicationLag(primaryNode, replicaNode);
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
						errmsg("Replica %s:%d failed to catch up with primary %s:%d within %d seconds.",
							   replicaNode->workerName, replicaNode->workerPort,
							   primaryNode->workerName, primaryNode->workerPort,
							   catchUpTimeoutSeconds)));
	}

	ereport(NOTICE, (errmsg("Replica %s:%d is now caught up with primary %s:%d.",
						   replicaNode->workerName, replicaNode->workerPort,
						   primaryNode->workerName, primaryNode->workerPort)));
						


	/* Step 3: PostgreSQL Replica Promotion */
	ereport(NOTICE, (errmsg("Attempting to promote replica %s:%d via pg_promote().",
						   replicaNode->workerName, replicaNode->workerPort)));

	PromoteReplicaNode(replicaNode);

	/* Step 4: Update Replica Metadata in pg_dist_node on Coordinator */

	ereport(NOTICE, (errmsg("Updating metadata for promoted replica %s:%d (ID %d)",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	ActivateReplicaNodeAsPrimary(replicaNode);

	/* We need to sync metadata changes to all nodes before rebalancing shards
	 * since the rebalancing algorithm depends on the latest metadata.
	 */
	SyncNodeMetadataToNodes();

	/* Step 5: Split Shards Between Primary and Replica */
	SplitShardsBetweenPrimaryAndReplica(primaryNode, replicaNode, PG_GETARG_NAME_OR_NULL(1));


	TransactionModifiedNodeMetadata = true; // Inform Citus about metadata change
	TriggerNodeMetadataSyncOnCommit();      // Ensure changes are propagated



	ereport(NOTICE, (errmsg("Replica node %s:%d (ID %d) metadata updated. It is now a primary",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));



	/* TODO: Step 6: Unblock Writes (should be handled by transaction commit) */
	ereport(NOTICE, (errmsg("TODO: Step 6: Unblock Writes")));

	PG_RETURN_VOID();
}


/*
 * GetReplicationLag calculates the replication lag between the primary and replica nodes.
 * It returns the lag in bytes.
 */
static int64
GetReplicationLag(WorkerNode *primaryWorkerNode, WorkerNode *replicaWorkerNode)
{

#if PG_VERSION_NUM >= 100000
    const char *primary_lsn_query = "SELECT pg_current_wal_lsn()";
    const char *replica_lsn_query = "SELECT pg_last_wal_replay_lsn()";
#else
    const char *primary_lsn_query = "SELECT pg_current_xlog_location()";
    const char *replica_lsn_query = "SELECT pg_last_xlog_replay_location()";
#endif

	int connectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(connectionFlag,
													primaryWorkerNode->workerName,
													primaryWorkerNode->workerPort);
	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to fetch replication status",
							   primaryWorkerNode->workerName, primaryWorkerNode->workerPort)));
	}
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag,
													replicaWorkerNode->workerName,
													replicaWorkerNode->workerPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to fetch replication status",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort)));
	}

	int primaryResultCode = SendRemoteCommand(primaryConnection, primary_lsn_query);
	if (primaryResultCode == 0)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *primaryResult = GetRemoteCommandResult(primaryConnection, true);
	if (!IsResponseOK(primaryResult))
	{
		ReportResultError(primaryConnection, primaryResult, ERROR);
	}

	int replicaResultCode = SendRemoteCommand(replicaConnection, replica_lsn_query);
	if (replicaResultCode == 0)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}
	PGresult *replicaResult = GetRemoteCommandResult(replicaConnection, true);
	if (!IsResponseOK(replicaResult))
	{
		ReportResultError(replicaConnection, replicaResult, ERROR);
	}


	List *primaryLsnList = ReadFirstColumnAsText(primaryResult);
	if (list_length(primaryLsnList) != 1)
	{
		PQclear(primaryResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse get primary LSN result from %s:%d",
								  primaryWorkerNode->workerName,
								  primaryWorkerNode->workerPort)));

	}
	StringInfo primaryLsnQueryResInfo = (StringInfo) linitial(primaryLsnList);
	char *primary_lsn_str = primaryLsnQueryResInfo->data;

	List *replicaLsnList = ReadFirstColumnAsText(replicaResult);
	if (list_length(replicaLsnList) != 1)
	{
		PQclear(replicaResult);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse get replica LSN result from %s:%d",
								  replicaWorkerNode->workerName,
								  replicaWorkerNode->workerPort)));

	}
	StringInfo replicaLsnQueryResInfo = (StringInfo) linitial(replicaLsnList);
	char *replica_lsn_str = replicaLsnQueryResInfo->data;

    if (!primary_lsn_str || !replica_lsn_str)
        return -1;

    int64 primary_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(primary_lsn_str)));
    int64 replica_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(replica_lsn_str)));

	int64 lag_bytes = primary_lsn - replica_lsn;

	PQclear(primaryResult);
	ForgetResults(primaryConnection);
	CloseConnection(primaryConnection);

	PQclear(replicaResult);
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);

	ereport(NOTICE, (errmsg("replication lag between %s:%d and %s:%d is %ld bytes",
							primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
							replicaWorkerNode->workerName, replicaWorkerNode->workerPort,
							lag_bytes)));
    return lag_bytes;
}

static void
PromoteReplicaNode(WorkerNode *replicaWorkerNode)
{
	int connectionFlag = 0;
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag,
													replicaWorkerNode->workerName,
													replicaWorkerNode->workerPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to promote replica",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort)));
	}

	const char *promoteQuery = "SELECT pg_promote(wait := true);";
	int resultCode = SendRemoteCommand(replicaConnection, promoteQuery);
	if (resultCode == 0)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);
	/* connect again and verify the replica is promoted */
	if ( GetNodeIsInRecoveryStatus(replicaWorkerNode) )
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Failed to promote replica %s:%d (ID %d). It is still in recovery.",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort, replicaWorkerNode->nodeId)));
	}
	else
	{
		ereport(NOTICE, (errmsg("Replica node %s:%d (ID %d) has been successfully promoted.",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort, replicaWorkerNode->nodeId)));
	}
}

static void
BlockAllWritesToWorkerNode(WorkerNode *workerNode)
{
	ereport(NOTICE, (errmsg("Blocking all writes to worker node %s:%d (ID %d)",
						   workerNode->workerName, workerNode->workerPort, workerNode->nodeId)));
	// List *placementsOnOldPrimaryGroup = AllShardPlacementsOnNodeGroup(workerNode->groupId);

	LockShardsInWorkerPlacementList(workerNode, AccessExclusiveLock);
}

bool
GetNodeIsInRecoveryStatus(WorkerNode *workerNode)
{
	int connectionFlag = 0;
	MultiConnection *nodeConnection = GetNodeConnection(connectionFlag,
													workerNode->workerName,
													workerNode->workerPort);

	if (PQstatus(nodeConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to check recovery status",
							   workerNode->workerName, workerNode->workerPort)));
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
	bool isInRecovery = (strcmp(recoveryStatusInfo->data, "t") == 0) || (strcmp(recoveryStatusInfo->data, "true") == 0);

	PQclear(result);
	ForgetResults(nodeConnection);
	CloseConnection(nodeConnection);

	return isInRecovery;
}