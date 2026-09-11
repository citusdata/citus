#include "postgres.h"

#include "miscadmin.h"

#include "portability/instr_time.h"
#include "storage/latch.h"
#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"
#include "utils/wait_event.h"

#include "distributed/argutils.h"
#include "distributed/clonenode_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/worker_transaction.h"


static void BlockAllWritesToWorkerNode(WorkerNode *workerNode);
static XLogRecPtr GetNodeWalPosition(MultiConnection *connection, bool replay);
static bool GetNodeIsInRecoveryStatus(WorkerNode *workerNode);
static void PromoteCloneNode(WorkerNode *cloneWorkerNode);
static void EnsureSingleNodePromotion(WorkerNode *primaryNode);
static void AdjustCloneSequenceRangesForNewGroup(WorkerNode *cloneNode);

PG_FUNCTION_INFO_V1(citus_promote_clone_and_rebalance);

/*
 * citus_promote_clone_and_rebalance promotes an inactive clone node to become
 * an additional primary node, sharing shards with its original primary node.
 *
 * This function performs the following steps:
 * 1. Validates that the clone node exists and is properly configured
 * 2. Ensures the clone is inactive and has a valid primary node reference
 * 3. Blocks all writes to the primary node to prevent data divergence
 * 4. Waits for clone replay to reach a fixed source WAL insertion position
 * 5. Promotes the clone node to become a standalone primary
 * 6. Updates metadata to mark the clone as active and primary
 * 7. Rebalances shards between the old primary and new primary
 * 8. Returns void on success
 *
 * Arguments:
 * - clone_nodeid: The node ID of the clone to promote
 * - rebalance_strategy: Optional strategy used to split the shards
 * - catchUpTimeoutSeconds: Catch-up polling budget in seconds (default: 300).
 *   Zero disables the catch-up deadline; negative values are rejected before
 *   acquiring locks. Unlimited waiting can hold shard write locks indefinitely,
 *   but remains interruptible by cancellation or statement_timeout.
 *   A positive budget starts after acquiring
 *   the write locks and includes fetching the source target and clone probes,
 *   but excludes promotion and rebalancing. No probe starts after the budget
 *   expires; a successful in-flight probe is accepted even if it finishes later.
 *   This is not a hard deadline on remote I/O; statement_timeout can cancel it.
 *
 * After the existing shard write fence is acquired, a source insertion LSN is
 * captured once, including WAL from completed asynchronous commits. Clone replay
 * must reach or exceed this target before promotion. Unrelated WAL can advance
 * beyond the target without extending the wait. Consistency depends on the write
 * fence preventing changes to the shard data being split; this wait does not
 * extend the scope of that fence.
 */
Datum
citus_promote_clone_and_rebalance(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* Ensure superuser and coordinator */
	EnsureSuperUser();
	EnsureCoordinator();

	/* Get clone_nodeid argument */
	int32 cloneNodeIdArg = PG_GETARG_INT32(0);

	/* Get catchUpTimeoutSeconds argument with default value of 300 */
	int32 catchUpTimeoutSeconds = PG_ARGISNULL(2) ? 300 : PG_GETARG_INT32(2);

	if (catchUpTimeoutSeconds < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("catchup_timeout_seconds must be nonnegative")));
	}

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
	if (catchUpTimeoutSeconds == 0)
	{
		ereport(NOTICE, (errmsg(
							 "Waiting for clone %s:%d to catch up with primary %s:%d (timeout disabled)",
							 cloneNode->workerName, cloneNode->workerPort,
							 primaryNode->workerName, primaryNode->workerPort)));
	}
	else
	{
		ereport(NOTICE, (errmsg(
							 "Waiting for clone %s:%d to catch up with primary %s:%d (timeout: %d seconds)",
							 cloneNode->workerName, cloneNode->workerPort,
							 primaryNode->workerName, primaryNode->workerPort,
							 catchUpTimeoutSeconds)));
	}

	bool caughtUp = false;
	instr_time startTime;
	INSTR_TIME_SET_CURRENT(startTime);
	XLogRecPtr targetLsn = InvalidXLogRecPtr;
	XLogRecPtr replayLsn = InvalidXLogRecPtr;

	/* Both private connections stay tracked until their owning PG_FINALLY closes them. */
	MultiConnection *sourceConnection = StartNodeConnection(FORCE_NEW_CONNECTION,
															primaryNode->workerName,
															primaryNode->workerPort);
	PG_TRY();
	{
		FinishConnectionEstablishment(sourceConnection);
		targetLsn = GetNodeWalPosition(sourceConnection, false);
	}
	PG_FINALLY();
	{
		CloseConnection(sourceConnection);
	}
	PG_END_TRY();

	MultiConnection *cloneConnection = StartNodeConnection(FORCE_NEW_CONNECTION,
														   cloneNode->workerName,
														   cloneNode
														   ->workerPort);
	PG_TRY();
	{
		FinishConnectionEstablishment(cloneConnection);
		long pollIntervalMilliseconds = 100;
		while (true)
		{
			CHECK_FOR_INTERRUPTS();
			instr_time elapsedTime;
			INSTR_TIME_SET_CURRENT(elapsedTime);
			INSTR_TIME_SUBTRACT(elapsedTime, startTime);
			if (catchUpTimeoutSeconds > 0 &&
				INSTR_TIME_GET_DOUBLE(elapsedTime) >= catchUpTimeoutSeconds)
			{
				break;
			}

			replayLsn = GetNodeWalPosition(cloneConnection, true);
			if (!XLogRecPtrIsInvalid(replayLsn) && replayLsn >= targetLsn)
			{
				caughtUp = true;
				break;
			}

			long waitMilliseconds = pollIntervalMilliseconds;
			if (catchUpTimeoutSeconds > 0)
			{
				INSTR_TIME_SET_CURRENT(elapsedTime);
				INSTR_TIME_SUBTRACT(elapsedTime, startTime);
				double remainingSeconds = catchUpTimeoutSeconds -
										  INSTR_TIME_GET_DOUBLE(elapsedTime);
				if (remainingSeconds <= 0)
				{
					break;
				}
				waitMilliseconds = Max(1L, (long) (Min(remainingSeconds * 1000,
													   waitMilliseconds)));
			}

			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  waitMilliseconds, PG_WAIT_EXTENSION);
			pollIntervalMilliseconds = Min(pollIntervalMilliseconds * 2, 1000L);
		}
	}
	PG_FINALLY();
	{
		CloseConnection(cloneConnection);
	}
	PG_END_TRY();

	if (!caughtUp)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Clone %s:%d failed to catch up with primary %s:%d within %d seconds.",
							cloneNode->workerName, cloneNode->workerPort,
							primaryNode->workerName, primaryNode->workerPort,
							catchUpTimeoutSeconds),
						errdetail("Target WAL position is %X/%X; last observed replay "
								  "position is %X/%X (0/0 means unavailable).",
								  LSN_FORMAT_ARGS(targetLsn), LSN_FORMAT_ARGS(replayLsn)))
				);
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

	/*
	 * Re-range the promoted clone's distributed-table sequences to its new
	 * group-id window. The clone is a physical replica, so its sequence objects
	 * still carry the source primary's (groupId << 48) range; the classical
	 * activation path does the equivalent re-ranging per group id. This must run
	 * after SyncNodeMetadataToNodes() so the clone's local group id is already
	 * corrected and visible over the reused metadata connection.
	 */
	AdjustCloneSequenceRangesForNewGroup(cloneNode);

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


/*
 * GetNodeWalPosition reads the insertion LSN, or the replay LSN when replay is
 * true. A clone must still be in recovery; NULL replay returns InvalidXLogRecPtr
 * and is not evidence of catch-up. An invalid source position is an error.
 * The result is released even if validation or parsing fails. The caller owns
 * the connection and must close it on success and error; successful probes drain
 * pending results so that the connection can be reused for the next probe.
 */
static XLogRecPtr
GetNodeWalPosition(MultiConnection *connection, bool replay)
{
	if (PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(connection, ERROR);
	}

	const char *query = replay ?
						"SELECT pg_last_wal_replay_lsn() WHERE pg_is_in_recovery()" :
						"SELECT pg_current_wal_insert_lsn()";
	if (SendRemoteCommand(connection, query) == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(connection, true);
	if (result == NULL)
	{
		ReportConnectionError(connection, ERROR);
	}
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}
	XLogRecPtr position = InvalidXLogRecPtr;
	PG_TRY();
	{
		if (replay && PQntuples(result) == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("clone node %s:%d is no longer in recovery",
								   connection->hostname, connection->port)));
		}
		if (PQntuples(result) != 1 || PQnfields(result) != 1)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("unexpected WAL position result from node %s:%d",
								   connection->hostname, connection->port)));
		}

		if (!PQgetisnull(result, 0, 0))
		{
			char *positionString = PQgetvalue(result, 0, 0);
			position = DatumGetLSN(DirectFunctionCall1(pg_lsn_in,
													   CStringGetDatum(positionString)));
		}
	}
	PG_FINALLY();
	{
		PQclear(result);
	}
	PG_END_TRY();
	if (!ClearResults(connection, true) || PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(connection, ERROR);
	}
	CHECK_FOR_INTERRUPTS();

	if (!replay && XLogRecPtrIsInvalid(position))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("invalid source WAL position from node %s:%d",
							   connection->hostname, connection->port)));
	}

	return position;
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


/*
 * AdjustCloneSequenceRangesForNewGroup re-ranges the promoted clone's
 * sequence-backed columns for the same table surface that classical
 * add/activate-node flow syncs to metadata workers (i.e. tables where
 * ShouldSyncTableMetadata(relationId) is true).
 *
 * A clone is a physical streaming replica of its source primary, so its
 * sequence objects are byte-for-byte copies that still carve out the source
 * primary's (groupId << 48) value window. The classical activation path
 * re-ranges sequences per group id (via AlterSequenceMinMax) so each group emits
 * globally-unique values; the clone path must do the same once its group id is
 * corrected.
 *
 * The command list is built on the coordinator (reusing the existing per-table
 * sequence command builders) and sent to the clone over the metadata connection
 * via SendMetadataCommandListToWorkerListInCoordinatedTransaction. That path
 * reuses the same connection SyncNodeMetadataToNodes() used to update
 * pg_dist_local_group, so the corrected local group id is visible when
 * AlterSequenceMinMax() runs on the clone. Therefore this MUST be called after
 * SyncNodeMetadataToNodes().
 */
static void
AdjustCloneSequenceRangesForNewGroup(WorkerNode *cloneNode)
{
	List *ddlCommandList = NIL;

	List *citusTableIdList = AllCitusTableIds();
	Oid relationId = InvalidOid;
	foreach_declared_oid(relationId, citusTableIdList)
	{
		if (!ShouldSyncTableMetadata(relationId))
		{
			continue;
		}

		ddlCommandList = list_concat(ddlCommandList,
									 SequenceRangeAdjustCommandList(relationId));
		ddlCommandList = list_concat(ddlCommandList,
									 IdentitySequenceDependencyCommandList(relationId));
	}

	if (ddlCommandList == NIL)
	{
		/* no sequence-backed Citus-table columns to re-range */
		return;
	}

	/*
	 * SendMetadataCommandListToWorkerListInCoordinatedTransaction expects plain
	 * command strings, so unwrap each TableDDLCommand.
	 */
	List *commandList = NIL;
	TableDDLCommand *ddlCommand = NULL;
	foreach_declared_ptr(ddlCommand, ddlCommandList)
	{
		commandList = lappend(commandList, GetTableDDLCommand(ddlCommand));
	}

	/*
	 * Re-fetch the clone node so its hasMetadata/metadataSynced flags reflect
	 * the post-activation, post-sync catalog state. The cloneNode passed in was
	 * loaded at the start of promotion (before ActivateCloneNodeAsPrimary and
	 * SyncNodeMetadataToNodes), so its in-memory metadata flags are still those
	 * of an inactive clone and would trip the metadata-node sanity checks in the
	 * send path.
	 */
	WorkerNode *freshCloneNode = FindNodeAnyClusterByNodeId(cloneNode->nodeId);
	if (freshCloneNode == NULL)
	{
		ereport(ERROR, (errmsg("could not find promoted clone node with ID %d "
							   "while re-ranging its sequences",
							   cloneNode->nodeId)));
	}

	SendMetadataCommandListToWorkerListInCoordinatedTransaction(
		list_make1(freshCloneNode),
		CurrentUserName(),
		commandList);

	ereport(NOTICE, (errmsg(
						 "re-ranged %d sequence(s) on promoted clone %s:%d (ID %d) for new group %d",
						 list_length(commandList), freshCloneNode->workerName,
						 freshCloneNode->workerPort, freshCloneNode->nodeId,
						 freshCloneNode->groupId)));
}
