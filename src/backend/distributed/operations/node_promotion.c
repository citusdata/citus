#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/pg_class.h"
#include "portability/instr_time.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/wait_event.h"

#include "distributed/argutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/clonenode_utils.h"
#include "distributed/cluster_changes_block.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/replication_origin_session_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_transfer.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_transaction.h"


static void ErrorIfCommandsBeforePromotion(void);
static void BlockAllWritesToWorkerNode(WorkerNode *workerNode);
static void LockShardsOnAllMetadataNodes(List *placementList);
static void BlockDistributedTransactionsStartedOnNode(WorkerNode *workerNode);
static void EnsureNoPreparedTransactionsOnNode(MultiConnection *connection);
static void FlushSourceWal(MultiConnection *connection);
static XLogRecPtr GetNodeWalPosition(MultiConnection *connection, bool replay);
static bool GetNodeIsInRecoveryStatus(WorkerNode *workerNode);
static void PromoteCloneNode(WorkerNode *cloneWorkerNode);
static void AssignCloneNodeIdentity(WorkerNode *cloneNode);
static void ExecuteCommandListInRemoteTransaction(MultiConnection *connection,
												  List *commandList);
static void EnsureSingleNodePromotion(WorkerNode *primaryNode);
static void ErrorIfPrimaryHasUnloggedPlacements(WorkerNode *primaryNode);
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
 * 4. Checks that the primary has no placements of unlogged tables, and decides
 *    which shards move to the clone, before any irreversible step
 * 5. Ensures that the primary has no unresolved prepared transactions, waits
 *    for clone replay to reach a fixed source WAL insertion position, and
 *    checks that the metadata workers still hold the write fence
 * 6. Promotes the clone node to become a standalone primary and gives it its
 *    own identity
 * 7. Updates metadata to mark the clone as active and primary
 * 8. Rebalances shards between the old primary and new primary
 * 9. Returns void on success
 *
 * Arguments:
 * - clone_nodeid: The node ID of the clone to promote
 * - rebalance_strategy: Optional strategy used to split the shards
 * - catchUpTimeoutSeconds: Catch-up polling budget in seconds (default: 300).
 *   Zero disables the catch-up deadline; negative values are rejected before
 *   acquiring locks. Unlimited waiting can hold shard write locks indefinitely,
 *   but remains interruptible by cancellation or statement_timeout.
 *   A positive budget starts after acquiring
 *   the write locks and includes fetching the source target, flushing the
 *   source WAL, and clone probes,
 *   but excludes promotion and rebalancing. No probe starts after the budget
 *   expires; a successful in-flight probe is accepted even if it finishes later.
 *   This is not a hard deadline on remote I/O; statement_timeout can cancel it.
 *
 * After the write fence is acquired, a source insertion LSN is captured once,
 * including WAL from completed asynchronous commits, and the source flushes its
 * WAL beyond it, see FlushSourceWal(). Clone replay must reach or
 * exceed this target before promotion. Unrelated WAL can advance beyond the
 * target without extending the wait. Consistency depends on the write fence
 * (see BlockAllWritesToWorkerNode) preventing the changes that the clone would
 * only receive through replication; this wait does not extend the scope of
 * that fence.
 *
 * Like shard moves, the promotion first cleans up the resources that earlier
 * operations left for cleanup, like the old placements of moved shards, so that
 * the clone does not copy them. It schedules the cleanup of the copies of the
 * ones that remain on the primary, for example because they are in use, on the
 * clone too.
 *
 * Only the Citus metadata of the current database is handled, so nodes that use
 * Citus in more than one database are not supported. In the other databases,
 * writes are not blocked, the clone is not added to the metadata, and it keeps
 * the identity of the primary, so its maintenance daemon acts as the primary
 * there.
 *
 * Unlogged tables are not supported either. Physical replication does not copy
 * their data, so the promotion errors out if the primary has placements of
 * unlogged distributed or reference tables, see
 * ErrorIfPrimaryHasUnloggedPlacements().
 *
 * The promotion cannot run inside a transaction block: its write fence would
 * last until the transaction ends, and a rollback could not undo pg_promote()
 * or the identity of the clone. Called from a function or a DO block, it errors
 * out if earlier commands of its transaction made changes or ran on other
 * nodes, see ErrorIfCommandsBeforePromotion().
 *
 * Until the clone has an identity of its own, its maintenance daemon acts as
 * the primary, see AssignCloneNodeIdentity(). Operators should therefore
 * disable the daemon tasks that would act for the primary before the
 * promotion, preferably by starting the clone with:
 *
 *   citus.recover_2pc_interval = -1
 *   citus.defer_shard_delete_interval = -1
 *   citus.background_task_queue_interval = -1
 *
 * If the clone already runs, reload its configuration before the promotion,
 * and allow its maintenance daemon to process the reload; a SET in a session
 * is not enough. Restore the previous values only once both the identity of
 * the clone and its registration have committed, that is, once the promotion
 * has committed. If either outcome is unknown, leave the tasks disabled. Leave
 * the settings of the primary unchanged. These settings do not stop the
 * recover_prepared_transactions() call of the identity transaction, which runs
 * under the write fence.
 *
 * If the promotion fails before it calls pg_promote(), the clone stays a
 * replica and an inactive clone in the metadata, and the promotion can be
 * retried. Once it calls pg_promote(), which it reports with a NOTICE, the
 * clone cannot follow the primary again, even if it is still in recovery when
 * the promotion fails, and it may keep the identity of the primary. So if the
 * promotion fails from then on, shut the clone down. To use the server as a
 * clone again, rebuild it as a replica of the primary, for example from a new
 * base backup. Citus does not manage replication slots, so if the clone used
 * one on the primary, drop it once the clone is promoted or discarded, since
 * the primary keeps WAL for it otherwise.
 */
Datum
citus_promote_clone_and_rebalance(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/* Ensure superuser and coordinator */
	EnsureSuperUser();
	EnsureCoordinator();

	if (IsTransactionBlock())
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("citus_promote_clone_and_rebalance cannot run inside a "
							   "transaction block"),
						errdetail("Its write fence would last until the transaction "
								  "ends, and a rollback could not undo the promotion "
								  "of the clone."),
						errhint("Run the promotion outside a transaction block, for "
								"example with autocommit enabled.")));
	}

	ErrorIfCommandsBeforePromotion();

	/* Get clone_nodeid argument */
	int32 cloneNodeIdArg = PG_GETARG_INT32(0);

	/* Get catchUpTimeoutSeconds argument with default value of 300 */
	int32 catchUpTimeoutSeconds = PG_ARGISNULL(2) ? 300 : PG_GETARG_INT32(2);

	if (catchUpTimeoutSeconds < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("catchup_timeout_seconds must be nonnegative")));
	}

	/*
	 * Lock pg_dist_node to prevent concurrent modifications during this
	 * operation. Unlike RowExclusiveLock, ShareRowExclusiveLock conflicts with
	 * itself, so a concurrent promotion waits here until this one finishes.
	 * Otherwise, both could hold the lock and deadlock when their write fences
	 * upgrade it to ExclusiveLock, see BlockAllWritesToWorkerNode().
	 */
	LockRelationOid(DistNodeRelationId(), ShareRowExclusiveLock);

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

	/*
	 * The cleanup records of the resources that the clone copies refer only to
	 * the primary, so clean up what we can before the clone stops replicating,
	 * as shard moves do before they copy shards. The locks taken above already
	 * keep shard transfers from leaving new resources on the primary.
	 */
	DropOrphanedResourcesInSeparateTransaction();

	/* Step 1: Block Writes on Original Primary's Shards */
	ereport(NOTICE, (errmsg(
						 "Blocking writes on shards of original primary node %s:%d (group %d)",
						 primaryNode->workerName, primaryNode->workerPort, primaryNode
						 ->groupId)));

	BlockAllWritesToWorkerNode(primaryNode);

	/*
	 * The write fence stops new shards from being created on the primary, so
	 * also lock the colocation groups that got shards on it meanwhile.
	 */
	EnsureSingleNodePromotion(primaryNode);

	/*
	 * The write fence also stops DDL, so the tables with placements on the
	 * primary stay logged until the clone is promoted.
	 */
	ErrorIfPrimaryHasUnloggedPlacements(primaryNode);

	/*
	 * pg_promote() cannot be undone, so decide how to split the shards before
	 * it, while errors such as an invalid rebalance strategy are still harmless.
	 * The locks taken above keep the shards on the primary that the plan is
	 * based on.
	 */
	List *primaryShardIdList = NIL;
	List *cloneShardIdList = NIL;
	PlanShardSplitBetweenPrimaryAndClone(primaryNode, cloneNode,
										 PG_GETARG_NAME_OR_NULL(1),
										 &primaryShardIdList, &cloneShardIdList);

	/*
	 * The clone also copies the resources that the cleanup above could not
	 * drop from the primary, for example because they were in use, so list
	 * them to clean up their copies on the clone as well. With the write fence
	 * in place, no other operation leaves resources on the primary before the
	 * clone stops replicating. The regular cleanup might still drop some of
	 * them before that, but cleaning up a copy that does not exist does nothing.
	 *
	 * List them before AdjustShardsForPrimaryCloneNodeSplit() adds the
	 * promotion's own records for the primary: those are for the shards that
	 * move to the clone, so copying them would drop these shards from the clone.
	 */
	List *primaryCleanupRecordList = ListCleanupRecordsForNodeGroup(primaryNode->groupId);

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
		EnsureNoPreparedTransactionsOnNode(sourceConnection);
		targetLsn = GetNodeWalPosition(sourceConnection, false);
		FlushSourceWal(sourceConnection);
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

	/*
	 * The metadata workers hold the write fence in their transactions, which
	 * end if, for example, their connections fail during the catch-up. Check
	 * that they still hold it while the promotion can still be undone: a
	 * command errors out on a connection whose transaction ended.
	 */
	SendCommandToWorkersWithMetadata("SELECT 1");

	/* Step 3: PostgreSQL Clone Promotion */
	ereport(NOTICE, (errmsg("Attempting to promote clone %s:%d via pg_promote().",
							cloneNode->workerName, cloneNode->workerPort)));

	PromoteCloneNode(cloneNode);

	/*
	 * Until it has a group of its own, the promoted clone acts as the primary,
	 * for example in 2PC recovery. Assign its identity right away and outside
	 * of this transaction, so that this holds even if the promotion fails later.
	 */
	AssignCloneNodeIdentity(cloneNode);

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
	 * after SyncNodeMetadataToNodes(), see the function comment.
	 */
	AdjustCloneSequenceRangesForNewGroup(cloneNode);

	/* Step 5: Split Shards Between Primary and Clone */
	AdjustShardsForPrimaryCloneNodeSplit(primaryNode, cloneNode, primaryShardIdList,
										 cloneShardIdList, primaryCleanupRecordList);


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
 * ErrorIfCommandsBeforePromotion errors out if earlier commands of the current
 * transaction made changes, or ran on other nodes. The changes commit only
 * after the clone is promoted, and the clone keeps only the changes that commit
 * before it, so it would miss them. The write fence does not stop them either,
 * since they hold locks of the same transaction.
 *
 * Transaction blocks are rejected before, so such commands can only come from
 * the same function or DO block. Distributed modifications set
 * XactModificationLevel. Local changes, including the metadata changes that are
 * also sent to other nodes, assign a transaction id. Commands that ran on other
 * nodes in a coordinated transaction are rejected too.
 */
static void
ErrorIfCommandsBeforePromotion(void)
{
	if (XactModificationLevel > XACT_MODIFICATION_NONE ||
		InCoordinatedTransaction() ||
		TransactionIdIsValid(GetTopTransactionIdIfAny()))
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot promote a clone node after other commands in "
							   "the same transaction"),
						errdetail("Their changes would only commit after the promotion, "
								  "so the clone would not receive them."),
						errhint("Promote the clone in a separate transaction.")));
	}
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
 * AssignCloneNodeIdentity turns the promoted clone into a node of its own. As a
 * physical copy, the clone has its primary's local group id and server id, and
 * if the primary is the coordinator, also its cleanup records and background
 * jobs, which the clone's maintenance daemon would act on.
 *
 * A first transaction deletes the cleanup records and background jobs. It
 * commits before the second one waits for transaction recovery, so that the
 * daemon stops acting on them meanwhile: deferred cleanup checks each record
 * again right before it drops the resource.
 *
 * The second transaction assigns the group id and server id. Transaction
 * recovery treats the prepared transactions named after the local group id as
 * its own, and reads the group id once per run. A run that started on the
 * clone after pg_promote() would therefore keep the primary's group id after
 * the clone gets its own one, and once the write fence is released, roll back
 * the prepared transactions that the primary starts. So the transaction starts
 * with recover_prepared_transactions(): it waits for such a run to end, and it
 * keeps the transaction recovery lock until the identity commits, so that later
 * runs read the new group id. It also resolves the primary's prepared
 * transactions once more itself, which it does as the primary would while the
 * write fence is held, see BlockDistributedTransactionsStartedOnNode().
 * Recovery connects to all nodes, so a node that does not respond delays the
 * promotion while it holds the write fence.
 *
 * Both transactions commit right away rather than with the current one, so
 * that the clone does not keep the primary's identity if the promotion fails
 * afterwards. They error out unless they commit, see
 * ExecuteCommandListInRemoteTransaction(), so that the promotion does not
 * register a clone that might lack its identity.
 *
 * This does not fully close the window after pg_promote(), unless operators
 * apply the settings that the comment of citus_promote_clone_and_rebalance()
 * describes, which keep the maintenance daemon of the clone from acting:
 * - Until the first transaction commits, the maintenance daemon of the clone
 *   can act on the cleanup records and background jobs of a coordinator, and
 *   for example drop the resources of operations that still run on it.
 * - If these transactions fail, also at COMMIT, or the coordinator fails before
 *   they commit, the promotion does not register the clone, and the clone can
 *   keep the identity of the primary. Once the write fence is released, its 2PC
 *   recovery could then roll back prepared transactions of the primary on other
 *   nodes, so such a clone needs to be shut down.
 */
static void
AssignCloneNodeIdentity(WorkerNode *cloneNode)
{
	List *copiedStateCommandList = list_make3(
		"DELETE FROM pg_catalog.pg_dist_cleanup",
		"DELETE FROM pg_catalog.pg_dist_background_task",
		"DELETE FROM pg_catalog.pg_dist_background_job");

	List *identityCommandList = list_make3(
		"SELECT pg_catalog.recover_prepared_transactions()",
		LocalGroupIdUpdateCommand(cloneNode->groupId),
		"UPDATE pg_catalog.pg_dist_node_metadata SET metadata = "
		"jsonb_set(metadata, '{server_id}', "
		"to_jsonb(pg_catalog.citus_server_id()::text))");

	int connectionFlags = FORCE_NEW_CONNECTION;
	MultiConnection *connection = GetNodeUserDatabaseConnection(connectionFlags,
																cloneNode->workerName,
																cloneNode->workerPort,
																CurrentUserName(),
																NULL);
	PG_TRY();
	{
		ExecuteCommandListInRemoteTransaction(connection, copiedStateCommandList);
		ExecuteCommandListInRemoteTransaction(connection, identityCommandList);
	}
	PG_FINALLY();
	{
		CloseConnection(connection);
	}
	PG_END_TRY();
}


/*
 * ExecuteCommandListInRemoteTransaction runs the given commands in a separate
 * transaction over the given connection, and errors out unless it commits.
 * RemoteTransactionCommit() only warns if COMMIT fails, for example on a
 * deferred constraint, or if its outcome is unknown since the connection
 * failed. A COMMIT also returns ROLLBACK, without an error, if the transaction
 * failed before, so its command tag is checked as well.
 */
static void
ExecuteCommandListInRemoteTransaction(MultiConnection *connection, List *commandList)
{
	ExecuteCriticalRemoteCommand(connection, "BEGIN");
	ExecuteCriticalRemoteCommandList(connection, commandList);

	if (SendRemoteCommand(connection, "COMMIT") == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	bool raiseInterrupts = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	bool committed = strcmp(PQcmdStatus(result), "COMMIT") == 0;
	PQclear(result);
	ForgetResults(connection);

	if (!committed)
	{
		ereport(ERROR, (errmsg("could not commit transaction on %s:%d",
							   connection->hostname, connection->port)));
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


/*
 * FlushSourceWal makes the source flush its WAL beyond the catch-up target,
 * which it just read over the given connection, so that the clone receives the
 * WAL up to the target right away: walsenders only send flushed WAL. The WAL
 * writer soon flushes full pages and asynchronous commits, but not, for
 * example, the WAL of a transaction that is still in progress, or of one that
 * aborted without a transaction id. Without the flush, such WAL could wait for
 * an unrelated flush for many seconds, while the write fence is held.
 *
 * A commit flushes the WAL only if its transaction wrote WAL before its commit
 * record. So rather than commit a transaction, the source writes a
 * non-transactional logical decoding message, and flushes the WAL up to its
 * end. This also gives the clone a record that ends after the target.
 * Otherwise, the clone might not reach a target at a page boundary: the
 * insertion LSN then points after the page header, while replay stops at the
 * end of the last record, before the header.
 *
 * With citus.enable_change_data_capture, the message gets the replication
 * origin that Citus uses for its internal changes, so that CDC skips it.
 */
static void
FlushSourceWal(MultiConnection *connection)
{
	SetupReplicationOriginRemoteSession(connection);
	ExecuteCriticalRemoteCommand(connection,
								 "SELECT pg_catalog.pg_logical_emit_message(false, "
								 "'citus_promote_clone_and_rebalance', '', true)");
}


/*
 * BlockAllWritesToWorkerNode blocks the changes that the clone of the given
 * node would only receive through replication, which stops at pg_promote():
 *
 * - writes to the shards on the node, from the coordinator or any metadata
 *   worker,
 * - DDL and metadata changes, which the node that runs them propagates only to
 *   the active nodes, and the creation of new shards,
 * - commit decisions of distributed transactions that the node starts, see
 *   BlockDistributedTransactionsStartedOnNode().
 *
 * Changes that started before are waited for. The locks are held until the end
 * of the transaction, and they are taken in the order that writers and DDL
 * take them: shard metadata locks before pg_dist_node.
 */
static void
BlockAllWritesToWorkerNode(WorkerNode *workerNode)
{
	ereport(NOTICE, (errmsg("Blocking all writes to worker node %s:%d (ID %d)",
							workerNode->workerName, workerNode->workerPort, workerNode->
							nodeId)));

	/*
	 * The metadata workers hold the fence in transactions that stay idle while
	 * the clone catches up. LockShardListMetadataOnWorkers() keeps
	 * idle_in_transaction_session_timeout from ending them only if it has
	 * shards to lock.
	 */
	SendCommandToWorkersWithMetadata("SET LOCAL idle_in_transaction_session_timeout = 0");

	List *lockedPlacementList = AllShardPlacementsOnNodeGroup(workerNode->groupId);
	LockShardsOnAllMetadataNodes(lockedPlacementList);

	/* DDL and metadata changes, on the coordinator and all metadata workers */
	BlockDistributedQueriesOnMetadataNodes();

	/* writes to the shards that were created on the node in the meantime */
	HTAB *lockedShardIdSet = CreateSimpleHashSet(uint64);
	GroupShardPlacement *placement = NULL;
	foreach_declared_ptr(placement, lockedPlacementList)
	{
		hash_search(lockedShardIdSet, &placement->shardId, HASH_ENTER, NULL);
	}

	List *newPlacementList = NIL;
	List *placementList = AllShardPlacementsOnNodeGroup(workerNode->groupId);
	foreach_declared_ptr(placement, placementList)
	{
		bool foundInSet = false;
		hash_search(lockedShardIdSet, &placement->shardId, HASH_FIND, &foundInSet);
		if (!foundInSet)
		{
			newPlacementList = lappend(newPlacementList, placement);
		}
	}

	hash_destroy(lockedShardIdSet);
	LockShardsOnAllMetadataNodes(newPlacementList);

	BlockDistributedTransactionsStartedOnNode(workerNode);
}


/*
 * LockShardsOnAllMetadataNodes blocks the writes to the shards of the given
 * placements, and waits for the ones in progress, on the coordinator and on
 * all metadata workers.
 *
 * Metadata workers take shard metadata locks only locally when they write to
 * shards, so lock the shards that they know about on them, too.
 * lock_shard_metadata() accepts at most ExclusiveLock, which conflicts with
 * the ShareLock that writers take.
 */
static void
LockShardsOnAllMetadataNodes(List *placementList)
{
	LockShardsInPlacementListMetadata(placementList, AccessExclusiveLock);

	List *shardIntervalList = NIL;
	GroupShardPlacement *placement = NULL;
	foreach_declared_ptr(placement, placementList)
	{
		ShardInterval *shardInterval = LoadShardInterval(placement->shardId);
		if (ShouldSyncTableMetadata(shardInterval->relationId))
		{
			shardIntervalList = lappend(shardIntervalList, shardInterval);
		}
	}

	shardIntervalList = SortList(shardIntervalList, CompareShardIntervalsById);
	LockShardListMetadataOnWorkers(ExclusiveLock, shardIntervalList);
}


/*
 * BlockDistributedTransactionsStartedOnNode blocks the commit decisions of the
 * distributed transactions that the given node starts, like
 * citus_create_restore_point() does: a node records a decision in its
 * pg_dist_transaction before committing, so locking that table blocks new
 * decisions and waits for the ones in progress.
 *
 * Until the promoted clone has its own group, its 2PC recovery resolves the
 * node's prepared transactions based on its copy of pg_dist_transaction, and
 * it considers none of them in progress. With this lock taken before the clone
 * catches up, the copy contains every decision that the node made and no
 * decision is in progress, so the clone resolves them as the node would.
 *
 * On a remote node, 2PC recovery also waits for the lock meanwhile.
 */
static void
BlockDistributedTransactionsStartedOnNode(WorkerNode *workerNode)
{
	if (workerNode->groupId == GetLocalGroupId())
	{
		/* make the local 2PC recovery skip a round rather than wait */
		LockTransactionRecovery(ShareUpdateExclusiveLock);

		LockRelationOid(DistTransactionRelationId(), ExclusiveLock);
		LockRelationOid(DistPartitionRelationId(), ExclusiveLock);
	}
	else if (workerNode->hasMetadata)
	{
		/* nodes without metadata cannot start distributed transactions */
		SendMetadataCommandListToWorkerListInCoordinatedTransaction(
			list_make1(workerNode), CurrentUserName(),
			list_make1(BLOCK_DISTRIBUTED_WRITES_COMMAND));
	}
}


/*
 * EnsureNoPreparedTransactionsOnNode errors out if the node behind the given
 * connection has prepared transactions of Citus. The clone would inherit them,
 * and once it has a group of its own, 2PC recovery would look for their commit
 * records under that group, find none, and roll them back on the clone, even
 * the ones that it commits on the node.
 *
 * The write fence stops distributed transactions from preparing on the node,
 * and waits for the ones in progress to commit or abort. So the remaining ones
 * are left for 2PC recovery, for example after a failed COMMIT PREPARED.
 *
 * Prepared transactions that Citus did not start, or that belong to other
 * databases, are not checked. The clone inherits them as well, but nothing
 * resolves them there, so they keep holding their locks on the clone until
 * they are resolved manually.
 */
static void
EnsureNoPreparedTransactionsOnNode(MultiConnection *connection)
{
	const char *query =
		"SELECT count(*) FROM pg_catalog.pg_prepared_xacts "
		"WHERE gid COLLATE pg_catalog.default LIKE 'citus\\_%' "
		"COLLATE pg_catalog.default AND database = pg_catalog.current_database()";

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

	int preparedTransactionCount = 0;
	PG_TRY();
	{
		if (PQntuples(result) != 1 || PQnfields(result) != 1)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("unexpected prepared transaction count from "
								   "node %s:%d", connection->hostname,
								   connection->port)));
		}

		preparedTransactionCount = pg_strtoint32(PQgetvalue(result, 0, 0));
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

	if (preparedTransactionCount > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node %s:%d has %d prepared transactions of Citus that "
							   "are not resolved yet", connection->hostname,
							   connection->port, preparedTransactionCount),
						errdetail("The clone would inherit them, but they would only "
								  "be resolved on the node."),
						errhint("Retry once 2PC recovery resolves them, or run "
								"recover_prepared_transactions() on the node that "
								"started them.")));
	}
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
 * EnsureSingleNodePromotion ensures that no other operation adds shards to or
 * removes shards from the primary concurrently with the promotion, or changes
 * the colocation groups with shards on it, and errors out otherwise:
 *
 * - The shard transfer lock of the primary stops the shard moves, copies,
 *   splits and rebalances that involve the primary, whatever the colocation
 *   group: a shard moved onto the primary meanwhile might never reach the
 *   clone.
 * - The colocation locks of the colocation groups with shards on the primary
 *   stop the other operations that change or rely on their placements, like
 *   adding a table to them. The split of the shards is planned on these
 *   placements.
 *
 * Throws ERROR if:
 * - A concurrent shard move, copy, split or rebalance holds one of the locks
 * - A rebalance is already running as a background job
 */
static void
EnsureSingleNodePromotion(WorkerNode *primaryNode)
{
	AcquireShardTransferNodeLock(primaryNode, ExclusiveLock, "promote its clone");
	AcquireColocationLocksForNodeGroup(primaryNode->groupId, "promote clone");

	/*
	 * Error out if a rebalance is running as a background job. A rebalance that
	 * is scheduled after this check cannot move shards to or from the primary
	 * while the promotion runs either: scheduling it fails on the rebalance
	 * locks taken above if the primary has shards of distributed tables, and
	 * otherwise its moves that involve the primary fail on the shard transfer
	 * lock.
	 */
	int64 jobId = 0;
	if (HasNonTerminalJobOfType("rebalance", &jobId))
	{
		ereport(ERROR, (
					errmsg("A rebalance operation is already running as job %ld", jobId),
					errdetail("A rebalance was already scheduled as background job"),
					errhint("To monitor progress, run: SELECT * FROM "
							"citus_rebalance_status();")));
	}
}


/*
 * ErrorIfPrimaryHasUnloggedPlacements errors out if the primary has a
 * placement of an unlogged distributed or reference table. Physical
 * replication does not copy the data of unlogged tables, and the clone resets
 * them to empty when it is promoted, so the shards that move to the clone
 * would lose their rows, and so would its reference table placements. The
 * placements of Citus local tables stay on the coordinator, and the copies on
 * its clone are dropped, so these do not matter. Neither do partitioned
 * tables, which have no data of their own; before PostgreSQL 18 one can be
 * unlogged while its partitions are not, and cannot be made logged.
 */
static void
ErrorIfPrimaryHasUnloggedPlacements(WorkerNode *primaryNode)
{
	List *placementList = AllShardPlacementsOnNodeGroup(primaryNode->groupId);

	GroupShardPlacement *placement = NULL;
	foreach_declared_ptr(placement, placementList)
	{
		Oid relationId = RelationIdForShard(placement->shardId);
		if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE) ||
			!RELKIND_HAS_STORAGE(get_rel_relkind(relationId)) ||
			get_rel_persistence(relationId) != RELPERSISTENCE_UNLOGGED)
		{
			continue;
		}

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot promote a clone of node %s:%d, which has "
							   "placements of the unlogged table %s",
							   primaryNode->workerName, primaryNode->workerPort,
							   generate_qualified_relation_name(relationId)),
						errdetail("Physical replication does not copy the data of "
								  "unlogged tables, so the clone would not have it."),
						errhint("Make the table logged with ALTER TABLE ... SET "
								"LOGGED before promoting the clone.")));
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
 * via SendMetadataCommandListToWorkerListInCoordinatedTransaction. The clone's
 * local group id is already corrected by AssignCloneNodeIdentity(), so it is
 * visible when AlterSequenceMinMax() runs on the clone. The metadata connection
 * requires the clone to be an active metadata node, therefore this MUST be
 * called after ActivateCloneNodeAsPrimary() and SyncNodeMetadataToNodes().
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
