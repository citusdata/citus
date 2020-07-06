/*-------------------------------------------------------------------------
 *
 * distributed_execution_locks.c
 *
 * Definitions of the functions used in executing distributed
 * execution locking.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "distributed/distributed_execution_locks.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"


static bool RequiresConsistentSnapshot(Task *task);
static void AcquireExecutorShardLockForRowModify(Task *task, RowModifyLevel modLevel);
static void AcquireExecutorShardLocksForRelationRowLockList(List *relationRowLockList);


/*
 * AcquireExecutorShardLocks acquires locks on shards for the given task if
 * necessary to avoid divergence between multiple replicas of the same shard.
 * No lock is obtained when there is only one replica.
 *
 * The function determines the appropriate lock mode based on the commutativity
 * rule of the command. In each case, it uses a lock mode that enforces the
 * commutativity rule.
 *
 * The mapping is overridden when all_modifications_commutative is set to true.
 * In that case, all modifications are treated as commutative, which can be used
 * to communicate that the application is only generating commutative
 * UPDATE/DELETE/UPSERT commands and exclusive locks are unnecessary.
 */
void
AcquireExecutorShardLocks(Task *task, RowModifyLevel modLevel)
{
	AcquireExecutorShardLockForRowModify(task, modLevel);
	AcquireExecutorShardLocksForRelationRowLockList(task->relationRowLockList);

	/*
	 * If the task has a subselect, then we may need to lock the shards from which
	 * the query selects as well to prevent the subselects from seeing different
	 * results on different replicas. In particular this prevents INSERT.. SELECT
	 * commands from having a different effect on different placements.
	 */
	if (RequiresConsistentSnapshot(task))
	{
		/*
		 * ExclusiveLock conflicts with all lock types used by modifications
		 * and therefore prevents other modifications from running
		 * concurrently.
		 */

		LockRelationShardResources(task->relationShardList, ExclusiveLock);
	}
}


/*
 * AcquireExecutorMultiShardLocks acquires shard locks needed for execution
 * of writes on multiple shards. In addition to honouring commutativity
 * rules, we currently only allow a single multi-shard command on a shard at
 * a time. Otherwise, concurrent multi-shard commands may take row-level
 * locks on the shard placements in a different order and create a distributed
 * deadlock. This applies even when writes are commutative and/or there is
 * no replication.
 *
 * 1. If citus.all_modifications_commutative is set to true, then all locks
 * are acquired as ShareUpdateExclusiveLock.
 *
 * 2. If citus.all_modifications_commutative is false, then only the shards
 * with 2 or more replicas are locked with ExclusiveLock. Otherwise, the
 * lock is acquired with ShareUpdateExclusiveLock.
 *
 * ShareUpdateExclusiveLock conflicts with itself such that only one
 * multi-shard modification at a time is allowed on a shard. It also conflicts
 * with ExclusiveLock, which ensures that updates/deletes/upserts are applied
 * in the same order on all placements. It does not conflict with
 * RowExclusiveLock, which is normally obtained by single-shard, commutative
 * writes.
 */
void
AcquireExecutorMultiShardLocks(List *taskList)
{
	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		LOCKMODE lockMode = NoLock;

		if (task->anchorShardId == INVALID_SHARD_ID)
		{
			/* no shard locks to take if the task is not anchored to a shard */
			continue;
		}

		if (AllModificationsCommutative || list_length(task->taskPlacementList) == 1)
		{
			/*
			 * When all writes are commutative then we only need to prevent multi-shard
			 * commands from running concurrently with each other and with commands
			 * that are explicitly non-commutative. When there is no replication then
			 * we only need to prevent concurrent multi-shard commands.
			 *
			 * In either case, ShareUpdateExclusive has the desired effect, since
			 * it conflicts with itself and ExclusiveLock (taken by non-commutative
			 * writes).
			 *
			 * However, some users find this too restrictive, so we allow them to
			 * reduce to a RowExclusiveLock when citus.enable_deadlock_prevention
			 * is enabled, which lets multi-shard modifications run in parallel as
			 * long as they all disable the GUC.
			 */

			if (EnableDeadlockPrevention)
			{
				lockMode = ShareUpdateExclusiveLock;
			}
			else
			{
				lockMode = RowExclusiveLock;
			}
		}
		else
		{
			/*
			 * When there is replication, prevent all concurrent writes to the same
			 * shards to ensure the writes are ordered.
			 */

			lockMode = ExclusiveLock;
		}

		/*
		 * If we are dealing with a partition we are also taking locks on parent table
		 * to prevent deadlocks on concurrent operations on a partition and its parent.
		 */
		LockParentShardResourceIfPartition(task->anchorShardId, lockMode);
		LockShardResource(task->anchorShardId, lockMode);

		/*
		 * If the task has a subselect, then we may need to lock the shards from which
		 * the query selects as well to prevent the subselects from seeing different
		 * results on different replicas.
		 */

		if (RequiresConsistentSnapshot(task))
		{
			/*
			 * ExclusiveLock conflicts with all lock types used by modifications
			 * and therefore prevents other modifications from running
			 * concurrently.
			 */

			LockRelationShardResources(task->relationShardList, ExclusiveLock);
		}
	}
}


/*
 * RequiresConsistentSnapshot returns true if the given task need to take
 * the necessary locks to ensure that a subquery in the modify query
 * returns the same output for all task placements.
 */
static bool
RequiresConsistentSnapshot(Task *task)
{
	bool requiresIsolation = false;

	if (!task->modifyWithSubquery)
	{
		/*
		 * Other commands do not read from other shards.
		 */

		requiresIsolation = false;
	}
	else if (list_length(task->taskPlacementList) == 1)
	{
		/*
		 * If there is only one replica then we fully rely on PostgreSQL to
		 * provide SELECT isolation. In this case, we do not provide isolation
		 * across the shards, but that was never our intention.
		 */

		requiresIsolation = false;
	}
	else if (AllModificationsCommutative)
	{
		/*
		 * An INSERT/SELECT is commutative with other writes if it excludes
		 * any ongoing writes based on the filter conditions. Without knowing
		 * whether this is true, we assume the user took this into account
		 * when enabling citus.all_modifications_commutative. This option
		 * gives users an escape from aggressive locking during INSERT/SELECT.
		 */

		requiresIsolation = false;
	}
	else
	{
		/*
		 * If this is a non-commutative write, then we need to block ongoing
		 * writes to make sure that the subselect returns the same result
		 * on all placements.
		 */

		requiresIsolation = true;
	}

	return requiresIsolation;
}


/*
 * AcquireMetadataLocks acquires metadata locks on each of the anchor
 * shards in the task list to prevent a shard being modified while it
 * is being copied.
 */
void
AcquireMetadataLocks(List *taskList)
{
	/*
	 * Note: to avoid the overhead of additional sorting, we assume tasks
	 * to be already sorted by shard ID such that deadlocks are avoided.
	 * This is true for INSERT/SELECT, which is the only multi-shard
	 * command right now.
	 */

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		LockShardDistributionMetadata(task->anchorShardId, ShareLock);
	}
}


static void
AcquireExecutorShardLockForRowModify(Task *task, RowModifyLevel modLevel)
{
	LOCKMODE lockMode = NoLock;
	int64 shardId = task->anchorShardId;

	if (shardId == INVALID_SHARD_ID)
	{
		return;
	}

	if (modLevel <= ROW_MODIFY_READONLY)
	{
		/*
		 * The executor shard lock is used to maintain consistency between
		 * replicas and therefore no lock is required for read-only queries
		 * or in general when there is only one replica.
		 */

		lockMode = NoLock;
	}
	else if (list_length(task->taskPlacementList) == 1)
	{
		if (task->replicationModel == REPLICATION_MODEL_2PC)
		{
			/*
			 * While we don't need a lock to ensure writes are applied in
			 * a consistent order when there is a single replica. We also use
			 * shard resource locks as a crude implementation of SELECT..
			 * FOR UPDATE on reference tables, so we should always take
			 * a lock that conflicts with the FOR UPDATE/SHARE locks.
			 */
			lockMode = RowExclusiveLock;
		}
		else
		{
			/*
			 * When there is no replication, the worker itself can decide on
			 * on the order in which writes are applied.
			 */
			lockMode = NoLock;
		}
	}
	else if (AllModificationsCommutative)
	{
		/*
		 * Bypass commutativity checks when citus.all_modifications_commutative
		 * is enabled.
		 *
		 * A RowExclusiveLock does not conflict with itself and therefore allows
		 * multiple commutative commands to proceed concurrently. It does
		 * conflict with ExclusiveLock, which may still be obtained by another
		 * session that executes an UPDATE/DELETE/UPSERT command with
		 * citus.all_modifications_commutative disabled.
		 */

		lockMode = RowExclusiveLock;
	}
	else if (modLevel < ROW_MODIFY_NONCOMMUTATIVE)
	{
		/*
		 * An INSERT commutes with other INSERT commands, since performing them
		 * out-of-order only affects the table order on disk, but not the
		 * contents.
		 *
		 * When a unique constraint exists, INSERTs are not strictly commutative,
		 * but whichever INSERT comes last will error out and thus has no effect.
		 * INSERT is not commutative with UPDATE/DELETE/UPSERT, since the
		 * UPDATE/DELETE/UPSERT may consider the INSERT, depending on execution
		 * order.
		 *
		 * A RowExclusiveLock does not conflict with itself and therefore allows
		 * multiple INSERT commands to proceed concurrently. It conflicts with
		 * ExclusiveLock obtained by UPDATE/DELETE/UPSERT, ensuring those do
		 * not run concurrently with INSERT.
		 */

		lockMode = RowExclusiveLock;
	}
	else
	{
		/*
		 * UPDATE/DELETE/UPSERT commands do not commute with other modifications
		 * since the rows modified by one command may be affected by the outcome
		 * of another command.
		 *
		 * We need to handle upsert before INSERT, because PostgreSQL models
		 * upsert commands as INSERT with an ON CONFLICT section.
		 *
		 * ExclusiveLock conflicts with all lock types used by modifications
		 * and therefore prevents other modifications from running
		 * concurrently.
		 */

		lockMode = ExclusiveLock;
	}

	if (lockMode != NoLock)
	{
		ShardInterval *shardInterval = LoadShardInterval(shardId);

		SerializeNonCommutativeWrites(list_make1(shardInterval), lockMode);
	}
}


static void
AcquireExecutorShardLocksForRelationRowLockList(List *relationRowLockList)
{
	LOCKMODE rowLockMode = NoLock;

	if (relationRowLockList == NIL)
	{
		return;
	}

	/*
	 * If lock clause exists and it affects any reference table, we need to get
	 * lock on shard resource. Type of lock is determined by the type of row lock
	 * given in the query. If the type of row lock is either FOR NO KEY UPDATE or
	 * FOR UPDATE we get ExclusiveLock on shard resource. We get ShareLock if it
	 * is FOR KEY SHARE or FOR KEY SHARE.
	 *
	 * We have selected these lock types according to conflict table given in the
	 * Postgres documentation. It is given that FOR UPDATE and FOR NO KEY UPDATE
	 * must be conflict with each other modify command. By getting ExlcusiveLock
	 * we guarantee that. Note that, getting ExlusiveLock does not mimic the
	 * behaviour of Postgres exactly. Getting row lock with FOR NO KEY UPDATE and
	 * FOR KEY SHARE do not conflict in Postgres, yet they block each other in
	 * our implementation. Since FOR SHARE and FOR KEY SHARE does not conflict
	 * with each other but conflicts with modify commands, we get ShareLock for
	 * them.
	 */
	RelationRowLock *relationRowLock = NULL;
	foreach_ptr(relationRowLock, relationRowLockList)
	{
		LockClauseStrength rowLockStrength = relationRowLock->rowLockStrength;
		Oid relationId = relationRowLock->relationId;

		if (PartitionMethod(relationId) == DISTRIBUTE_BY_NONE)
		{
			List *shardIntervalList = LoadShardIntervalList(relationId);

			if (rowLockStrength == LCS_FORKEYSHARE || rowLockStrength == LCS_FORSHARE)
			{
				rowLockMode = ShareLock;
			}
			else if (rowLockStrength == LCS_FORNOKEYUPDATE ||
					 rowLockStrength == LCS_FORUPDATE)
			{
				rowLockMode = ExclusiveLock;
			}

			SerializeNonCommutativeWrites(shardIntervalList, rowLockMode);
		}
	}
}


/*
 * LockPartitionsInRelationList iterates over given list and acquires locks on
 * partitions of each partitioned table. It does nothing for non-partitioned tables.
 */
void
LockPartitionsInRelationList(List *relationIdList, LOCKMODE lockmode)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (PartitionedTable(relationId))
		{
			LockPartitionRelations(relationId, lockmode);
		}
	}
}


/*
 * LockPartitionRelations acquires relation lock on all partitions of given
 * partitioned relation. This function expects that given relation is a
 * partitioned relation.
 */
void
LockPartitionRelations(Oid relationId, LOCKMODE lockMode)
{
	/*
	 * PartitionList function generates partition list in the same order
	 * as PostgreSQL. Therefore we do not need to sort it before acquiring
	 * locks.
	 */
	List *partitionList = PartitionList(relationId);
	Oid partitionRelationId = InvalidOid;
	foreach_oid(partitionRelationId, partitionList)
	{
		LockRelationOid(partitionRelationId, lockMode);
	}
}
