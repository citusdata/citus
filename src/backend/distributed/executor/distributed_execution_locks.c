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
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"


/*
 * RequiresConsistentSnapshot returns true if the given task need to take
 * the necessary locks to ensure that a subquery in the modify query
 * returns the same output for all task placements.
 */
bool
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


void
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

		if (IsCitusTableType(relationId, REFERENCE_TABLE))
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
