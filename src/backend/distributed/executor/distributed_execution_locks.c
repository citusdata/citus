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
#include "distributed/coordinator_protocol.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/executor_util.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"


/*
 * AcquireExecutorShardLocksForExecution acquires advisory lock on shard IDs
 * to prevent unsafe concurrent modifications of shards.
 *
 * We prevent concurrent modifications of shards in two cases:
 * 1. Any non-commutative writes to a replicated table
 * 2. Multi-shard writes that are executed in parallel
 *
 * The first case ensures we do not apply updates in different orders on
 * different replicas (e.g. of a reference table), which could lead the
 * replicas to diverge.
 *
 * The second case prevents deadlocks due to out-of-order execution.
 *
 * There are two GUCs that can override the default behaviors.
 *  'citus.all_modifications_commutative' relaxes locking
 *  that's done for the purpose of keeping replicas consistent.
 *  'citus.enable_deadlock_prevention' relaxes locking done for
 *  the purpose of avoiding deadlocks between concurrent
 *  multi-shard commands.
 *
 * We do not take executor shard locks for utility commands such as
 * TRUNCATE because the table locks already prevent concurrent access.
 */
void
AcquireExecutorShardLocksForExecution(RowModifyLevel modLevel, List *taskList)
{
	if (modLevel <= ROW_MODIFY_READONLY &&
		!SelectForUpdateOnReferenceTable(taskList))
	{
		/*
		 * Executor locks only apply to DML commands and SELECT FOR UPDATE queries
		 * touching reference tables.
		 */
		return;
	}

	bool requiresParallelExecutionLocks =
		!(list_length(taskList) == 1 || ShouldRunTasksSequentially(taskList));

	bool modifiedTableReplicated = ModifiedTableReplicated(taskList);
	if (!modifiedTableReplicated && !requiresParallelExecutionLocks)
	{
		/*
		 * When a distributed query on tables with replication
		 * factor == 1 and command hits only a single shard, we
		 * rely on Postgres to handle the serialization of the
		 * concurrent modifications on the workers.
		 *
		 * For reference tables, even if their placements are replicated
		 * ones (e.g., single node), we acquire the distributed execution
		 * locks to be consistent when new node(s) are added. So, they
		 * do not return at this point.
		 */
		return;
	}

	/*
	 * We first assume that all the remaining modifications are going to
	 * be serialized. So, start with an ExclusiveLock and lower the lock level
	 * as much as possible.
	 */
	int lockMode = ExclusiveLock;

	/*
	 * In addition to honouring commutativity rules, we currently only
	 * allow a single multi-shard command on a shard at a time. Otherwise,
	 * concurrent multi-shard commands may take row-level locks on the
	 * shard placements in a different order and create a distributed
	 * deadlock. This applies even when writes are commutative and/or
	 * there is no replication. This can be relaxed via
	 * EnableDeadlockPrevention.
	 *
	 * 1. If citus.all_modifications_commutative is set to true, then all locks
	 * are acquired as RowExclusiveLock.
	 *
	 * 2. If citus.all_modifications_commutative is false, then only the shards
	 * with more than one replicas are locked with ExclusiveLock. Otherwise, the
	 * lock is acquired with ShareUpdateExclusiveLock.
	 *
	 * ShareUpdateExclusiveLock conflicts with itself such that only one
	 * multi-shard modification at a time is allowed on a shard. It also conflicts
	 * with ExclusiveLock, which ensures that updates/deletes/upserts are applied
	 * in the same order on all placements. It does not conflict with
	 * RowExclusiveLock, which is normally obtained by single-shard, commutative
	 * writes.
	 */
	if (!modifiedTableReplicated && requiresParallelExecutionLocks)
	{
		/*
		 * When there is no replication then we only need to prevent
		 * concurrent multi-shard commands on the same shards. This is
		 * because concurrent, parallel commands may modify the same
		 * set of shards, but in different orders. The order of the
		 * accesses might trigger distributed deadlocks that are not
		 * possible to happen on non-distributed systems such
		 * regular Postgres.
		 *
		 * As an example, assume that we have two queries: query-1 and query-2.
		 * Both queries access shard-1 and shard-2. If query-1 first accesses to
		 * shard-1 then shard-2, and query-2 accesses shard-2 then shard-1, these
		 * two commands might block each other in case they modify the same rows
		 * (e.g., cause distributed deadlocks).
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
		lockMode =
			EnableDeadlockPrevention ? ShareUpdateExclusiveLock : RowExclusiveLock;

		if (!IsCoordinator())
		{
			/*
			 * We also skip taking a heavy-weight lock when running a multi-shard
			 * commands from workers, since we currently do not prevent concurrency
			 * across workers anyway.
			 */
			lockMode = RowExclusiveLock;
		}
	}
	else if (modifiedTableReplicated)
	{
		/*
		 * When we are executing distributed queries on replicated tables, our
		 * default behaviour is to prevent any concurrency. This is valid
		 * for when parallel execution is happening or not.
		 *
		 * The reason is that we cannot control the order of the placement accesses
		 * of two distributed queries to the same shards. The order of the accesses
		 * might cause the replicas of the same shard placements diverge. This is
		 * not possible to happen on non-distributed systems such regular Postgres.
		 *
		 * As an example, assume that we have two queries: query-1 and query-2.
		 * Both queries only access the placements of shard-1, say p-1 and p-2.
		 *
		 * And, assume that these queries are non-commutative, such as:
		 *  query-1: UPDATE table SET b = 1 WHERE key = 1;
		 *  query-2: UPDATE table SET b = 2 WHERE key = 1;
		 *
		 * If query-1 accesses to p-1 then p-2, and query-2 accesses
		 * p-2 then p-1, these two commands would leave the p-1 and p-2
		 * diverged (e.g., the values for the column "b" would be different).
		 *
		 * The only exception to this rule is the single shard commutative
		 * modifications, such as INSERTs. In that case, we can allow
		 * concurrency among such backends, hence lowering the lock level
		 * to RowExclusiveLock.
		 */
		if (!requiresParallelExecutionLocks && modLevel < ROW_MODIFY_NONCOMMUTATIVE)
		{
			lockMode = RowExclusiveLock;
		}
	}

	if (AllModificationsCommutative)
	{
		/*
		 * The mapping is overridden when all_modifications_commutative is set to true.
		 * In that case, all modifications are treated as commutative, which can be used
		 * to communicate that the application is only generating commutative
		 * UPDATE/DELETE/UPSERT commands and exclusive locks are unnecessary. This
		 * is irrespective of single-shard/multi-shard or replicated tables.
		 */
		lockMode = RowExclusiveLock;
	}

	/* now, iterate on the tasks and acquire the executor locks on the shards */
	List *anchorShardIntervalList = NIL;
	List *relationRowLockList = NIL;
	List *requiresConsistentSnapshotRelationShardList = NIL;

	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		ShardInterval *anchorShardInterval = LoadShardInterval(task->anchorShardId);
		anchorShardIntervalList = lappend(anchorShardIntervalList, anchorShardInterval);

		/* Acquire additional locks for SELECT .. FOR UPDATE on reference tables */
		AcquireExecutorShardLocksForRelationRowLockList(task->relationRowLockList);

		relationRowLockList =
			list_concat(relationRowLockList,
						task->relationRowLockList);

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
			requiresConsistentSnapshotRelationShardList =
				list_concat(requiresConsistentSnapshotRelationShardList,
							task->relationShardList);
		}
	}

	/*
	 * Acquire the locks in a sorted way to avoid deadlocks due to lock
	 * ordering across concurrent sessions.
	 */
	anchorShardIntervalList =
		SortList(anchorShardIntervalList, CompareShardIntervalsById);

	/*
	 * If we are dealing with a partition we are also taking locks on parent table
	 * to prevent deadlocks on concurrent operations on a partition and its parent.
	 *
	 * Note that this function currently does not acquire any remote locks as that
	 * is necessary to control the concurrency across multiple nodes for replicated
	 * tables. That is because Citus currently does not allow modifications to
	 * partitions from any node other than the coordinator.
	 */
	LockParentShardResourceIfPartition(anchorShardIntervalList, lockMode);

	/* Acquire distribution execution locks on the affected shards */
	SerializeNonCommutativeWrites(anchorShardIntervalList, lockMode);

	if (relationRowLockList != NIL)
	{
		/* Acquire additional locks for SELECT .. FOR UPDATE on reference tables */
		AcquireExecutorShardLocksForRelationRowLockList(relationRowLockList);
	}


	if (requiresConsistentSnapshotRelationShardList != NIL)
	{
		/*
		 * If the task has a subselect, then we may need to lock the shards from which
		 * the query selects as well to prevent the subselects from seeing different
		 * results on different replicas.
		 *
		 * ExclusiveLock conflicts with all lock types used by modifications
		 * and therefore prevents other modifications from running
		 * concurrently.
		 */
		LockRelationShardResources(requiresConsistentSnapshotRelationShardList,
								   ExclusiveLock);
	}
}


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
	foreach_declared_ptr(task, taskList)
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
	 * we guarantee that. Note that, getting ExclusiveLock does not mimic the
	 * behaviour of Postgres exactly. Getting row lock with FOR NO KEY UPDATE and
	 * FOR KEY SHARE do not conflict in Postgres, yet they block each other in
	 * our implementation. Since FOR SHARE and FOR KEY SHARE does not conflict
	 * with each other but conflicts with modify commands, we get ShareLock for
	 * them.
	 */
	RelationRowLock *relationRowLock = NULL;
	foreach_declared_ptr(relationRowLock, relationRowLockList)
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
	foreach_declared_oid(relationId, relationIdList)
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
	foreach_declared_oid(partitionRelationId, partitionList)
	{
		LockRelationOid(partitionRelationId, lockMode);
	}
}


/*
 * LockPartitionsForDistributedPlan ensures commands take locks on all partitions
 * of a distributed table that appears in the query. We do this primarily out of
 * consistency with PostgreSQL locking.
 */
void
LockPartitionsForDistributedPlan(DistributedPlan *plan)
{
	if (TaskListModifiesDatabase(plan->modLevel, plan->workerJob->taskList))
	{
		Oid targetRelationId = plan->targetRelationId;

		LockPartitionsInRelationList(list_make1_oid(targetRelationId), RowExclusiveLock);
	}

	/*
	 * Lock partitions of tables that appear in a SELECT or subquery. In the
	 * DML case this also includes the target relation, but since we already
	 * have a stronger lock this doesn't do any harm.
	 */
	LockPartitionsInRelationList(plan->relationIdList, AccessShareLock);
}
