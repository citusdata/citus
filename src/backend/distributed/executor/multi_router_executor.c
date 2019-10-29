/*
 * multi_router_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * with synchronous connections. The routines utilize the connection cache.
 * Therefore, only a single connection is opened for each worker. Also, router
 * executor does not require a master table and a master query. In other words,
 * the results that are fetched from a single worker is sent to the output console
 * directly. Lastly, router executor can only execute a single task.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"
#include "fmgr.h" /* IWYU pragma: keep */
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <string.h>

#include "access/htup.h"
#include "access/sdir.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "distributed/backend_data.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/subplan_execution.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_oper.h"
#include "nodes/plannodes.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/tuplestore.h"


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

/* we've deprecated this flag, keeping here for some time not to break existing users */
bool EnableDeadlockPrevention = true;

/* number of nested stored procedure call levels we are currently in */
int StoredProcedureLevel = 0;

/* number of nested DO block levels we are currently in */
int DoBlockLevel = 0;

/* sort the returning to get consistent outputs */
bool SortReturning = false;

/* functions needed during run phase */
static void AcquireMetadataLocks(List *taskList);
static List * BuildPlacementAccessList(int32 groupId, List *relationShardList,
									   ShardPlacementAccessType accessType);
static void AcquireExecutorShardLockForRowModify(Task *task, RowModifyLevel modLevel);
static void AcquireExecutorShardLocksForRelationRowLockList(List *relationRowLockList);
static bool RequiresConsistentSnapshot(Task *task);

/*
 * AcquireMetadataLocks acquires metadata locks on each of the anchor
 * shards in the task list to prevent a shard being modified while it
 * is being copied.
 */
static void
AcquireMetadataLocks(List *taskList)
{
	ListCell *taskCell = NULL;

	/*
	 * Note: to avoid the overhead of additional sorting, we assume tasks
	 * to be already sorted by shard ID such that deadlocks are avoided.
	 * This is true for INSERT/SELECT, which is the only multi-shard
	 * command right now.
	 */

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);

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
	ListCell *relationRowLockCell = NULL;
	LOCKMODE rowLockMode = NoLock;

	if (relationRowLockList == NIL)
	{
		return;
	}

	/*
	 * If lock clause exists and it effects any reference table, we need to get
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
	foreach(relationRowLockCell, relationRowLockList)
	{
		RelationRowLock *relationRowLock = lfirst(relationRowLockCell);
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
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
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
 * CitusModifyBeginScan first evaluates expressions in the query and then
 * performs shard pruning in case the partition column in an insert was
 * defined as a function call.
 *
 * The function also checks the validity of the given custom scan node and
 * gets locks on the shards involved in the task list of the distributed plan.
 */
void
CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = NULL;
	Job *workerJob = NULL;
	Query *jobQuery = NULL;
	List *taskList = NIL;

	/*
	 * We must not change the distributed plan since it may be reused across multiple
	 * executions of a prepared statement. Instead we create a deep copy that we only
	 * use for the current execution.
	 */
	distributedPlan = scanState->distributedPlan = copyObject(scanState->distributedPlan);

	workerJob = distributedPlan->workerJob;
	jobQuery = workerJob->jobQuery;
	taskList = workerJob->taskList;

	if (workerJob->requiresMasterEvaluation)
	{
		PlanState *planState = &(scanState->customScanState.ss.ps);
		EState *executorState = planState->state;

		ExecuteMasterEvaluableFunctions(jobQuery, planState);

		/*
		 * We've processed parameters in ExecuteMasterEvaluableFunctions and
		 * don't need to send their values to workers, since they will be
		 * represented as constants in the deparsed query. To avoid sending
		 * parameter values, we set the parameter list to NULL.
		 */
		executorState->es_param_list_info = NULL;

		if (workerJob->deferredPruning)
		{
			DeferredErrorMessage *planningError = NULL;

			/* need to perform shard pruning, rebuild the task list from scratch */
			taskList = RouterInsertTaskList(jobQuery, &planningError);

			if (planningError != NULL)
			{
				RaiseDeferredError(planningError, ERROR);
			}

			workerJob->taskList = taskList;
			workerJob->partitionKeyValue = ExtractInsertPartitionKeyValue(jobQuery);
		}

		RebuildQueryStrings(jobQuery, taskList);
	}

	/* prevent concurrent placement changes */
	AcquireMetadataLocks(taskList);

	/*
	 * We are taking locks on partitions of partitioned tables. These locks are
	 * necessary for locking tables that appear in the SELECT part of the query.
	 */
	LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

	/* modify tasks are always assigned using first-replica policy */
	workerJob->taskList = FirstReplicaAssignTaskList(taskList);
}




/*
 * SortTupleStore gets a CitusScanState and sorts the tuplestore by all the
 * entries in the target entry list, starting from the first one and
 * ending with the last entry.
 *
 * The sorting is done in ASC order.
 */
void
SortTupleStore(CitusScanState *scanState)
{
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	Tuplestorestate *tupleStore = scanState->tuplestorestate;

	List *targetList = scanState->customScanState.ss.ps.plan->targetlist;
	uint32 expectedColumnCount = list_length(targetList);

	/* Convert list-ish representation to arrays wanted by executor */
	int numberOfSortKeys = expectedColumnCount;
	AttrNumber *sortColIdx = (AttrNumber *) palloc(numberOfSortKeys * sizeof(AttrNumber));
	Oid *sortOperators = (Oid *) palloc(numberOfSortKeys * sizeof(Oid));
	Oid *collations = (Oid *) palloc(numberOfSortKeys * sizeof(Oid));
	bool *nullsFirst = (bool *) palloc(numberOfSortKeys * sizeof(bool));

	ListCell *targetCell = NULL;
	int sortKeyIndex = 0;

	Tuplesortstate *tuplesortstate = NULL;

	/*
	 * Iterate on the returning target list and generate the necessary information
	 * for sorting the tuples.
	 */
	foreach(targetCell, targetList)
	{
		TargetEntry *returningEntry = (TargetEntry *) lfirst(targetCell);
		Oid sortop = InvalidOid;

		/* determine the sortop, we don't need anything else */
		get_sort_group_operators(exprType((Node *) returningEntry->expr),
								 true, false, false,
								 &sortop, NULL, NULL,
								 NULL);

		sortColIdx[sortKeyIndex] = sortKeyIndex + 1;
		sortOperators[sortKeyIndex] = sortop;
		collations[sortKeyIndex] = exprCollation((Node *) returningEntry->expr);
		nullsFirst[sortKeyIndex] = false;

		sortKeyIndex++;
	}

	tuplesortstate =
		tuplesort_begin_heap(tupleDescriptor, numberOfSortKeys, sortColIdx, sortOperators,
							 collations, nullsFirst, work_mem, NULL, false);

	while (true)
	{
		TupleTableSlot *slot = ReturnTupleFromTuplestore(scanState);

		if (TupIsNull(slot))
		{
			break;
		}

		/* tuplesort_puttupleslot copies the slot into sort context */
		tuplesort_puttupleslot(tuplesortstate, slot);
	}

	/* perform the actual sort operation */
	tuplesort_performsort(tuplesortstate);

	/*
	 * Truncate the existing tupleStore, because we'll fill it back
	 * from the sorted tuplestore.
	 */
	tuplestore_clear(tupleStore);

	/* iterate over all the sorted tuples, add them to original tuplestore */
	while (true)
	{
		TupleTableSlot *newSlot = MakeSingleTupleTableSlotCompat(tupleDescriptor,
																 &TTSOpsMinimalTuple);
		bool found = tuplesort_gettupleslot(tuplesortstate, true, false, newSlot, NULL);

		if (!found)
		{
			break;
		}

		/* tuplesort_puttupleslot copies the slot into the tupleStore context */
		tuplestore_puttupleslot(tupleStore, newSlot);
	}

	tuplestore_rescan(scanState->tuplestorestate);

	/* terminate the sort, clear unnecessary resources */
	tuplesort_end(tuplesortstate);
}



/*
 * TaskListRequires2PC determines whether the given task list requires 2PC
 * because the tasks provided operates on a reference table or there are multiple
 * tasks and the commit protocol is 2PC.
 *
 * Note that we currently do not generate tasks lists that involves multiple different
 * tables, thus we only check the first task in the list for reference tables.
 */
bool
TaskListRequires2PC(List *taskList)
{
	Task *task = NULL;
	bool multipleTasks = false;
	uint64 anchorShardId = INVALID_SHARD_ID;

	if (taskList == NIL)
	{
		return false;
	}

	task = (Task *) linitial(taskList);
	if (task->replicationModel == REPLICATION_MODEL_2PC)
	{
		return true;
	}

	/*
	 * Some tasks don't set replicationModel thus we rely on
	 * the anchorShardId as well replicationModel.
	 *
	 * TODO: Do we ever need replicationModel in the Task structure?
	 * Can't we always rely on anchorShardId?
	 */
	anchorShardId = task->anchorShardId;
	if (anchorShardId != INVALID_SHARD_ID && ReferenceTableShardId(anchorShardId))
	{
		return true;
	}

	multipleTasks = list_length(taskList) > 1;
	if (!ReadOnlyTask(task->taskType) &&
		multipleTasks && MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		return true;
	}

	if (task->taskType == DDL_TASK)
	{
		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC ||
			task->replicationModel == REPLICATION_MODEL_2PC)
		{
			return true;
		}
	}

	return false;
}


/*
 * ReadOnlyTask returns true if the input task does a read-only operation
 * on the database.
 */
bool
ReadOnlyTask(TaskType taskType)
{
	if (taskType == ROUTER_TASK || taskType == SQL_TASK)
	{
		/*
		 * TODO: We currently do not execute modifying CTEs via ROUTER_TASK/SQL_TASK.
		 * When we implement it, we should either not use the mentioned task types for
		 * modifying CTEs detect them here.
		 */
		return true;
	}

	return false;
}


/*
 * BuildPlacementSelectList builds a list of SELECT placement accesses
 * which can be used to call StartPlacementListConnection or
 * GetPlacementListConnection. If the node group does not have a placement
 * (e.g. in case of a broadcast join) then the shard is skipped.
 */
List *
BuildPlacementSelectList(int32 groupId, List *relationShardList)
{
	return BuildPlacementAccessList(groupId, relationShardList, PLACEMENT_ACCESS_SELECT);
}


/*
 * BuildPlacementDDLList is a warpper around BuildPlacementAccessList() for DDL access.
 */
List *
BuildPlacementDDLList(int32 groupId, List *relationShardList)
{
	return BuildPlacementAccessList(groupId, relationShardList, PLACEMENT_ACCESS_DDL);
}


/*
 * BuildPlacementAccessList returns a list of placement accesses for the given
 * relationShardList and the access type.
 */
static List *
BuildPlacementAccessList(int32 groupId, List *relationShardList,
						 ShardPlacementAccessType accessType)
{
	ListCell *relationShardCell = NULL;
	List *placementAccessList = NIL;

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
		ShardPlacement *placement = NULL;
		ShardPlacementAccess *placementAccess = NULL;

		placement = FindShardPlacementOnGroup(groupId, relationShard->shardId);
		if (placement == NULL)
		{
			continue;
		}

		placementAccess = CreatePlacementAccess(placement, accessType);
		placementAccessList = lappend(placementAccessList, placementAccess);
	}

	return placementAccessList;
}


/*
 * CreatePlacementAccess returns a new ShardPlacementAccess for the given placement
 * and access type.
 */
ShardPlacementAccess *
CreatePlacementAccess(ShardPlacement *placement, ShardPlacementAccessType accessType)
{
	ShardPlacementAccess *placementAccess = NULL;

	placementAccess = (ShardPlacementAccess *) palloc0(sizeof(ShardPlacementAccess));
	placementAccess->placement = placement;
	placementAccess->accessType = accessType;

	return placementAccess;
}


/*
 * ExtractParametersForRemoteExecution extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * It changes oid of custom types to InvalidOid so that they are the same in workers
 * and coordinators.
 */
void
ExtractParametersForRemoteExecution(ParamListInfo paramListInfo, Oid **parameterTypes,
									const char ***parameterValues)
{
	ExtractParametersFromParamList(paramListInfo, parameterTypes,
								   parameterValues, false);
}


/*
 * ExtractParametersFromParamList extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * If useOriginalCustomTypeOids is true, it uses the original oids for custom types.
 */
void
ExtractParametersFromParamList(ParamListInfo paramListInfo,
							   Oid **parameterTypes,
							   const char ***parameterValues, bool
							   useOriginalCustomTypeOids)
{
	int parameterIndex = 0;
	int parameterCount = paramListInfo->numParams;

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the master and worker nodes. Therefore, the worker nodes can
		 * infer the correct oid.
		 */
		if (parameterData->ptype >= FirstNormalObjectId && !useOriginalCustomTypeOids)
		{
			(*parameterTypes)[parameterIndex] = 0;
		}
		else
		{
			(*parameterTypes)[parameterIndex] = parameterData->ptype;
		}

		/*
		 * If the parameter is not referenced / used (ptype == 0) and
		 * would otherwise have errored out inside standard_planner()),
		 * don't pass a value to the remote side, and pass text oid to prevent
		 * undetermined data type errors on workers.
		 */
		if (parameterData->ptype == 0)
		{
			(*parameterValues)[parameterIndex] = NULL;
			(*parameterTypes)[parameterIndex] = TEXTOID;

			continue;
		}

		/*
		 * If the parameter is NULL then we preserve its type, but
		 * don't need to evaluate its value.
		 */
		if (parameterData->isnull)
		{
			(*parameterValues)[parameterIndex] = NULL;

			continue;
		}

		getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId,
						  &variableLengthType);

		(*parameterValues)[parameterIndex] = OidOutputFunctionCall(typeOutputFunctionId,
																   parameterData->value);
	}
}

