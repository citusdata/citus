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
 * Copyright (c) 2012-2016, Citus Data, Inc.
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

/* sort the returning to get consistent outputs */
bool SortReturning = false;

/* functions needed during run phase */
static void AcquireMetadataLocks(List *taskList);
static int64 ExecuteSingleModifyTask(CitusScanState *scanState, Task *task,
									 RowModifyLevel modLevel,
									 bool alwaysThrowErrorOnFailure, bool expectResults);
static void ExecuteSingleSelectTask(CitusScanState *scanState, Task *task);
static List * BuildPlacementAccessList(int32 groupId, List *relationShardList,
									   ShardPlacementAccessType accessType);
static List * GetModifyConnections(Task *task, bool markCritical);
static int64 ExecuteModifyTasks(List *taskList, bool expectResults,
								ParamListInfo paramListInfo, CitusScanState *scanState);
static void AcquireExecutorShardLockForRowModify(Task *task, RowModifyLevel modLevel);
static void AcquireExecutorShardLocksForRelationRowLockList(List *relationRowLockList);
static bool RequiresConsistentSnapshot(Task *task);
static void RouterMultiModifyExecScan(CustomScanState *node);
static void RouterSequentialModifyExecScan(CustomScanState *node);
static bool SendQueryInSingleRowMode(MultiConnection *connection, char *query,
									 ParamListInfo paramListInfo);
static bool StoreQueryResult(CitusScanState *scanState, MultiConnection *connection, bool
							 alwaysThrowErrorOnFailure, int64 *rows,
							 DistributedExecutionStats *executionStats);
static bool ConsumeQueryResult(MultiConnection *connection, bool
							   alwaysThrowErrorOnFailure, int64 *rows);


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
 * RouterModifyExecScan executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used or SELECT FOR UPDATE executed,
 * returns the results with a TupleTableSlot.
 *
 * The function can handle both single task query executions,
 * sequential or parallel multi-task query executions.
 */
TupleTableSlot *
RouterModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Job *workerJob = distributedPlan->workerJob;
		List *taskList = workerJob->taskList;
		bool parallelExecution = true;

		ErrorIfLocalExecutionHappened();
		DisableLocalExecution();

		ExecuteSubPlans(distributedPlan);

		if (list_length(taskList) <= 1 ||
			IsMultiRowInsert(workerJob->jobQuery) ||
			MultiShardConnectionType == SEQUENTIAL_CONNECTION)
		{
			parallelExecution = false;
		}

		if (parallelExecution)
		{
			RouterMultiModifyExecScan(node);
		}
		else
		{
			RouterSequentialModifyExecScan(node);
		}

		if (SortReturning && distributedPlan->hasReturning)
		{
			SortTupleStore(scanState);
		}

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
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

#if (PG_VERSION_NUM >= 110000)
	tuplesortstate =
		tuplesort_begin_heap(tupleDescriptor, numberOfSortKeys, sortColIdx, sortOperators,
							 collations, nullsFirst, work_mem, NULL, false);
#else
	tuplesortstate =
		tuplesort_begin_heap(tupleDescriptor, numberOfSortKeys, sortColIdx, sortOperators,
							 collations, nullsFirst, work_mem, false);
#endif

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
 * RouterSequentialModifyExecScan executes 0 or more modifications on a
 * distributed table sequentially and stores them in custom scan's tuple
 * store. Note that we also use this path for SELECT ... FOR UPDATE queries.
 */
static void
RouterSequentialModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	RowModifyLevel modLevel = distributedPlan->modLevel;
	bool hasReturning = distributedPlan->hasReturning;
	Job *workerJob = distributedPlan->workerJob;
	List *taskList = workerJob->taskList;
	EState *executorState = ScanStateGetExecutorState(scanState);

	Assert(!scanState->finishedRemoteScan);

	executorState->es_processed +=
		ExecuteModifyTasksSequentially(scanState, taskList, modLevel, hasReturning);
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
 * RouterMultiModifyExecScan executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used, stores them in custom scan's tuple store.
 */
static void
RouterMultiModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Job *workerJob = distributedPlan->workerJob;
	List *taskList = workerJob->taskList;
	bool hasReturning = distributedPlan->hasReturning;
	bool isModificationQuery = true;

	Assert(!scanState->finishedRemoteScan);

	ExecuteMultipleTasks(scanState, taskList, isModificationQuery, hasReturning);
}


/*
 * RouterSelectExecScan executes a single select task on the remote node,
 * retrieves the results and stores them in custom scan's tuple store. Then, it
 * returns tuples one by one from this tuple store.
 */
TupleTableSlot *
RouterSelectExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Job *workerJob = distributedPlan->workerJob;
		List *taskList = workerJob->taskList;

		ErrorIfLocalExecutionHappened();
		DisableLocalExecution();

		/* we are taking locks on partitions of partitioned tables */
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

		ExecuteSubPlans(distributedPlan);

		if (list_length(taskList) > 0)
		{
			Task *task = (Task *) linitial(taskList);

			ExecuteSingleSelectTask(scanState, task);
		}

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * ExecuteSingleSelectTask executes the task on the remote node, retrieves the
 * results and stores them in a tuple store.
 *
 * If the task fails on one of the placements, the function retries it on
 * other placements or errors out if the query fails on all placements.
 */
static void
ExecuteSingleSelectTask(CitusScanState *scanState, Task *task)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;
	char *queryString = task->queryString;
	List *relationShardList = task->relationShardList;
	DistributedExecutionStats executionStats = { 0 };

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		bool queryOK = false;
		bool dontFailOnError = false;
		int64 currentAffectedTupleCount = 0;
		int connectionFlags = 0;
		List *placementAccessList = NIL;
		MultiConnection *connection = NULL;

		if (list_length(relationShardList) > 0)
		{
			placementAccessList = BuildPlacementSelectList(taskPlacement->groupId,
														   relationShardList);
		}
		else
		{
			/*
			 * When the SELECT prunes down to 0 shards, just use the dummy placement.
			 *
			 * FIXME: it would be preferable to evaluate the SELECT locally since no
			 * data from the workers is required.
			 */

			ShardPlacementAccess *placementAccess =
				CreatePlacementAccess(taskPlacement, PLACEMENT_ACCESS_SELECT);

			placementAccessList = list_make1(placementAccess);
		}

		if (placementAccessList == NIL)
		{
			ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							errmsg("a placement was moved after the SELECT was "
								   "planned")));
		}

		connection = GetPlacementListConnection(connectionFlags, placementAccessList,
												NULL);

		/*
		 * Make sure we open a transaction block and assign a distributed transaction
		 * ID if we are in a coordinated transaction.
		 *
		 * This can happen when the SELECT goes to a node that was not involved in
		 * the transaction so far, or when existing connections to the node are
		 * claimed exclusively, e.g. the connection might be claimed to copy the
		 * intermediate result of a CTE to the node. Especially in the latter case,
		 * we want to make sure that we open a transaction block and assign a
		 * distributed transaction ID, such that the query can read intermediate
		 * results.
		 */
		RemoteTransactionBeginIfNecessary(connection);

		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{
			continue;
		}

		queryOK = StoreQueryResult(scanState, connection, dontFailOnError,
								   &currentAffectedTupleCount,
								   &executionStats);

		if (CheckIfSizeLimitIsExceeded(&executionStats))
		{
			ErrorSizeLimitIsExceeded();
		}

		if (queryOK)
		{
			return;
		}
	}

	ereport(ERROR, (errmsg("could not receive query results")));
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
 * ExecuteSingleModifyTask executes the task on the remote node, retrieves the
 * results and stores them, if RETURNING is used, in a tuple store. The function
 * can execute both DDL and DML tasks. When a DDL task is passed, the function
 * does not expect scanState to be present.
 *
 * If the task fails on one of the placements, the function reraises the
 * remote error (constraint violation in DML), marks the affected placement as
 * invalid (other error on some placements, via the placement connection
 * framework), or errors out (failed on all placements).
 *
 * The function returns affectedTupleCount if applicable.
 */
static int64
ExecuteSingleModifyTask(CitusScanState *scanState, Task *task, RowModifyLevel modLevel,
						bool alwaysThrowErrorOnFailure, bool expectResults)
{
	EState *executorState = NULL;
	ParamListInfo paramListInfo = NULL;
	List *taskPlacementList = task->taskPlacementList;
	List *connectionList = NIL;
	ListCell *taskPlacementCell = NULL;
	ListCell *connectionCell = NULL;
	int64 affectedTupleCount = -1;
	int failureCount = 0;
	bool resultsOK = false;
	bool gotResults = false;

	char *queryString = task->queryString;

	ShardInterval *shardInterval = LoadShardInterval(task->anchorShardId);
	Oid relationId = shardInterval->relationId;

	if (scanState)
	{
		executorState = ScanStateGetExecutorState(scanState);
		paramListInfo = executorState->es_param_list_info;
	}

	/*
	 * Get connections required to execute task. This will, if necessary,
	 * establish the connection, mark as critical (when modifying reference
	 * table or multi-shard command) and start a transaction (when in a
	 * transaction).
	 */
	connectionList = GetModifyConnections(task, alwaysThrowErrorOnFailure);

	/*
	 * If we are dealing with a partitioned table, we also need to lock its
	 * partitions.
	 *
	 * For DDL commands, we already obtained the appropriate locks in
	 * ProcessUtility, so we only need to do this for DML commands.
	 */
	if (PartitionedTable(relationId) && task->taskType == MODIFY_TASK)
	{
		LockPartitionRelations(relationId, RowExclusiveLock);
	}

	/*
	 * Prevent replicas of the same shard from diverging. We don't
	 * need to acquire lock for TRUNCATE and DDLs since they already
	 * acquire the necessary locks on the relations, and blocks any
	 * unsafe concurrent operations.
	 */
	if (modLevel > ROW_MODIFY_NONE)
	{
		AcquireExecutorShardLocks(task, modLevel);
	}

	/* try to execute modification on all placements */
	forboth(taskPlacementCell, taskPlacementList, connectionCell, connectionList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		bool queryOK = false;
		int64 currentAffectedTupleCount = 0;

		if (connection->remoteTransaction.transactionFailed)
		{
			/*
			 * If GetModifyConnections failed to send BEGIN this connection will have
			 * been marked as failed, and should not have any more commands sent to
			 * it! Skip it for now, at the bottom of this method we call
			 * MarkFailedShardPlacements() to ensure future statements will not use this
			 * placement.
			 */
			failureCount++;
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{
			failureCount++;
			continue;
		}

		if (failureCount + 1 == list_length(taskPlacementList))
		{
			/*
			 * If we already failed on all other placements (possibly 0),
			 * relay errors directly.
			 */
			alwaysThrowErrorOnFailure = true;
		}

		/*
		 * If caller is interested, store query results the first time
		 * through. The output of the query's execution on other shards is
		 * discarded if we run there (because it's a modification query).
		 */
		if (!gotResults && expectResults)
		{
			queryOK = StoreQueryResult(scanState, connection, alwaysThrowErrorOnFailure,
									   &currentAffectedTupleCount, NULL);
		}
		else
		{
			queryOK = ConsumeQueryResult(connection, alwaysThrowErrorOnFailure,
										 &currentAffectedTupleCount);
		}

		if (queryOK)
		{
			if ((affectedTupleCount == -1) ||
				(affectedTupleCount == currentAffectedTupleCount))
			{
				affectedTupleCount = currentAffectedTupleCount;
			}
			else
			{
				ereport(WARNING,
						(errmsg("modified "INT64_FORMAT " tuples, but expected "
														"to modify "INT64_FORMAT,
								currentAffectedTupleCount, affectedTupleCount),
						 errdetail("modified placement on %s:%d",
								   taskPlacement->nodeName, taskPlacement->nodePort)));
			}

			resultsOK = true;
			gotResults = true;
		}
		else
		{
			failureCount++;
		}
	}

	/*
	 * If a command results in an error on all workers, we relay the last error
	 * in the loop above by setting alwaysThrowErrorOnFailure. However, if all
	 * connections fail we still complete the loop without throwing an error.
	 * In that case, throw an error below.
	 */
	if (!resultsOK)
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* if some placements failed, ensure future statements don't access them */
	MarkFailedShardPlacements();

	if (IsMultiStatementTransaction())
	{
		XactModificationLevel = XACT_MODIFICATION_DATA;
	}

	return affectedTupleCount;
}


/*
 * GetModifyConnections returns the list of connections required to execute
 * modify commands on the placements in tasPlacementList.  If necessary remote
 * transactions are started.
 *
 * If markCritical is true remote transactions are marked as critical.
 */
static List *
GetModifyConnections(Task *task, bool markCritical)
{
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;
	List *multiConnectionList = NIL;
	List *relationShardList = task->relationShardList;

	/* first initiate connection establishment for all necessary connections */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		int connectionFlags = 0;
		MultiConnection *multiConnection = NULL;
		List *placementAccessList = NIL;
		ShardPlacementAccess *placementModification = NULL;
		ShardPlacementAccessType accessType = PLACEMENT_ACCESS_DML;

		if (task->taskType == DDL_TASK)
		{
			connectionFlags = connectionFlags | FOR_DDL;
			accessType = PLACEMENT_ACCESS_DDL;
		}
		else
		{
			connectionFlags = connectionFlags | FOR_DML;
			accessType = PLACEMENT_ACCESS_DML;
		}

		if (accessType == PLACEMENT_ACCESS_DDL)
		{
			/*
			 * All relations appearing inter-shard DDL commands should be marked
			 * with DDL access.
			 */
			placementAccessList =
				BuildPlacementDDLList(taskPlacement->groupId, relationShardList);
		}
		else
		{
			/* create placement accesses for placements that appear in a subselect */
			placementAccessList =
				BuildPlacementSelectList(taskPlacement->groupId, relationShardList);
		}


		Assert(list_length(placementAccessList) == list_length(relationShardList));

		/* create placement access for the placement that we're modifying */
		placementModification = CreatePlacementAccess(taskPlacement, accessType);
		placementAccessList = lcons(placementModification, placementAccessList);

		/* get an appropriate connection for the DML statement */
		multiConnection = GetPlacementListConnection(connectionFlags, placementAccessList,
													 NULL);

		/*
		 * If we're expanding the set nodes that participate in the distributed
		 * transaction, conform to MultiShardCommitProtocol.
		 */
		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC &&
			InCoordinatedTransaction() &&
			XactModificationLevel == XACT_MODIFICATION_DATA)
		{
			RemoteTransaction *transaction = &multiConnection->remoteTransaction;
			if (transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				CoordinatedTransactionUse2PC();
			}
		}

		if (markCritical)
		{
			MarkRemoteTransactionCritical(multiConnection);
		}

		multiConnectionList = lappend(multiConnectionList, multiConnection);
	}

	/* and start transactions if applicable */
	RemoteTransactionsBeginIfNecessary(multiConnectionList);

	return multiConnectionList;
}


/*
 * ExecuteMultipleTasks executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used, stores them in a tuple store.
 *
 * If a task fails on one of the placements, the transaction rolls back.
 * Otherwise, the changes are committed using 2PC when the local transaction
 * commits.
 */
void
ExecuteMultipleTasks(CitusScanState *scanState, List *taskList,
					 bool isModificationQuery, bool expectResults)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	int64 affectedTupleCount = -1;

	/* can only support modifications right now */
	Assert(isModificationQuery);

	affectedTupleCount = ExecuteModifyTasks(taskList, expectResults, paramListInfo,
											scanState);

	executorState->es_processed = affectedTupleCount;
}


/*
 * ExecuteModifyTasksWithoutResults provides a wrapper around ExecuteModifyTasks
 * for calls that do not require results. In this case, the expectResults flag
 * is set to false and arguments related to result sets and query parameters are
 * NULL. This function is primarily intended to allow DDL and
 * master_modify_multiple_shards to use the router executor infrastructure.
 */
int64
ExecuteModifyTasksWithoutResults(List *taskList)
{
	return ExecuteModifyTasks(taskList, false, NULL, NULL);
}


/*
 * ExecuteModifyTasksSequentiallyWithoutResults calls ExecuteModifyTasksSequentially
 * and ignores the results.
 */
int64
ExecuteModifyTasksSequentiallyWithoutResults(List *taskList, RowModifyLevel modLevel)
{
	return ExecuteModifyTasksSequentially(NULL, taskList, modLevel, false);
}


/*
 * ExecuteModifyTasksSequentially basically calls ExecuteSingleModifyTask in
 * a loop in order to simulate sequential execution of a list of tasks. Useful
 * in cases where issuing commands in parallel before waiting for results could
 * result in deadlocks (such as foreign key creation to reference tables).
 *
 * The function returns the affectedTupleCount if applicable. Otherwise, the function
 * returns 0.
 */
int64
ExecuteModifyTasksSequentially(CitusScanState *scanState, List *taskList,
							   RowModifyLevel modLevel, bool hasReturning)
{
	ListCell *taskCell = NULL;
	bool multipleTasks = list_length(taskList) > 1;
	int64 affectedTupleCount = 0;
	bool alwaysThrowErrorOnFailure = false;
	bool taskListRequires2PC = TaskListRequires2PC(taskList);

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		alwaysThrowErrorOnFailure = true;

		/* we don't run CREATE INDEX CONCURRENTLY in a distributed transaction */
	}
	else if (IsMultiStatementTransaction() || multipleTasks || taskListRequires2PC)
	{
		BeginOrContinueCoordinatedTransaction();

		/*
		 * Although using two phase commit protocol is an independent decision than
		 * failing on any error, we prefer to couple them. Our motivation is that
		 * the failures are rare, and we prefer to avoid marking placements invalid
		 * in case of failures.
		 *
		 * For reference tables, we always set alwaysThrowErrorOnFailure since we
		 * absolutely want to avoid marking any placements invalid.
		 *
		 * We also cannot handle failures when there is RETURNING and there are more
		 * than one task to execute.
		 */
		if (taskListRequires2PC)
		{
			CoordinatedTransactionUse2PC();

			alwaysThrowErrorOnFailure = true;
		}
		else if (multipleTasks && hasReturning)
		{
			alwaysThrowErrorOnFailure = true;
		}
	}

	/* now that we've decided on the transaction status, execute the tasks */
	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		bool expectResults = (hasReturning || task->relationRowLockList != NIL);

		affectedTupleCount +=
			ExecuteSingleModifyTask(scanState, task, modLevel, alwaysThrowErrorOnFailure,
									expectResults);
	}

	return affectedTupleCount;
}


/*
 * ExecuteModifyTasks executes a list of tasks on remote nodes, and
 * optionally retrieves the results and stores them in a tuple store.
 *
 * If a task fails on one of the placements, the transaction rolls back.
 * Otherwise, the changes are committed using 2PC when the local transaction
 * commits.
 */
static int64
ExecuteModifyTasks(List *taskList, bool expectResults, ParamListInfo paramListInfo,
				   CitusScanState *scanState)
{
	int64 totalAffectedTupleCount = 0;
	ListCell *taskCell = NULL;
	Task *firstTask = NULL;
	int connectionFlags = 0;
	List *affectedTupleCountList = NIL;
	HTAB *shardConnectionHash = NULL;
	bool tasksPending = true;
	int placementIndex = 0;

	if (taskList == NIL)
	{
		return 0;
	}

	/*
	 * In multi shard modification, we expect that all tasks operates on the
	 * same relation, so it is enough to acquire a lock on the first task's
	 * anchor relation's partitions.
	 *
	 * For DDL commands, we already obtained the appropriate locks in
	 * ProcessUtility, so we only need to do this for DML commands.
	 */
	firstTask = (Task *) linitial(taskList);
	if (firstTask->taskType == MODIFY_TASK)
	{
		ShardInterval *firstShardInterval = NULL;

		firstShardInterval = LoadShardInterval(firstTask->anchorShardId);
		if (PartitionedTable(firstShardInterval->relationId))
		{
			LockPartitionRelations(firstShardInterval->relationId, RowExclusiveLock);
		}
	}

	/*
	 * Assign the distributed transaction id before trying to acquire the
	 * executor advisory locks. This is useful to show this backend in citus
	 * lock graphs (e.g., dump_global_wait_edges() and citus_lock_waits).
	 */
	BeginOrContinueCoordinatedTransaction();

	/*
	 * Ensure that there are no concurrent modifications on the same
	 * shards. In general, for DDL commands, we already obtained the
	 * appropriate locks in ProcessUtility. However, we still prefer to
	 * acquire the executor locks for DDLs specifically for TRUNCATE
	 * command on a partition table since AcquireExecutorMultiShardLocks()
	 * ensures that no concurrent modifications happens on the parent
	 * tables.
	 */
	AcquireExecutorMultiShardLocks(taskList);

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC ||
		firstTask->replicationModel == REPLICATION_MODEL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	RecordParallelRelationAccessForTaskList(taskList);

	if (firstTask->taskType == DDL_TASK || firstTask->taskType == VACUUM_ANALYZE_TASK)
	{
		connectionFlags = FOR_DDL;
	}
	else
	{
		connectionFlags = FOR_DML;
	}

	/* open connection to all relevant placements, if not already open */
	shardConnectionHash = OpenTransactionsForAllTasks(taskList, connectionFlags);

	XactModificationLevel = XACT_MODIFICATION_DATA;

	/* iterate over placements in rounds, to ensure in-order execution */
	while (tasksPending)
	{
		int taskIndex = 0;

		tasksPending = false;

		/* send command to all shard placements with the current index in parallel */
		foreach(taskCell, taskList)
		{
			Task *task = (Task *) lfirst(taskCell);
			int64 shardId = task->anchorShardId;
			char *queryString = task->queryString;
			bool shardConnectionsFound = false;
			ShardConnections *shardConnections = NULL;
			List *connectionList = NIL;
			MultiConnection *connection = NULL;
			bool queryOK = false;

			shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
													   &shardConnectionsFound);
			connectionList = shardConnections->connectionList;

			if (placementIndex >= list_length(connectionList))
			{
				/* no more active placements for this task */
				continue;
			}

			connection = (MultiConnection *) list_nth(connectionList, placementIndex);

			queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
			if (!queryOK)
			{
				UnclaimAllShardConnections(shardConnectionHash);
				ReportConnectionError(connection, ERROR);
			}
		}

		/* collects results from all relevant shard placements */
		foreach(taskCell, taskList)
		{
			Task *task = (Task *) lfirst(taskCell);
			int64 shardId = task->anchorShardId;
			bool shardConnectionsFound = false;
			ShardConnections *shardConnections = NULL;
			List *connectionList = NIL;
			MultiConnection *connection = NULL;
			int64 currentAffectedTupleCount = 0;
			bool alwaysThrowErrorOnFailure = true;
			bool queryOK PG_USED_FOR_ASSERTS_ONLY = false;

			/* abort in case of cancellation */
			CHECK_FOR_INTERRUPTS();

			shardConnections = GetShardHashConnections(shardConnectionHash, shardId,
													   &shardConnectionsFound);
			connectionList = shardConnections->connectionList;

			if (placementIndex >= list_length(connectionList))
			{
				/* no more active placements for this task */
				taskIndex++;
				continue;
			}

			connection = (MultiConnection *) list_nth(connectionList, placementIndex);

			/*
			 * if the task is a VACUUM or ANALYZE, we set CitusNoticeLogLevel to INFO
			 * to see the logs in console.
			 */
			if (task->taskType == VACUUM_ANALYZE_TASK)
			{
				SetCitusNoticeLevel(INFO);
			}

			PG_TRY();
			{
				/*
				 * If caller is interested, store query results the first time
				 * through. The output of the query's execution on other shards is
				 * discarded if we run there (because it's a modification query).
				 */
				if (placementIndex == 0 && expectResults)
				{
					Assert(scanState != NULL);

					queryOK = StoreQueryResult(scanState, connection,
											   alwaysThrowErrorOnFailure,
											   &currentAffectedTupleCount, NULL);
				}
				else
				{
					queryOK = ConsumeQueryResult(connection, alwaysThrowErrorOnFailure,
												 &currentAffectedTupleCount);
				}
			}
			PG_CATCH();
			{
				/*
				 * We might be able to recover from errors with ROLLBACK TO SAVEPOINT,
				 * so unclaim the connections before throwing errors.
				 */
				UnclaimAllShardConnections(shardConnectionHash);
				PG_RE_THROW();
			}
			PG_END_TRY();

			/* We error out if the worker fails to return a result for the query. */
			if (!queryOK)
			{
				UnclaimAllShardConnections(shardConnectionHash);
				ReportConnectionError(connection, ERROR);
			}

			if (placementIndex == 0)
			{
				totalAffectedTupleCount += currentAffectedTupleCount;

				/* keep track of the initial affected tuple count */
				affectedTupleCountList = lappend_int(affectedTupleCountList,
													 currentAffectedTupleCount);
			}
			else
			{
				/* warn the user if shard placements have diverged */
				int64 previousAffectedTupleCount = list_nth_int(affectedTupleCountList,
																taskIndex);

				if (currentAffectedTupleCount != previousAffectedTupleCount)
				{
					ereport(WARNING,
							(errmsg("modified "INT64_FORMAT " tuples of shard "
									UINT64_FORMAT ", but expected to modify "INT64_FORMAT,
									currentAffectedTupleCount, shardId,
									previousAffectedTupleCount),
							 errdetail("modified placement on %s:%d",
									   connection->hostname, connection->port)));
				}
			}

			if (!tasksPending && placementIndex + 1 < list_length(connectionList))
			{
				/* more tasks to be done after thise one */
				tasksPending = true;
			}

			taskIndex++;
		}

		placementIndex++;
	}

	/* we should set the log level back to its default value since the task is done */
	UnsetCitusNoticeLevel();

	UnclaimAllShardConnections(shardConnectionHash);

	CHECK_FOR_INTERRUPTS();

	return totalAffectedTupleCount;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(MultiConnection *connection, char *query,
						 ParamListInfo paramListInfo)
{
	int querySent = 0;
	int singleRowMode = 0;

	if (paramListInfo != NULL)
	{
		int parameterCount = paramListInfo->numParams;
		Oid *parameterTypes = NULL;
		const char **parameterValues = NULL;

		/* force evaluation of bound params */
		paramListInfo = copyParamList(paramListInfo);

		ExtractParametersFromParamListInfo(paramListInfo, &parameterTypes,
										   &parameterValues);

		querySent = SendRemoteCommandParams(connection, query, parameterCount,
											parameterTypes, parameterValues);
	}
	else
	{
		querySent = SendRemoteCommand(connection, query);
	}

	if (querySent == 0)
	{
		const bool raiseIfTransactionIsCritical = true;

		HandleRemoteTransactionConnectionError(connection, raiseIfTransactionIsCritical);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection->pgConn);
	if (singleRowMode == 0)
	{
		const bool raiseIfTransactionIsCritical = true;

		HandleRemoteTransactionConnectionError(connection, raiseIfTransactionIsCritical);
		return false;
	}

	return true;
}


/*
 * ExtractParametersFromParamListInfo extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 */
void
ExtractParametersFromParamListInfo(ParamListInfo paramListInfo, Oid **parameterTypes,
								   const char ***parameterValues)
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
		if (parameterData->ptype >= FirstNormalObjectId)
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


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results, and stores them in the a newly created
 * tuple-store. If the function can't receive query results, it returns
 * false. Note that this function assumes the query has already been sent on
 * the connection.
 */
static bool
StoreQueryResult(CitusScanState *scanState, MultiConnection *connection,
				 bool alwaysThrowErrorOnFailure, int64 *rows,
				 DistributedExecutionStats *executionStats)
{
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	List *targetList = scanState->customScanState.ss.ps.plan->targetlist;
	uint32 expectedColumnCount = ExecCleanTargetListLength(targetList);
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	Tuplestorestate *tupleStore = NULL;
	bool randomAccess = true;
	bool interTransactions = false;
	bool commandFailed = false;
	MemoryContext ioContext = AllocSetContextCreateExtended(CurrentMemoryContext,
															"StoreQueryResult",
															ALLOCSET_DEFAULT_MINSIZE,
															ALLOCSET_DEFAULT_INITSIZE,
															ALLOCSET_DEFAULT_MAXSIZE);

	*rows = 0;

	if (scanState->tuplestorestate == NULL)
	{
		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
	}
	else if (!alwaysThrowErrorOnFailure)
	{
		/* might have failed query execution on another placement before */
		tuplestore_clear(scanState->tuplestorestate);
	}

	tupleStore = scanState->tuplestorestate;

	for (;;)
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;
		bool doRaiseInterrupts = true;

		PGresult *result = GetRemoteCommandResult(connection, doRaiseInterrupts);
		if (result == NULL)
		{
			break;
		}

		resultStatus = PQresultStatus(result);
		if ((resultStatus != PGRES_SINGLE_TUPLE) && (resultStatus != PGRES_TUPLES_OK))
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool isConstraintViolation = false;

			/*
			 * Mark transaction as failed, but don't throw an error. This allows us
			 * to give a more meaningful error message below.
			 */
			MarkRemoteTransactionFailed(connection, false);

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || alwaysThrowErrorOnFailure ||
				IsRemoteTransactionCritical(connection))
			{
				ReportResultError(connection, result, ERROR);
			}
			else
			{
				ReportResultError(connection, result, WARNING);
			}

			PQclear(result);

			commandFailed = true;

			/* an error happened, there is nothing we can do more */
			if (resultStatus == PGRES_FATAL_ERROR)
			{
				break;
			}

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);
		Assert(columnCount == expectedColumnCount);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
					if (SubPlanLevel > 0 && executionStats)
					{
						int rowLength = PQgetlength(result, rowIndex, columnIndex);
						executionStats->totalIntermediateResultSize += rowLength;
					}
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);
			(*rows)++;
		}

		PQclear(result);
	}

	pfree(columnArray);

	return !commandFailed;
}


/*
 * ConsumeQueryResult gets a query result from a connection, counting the rows
 * and checking for errors, but otherwise discarding potentially returned
 * rows.  Returns true if a non-error result has been returned, false if there
 * has been an error.
 */
static bool
ConsumeQueryResult(MultiConnection *connection, bool alwaysThrowErrorOnFailure,
				   int64 *rows)
{
	bool commandFailed = false;
	bool gotResponse = false;

	*rows = 0;

	/*
	 * Due to single row mode we have to do multiple GetRemoteCommandResult()
	 * to finish processing of this query, even without RETURNING. For
	 * single-row mode we have to loop until all rows are consumed.
	 */
	while (true)
	{
		const bool doRaiseInterrupts = true;
		PGresult *result = GetRemoteCommandResult(connection, doRaiseInterrupts);
		ExecStatusType status = PGRES_COMMAND_OK;

		if (result == NULL)
		{
			break;
		}

		status = PQresultStatus(result);

		if (status != PGRES_COMMAND_OK &&
			status != PGRES_SINGLE_TUPLE &&
			status != PGRES_TUPLES_OK)
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool isConstraintViolation = false;

			/*
			 * Mark transaction as failed, but don't throw an error even if the
			 * transaction is critical. This allows us to give a more meaningful
			 * error message below.
			 */
			MarkRemoteTransactionFailed(connection, false);

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || alwaysThrowErrorOnFailure ||
				IsRemoteTransactionCritical(connection))
			{
				ReportResultError(connection, result, ERROR);
			}
			else
			{
				ReportResultError(connection, result, WARNING);
			}

			PQclear(result);

			commandFailed = true;

			/* an error happened, there is nothing we can do more */
			if (status == PGRES_FATAL_ERROR)
			{
				break;
			}

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		if (status == PGRES_COMMAND_OK)
		{
			char *currentAffectedTupleString = PQcmdTuples(result);
			int64 currentAffectedTupleCount = 0;

			if (*currentAffectedTupleString != '\0')
			{
				scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
				Assert(currentAffectedTupleCount >= 0);
			}

			*rows += currentAffectedTupleCount;
		}
		else
		{
			*rows += PQntuples(result);
		}

		PQclear(result);
		gotResponse = true;
	}

	return gotResponse && !commandFailed;
}
