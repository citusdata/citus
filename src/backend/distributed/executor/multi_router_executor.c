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
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/placement_connection.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/resource_lock.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
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
bool EnableDeadlockPrevention = true;

/* functions needed during run phase */
static void ReacquireMetadataLocks(List *taskList);
static void AssignInsertTaskShardId(Query *jobQuery, List *taskList);
static ShardPlacementAccess * CreatePlacementAccess(ShardPlacement *placement,
													ShardPlacementAccessType accessType);
static void ExecuteSingleModifyTask(CitusScanState *scanState, Task *task,
									bool expectResults);
static void ExecuteSingleSelectTask(CitusScanState *scanState, Task *task);
static List * GetModifyConnections(List *taskPlacementList, bool markCritical,
								   bool startedInTransaction);
static void ExecuteMultipleTasks(CitusScanState *scanState, List *taskList,
								 bool isModificationQuery, bool expectResults);
static int64 ExecuteModifyTasks(List *taskList, bool expectResults,
								ParamListInfo paramListInfo, CitusScanState *scanState);
static void AcquireExecutorShardLock(Task *task, CmdType commandType);
static void AcquireExecutorMultiShardLocks(List *taskList);
static bool RequiresConsistentSnapshot(Task *task);
static void ExtractParametersFromParamListInfo(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static bool SendQueryInSingleRowMode(MultiConnection *connection, char *query,
									 ParamListInfo paramListInfo);
static bool StoreQueryResult(CitusScanState *scanState, MultiConnection *connection,
							 bool failOnError, int64 *rows);
static bool ConsumeQueryResult(MultiConnection *connection, bool failOnError,
							   int64 *rows);


/*
 * ReacquireMetadataLocks re-acquires the metadata locks that are normally
 * acquired during planning.
 *
 * If we are executing a prepared statement, then planning might have
 * happened in a separate transaction and advisory locks are no longer
 * held. If a shard is currently being repaired/copied/moved, then
 * obtaining the locks will fail and this function throws an error to
 * prevent executing a stale plan.
 *
 * If we are executing a non-prepared statement or planning happened in
 * the same transaction, then we already have the locks and obtain them
 * again here. Since we always release these locks at the end of the
 * transaction, this is effectively a no-op.
 */
static void
ReacquireMetadataLocks(List *taskList)
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

		/*
		 * Only obtain metadata locks for modifications to allow reads to
		 * proceed during shard copy.
		 */
		if (task->taskType == MODIFY_TASK &&
			!TryLockShardDistributionMetadata(task->anchorShardId, ShareLock))
		{
			/*
			 * We could error out immediately to give quick feedback to the
			 * client, but this might complicate flow control and our default
			 * behaviour during shard copy is to block.
			 *
			 * Block until the lock becomes available such that the next command
			 * will likely succeed and use the serialization failure error code
			 * to signal to the client that it should retry the current command.
			 */
			LockShardDistributionMetadata(task->anchorShardId, ShareLock);

			ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							errmsg("prepared modifications cannot be executed on "
								   "a shard while it is being copied")));
		}
	}
}


/*
 * AcquireExecutorShardLock acquires a lock on the shard for the given task and
 * command type if necessary to avoid divergence between multiple replicas of
 * the same shard. No lock is obtained when there is only one replica.
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
static void
AcquireExecutorShardLock(Task *task, CmdType commandType)
{
	LOCKMODE lockMode = NoLock;
	int64 shardId = task->anchorShardId;

	if (commandType == CMD_SELECT || list_length(task->taskPlacementList) == 1)
	{
		/*
		 * The executor shard lock is used to maintain consistency between
		 * replicas and therefore no lock is required for read-only queries
		 * or in general when there is only one replica.
		 */

		lockMode = NoLock;
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
	else if (task->upsertQuery || commandType == CMD_UPDATE || commandType == CMD_DELETE)
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
	else if (commandType == CMD_INSERT)
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
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	if (shardId != INVALID_SHARD_ID && lockMode != NoLock)
	{
		LockShardResource(shardId, lockMode);
	}

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
static void
AcquireExecutorMultiShardLocks(List *taskList)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		LOCKMODE lockMode = NoLock;

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
			 */

			lockMode = ShareUpdateExclusiveLock;
		}
		else
		{
			/*
			 * When there is replication, prevent all concurrent writes to the same
			 * shards to ensure the writes are ordered.
			 */

			lockMode = ExclusiveLock;
		}

		LockShardResource(task->anchorShardId, lockMode);

		/*
		 * If the task has a subselect, then we may need to lock the shards from which
		 * the query selects as well to prevent the subselects from seeing different
		 * results on different replicas. In particular this prevents INSERT..SELECT
		 * commands from having different effects on different placements.
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
 * the necessary locks to ensure that a subquery in the INSERT ... SELECT
 * query returns the same output for all task placements.
 */
static bool
RequiresConsistentSnapshot(Task *task)
{
	bool requiresIsolation = false;

	if (!task->insertSelectQuery)
	{
		/*
		 * Only INSERT/SELECT commands currently require SELECT isolation.
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
	MultiPlan *multiPlan = scanState->multiPlan;
	Job *workerJob = multiPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;
	List *taskList = workerJob->taskList;
	bool deferredPruning = workerJob->deferredPruning;

	if (workerJob->requiresMasterEvaluation)
	{
		PlanState *planState = &(scanState->customScanState.ss.ps);

		ExecuteMasterEvaluableFunctions(jobQuery, planState);

		if (deferredPruning)
		{
			AssignInsertTaskShardId(jobQuery, taskList);
		}

		RebuildQueryStrings(jobQuery, taskList);
	}

	/*
	 * If we are executing a prepared statement, then we may not yet have obtained
	 * the metadata locks in this transaction. To prevent a concurrent shard copy,
	 * we re-obtain them here or error out if a shard copy has already started.
	 *
	 * If a shard copy finishes in between fetching a plan from cache and
	 * re-acquiring the locks, then we might still run a stale plan, which could
	 * cause shard placements to diverge. To minimize this window, we take the
	 * locks as early as possible.
	 */
	ReacquireMetadataLocks(taskList);

	/*
	 * If we deferred shard pruning to the executor, then we still need to assign
	 * shard placements. We do this after acquiring the metadata locks to ensure
	 * we can't get stale metadata. At some point, we may want to load all
	 * placement metadata here.
	 */
	if (deferredPruning)
	{
		/* modify tasks are always assigned using first-replica policy */
		workerJob->taskList = FirstReplicaAssignTaskList(taskList);
	}
}


/*
 * AssignInsertTaskShardId performs shard pruning for an insert and sets
 * anchorShardId accordingly.
 */
static void
AssignInsertTaskShardId(Query *jobQuery, List *taskList)
{
	ShardInterval *shardInterval = NULL;
	Task *insertTask = NULL;
	DeferredErrorMessage *planningError = NULL;

	Assert(jobQuery->commandType == CMD_INSERT);

	/*
	 * We skipped shard pruning in the planner because the partition
	 * column contained an expression. Perform shard pruning now.
	 */
	shardInterval = FindShardForInsert(jobQuery, &planningError);
	if (planningError != NULL)
	{
		RaiseDeferredError(planningError, ERROR);
	}
	else if (shardInterval == NULL)
	{
		/* expression could not be evaluated */
		ereport(ERROR, (errmsg("parameters in the partition column are not "
							   "allowed")));
	}

	/* assign a shard ID to the task */
	insertTask = (Task *) linitial(taskList);
	insertTask->anchorShardId = shardInterval->shardId;
}


/*
 * RouterSingleModifyExecScan executes a single modification query on a
 * distributed plan and returns results if there is any.
 */
TupleTableSlot *
RouterSingleModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		bool hasReturning = multiPlan->hasReturning;
		Job *workerJob = multiPlan->workerJob;
		List *taskList = workerJob->taskList;
		Task *task = (Task *) linitial(taskList);

		ExecuteSingleModifyTask(scanState, task, hasReturning);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * RouterMultiModifyExecScan executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used, stores them in custom scan's tuple store.
 * Then, it returns tuples one by one from this tuple store.
 */
TupleTableSlot *
RouterMultiModifyExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;
		List *taskList = workerJob->taskList;
		bool hasReturning = multiPlan->hasReturning;
		bool isModificationQuery = true;

		ExecuteMultipleTasks(scanState, taskList, isModificationQuery, hasReturning);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
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
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;
		List *taskList = workerJob->taskList;
		Task *task = (Task *) linitial(taskList);

		ExecuteSingleSelectTask(scanState, task);

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
	ParamListInfo paramListInfo =
		scanState->customScanState.ss.ps.state->es_param_list_info;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;
	char *queryString = task->queryString;
	List *relationShardList = task->relationShardList;

	if (XactModificationLevel == XACT_MODIFICATION_MULTI_SHARD)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("single-shard query may not appear in transaction blocks "
							   "which contain multi-shard data modifications")));
	}

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
		int connectionFlags = SESSION_LIFESPAN;
		List *placementAccessList = NIL;
		MultiConnection *connection = NULL;

		if (list_length(relationShardList) > 0)
		{
			placementAccessList = BuildPlacementSelectList(taskPlacement->nodeName,
														   taskPlacement->nodePort,
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

		connection = GetPlacementListConnection(connectionFlags, placementAccessList,
												NULL);

		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{
			continue;
		}

		queryOK = StoreQueryResult(scanState, connection, dontFailOnError,
								   &currentAffectedTupleCount);
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
 * GetPlacementListConnection.
 */
List *
BuildPlacementSelectList(char *nodeName, int nodePort, List *relationShardList)
{
	ListCell *relationShardCell = NULL;
	List *placementAccessList = NIL;

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
		ShardPlacement *placement = NULL;
		ShardPlacementAccess *placementAccess = NULL;

		placement = FindShardPlacementOnNode(nodeName, nodePort, relationShard->shardId);
		if (placement == NULL)
		{
			ereport(ERROR, (errmsg("no active placement of shard %ld found on node "
								   "%s:%d",
								   relationShard->shardId, nodeName, nodePort)));
		}

		placementAccess = CreatePlacementAccess(placement, PLACEMENT_ACCESS_SELECT);
		placementAccessList = lappend(placementAccessList, placementAccess);
	}

	return placementAccessList;
}


/*
 * CreatePlacementAccess returns a new ShardPlacementAccess for the given placement
 * and access type.
 */
static ShardPlacementAccess *
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
 * results and stores them, if RETURNING is used, in a tuple store.
 *
 * If the task fails on one of the placements, the function reraises the
 * remote error (constraint violation in DML), marks the affected placement as
 * invalid (other error on some placements, via the placement connection
 * framework), or errors out (failed on all placements).
 */
static void
ExecuteSingleModifyTask(CitusScanState *scanState, Task *task, bool expectResults)
{
	CmdType operation = scanState->multiPlan->operation;
	EState *executorState = scanState->customScanState.ss.ps.state;
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	List *taskPlacementList = task->taskPlacementList;
	List *connectionList = NIL;
	ListCell *taskPlacementCell = NULL;
	ListCell *connectionCell = NULL;
	int64 affectedTupleCount = -1;
	bool resultsOK = false;
	bool gotResults = false;

	char *queryString = task->queryString;
	bool taskRequiresTwoPhaseCommit = (task->replicationModel == REPLICATION_MODEL_2PC);
	bool startedInTransaction =
		InCoordinatedTransaction() && XactModificationLevel == XACT_MODIFICATION_DATA;

	if (XactModificationLevel == XACT_MODIFICATION_MULTI_SHARD)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("single-shard DML commands must not appear in "
							   "transaction blocks which contain multi-shard data "
							   "modifications")));
	}

	/*
	 * Modifications for reference tables are always done using 2PC. First
	 * ensure that distributed transaction is started. Then force the
	 * transaction manager to use 2PC while running the task on the
	 * placements.
	 */
	if (taskRequiresTwoPhaseCommit)
	{
		BeginOrContinueCoordinatedTransaction();
		CoordinatedTransactionUse2PC();
	}

	/*
	 * We could naturally handle function-based transactions (i.e. those using
	 * PL/pgSQL or similar) by checking the type of queryDesc->dest, but some
	 * customers already use functions that touch multiple shards from within
	 * a function, so we'll ignore functions for now.
	 */
	if (IsTransactionBlock())
	{
		BeginOrContinueCoordinatedTransaction();
	}

	/*
	 * Get connections required to execute task. This will, if necessary,
	 * establish the connection, mark as critical (when modifying reference
	 * table) and start a transaction (when in a transaction).
	 */
	connectionList = GetModifyConnections(taskPlacementList,
										  taskRequiresTwoPhaseCommit,
										  startedInTransaction);

	/* prevent replicas of the same shard from diverging */
	AcquireExecutorShardLock(task, operation);

	/* try to execute modification on all placements */
	forboth(taskPlacementCell, taskPlacementList, connectionCell, connectionList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		bool queryOK = false;
		bool failOnError = false;
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
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{
			continue;
		}

		/* if we're running a 2PC, the query should fail on error */
		failOnError = taskRequiresTwoPhaseCommit;

		/*
		 * If caller is interested, store query results the first time
		 * through. The output of the query's execution on other shards is
		 * discarded if we run there (because it's a modification query).
		 */
		if (!gotResults && expectResults)
		{
			queryOK = StoreQueryResult(scanState, connection, failOnError,
									   &currentAffectedTupleCount);
		}
		else
		{
			queryOK = ConsumeQueryResult(connection, failOnError,
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
	}

	/* if all placements failed, error out */
	if (!resultsOK)
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* if some placements failed, ensure future statements don't access them */
	MarkFailedShardPlacements();

	executorState->es_processed = affectedTupleCount;

	if (IsTransactionBlock())
	{
		XactModificationLevel = XACT_MODIFICATION_DATA;
	}
}


/*
 * GetModifyConnections returns the list of connections required to execute
 * modify commands on the placements in tasPlacementList.  If necessary remote
 * transactions are started.
 *
 * If markCritical is true remote transactions are marked as critical. If
 * noNewTransactions is true, this function errors out if there's no
 * transaction in progress.
 */
static List *
GetModifyConnections(List *taskPlacementList, bool markCritical, bool noNewTransactions)
{
	ListCell *taskPlacementCell = NULL;
	List *multiConnectionList = NIL;

	/* first initiate connection establishment for all necessary connections */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		int connectionFlags = SESSION_LIFESPAN | FOR_DML;
		MultiConnection *multiConnection = NULL;

		/*
		 * FIXME: It's not actually correct to use only one shard placement
		 * here for router queries involving multiple relations. We should
		 * check that this connection is the only modifying one associated
		 * with all the involved shards.
		 */
		multiConnection = StartPlacementConnection(connectionFlags, taskPlacement, NULL);

		/*
		 * If already in a transaction, disallow expanding set of remote
		 * transactions. That prevents some forms of distributed deadlocks.
		 */
		if (noNewTransactions)
		{
			RemoteTransaction *transaction = &multiConnection->remoteTransaction;

			if (EnableDeadlockPrevention &&
				transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
								errmsg("no transaction participant matches %s:%d",
									   taskPlacement->nodeName, taskPlacement->nodePort),
								errdetail("Transactions which modify distributed tables "
										  "may only target nodes affected by the "
										  "modification command which began the transaction.")));
			}
		}

		if (markCritical)
		{
			MarkRemoteTransactionCritical(multiConnection);
		}

		multiConnectionList = lappend(multiConnectionList, multiConnection);
	}

	/* then finish in parallel */
	FinishConnectionListEstablishment(multiConnectionList);

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
static void
ExecuteMultipleTasks(CitusScanState *scanState, List *taskList,
					 bool isModificationQuery, bool expectResults)
{
	EState *executorState = scanState->customScanState.ss.ps.state;
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
 * ExecuteTasksSequentiallyWithoutResults basically calls ExecuteModifyTasks in
 * a loop in order to simulate sequential execution of a list of tasks. Useful
 * in cases where issuing commands in parallel before waiting for results could
 * result in deadlocks (such as CREATE INDEX CONCURRENTLY).
 */
void
ExecuteTasksSequentiallyWithoutResults(List *taskList)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *singleTask = list_make1(task);

		ExecuteModifyTasksWithoutResults(singleTask);
	}
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

	if (XactModificationLevel == XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("multi-shard data modifications must not appear in "
							   "transaction blocks which contain single-shard DML "
							   "commands")));
	}

	/* ensure that there are no concurrent modifications on the same shards */
	AcquireExecutorMultiShardLocks(taskList);

	BeginOrContinueCoordinatedTransaction();

	firstTask = (Task *) linitial(taskList);

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC ||
		firstTask->replicationModel == REPLICATION_MODEL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	if (firstTask->taskType == DDL_TASK)
	{
		connectionFlags = FOR_DDL;
	}
	else
	{
		connectionFlags = FOR_DML;
	}

	/* open connection to all relevant placements, if not already open */
	shardConnectionHash = OpenTransactionsForAllTasks(taskList, connectionFlags);

	XactModificationLevel = XACT_MODIFICATION_MULTI_SHARD;

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
			bool failOnError = true;
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
			 * If caller is interested, store query results the first time
			 * through. The output of the query's execution on other shards is
			 * discarded if we run there (because it's a modification query).
			 */
			if (placementIndex == 0 && expectResults)
			{
				Assert(scanState != NULL);

				queryOK = StoreQueryResult(scanState, connection, failOnError,
										   &currentAffectedTupleCount);
			}
			else
			{
				queryOK = ConsumeQueryResult(connection, failOnError,
											 &currentAffectedTupleCount);
			}

			/* should have rolled back on error */
			Assert(queryOK);

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
		MarkRemoteTransactionFailed(connection, false);
		ReportConnectionError(connection, WARNING);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection->pgConn);
	if (singleRowMode == 0)
	{
		MarkRemoteTransactionFailed(connection, false);
		ReportConnectionError(connection, WARNING);
		return false;
	}

	return true;
}


/*
 * ExtractParametersFromParamListInfo extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 */
static void
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
				 bool failOnError, int64 *rows)
{
	TupleDesc tupleDescriptor =
		scanState->customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	List *targetList = scanState->customScanState.ss.ps.plan->targetlist;
	uint32 expectedColumnCount = ExecCleanTargetListLength(targetList);
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	Tuplestorestate *tupleStore = NULL;
	bool randomAccess = true;
	bool interTransactions = false;
	bool commandFailed = false;
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
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
	else if (!failOnError)
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

			MarkRemoteTransactionFailed(connection, false);

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || failOnError)
			{
				ReportResultError(connection, result, ERROR);
			}
			else
			{
				ReportResultError(connection, result, WARNING);
			}

			PQclear(result);

			commandFailed = true;

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
ConsumeQueryResult(MultiConnection *connection, bool failOnError, int64 *rows)
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

			MarkRemoteTransactionFailed(connection, false);

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || failOnError)
			{
				ReportResultError(connection, result, ERROR);
			}
			else
			{
				ReportResultError(connection, result, WARNING);
			}

			PQclear(result);

			commandFailed = true;

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
