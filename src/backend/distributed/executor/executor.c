/*-------------------------------------------------------------------------
 *
 * executor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/connection_management.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "lib/ilist.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/int8.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/*
 * DistributedExecution represents the execution of a distributed query
 * plan.
 */
typedef struct DistributedExecution
{
	/* the corresponding distributed plan's operation */
	CmdType operation;

	List *tasksToExecute;

	/* the corresponding distributed plan has RETURNING */
	bool hasReturning;

	/* Parameters for parameterized plans. Can be NULL. */
	ParamListInfo paramListInfo;

	/* Tuple descriptor and destination for result. Can be NULL. */
	TupleDesc tupleDescriptor;
	Tuplestorestate *tupleStore;


	/* list of workers involved in the execution */
	List *workerList;

	/* list of all connections used for distributed execution */
	List *sessionList;

	/*
	 * Flag to indiciate that the set of connections we are interested
	 * in has changed and waitEventSet needs to be rebuilt.
	 */
	bool connectionSetChanged;

	/*
	 * Flag to indiciate that the set of wait events we are interested
	 * in might have changed and waitEventSet needs to be updated.
	 *
	 * Note that we set this flag whenever we assign a value to waitFlags,
	 * but we don't check that the waitFlags is actually different from the
	 * previous value. So we might have some false positives for this flag,
	 * which is OK, because in this case ModifyWaitEvent() is noop.
	 */
	bool waitFlagsChanged;

	/*
	 * WaitEventSet used for waiting for I/O events.
	 *
	 * This could also be local to RunDistributedExecution(), but in that case
	 * we had to mark it as "volatile" to avoid PG_TRY()/PG_CATCH() issues, and
	 * cast it to non-volatile when doing WaitEventSetFree(). We thought that
	 * would make code a bit harder to read than making this non-local, so we
	 * move it here. See comments for PG_TRY() in postgres/src/include/elog.h
	 * and "man 3 siglongjmp" for more context.
	 */
	WaitEventSet *waitEventSet;

	/*
	 * The number of connections we aim to open per worker.
	 *
	 * If there are no more tasks to assigned, the actual number may be lower.
	 * If there are already more connections, the actual number may be higher.
	 */
	int targetPoolSize;

	/* total number of tasks to execute */
	int totalTaskCount;

	/* number of tasks that still need to be executed */
	int unfinishedTaskCount;

	/*
	 * Flag to indicate whether throwing errors on cancellation is
	 * allowed.
	 */
	bool raiseInterrupts;

	/*
	 * Flag to indicate whether the query is running in a distributed
	 * transaction.
	 */
	bool isTransaction;

	/* indicates whether distributed execution has failed */
	bool failed;

	/* set to true when we prefer to bail out early */
	bool errorOnAnyFailure;

	/*
	 * For SELECT commands or INSERT/UPDATE/DELETE commands with RETURNING,
	 * the total number of rows received from the workers. For
	 * INSERT/UPDATE/DELETE commands without RETURNING, the total number of
	 * tuples modified.
	 *
	 * Note that for replicated tables (e.g., reference tables), we only consider
	 * a single replica's rows that are processed.
	 */
	uint64 rowsProcessed;

	/* statistics on distributed execution */
	DistributedExecutionStats *executionStats;
} DistributedExecution;

/*
 * WorkerPool represents a pool of sessions on the same worker.
 */
typedef struct WorkerPool
{
	/* distributed execution in which the worker participates */
	DistributedExecution *distributedExecution;

	/* worker node on which we have a pool of sessions */
	WorkerNode *node;

	/* all sessions on the worker that are part of the current execution */
	List *sessionList;

	/* number of connections that were established */
	int activeConnectionCount;

	/*
	 * Keep track of how many connections are ready for execution, in
	 * order to (efficiently) know whether more connections to the worker
	 * are needed.
	 */
	int idleConnectionCount;

	/* number of connections that did not send a command */
	int unusedConnectionCount;

	/* number of failed connections */
	int failedConnectionCount;

	/*
	 * Placement executions destined for worker node, but not assigned to any
	 * connection and not yet ready to start (depends on other placement
	 * executions).
	 */
	dlist_head pendingTaskQueue;

	/*
	 * Placement executions destined for worker node, but not assigned to any
	 * connection and not ready to start.
	 */
	dlist_head readyTaskQueue;
	int readyTaskCount;

	/*
	 * We keep this for enforcing the connection timeouts. In our definition, a pool
	 * starts when the first connection establishment starts.
	 */
	TimestampTz poolStartTime;

	/*
	 * This is only set in WorkerPoolFailed() function. Once a pool fails, we do not
	 * use it anymore.
	 */
	bool failed;
} WorkerPool;

struct TaskPlacementExecution;

/*
 * WorkerSession represents a session on a worker that can execute tasks
 * (sequentially).
 */
typedef struct WorkerSession
{
	/* only useful for debugging */
	uint64 sessionId;

	/* worker pool of which this session is part */
	WorkerPool *workerPool;

	/* connection over which the session is established */
	MultiConnection *connection;

	/* tasks that need to be executed on this connection, but are not ready to start  */
	dlist_head pendingTaskQueue;

	/* tasks that need to be executed on this connection and are ready to start */
	dlist_head readyTaskQueue;

	/* task the worker should work on or NULL */
	struct TaskPlacementExecution *currentTask;

	/*
	 * The number of commands sent to the worker over the session. Excludes
	 * distributed transaction related commands such as BEGIN/COMMIT etc.
	 */
	uint64 commandsSent;
} WorkerSession;


struct TaskPlacementExecution;


/*
 * TaskExecutionState indicates whether or not a command on a shard
 * has finished, or whether it has failed.
 */
typedef enum TaskExecutionState
{
	TASK_EXECUTION_NOT_FINISHED,
	TASK_EXECUTION_FINISHED,
	TASK_EXECUTION_FAILED
} TaskExecutionState;

/*
 * PlacementExecutionOrder indicates whether a command should be executed
 * on any replica, on all replicas sequentially (in order), or on all
 * replicas in parallel.
 */
typedef enum PlacementExecutionOrder
{
	EXECUTION_ORDER_ANY,
	EXECUTION_ORDER_SEQUENTIAL,
	EXECUTION_ORDER_PARALLEL,
} PlacementExecutionOrder;


/*
 * ShardCommandExecution represents an execution of a command on a shard
 * that may (need to) run across multiple placements.
 */
typedef struct ShardCommandExecution
{
	/* description of the task */
	Task *task;

	/* order in which the command should be replicated on replicas */
	PlacementExecutionOrder executionOrder;

	/* executions of the command on the placements of the shard */
	struct TaskPlacementExecution **placementExecutions;
	int placementExecutionCount;

	/* whether we expect results to come back */
	bool expectResults;

	/*
	 * RETURNING results from other shard placements can be ignored
	 * after we got results from the first placements.
	 */
	bool gotResults;

	TaskExecutionState executionState;
} ShardCommandExecution;

/*
 * TaskPlacementExecutionState indicates whether a command is running
 * on a shard placement, or finished or failed.
 */
typedef enum TaskPlacementExecutionState
{
	PLACEMENT_EXECUTION_NOT_READY,
	PLACEMENT_EXECUTION_READY,
	PLACEMENT_EXECUTION_RUNNING,
	PLACEMENT_EXECUTION_FINISHED,
	PLACEMENT_EXECUTION_FAILED
} TaskPlacementExecutionState;

/*
 * TaskPlacementExecution represents the an execution of a command
 * on a shard placement.
 */
typedef struct TaskPlacementExecution
{
	/* shard command execution of which this placement execution is part */
	ShardCommandExecution *shardCommandExecution;

	/* shard placement on which this command runs */
	ShardPlacement *shardPlacement;

	/* state of the execution of the command on the placement */
	TaskPlacementExecutionState executionState;

	/* worker pool on which the placement needs to be executed */
	WorkerPool *workerPool;

	/* the session the placement execution is assigned to or NULL */
	WorkerSession *assignedSession;

	/* membership in assigned task queue of a particular session */
	dlist_node sessionPendingQueueNode;

	/* membership in ready-to-start assigned task queue of a particular session */
	dlist_node sessionReadyQueueNode;

	/* membership in assigned task queue of worker */
	dlist_node workerPendingQueueNode;

	/* membership in ready-to-start task queue of worker */
	dlist_node workerReadyQueueNode;

	/* index in array of placement executions in a ShardCommandExecution */
	int placementExecutionIndex;
} TaskPlacementExecution;


/* GUC, determining whether Citus opens 1 connection per task */
bool ForceMaxQueryParallelization = false;


/* local functions */
static DistributedExecution * CreateDistributedExecution(CmdType operation,
														 List *taskList,
														 bool hasReturning,
														 ParamListInfo paramListInfo,
														 TupleDesc tupleDescriptor,
														 Tuplestorestate *tupleStore,
														 int targetPoolSize);
static void StartDistributedExecution(DistributedExecution *execution);
static void RunDistributedExecution(DistributedExecution *execution);
static bool ShouldRunTasksSequentially(List *taskList);
static void SequentialRunDistributedExecution(DistributedExecution *execution);

static void FinishDistributedExecution(DistributedExecution *execution);
static void CleanUpSessions(DistributedExecution *execution);

static void LockPartitionsForDistributedPlan(DistributedPlan *distributedPlan);
static bool DistributedExecutionModifiesDatabase(DistributedExecution *execution);
static bool DistributedPlanModifiesDatabase(DistributedPlan *plan);
static bool TaskListModifiesDatabase(CmdType operation, List *taskList);
static bool DistributedExecutionRequiresRollback(DistributedExecution *execution);
static void AssignTasksToConnections(DistributedExecution *execution);
static void UnclaimAllSessionConnections(List *sessionList);
static bool UseConnectionPerPlacement(void);
static PlacementExecutionOrder ExecutionOrderForTask(CmdType operation, Task *task);
static WorkerPool * FindOrCreateWorkerPool(DistributedExecution *execution,
										   WorkerNode *workerNode);
static WorkerSession * FindOrCreateWorkerSession(WorkerPool *workerPool,
												 MultiConnection *connection);
static void ManageWorkerPool(WorkerPool *workerPool);
static void CheckConnectionTimeout(WorkerPool *workerPool);
static WaitEventSet * BuildWaitEventSet(List *sessionList);
static TaskPlacementExecution * PopPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopAssignedPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopUnassignedPlacementExecution(WorkerPool *workerPool);
static bool StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
											 WorkerSession *session);
static List * PlacementAccessListForTask(Task *task, ShardPlacement *taskPlacement);
static void ConnectionStateMachine(WorkerSession *session);
static void Activate2PCIfModifyingTransactionExpandsToNewNode(WorkerSession *session);
static bool TransactionModifiedDistributedTable(DistributedExecution *execution);
static void TransactionStateMachine(WorkerSession *session);
static void UpdateConnectionWaitFlags(WorkerSession *session, int waitFlags);
static bool CheckConnectionReady(WorkerSession *session);
static bool ReceiveResults(WorkerSession *session, bool storeRows);
static void WorkerSessionFailed(WorkerSession *session);
static void WorkerPoolFailed(WorkerPool *workerPool);
static void PlacementExecutionDone(TaskPlacementExecution *placementExecution,
								   bool succeeded);
static void ScheduleNextPlacementExecution(TaskPlacementExecution *placementExecution,
										   bool succeeded);
static bool ShouldMarkPlacementsInvalidOnFailure(DistributedExecution *execution);
static void PlacementExecutionReady(TaskPlacementExecution *placementExecution);
static TaskExecutionState TaskExecutionStateMachine(ShardCommandExecution *
													shardCommandExecution);


/*
 * CitusExecScan is called when a tuple is pulled from a custom scan.
 * On the first call, it executes the distributed query and writes the
 * results to a tuple store. The postgres executor calls this function
 * repeatedly to read tuples from the tuple store.
 */
TupleTableSlot *
CitusExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		DistributedExecution *execution = NULL;
		EState *executorState = ScanStateGetExecutorState(scanState);
		ParamListInfo paramListInfo = executorState->es_param_list_info;
		Tuplestorestate *tupleStore = NULL;
		TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
		bool randomAccess = true;
		bool interTransactions = false;
		int targetPoolSize = DEFAULT_POOL_SIZE;

		Job *job = distributedPlan->workerJob;
		List *taskList = job->taskList;

		/*
		 * PostgreSQL takes locks on all partitions in the executor. It's not entirely
		 * clear why this is necessary (instead of locking the parent during DDL), but
		 * We do the same for consistency.
		 */
		LockPartitionsForDistributedPlan(distributedPlan);

		ExecuteSubPlans(distributedPlan);

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
		tupleStore = scanState->tuplestorestate;

		if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
		{
			targetPoolSize = 1;
		}

		execution = CreateDistributedExecution(distributedPlan->operation, taskList,
											   distributedPlan->hasReturning,
											   paramListInfo, tupleDescriptor,
											   tupleStore, targetPoolSize);

		StartDistributedExecution(execution);

		if (ShouldRunTasksSequentially(execution->tasksToExecute))
		{
			SequentialRunDistributedExecution(execution);
		}
		else
		{
			RunDistributedExecution(execution);
		}

		if (distributedPlan->operation != CMD_SELECT)
		{
			executorState->es_processed = execution->rowsProcessed;
		}

		FinishDistributedExecution(execution);

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
 * ExecuteTaskList is a proxy to ExecuteTaskListExtended() with defaults
 * for some of the arguments.
 */
uint64
ExecuteTaskList(CmdType operation, List *taskList, int targetPoolSize)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = NULL;
	bool hasReturning = false;
	return ExecuteTaskListExtended(operation, taskList, tupleDescriptor,
								   tupleStore, hasReturning, targetPoolSize);
}


/*
 * ExecuteTaskListExtended sets up the execution for given task list and
 * runs it.
 */
uint64
ExecuteTaskListExtended(CmdType operation, List *taskList,
						TupleDesc tupleDescriptor, Tuplestorestate *tupleStore,
						bool hasReturning, int targetPoolSize)
{
	DistributedExecution *execution = NULL;
	ParamListInfo paramListInfo = NULL;

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		targetPoolSize = 1;
	}

	execution =
		CreateDistributedExecution(operation, taskList, hasReturning, paramListInfo,
								   tupleDescriptor, tupleStore, targetPoolSize);

	StartDistributedExecution(execution);
	RunDistributedExecution(execution);
	FinishDistributedExecution(execution);

	return execution->rowsProcessed;
}


/*
 * CreateDistributedExecution creates a distributed execution data structure for
 * a distributed plan.
 */
DistributedExecution *
CreateDistributedExecution(CmdType operation, List *taskList, bool hasReturning,
						   ParamListInfo paramListInfo, TupleDesc tupleDescriptor,
						   Tuplestorestate *tupleStore, int targetPoolSize)
{
	DistributedExecution *execution =
		(DistributedExecution *) palloc0(sizeof(DistributedExecution));

	execution->operation = operation;
	execution->tasksToExecute = taskList;
	execution->hasReturning = hasReturning;

	execution->executionStats =
		(DistributedExecutionStats *) palloc0(sizeof(DistributedExecutionStats));
	execution->paramListInfo = paramListInfo;
	execution->tupleDescriptor = tupleDescriptor;
	execution->tupleStore = tupleStore;

	execution->workerList = NIL;
	execution->sessionList = NIL;
	execution->targetPoolSize = targetPoolSize;

	execution->totalTaskCount = list_length(taskList);
	execution->unfinishedTaskCount = list_length(taskList);
	execution->rowsProcessed = 0;

	execution->raiseInterrupts = true;

	execution->connectionSetChanged = false;
	execution->waitFlagsChanged = false;

	return execution;
}


/*
 * StartDistributedExecution sets up the coordinated transaction and 2PC for
 * the execution whenever necessary. It also keeps track of parallel relation
 * accesses to enforce restrictions that arise due to foreign keys to reference
 * tables.
 */
void
StartDistributedExecution(DistributedExecution *execution)
{
	List *taskList = execution->tasksToExecute;

	if (MultiShardCommitProtocol != COMMIT_PROTOCOL_BARE)
	{
		if (DistributedExecutionRequiresRollback(execution))
		{
			BeginOrContinueCoordinatedTransaction();

			if (TaskListRequires2PC(taskList))
			{
				/*
				 * Although using two phase commit protocol is an independent decision than
				 * failing on any error, we prefer to couple them. Our motivation is that
				 * the failures are rare, and we prefer to avoid marking placements invalid
				 * in case of failures.
				 */
				CoordinatedTransactionUse2PC();

				execution->errorOnAnyFailure = true;
			}
			else if (MultiShardCommitProtocol != COMMIT_PROTOCOL_2PC &&
					 list_length(taskList) > 1 &&
					 DistributedExecutionModifiesDatabase(execution))
			{
				/*
				 * Even if we're not using 2PC, we prefer to error out
				 * on any failures during multi shard modifications/DDLs.
				 */
				execution->errorOnAnyFailure = true;
			}
		}
	}
	else
	{
		/*
		 * We prefer to error on any failures for CREATE INDEX
		 * CONCURRENTLY or VACUUM//VACUUM ANALYZE (e.g., COMMIT_PROTOCOL_BARE).
		 */
		execution->errorOnAnyFailure = true;
	}

	execution->isTransaction = InCoordinatedTransaction();

	/*
	 * We should not record parallel access if the target pool size is less than 2.
	 * The reason is that we define parallel access as at least two connections
	 * accessing established to worker node.
	 *
	 * It is not ideal to have this check here, it'd have been better if we simply passed
	 * DistributedExecution directly to the RecordParallelAccess*() function. However,
	 * since we have two other executors that rely on the function, we had to only pass
	 * the tasklist to have a common API.
	 */
	if (execution->targetPoolSize > 1)
	{
		RecordParallelRelationAccessForTaskList(taskList);
	}
}


/*
 *  DistributedExecutionModifiesDatabase returns true if the execution modifies the data
 *  or the schema.
 */
static bool
DistributedExecutionModifiesDatabase(DistributedExecution *execution)
{
	return TaskListModifiesDatabase(execution->operation, execution->tasksToExecute);
}


/*
 *  DistributedPlanModifiesDatabase returns true if the plan modifies the data
 *  or the schema.
 */
static bool
DistributedPlanModifiesDatabase(DistributedPlan *plan)
{
	return TaskListModifiesDatabase(plan->operation, plan->workerJob->taskList);
}


/*
 *  TaskListModifiesDatabase is a helper function for DistributedExecutionModifiesDatabase and
 *  DistributedPlanModifiesDatabase.
 */
static bool
TaskListModifiesDatabase(CmdType operation, List *taskList)
{
	Task *firstTask = NULL;

	if (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		return true;
	}

	/*
	 * If we cannot decide by only checking the operation, we should look closer
	 * to the tasks.
	 */
	if (list_length(taskList) < 1)
	{
		/* does this ever possible? */
		return false;
	}

	firstTask = (Task *) linitial(taskList);

	return !ReadOnlyTask(firstTask->taskType);
}


static bool
DistributedExecutionRequiresRollback(DistributedExecution *execution)
{
	List *taskList = execution->tasksToExecute;
	Task *task = NULL;

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		return false;
	}

	if (execution->operation == CMD_SELECT)
	{
		return SelectOpensTransactionBlock && IsTransactionBlock();
	}

	if (taskList == NIL)
	{
		return false;
	}

	if (IsMultiStatementTransaction())
	{
		return true;
	}

	if (list_length(taskList) > 1)
	{
		return true;
	}

	/*
	 * Checking the first task's placement list is not sufficient for all purposes since
	 * for append/range distributed tables we might have unequal number of placements for
	 * shards. However, it is safe to do here, because we're searching for a reference
	 * table. All other cases return false for this purpose.
	 */
	task = (Task *) linitial(taskList);
	if (list_length(task->taskPlacementList) > 1)
	{
		/*
		 * Some tasks don't set replicationModel thus we only
		 * rely on the anchorShardId, not replicationModel.
		 *
		 * TODO: Do we ever need replicationModel in the Task structure?
		 * Can't we always rely on anchorShardId?
		 */
		uint64 anchorShardId = task->anchorShardId;
		if (anchorShardId != INVALID_SHARD_ID && ReferenceTableShardId(anchorShardId))
		{
			return true;
		}

		/*
		 * Single DML/DDL tasks with replicated tables (non-reference)
		 * should not require BEGIN/COMMIT/ROLLBACK.
		 */
		return false;
	}

	return false;
}


/*
 * LockPartitionsForDistributedPlan ensures commands take locks on all partitions
 * of a distributed table that appears in the query. We do this primarily out of
 * consistency with PostgreSQL locking.
 */
static void
LockPartitionsForDistributedPlan(DistributedPlan *distributedPlan)
{
	if (DistributedPlanModifiesDatabase(distributedPlan))
	{
		Oid targetRelationId = distributedPlan->targetRelationId;

		LockPartitionsInRelationList(list_make1_oid(targetRelationId), RowExclusiveLock);
	}

	/*
	 * Lock partitions of tables that appear in a SELECT or subquery. In the
	 * DML case this also includes the target relation, but since we already
	 * have a stronger lock this doesn't do any harm.
	 */
	LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);
}


/*
 * FinishDistributedExecution cleans up resources associated with a
 * distributed execution. In particular, it releases connections and
 * clears their state.
 */
static void
FinishDistributedExecution(DistributedExecution *execution)
{
	UnsetCitusNoticeLevel();

	if (DistributedExecutionModifiesDatabase(execution))
	{
		/* prevent copying shards in same transaction */
		XactModificationLevel = XACT_MODIFICATION_DATA;
	}
}


/*
 * CleanUpSessions does any clean-up necessary for the session
 * used during the execution.
 */
static void
CleanUpSessions(DistributedExecution *execution)
{
	List *sessionList = execution->sessionList;
	ListCell *sessionCell = NULL;

	/* always trigger wait event set in the first round */
	foreach(sessionCell, sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);

		MultiConnection *connection = session->connection;
		RemoteTransaction *transaction = &(connection->remoteTransaction);
		RemoteTransactionState transactionState = transaction->transactionState;

		elog(DEBUG4, "Total number of commands sent over the session %ld: %ld",
			 session->sessionId, session->commandsSent);

		UnclaimConnection(connection);

		if (connection->connectionState == MULTI_CONNECTION_CONNECTING ||
			connection->connectionState == MULTI_CONNECTION_FAILED)
		{
			/*
			 * We want the MultiConnection go away and not used in
			 * the subsequent executions.
			 */
			CloseConnection(connection);
		}
		else if (transactionState == REMOTE_TRANS_CLEARING_RESULTS)
		{
			/* TODO: should we keep this in the state machine? or cancel? */
			ClearResults(connection, false);
		}

		/* TODO: can we be in SENT_COMMAND? */

		/* get ready for the next executions if we need use the same connection */
		connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
		execution->waitFlagsChanged = true;
	}
}


/*
 * UnclaimAllSessionConnections unclaims all of the connections for the given
 * sessionList.
 */
static void
UnclaimAllSessionConnections(List *sessionList)
{
	ListCell *sessionCell = NULL;
	foreach(sessionCell, sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);
		MultiConnection *connection = session->connection;

		UnclaimConnection(connection);
	}
}


/*
 * AssignTasksToConnections goes through the list of tasks to determine whether any
 * task placements need to be assigned to particular connections because of preceding
 * operations in the transaction. It then adds those connections to the pool and adds
 * the task placement executions to the assigned task queue of the connection.
 */
static void
AssignTasksToConnections(DistributedExecution *execution)
{
	CmdType operation = execution->operation;
	List *taskList = execution->tasksToExecute;
	bool hasReturning = execution->hasReturning;

	ListCell *taskCell = NULL;
	ListCell *sessionCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		ShardCommandExecution *shardCommandExecution = NULL;
		ListCell *taskPlacementCell = NULL;
		bool placementExecutionReady = true;
		int placementExecutionIndex = 0;
		int placementExecutionCount = list_length(task->taskPlacementList);

		/*
		 * Execution of a command on a shard, which may have multiple replicas.
		 */
		shardCommandExecution =
			(ShardCommandExecution *) palloc0(sizeof(ShardCommandExecution));
		shardCommandExecution->task = task;
		shardCommandExecution->executionOrder = ExecutionOrderForTask(operation, task);
		shardCommandExecution->executionState = TASK_EXECUTION_NOT_FINISHED;
		shardCommandExecution->placementExecutions =
			(TaskPlacementExecution **) palloc0(placementExecutionCount *
												sizeof(TaskPlacementExecution *));
		shardCommandExecution->placementExecutionCount = placementExecutionCount;
		shardCommandExecution->expectResults = hasReturning || operation == CMD_SELECT;


		foreach(taskPlacementCell, task->taskPlacementList)
		{
			ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
			List *placementAccessList = NULL;
			MultiConnection *connection = NULL;
			int connectionFlags = 0;
			TaskPlacementExecution *placementExecution = NULL;
			WorkerNode *node = FindWorkerNode(taskPlacement->nodeName,
											  taskPlacement->nodePort);
			WorkerPool *workerPool = FindOrCreateWorkerPool(execution, node);

			/*
			 * Execution of a command on a shard placement, which may not always
			 * happen if the query is read-only and the shard has multiple placements.
			 */
			placementExecution =
				(TaskPlacementExecution *) palloc0(sizeof(TaskPlacementExecution));
			placementExecution->shardCommandExecution = shardCommandExecution;
			placementExecution->shardPlacement = taskPlacement;
			placementExecution->workerPool = workerPool;
			placementExecution->placementExecutionIndex = placementExecutionIndex;

			if (placementExecutionReady)
			{
				placementExecution->executionState = PLACEMENT_EXECUTION_READY;
			}
			else
			{
				placementExecution->executionState = PLACEMENT_EXECUTION_NOT_READY;
			}

			shardCommandExecution->placementExecutions[placementExecutionIndex] =
				placementExecution;

			placementExecutionIndex++;

			placementAccessList = PlacementAccessListForTask(task, taskPlacement);

			/*
			 * Determine whether the task has to be assigned to a particular connection
			 * due to a preceding access to the placement in the same transaction.
			 */
			connection = GetPlacementListConnectionIfCached(connectionFlags,
															placementAccessList,
															NULL);
			if (connection != NULL)
			{
				/*
				 * Note: We may get the same connection for multiple task placements.
				 * FindOrCreateWorkerSession ensures that we only have one session per
				 * connection.
				 */
				WorkerSession *session =
					FindOrCreateWorkerSession(workerPool, connection);

				elog(DEBUG4, "%s:%d has an assigned task", connection->hostname,
					 connection->port);

				placementExecution->assignedSession = session;

				/* if executed, this task placement must use this session */
				if (placementExecutionReady)
				{
					dlist_push_tail(&session->readyTaskQueue,
									&placementExecution->sessionReadyQueueNode);
				}
				else
				{
					dlist_push_tail(&session->pendingTaskQueue,
									&placementExecution->sessionPendingQueueNode);
				}

				/* always poll the connection in the first round */
				UpdateConnectionWaitFlags(session,
										  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

				/* If the connections are already avaliable, make sure to activate
				 * 2PC when necessary.
				 */
				Activate2PCIfModifyingTransactionExpandsToNewNode(session);
			}
			else
			{
				placementExecution->assignedSession = NULL;

				if (placementExecutionReady)
				{
					/* task is ready to execute on any session */
					dlist_push_tail(&workerPool->readyTaskQueue,
									&placementExecution->workerReadyQueueNode);

					workerPool->readyTaskCount++;
				}
				else
				{
					/* task can be executed on any session, but is not yet ready */
					dlist_push_tail(&workerPool->pendingTaskQueue,
									&placementExecution->workerPendingQueueNode);
				}
			}

			if (shardCommandExecution->executionOrder != EXECUTION_ORDER_PARALLEL)
			{
				/*
				 * Except for commands that can be executed across all placements
				 * in parallel, only the first placement execution is immediately
				 * ready. Set placementExecutionReady to false for the remaining
				 * placements.
				 */
				placementExecutionReady = false;
			}
		}
	}

	/*
	 * The executor claims connections exclusively to make sure that calls to
	 * StartNodeUserDatabaseConnection do not return the same connections.
	 *
	 * We need to do this after assigning tasks to connections because the same
	 * connection may be be returned multiple times by GetPlacementListConnectionIfCached.
	 */
	foreach(sessionCell, execution->sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);
		MultiConnection *connection = session->connection;

		ClaimConnectionExclusively(connection);
	}
}


/*
 * UseConnectionPerPlacement returns whether we should use a separate connection
 * per placement even if another connection is idle. We mostly use this in testing
 * scenarios.
 */
static bool
UseConnectionPerPlacement(void)
{
	return ForceMaxQueryParallelization &&
		   MultiShardConnectionType != SEQUENTIAL_CONNECTION;
}


/*
 * ExecutionOrderForTask gives the appropriate execution order for a task.
 */
static PlacementExecutionOrder
ExecutionOrderForTask(CmdType operation, Task *task)
{
	switch (task->taskType)
	{
		case SQL_TASK:
		case ROUTER_TASK:
		{
			return EXECUTION_ORDER_ANY;
		}

		case MODIFY_TASK:
		{
			if (operation == CMD_INSERT && !task->upsertQuery)
			{
				return EXECUTION_ORDER_SEQUENTIAL;
			}
			else
			{
				return EXECUTION_ORDER_PARALLEL;
			}
		}

		case DDL_TASK:
		case VACUUM_ANALYZE_TASK:
		{
			return EXECUTION_ORDER_PARALLEL;
		}

		case MAP_TASK:
		case MERGE_TASK:
		case MAP_OUTPUT_FETCH_TASK:
		case MERGE_FETCH_TASK:
		default:
		{
			elog(ERROR, "unsupported task type %d in unified executor", task->taskType);
		}
	}
}


/*
 * FindOrCreateWorkerPool gets the pool of connections for a particular worker.
 */
static WorkerPool *
FindOrCreateWorkerPool(DistributedExecution *execution, WorkerNode *workerNode)
{
	WorkerPool *workerPool = NULL;
	ListCell *workerCell = NULL;

	foreach(workerCell, execution->workerList)
	{
		workerPool = lfirst(workerCell);

		if (WorkerNodeCompare(workerPool->node, workerNode, 0) == 0)
		{
			return workerPool;
		}
	}

	workerPool = (WorkerPool *) palloc0(sizeof(WorkerPool));
	workerPool->node = workerNode;
	workerPool->poolStartTime = 0;
	workerPool->distributedExecution = execution;
	dlist_init(&workerPool->pendingTaskQueue);
	dlist_init(&workerPool->readyTaskQueue);

	execution->workerList = lappend(execution->workerList, workerPool);

	return workerPool;
}


/*
 * FindOrCreateWorkerSession returns a session with the given connection,
 * either existing or new. New sessions are added to the worker pool and
 * the distributed execution.
 */
static WorkerSession *
FindOrCreateWorkerSession(WorkerPool *workerPool, MultiConnection *connection)
{
	DistributedExecution *execution = workerPool->distributedExecution;
	WorkerSession *session = NULL;
	ListCell *sessionCell = NULL;
	static uint64 sessionId = 1;

	foreach(sessionCell, workerPool->sessionList)
	{
		session = lfirst(sessionCell);

		if (session->connection == connection)
		{
			return session;
		}
	}

	session = (WorkerSession *) palloc0(sizeof(WorkerSession));
	session->sessionId = sessionId++;
	session->connection = connection;
	session->workerPool = workerPool;
	session->commandsSent = 0;
	dlist_init(&session->pendingTaskQueue);
	dlist_init(&session->readyTaskQueue);

	/* keep track of how many connections are ready */
	if (connection->connectionState == MULTI_CONNECTION_CONNECTED)
	{
		workerPool->activeConnectionCount++;
		workerPool->idleConnectionCount++;
	}

	workerPool->unusedConnectionCount++;

	/*
	 * Record the first connection establishment time to the pool. We need this
	 * to enforce NodeConnectionTimeout.
	 */
	if (list_length(workerPool->sessionList) == 0)
	{
		workerPool->poolStartTime = GetCurrentTimestamp();
	}

	workerPool->sessionList = lappend(workerPool->sessionList, session);
	execution->sessionList = lappend(execution->sessionList, session);

	return session;
}


/*
 * ShouldRunTasksSequentially returns true if each of the individual tasks
 * should be executed one by one. Note that this is different than
 * MultiShardConnectionType == SEQUENTIAL_CONNECTION case. In that case,
 * running the tasks across the nodes in parallel is acceptable and implemented
 * in that way.
 *
 * However, the executions that are qualified here would perform poorly if the
 * tasks across the workers are executed in parallel. We currently qualify only
 * one class of distributed queries here, multi-row INSERTs. If we do not enforce
 * true sequential execution, concurrent multi-row upserts could easily form
 * a distributed deadlock when the upserts touch the same rows.
 */
static bool
ShouldRunTasksSequentially(List *taskList)
{
	Task *initialTask = NULL;

	if (list_length(taskList) < 2)
	{
		/* single task plans are already qualified as sequential by definition */
		return false;
	}

	/* all the tasks are the same, so we only look one */
	initialTask = (Task *) linitial(taskList);
	if (initialTask->rowValuesLists != NIL)
	{
		/* found a multi-row INSERT */
		return true;
	}

	return false;
}


/*
 * SequentialRunDistributedExecution gets a distributed execution and
 * executes each individual task in the exection sequentially, one
 * task at a time. See related function ShouldRunTasksSequentially()
 * for more detail on the definition of SequentialRun.
 */
static void
SequentialRunDistributedExecution(DistributedExecution *execution)
{
	List *taskList = execution->tasksToExecute;

	ListCell *taskCell = NULL;
	int connectionMode = MultiShardConnectionType;

	/*
	 * There are some implicit assumptions about this setting for the sequential
	 * executions, so make sure to set it.
	 */
	MultiShardConnectionType = SEQUENTIAL_CONNECTION;

	foreach(taskCell, taskList)
	{
		Task *taskToExecute = (Task *) lfirst(taskCell);

		/* execute each task one by one */
		execution->tasksToExecute = list_make1(taskToExecute);
		execution->totalTaskCount = 1;
		execution->unfinishedTaskCount = 1;

		/* simply call the regular execution function */
		RunDistributedExecution(execution);
	}

	/* set back the original execution mode */
	MultiShardConnectionType = connectionMode;
}


/*
 * RunDistributedExecution runs a distributed execution to completion. It first opens
 * connections for distributed execution and assigns each task with shard placements
 * that have previously been modified in the current transaction to the connection
 * that modified them. Then, it creates a wait event set to listen for events on
 * any of the connections and runs the connection state machine when a connection
 * has an event.
 */
void
RunDistributedExecution(DistributedExecution *execution)
{
	TimestampTz deadline = 0;
	WaitEvent *events = NULL;

	deadline =
		TimestampTzPlusMilliseconds(GetCurrentTimestamp(), NodeConnectionTimeout);

	AssignTasksToConnections(execution);

	PG_TRY();
	{
		bool cancellationReceived = false;

		/* additional 2 is for postmaster and latch */
		int eventSetSize = list_length(execution->sessionList) + 2;

		execution->waitEventSet = BuildWaitEventSet(execution->sessionList);
		events = palloc0(eventSetSize * sizeof(WaitEvent));

		while (execution->unfinishedTaskCount > 0 && !cancellationReceived)
		{
			long timeout = DeadlineTimestampTzToTimeout(deadline);
			int eventCount = 0;
			int eventIndex = 0;
			ListCell *workerCell = NULL;

			foreach(workerCell, execution->workerList)
			{
				WorkerPool *workerPool = lfirst(workerCell);
				ManageWorkerPool(workerPool);
			}

			if (execution->connectionSetChanged || execution->waitFlagsChanged)
			{
				FreeWaitEventSet(execution->waitEventSet);

				execution->waitEventSet = BuildWaitEventSet(execution->sessionList);

				if (execution->connectionSetChanged)
				{
					/*
					 * The execution might take a while, so explicitly free at this point
					 * because we don't need anymore.
					  */
					pfree(events);

					/* recalculate (and allocate) since the sessions have changed */
					eventSetSize = list_length(execution->sessionList) + 2;

					events = palloc0(eventSetSize * sizeof(WaitEvent));
				}

				execution->connectionSetChanged = false;
				execution->waitFlagsChanged = false;
			}

			/* wait for I/O events */
#if (PG_VERSION_NUM >= 100000)
			eventCount = WaitEventSetWait(execution->waitEventSet, timeout, events,
										  eventSetSize, WAIT_EVENT_CLIENT_READ);
#else
			eventCount = WaitEventSetWait(execution->waitEventSet, timeout, events,
										  eventSetSize);
#endif

			/* process I/O events */
			for (; eventIndex < eventCount; eventIndex++)
			{
				WaitEvent *event = &events[eventIndex];
				WorkerSession *session = NULL;

				if (event->events & WL_POSTMASTER_DEATH)
				{
					ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
				}

				if (event->events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);

					if (execution->raiseInterrupts)
					{
						CHECK_FOR_INTERRUPTS();
					}

					if (InterruptHoldoffCount > 0 && (QueryCancelPending ||
													  ProcDiePending))
					{
						/*
						 * Break out of event loop immediately in case of cancellation.
						 * We cannot use "return" here inside a PG_TRY() block since
						 * then the exception stack won't be reset.
						 */
						cancellationReceived = true;
						break;
					}

					continue;
				}

				session = (WorkerSession *) event->user_data;

				ConnectionStateMachine(session);
			}
		}

		pfree(events);
		FreeWaitEventSet(execution->waitEventSet);

		CleanUpSessions(execution);
	}
	PG_CATCH();
	{
		/*
		 * We can still recover from error using ROLLBACK TO SAVEPOINT,
		 * unclaim all connections to allow that.
		 */
		UnclaimAllSessionConnections(execution->sessionList);

		FreeWaitEventSet(execution->waitEventSet);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ManageWorkerPool ensures the worker pool has the appropriate number of connections
 * based on the number of pending tasks.
 */
static void
ManageWorkerPool(WorkerPool *workerPool)
{
	DistributedExecution *execution = workerPool->distributedExecution;
	WorkerNode *workerNode = workerPool->node;
	int targetPoolSize = execution->targetPoolSize;
	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int activeConnectionCount = workerPool->activeConnectionCount;
	int failedConnectionCount = workerPool->failedConnectionCount;
	int idleConnectionCount = workerPool->idleConnectionCount;
	int readyTaskCount = workerPool->readyTaskCount;
	int newConnectionCount = 0;
	int connectionIndex = 0;

	/* we should always have more (or equal) active connections than idle connections */
	Assert(activeConnectionCount >= idleConnectionCount);

	/* we should always have more (or equal) initiated connections than active connections */
	Assert (initiatedConnectionCount >= activeConnectionCount);

	/* we should never have less than 0 connections ever */
	Assert(activeConnectionCount >= 0 && idleConnectionCount >= 0);

	/* we might fail the execution or warn the user about connection timeouts */
	CheckConnectionTimeout(workerPool);

	if (failedConnectionCount >= 1)
	{
		/* connection pool failed */
		return;
	}

	if (UseConnectionPerPlacement())
	{
		int unusedConnectionCount = workerPool->unusedConnectionCount;

		/*
		 * If force_max_query_parallelization is enabled then we ignore pool size
		 * and idle connections. Instead, we open new connections as long as there
		 * are more tasks than unused connections.
		 */

		newConnectionCount = Max(readyTaskCount - unusedConnectionCount, 0);
	}
	else
	{
		if (initiatedConnectionCount >= targetPoolSize)
		{
			/* already reached the minimal pool size */
			return;
		}

		if (readyTaskCount - idleConnectionCount <= initiatedConnectionCount)
		{
			/* after assigning tasks to idle connections we don't need more connections */
			return;
		}

		/*
		 * Open enough connections to handle all tasks that are ready, but no more
		 * than the target pool size.
		 */
		newConnectionCount = Min(readyTaskCount - idleConnectionCount, targetPoolSize) -
							 activeConnectionCount;
	}

	if (newConnectionCount == 0)
	{
		return;
	}

	elog(DEBUG4, "opening %d new connections to %s:%d", newConnectionCount,
		 workerNode->workerName, workerNode->workerPort);

	for (connectionIndex = 0; connectionIndex < newConnectionCount; connectionIndex++)
	{
		MultiConnection *connection = NULL;
		WorkerSession *session = NULL;

		/* experimental: just to see the perf benefits of caching connections */
		int connectionFlags = 0;

		/* open a new connection to the worker */
		connection = StartNodeUserDatabaseConnection(connectionFlags,
													 workerNode->workerName,
													 workerNode->workerPort,
													 NULL, NULL);

		/*
		 * Assign the initial state in the connection state machine. The connection
		 * may already be open, but ConnectionStateMachine will immediately detect
		 * this.
		 */
		connection->connectionState = MULTI_CONNECTION_CONNECTING;

		/*
		 * Ensure that subsequent calls to StartNodeUserDatabaseConnection get a
		 * different connection.
		 */
		connection->claimedExclusively = true;

		/* create a session for the connection */
		session = FindOrCreateWorkerSession(workerPool, connection);

		/* always poll the connection in the first round */
		UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
	}

	if (newConnectionCount > 0)
	{
		execution->connectionSetChanged = true;
	}
}


/*
 * CheckConnectionTimeout makes sure that the execution enforces the connection
 * establishment timeout defined by the user (NodeConnectionTimeout).
 *
 * The rule is that if a worker pool has already initiated connection establishment
 * and has not succeeded to finish establishments that are necessary to execute tasks,
 * take an action. For the types of actions, see the comments in the function.
 *
 * Enforcing the timeout per pool (over per session) helps the execution to continue
 * even if we can establish a single connection as we expect to have target pool size
 * number of connections. In the end, the executor is capable of using one connection
 * to execute multiple tasks.
 */
static void
CheckConnectionTimeout(WorkerPool *workerPool)
{
	DistributedExecution *execution = workerPool->distributedExecution;
	TimestampTz poolStartTime = workerPool->poolStartTime;
	TimestampTz now = GetCurrentTimestamp();

	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int activeConnectionCount = workerPool->activeConnectionCount;
	int requiredActiveConnectionCount = 1;

	if (initiatedConnectionCount == 0)
	{
		/* no connection has been planned for the pool yet */
		Assert(poolStartTime == 0);
		return;
	}

	/*
	 * This is a special case where we assign tasks to sessions even before
	 * the connections are established. So, make sure to apply similar
	 * restrictions. In this case, make sure that we get all the connections
	 * established.
	 */
	if (UseConnectionPerPlacement())
	{
		requiredActiveConnectionCount = initiatedConnectionCount;
	}

	/*
	 * The enforcement is not always erroring out. For example, if a SELECT task
	 * has two different placements, we'd warn the user, fail the pool and continue
	 * with the next placement.
	 *
	 * !workerPool->failed is only useful to prevent emitting WARNINGs over and over
	 * for failed pools where failed pools have 0 ready tasks.
	 */
	if (activeConnectionCount < requiredActiveConnectionCount && !workerPool->failed &&
		TimestampDifferenceExceeds(poolStartTime, now, NodeConnectionTimeout))
	{
		int logLevel = WARNING;

		/*
		 * First fail the pool and create an opportunity to execute tasks
		 * over other pools when tasks have more than one placement to execute.
		 */
		WorkerPoolFailed(workerPool);

		logLevel = execution->errorOnAnyFailure || execution->failed ? ERROR : WARNING;

		ereport(logLevel, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("could not establish any connections to the node "
								  "%s:%d after %u ms", workerPool->node->workerName,
								  workerPool->node->workerPort,
								  NodeConnectionTimeout)));
	}
}


/*
 * ConnectionStateMachine opens a connection and descends into the transaction
 * state machine when ready.
 */
static void
ConnectionStateMachine(WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;

	MultiConnection *connection = session->connection;
	MultiConnectionState currentState;

	do {
		currentState = connection->connectionState;

		switch (currentState)
		{
			case MULTI_CONNECTION_INITIAL:
			{
				/* simply iterate the state machine */
				connection->connectionState = MULTI_CONNECTION_CONNECTING;
				break;
			}

			case MULTI_CONNECTION_CONNECTING:
			{
				PostgresPollingStatusType pollMode;

				ConnStatusType status = PQstatus(connection->pgConn);
				if (status == CONNECTION_OK)
				{
					elog(DEBUG4, "established connection to %s:%d", connection->hostname,
						 connection->port);

					workerPool->activeConnectionCount++;
					workerPool->idleConnectionCount++;

					connection->connectionState = MULTI_CONNECTION_CONNECTED;
					break;
				}
				else if (status == CONNECTION_BAD)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					break;
				}

				pollMode = PQconnectPoll(connection->pgConn);
				if (pollMode == PGRES_POLLING_FAILED)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
				}
				else if (pollMode == PGRES_POLLING_READING)
				{
					UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);
				}
				else if (pollMode == PGRES_POLLING_WRITING)
				{
					UpdateConnectionWaitFlags(session, WL_SOCKET_WRITEABLE);
				}
				else
				{
					elog(DEBUG4, "established connection to %s:%d", connection->hostname,
						 connection->port);

					workerPool->activeConnectionCount++;
					workerPool->idleConnectionCount++;

					UpdateConnectionWaitFlags(session, WL_SOCKET_WRITEABLE);

					connection->connectionState = MULTI_CONNECTION_CONNECTED;
				}

				break;
			}

			case MULTI_CONNECTION_CONNECTED:
			{
				/* connection is ready, run the transaction state machine */
				TransactionStateMachine(session);
				break;
			}

			case MULTI_CONNECTION_LOST:
			{
				/* managed to connect, but connection was lost */
				workerPool->activeConnectionCount--;

				connection->connectionState = MULTI_CONNECTION_FAILED;
				break;
			}

			case MULTI_CONNECTION_FAILED:
			{
				/* connection failed or was lost */
				int totalConnectionCount = list_length(workerPool->sessionList);

				workerPool->failedConnectionCount++;

				/* if the connection executed a critical command it should fail */
				MarkRemoteTransactionFailed(connection, false);

				/* mark all assigned placement executions as failed */
				WorkerSessionFailed(session);

				if (workerPool->failedConnectionCount >= totalConnectionCount)
				{
					/*
					 * All current connection attempts have failed.
					 * Mark all unassigned placement executions as failed.
					 *
					 * We do not currently retry if the first connection
					 * attempt fails.
					 */
					WorkerPoolFailed(workerPool);
				}

				if (execution->failed || execution->errorOnAnyFailure)
				{
					/* a task has failed due to this connection failure */
					ReportConnectionError(connection, ERROR);
				}
				else
				{
					/* can continue with the remaining nodes */
					ReportConnectionError(connection, WARNING);
				}

				/* remove the connection */
				UnclaimConnection(connection);

				/*
				 * We forcefully close the underlying libpq connection because
				 * we don't want any subsequent execution (either subPlan executions
				 * or new command executions within a transaction block) use the
				 * connection.
				 *
				 * However, we prefer to keep the MultiConnection around until
				 * the end of FinishDistributedExecution() to simplify the code.
				 * Thus, we prefer ShutdownConnection() over CloseConnection().
				 */
				ShutdownConnection(connection);

				/* remove connection from wait event set */
				execution->connectionSetChanged = true;

				/*
				 * Reset the transaction state machine since CloseConnection()
				 * relies on it and even if we're not inside a distributed transaction
				 * we set the transaction state (e.g., REMOTE_TRANS_SENT_COMMAND).
				 */
				if (!connection->remoteTransaction.beginSent)
				{
					connection->remoteTransaction.transactionState =
						REMOTE_TRANS_INVALID;
				}

				break;
			}

			default:
			{
				break;
			}
		}
	} while (connection->connectionState != currentState);
}


/*
 * Activate2PCIfModifyingTransactionExpandsToNewNode sets the coordinated
 * transaction to use 2PC under the following circumstances:
 *     - We're already in a transaction block
 *     - At least one of the previous commands in the transaction block
 *       made a modification, which have not set 2PC itself because it
 *       was a single shard command
 *     - The input "session" is used for a distributed execution which
 *       modifies the database. However, the session (and hence the
 *       connection) is established to a different worker than the ones
 *       that is used previously in the transaction.
 *
 *  To give an example,
 *      BEGIN;
 *          -- assume that the following INSERT goes to worker-A
 *          -- also note that this single command does not activate
 *          -- 2PC itself since it is a single shard mofication
 *          INSERT INTO distributed_table (dist_key) VALUES (1);
 *
 *          -- do one more single shard UPDATE hitting the same
 *          shard (or worker node in general)
 *          -- this wouldn't activate 2PC, since we're operating on the
 *          -- same worker node that we've modified earlier
 *          -- so the executor would use the same connection
 *			UPDATE distributed_table SET value = 10 WHERE dist_key = 1;
 *
 *          -- now, do one more INSERT, which goes to worker-B
 *          -- At this point, this function would activate 2PC
 *          -- since we're now expanding to a new node
 *          -- for example, if this command were a SELECT, we wouldn't
 *          -- activate 2PC since we're only interested in modifications/DDLs
 *          INSERT INTO distributed_table (dist_key) VALUES (2);
 */
static void
Activate2PCIfModifyingTransactionExpandsToNewNode(WorkerSession *session)
{
	DistributedExecution *execution = NULL;

	if (MultiShardCommitProtocol != COMMIT_PROTOCOL_2PC)
	{
		/* we don't need 2PC, so no need to continue */
		return;
	}

	execution = session->workerPool->distributedExecution;
	if (TransactionModifiedDistributedTable(execution) &&
		DistributedExecutionModifiesDatabase(execution) &&
		!ConnectionModifiedPlacement(session->connection))
	{
		/*
		 * We already did a modification, but not on the connection that we
		 * just opened, which means we're now going to make modifications
		 * over multiple connections. Activate 2PC!
		 */
		CoordinatedTransactionUse2PC();
	}
}


/*
 * TransactionModifiedDistributedTable returns true if the current transaction already
 * executed a command which modified at least one distributed table in the current
 * transaction.
 */
static bool
TransactionModifiedDistributedTable(DistributedExecution *execution)
{
	/*
	 * We need to explicitly check for isTransaction due to
	 * citus.function_opens_transaction_block flag. When set to false, we
	 * should not be pretending that we're in a coordinated transaction even
	 * if XACT_MODIFICATION_DATA is set. That's why we implemented this workaround.
	 */
	return execution->isTransaction && XactModificationLevel == XACT_MODIFICATION_DATA;
}


/*
 * TransactionStateMachine manages the execution of tasks over a connection.
 */
static void
TransactionStateMachine(WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;

	MultiConnection *connection = session->connection;
	RemoteTransaction *transaction = &(connection->remoteTransaction);
	RemoteTransactionState currentState;

	do {
		currentState = transaction->transactionState;

		if (!CheckConnectionReady(session))
		{
			/* connection is busy, no state transitions to make */
			break;
		}

		switch (currentState)
		{
			case REMOTE_TRANS_INVALID:
			{
				if (execution->isTransaction)
				{
					/* if we're expanding the nodes in a transaction, use 2PC */
					Activate2PCIfModifyingTransactionExpandsToNewNode(session);

					/* need to open a transaction block first */
					StartRemoteTransactionBegin(connection);

					transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
				}
				else
				{
					TaskPlacementExecution *placementExecution = NULL;

					placementExecution = PopPlacementExecution(session);
					if (placementExecution == NULL)
					{
						/*
						 * No tasks are ready to be executed at the moment. But we
						 * still mark the socket readable to get any notices if exists.
						 */
						UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);

						break;
					}

					StartPlacementExecutionOnSession(placementExecution, session);
					transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				}

				UpdateConnectionWaitFlags(session,
										  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
				break;
			}

			case REMOTE_TRANS_SENT_BEGIN:
			case REMOTE_TRANS_CLEARING_RESULTS:
			{
				PGresult *result = NULL;

				result = PQgetResult(connection->pgConn);
				if (result != NULL)
				{
					if (!IsResponseOK(result))
					{
						/* query failures are always hard errors */
						ReportResultError(connection, result, ERROR);
					}

					PQclear(result);

					/* keep consuming results */
					break;
				}

				if (session->currentTask != NULL)
				{
					TaskPlacementExecution *placementExecution = session->currentTask;
					bool succeeded = true;

					/*
					 * Once we finished a task on a connection, we no longer
					 * allow that connection to fail.
					 */
					MarkRemoteTransactionCritical(connection);

					session->currentTask = NULL;

					PlacementExecutionDone(placementExecution, succeeded);

					/* connection is ready to use for executing commands */
					workerPool->idleConnectionCount++;
				}

				/* connection needs to be writeable to send next command */
				UpdateConnectionWaitFlags(session,
										  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

				if (execution->isTransaction)
				{
					transaction->transactionState = REMOTE_TRANS_STARTED;
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_INVALID;
				}
				break;
			}

			case REMOTE_TRANS_STARTED:
			{
				TaskPlacementExecution *placementExecution = NULL;

				placementExecution = PopPlacementExecution(session);
				if (placementExecution == NULL)
				{
					/* no tasks are ready to be executed at the moment */
					UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);
					break;
				}

				StartPlacementExecutionOnSession(placementExecution, session);
				transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				break;
			}

			case REMOTE_TRANS_SENT_COMMAND:
			{
				bool fetchDone = false;
				TaskPlacementExecution *placementExecution = session->currentTask;
				ShardCommandExecution *shardCommandExecution =
					placementExecution->shardCommandExecution;
				bool storeRows = shardCommandExecution->expectResults;

				if (shardCommandExecution->gotResults)
				{
					/* already received results from another replica */
					storeRows = false;
				}

				fetchDone = ReceiveResults(session, storeRows);
				if (!fetchDone)
				{
					break;
				}

				shardCommandExecution->gotResults = true;
				transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
				break;
			}

			default:
			{
				break;
			}
		}
	}

	/* iterate in case we can perform multiple transitions at once */
	while (transaction->transactionState != currentState);
}


/*
 * UpdateConnectionWaitFlags is a wrapper around setting waitFlags of the connection.
 *
 * This function might further improved in a sense that to use use ModifyWaitEvent on
 * waitFlag changes as opposed to what we do now: always rebuild the wait event sets.
 * Our initial benchmarks didn't show any significant performance improvements, but
 * good to keep in mind the potential improvements.
 */
static void
UpdateConnectionWaitFlags(WorkerSession *session, int waitFlags)
{
	MultiConnection *connection = session->connection;
	DistributedExecution *execution = session->workerPool->distributedExecution;

	/* do not take any actions if the flags not changed */
	if (connection->waitFlags == waitFlags)
	{
		return;
	}

	connection->waitFlags = waitFlags;

	/* without signalling the execution, the flag changes won't be reflected */
	execution->waitFlagsChanged = true;
}


/*
 * CheckConnectionReady returns true if the the connection is ready to
 * read or write, or false if it still has bytes to send/receive.
 */
static bool
CheckConnectionReady(WorkerSession *session)
{
	int sendStatus = 0;
	MultiConnection *connection = session->connection;

	ConnStatusType status = PQstatus(connection->pgConn);
	if (status == CONNECTION_BAD)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	/* try to send all pending data */
	sendStatus = PQflush(connection->pgConn);
	if (sendStatus == -1)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}
	else if (sendStatus == 1)
	{
		int waitFlags = session->connection->waitFlags;

		/* more data to send, wait for socket to become writable */
		UpdateConnectionWaitFlags(session, waitFlags | WL_SOCKET_WRITEABLE);

		return false;
	}

	/* if reading fails, there's not much we can do */
	if (PQconsumeInput(connection->pgConn) == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	if (PQisBusy(connection->pgConn))
	{
		int waitFlags = session->connection->waitFlags;

		/* did not get a full result, wait for socket to become readable */
		UpdateConnectionWaitFlags(session, waitFlags | WL_SOCKET_READABLE);

		return false;
	}

	return true;
}


/*
 * PopPlacementExecution returns the next available assigned or unassigned
 * placement execution for the given session.
 */
static TaskPlacementExecution *
PopPlacementExecution(WorkerSession *session)
{
	TaskPlacementExecution *placementExecution = NULL;
	WorkerPool *workerPool = session->workerPool;

	placementExecution = PopAssignedPlacementExecution(session);
	if (placementExecution == NULL)
	{
		if (session->commandsSent > 0 && UseConnectionPerPlacement())
		{
			/*
			 * Only send one command per connection if force_max_query_parallelisation
			 * is enabled, unless it's an assigned placement execution.
			 */
			return NULL;
		}

		/* no more assigned tasks, pick an unassigned task */
		placementExecution = PopUnassignedPlacementExecution(workerPool);
	}

	return placementExecution;
}


/*
 * PopAssignedPlacementExecution finds an executable task from the queue of assigned tasks.
 */
static TaskPlacementExecution *
PopAssignedPlacementExecution(WorkerSession *session)
{
	TaskPlacementExecution *placementExecution = NULL;
	dlist_head *readyTaskQueue = &(session->readyTaskQueue);

	if (dlist_is_empty(readyTaskQueue))
	{
		return NULL;
	}

	placementExecution = dlist_container(TaskPlacementExecution,
										 sessionReadyQueueNode,
										 dlist_pop_head_node(readyTaskQueue));

	return placementExecution;
}


/*
 * PopAssignedPlacementExecution finds an executable task from the queue of assigned tasks.
 */
static TaskPlacementExecution *
PopUnassignedPlacementExecution(WorkerPool *workerPool)
{
	TaskPlacementExecution *placementExecution = NULL;
	dlist_head *readyTaskQueue = &(workerPool->readyTaskQueue);

	if (dlist_is_empty(readyTaskQueue))
	{
		return NULL;
	}

	placementExecution = dlist_container(TaskPlacementExecution,
										 workerReadyQueueNode,
										 dlist_pop_head_node(readyTaskQueue));

	workerPool->readyTaskCount--;

	return placementExecution;
}


static bool
StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
								 WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	ParamListInfo paramListInfo = execution->paramListInfo;
	MultiConnection *connection = session->connection;
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	Task *task = shardCommandExecution->task;
	ShardPlacement *taskPlacement = placementExecution->shardPlacement;
	List *placementAccessList = PlacementAccessListForTask(task, taskPlacement);
	char *queryString = task->queryString;
	int querySent = 0;
	int singleRowMode = 0;

	/*
	 * Make sure that subsequent commands on the same placement
	 * use the same connection.
	 */
	AssignPlacementListToConnection(placementAccessList, connection);

	/* one more command is sent over the session */
	session->commandsSent++;

	if (session->commandsSent == 1)
	{
		/* first time we send a command, consider the connection used (not unused) */
		workerPool->unusedConnectionCount--;
	}

	/* connection is going to be in use */
	workerPool->idleConnectionCount--;

	if (paramListInfo != NULL)
	{
		int parameterCount = paramListInfo->numParams;
		Oid *parameterTypes = NULL;
		const char **parameterValues = NULL;

		/* force evaluation of bound params */
		paramListInfo = copyParamList(paramListInfo);

		ExtractParametersFromParamListInfo(paramListInfo, &parameterTypes,
										   &parameterValues);
		querySent = SendRemoteCommandParams(connection, queryString, parameterCount,
											parameterTypes, parameterValues);
	}
	else
	{
		querySent = SendRemoteCommand(connection, queryString);
	}

	if (querySent == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection->pgConn);
	if (singleRowMode == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	session->currentTask = placementExecution;
	placementExecution->executionState = PLACEMENT_EXECUTION_RUNNING;

	return true;
}


/*
 * PlacementAccessListForTask returns a list of placement accesses for a given
 * task and task placement.
 */
static List *
PlacementAccessListForTask(Task *task, ShardPlacement *taskPlacement)
{
	List *placementAccessList = NIL;
	List *relationShardList = task->relationShardList;
	bool addAnchorAccess = false;
	ShardPlacementAccessType accessType = PLACEMENT_ACCESS_SELECT;

	if (task->taskType == MODIFY_TASK)
	{
		/* DML command */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_DML;
	}
	else if (task->taskType == DDL_TASK || task->taskType == VACUUM_ANALYZE_TASK)
	{
		/* DDL command */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_DDL;
	}
	else if (relationShardList == NIL)
	{
		/* SELECT query that does not touch any shard placements */
		addAnchorAccess = true;
		accessType = PLACEMENT_ACCESS_SELECT;
	}

	if (addAnchorAccess)
	{
		ShardPlacementAccess *placementAccess =
			CreatePlacementAccess(taskPlacement, accessType);

		placementAccessList = lappend(placementAccessList, placementAccess);
	}

	/*
	 * We've already added anchor shardId's placement access to the list. Now,
	 * add the other placements in the relationShardList.
	 */
	if (accessType == PLACEMENT_ACCESS_DDL)
	{
		/*
		 * All relations appearing inter-shard DDL commands should be marked
		 * with DDL access.
		 */
		List *relationShardAccessList =
			BuildPlacementDDLList(taskPlacement->groupId, relationShardList);

		placementAccessList = list_concat(placementAccessList, relationShardAccessList);
	}
	else
	{
		/*
		 * In case of SELECTs or DML's, we add SELECT placement accesses to the
		 * elements in relationShardList. For SELECT queries, it is trivial, since
		 * the query is literally accesses the relationShardList in the same query.
		 *
		 * For DMLs, create placement accesses for placements that appear in a
		 * subselect.
		 */
		List *relationShardAccessList =
			BuildPlacementSelectList(taskPlacement->groupId, relationShardList);

		placementAccessList = list_concat(placementAccessList, relationShardAccessList);
	}

	return placementAccessList;
}


/*
 * ReceiveResults reads the result of a command or query and writes returned
 * rows to the tuple store of the scan state.
 */
static bool
ReceiveResults(WorkerSession *session, bool storeRows)
{
	MultiConnection *connection = session->connection;
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	DistributedExecutionStats *executionStats = execution->executionStats;
	TupleDesc tupleDescriptor = execution->tupleDescriptor;
	AttInMetadata *attributeInputMetadata = NULL;
	uint32 expectedColumnCount = 0;
	char **columnArray = NULL;
	Tuplestorestate *tupleStore = execution->tupleStore;

	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"ReceiveResults",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);
	if (tupleDescriptor != NULL)
	{
		attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
		expectedColumnCount = tupleDescriptor->natts;
		columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	}


	while (!PQisBusy(connection->pgConn))
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowsProcessed = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection->pgConn);
		if (result == NULL)
		{
			/* no more results */
			return true;
		}

		resultStatus = PQresultStatus(result);
		if (resultStatus == PGRES_COMMAND_OK)
		{
			char *currentAffectedTupleString = PQcmdTuples(result);
			int64 currentAffectedTupleCount = 0;
			ShardCommandExecution *shardCommandExecution =
				session->currentTask->shardCommandExecution;

			/* if there are multiple replicas, make sure to consider only one */
			if (!shardCommandExecution->gotResults && *currentAffectedTupleString != '\0')
			{
				scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
				Assert(currentAffectedTupleCount >= 0);

				execution->rowsProcessed += currentAffectedTupleCount;
			}

			/* no more results */
			return true;
		}
		if (resultStatus == PGRES_TUPLES_OK)
		{
			/* we've already consumed all the tuples, no more results */
			Assert(PQntuples(result) == 0);
			return true;
		}
		else if (resultStatus != PGRES_SINGLE_TUPLE)
		{
			/* query failures are always hard errors */
			ReportResultError(connection, result, ERROR);
		}
		else if (!storeRows)
		{
			/*
			 * Already receieved rows from executing on another shard placement or
			 * doesn't need at all (e.g., DDL).
			 */
			PQclear(result);
			continue;
		}

		rowsProcessed = PQntuples(result);
		columnCount = PQnfields(result);

		if (columnCount != expectedColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected number of columns from worker: %d, "
								   "expected %d",
								   columnCount, expectedColumnCount)));
		}

		for (rowIndex = 0; rowIndex < rowsProcessed; rowIndex++)
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
					if (SubPlanLevel > 0 && executionStats != NULL)
					{
						executionStats->totalIntermediateResultSize += PQgetlength(result,
																				   rowIndex,
																				   columnIndex);
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

			execution->rowsProcessed++;
		}

		PQclear(result);

		if (executionStats != NULL && CheckIfSizeLimitIsExceeded(executionStats))
		{
			ErrorSizeLimitIsExceeded();
		}
	}

	return false;
}


/*
 * WorkerPoolFailed marks a worker pool and all the placement executions scheduled
 * on it as failed.
 */
static void
WorkerPoolFailed(WorkerPool *workerPool)
{
	bool succeeded = false;
	dlist_iter iter;
	ListCell *sessionCell = NULL;

	/* a pool cannot fail multiple times */
	Assert (!workerPool->failed);

	dlist_foreach(iter, &workerPool->pendingTaskQueue)
	{
		TaskPlacementExecution *placementExecution =
			dlist_container(TaskPlacementExecution, workerPendingQueueNode, iter.cur);

		PlacementExecutionDone(placementExecution, succeeded);
	}

	dlist_foreach(iter, &workerPool->readyTaskQueue)
	{
		TaskPlacementExecution *placementExecution =
			dlist_container(TaskPlacementExecution, workerReadyQueueNode, iter.cur);

		PlacementExecutionDone(placementExecution, succeeded);
	}

	foreach(sessionCell, workerPool->sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);

		WorkerSessionFailed(session);
	}

	/* we do not want more connections in this pool */
	workerPool->readyTaskCount = 0;
	workerPool->failed = true;

	/*
	 * The reason is that when replication factor is > 1 and we are performing
	 * a SELECT, then we only establish connections for the specific placements
	 * that we will read from. However, when a worker pool fails, we will need
	 * to establish multiple new connection to other workers and the query
	 * can only succeed if all those connections are established.
	 */
	if (UseConnectionPerPlacement())
	{
		ListCell *workerCell = NULL;
		List *workerList = workerPool->distributedExecution->workerList;

		foreach(workerCell, workerList)
		{
			WorkerPool *pool = (WorkerPool *) lfirst(workerCell);

			/* failed pools or pools without any connection attempts ignored */
			if (pool->failed || pool->poolStartTime == 0)
			{
				continue;
			}

			/*
			 * This should give another NodeConnectionTimeout until all
			 * the necessary connections are established.
			 */
			pool->poolStartTime = GetCurrentTimestamp();
		}
	}
}


/*
 * WorkerSessionFailed marks all placement executions scheduled on the
 * connection as failed.
 */
static void
WorkerSessionFailed(WorkerSession *session)
{
	TaskPlacementExecution *placementExecution = session->currentTask;
	bool succeeded = false;
	dlist_iter iter;

	if (placementExecution != NULL)
	{
		/* connection failed while a task was active */
		PlacementExecutionDone(placementExecution, succeeded);
	}

	dlist_foreach(iter, &session->pendingTaskQueue)
	{
		placementExecution =
			dlist_container(TaskPlacementExecution, sessionPendingQueueNode, iter.cur);

		PlacementExecutionDone(placementExecution, succeeded);
	}

	dlist_foreach(iter, &session->readyTaskQueue)
	{
		placementExecution =
			dlist_container(TaskPlacementExecution, sessionReadyQueueNode, iter.cur);

		PlacementExecutionDone(placementExecution, succeeded);
	}
}


/*
 * PlacementExecutionDone marks the given placement execution as done when
 * the results have been received or a failure occurred and sets the succeeded
 * flag accordingly. It also adds other placement executions of the same
 * task to the appropriate ready queues.
 */
static void
PlacementExecutionDone(TaskPlacementExecution *placementExecution, bool succeeded)
{
	WorkerPool *workerPool = placementExecution->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	TaskExecutionState executionState = shardCommandExecution->executionState;
	TaskExecutionState newExecutionState = TASK_EXECUTION_NOT_FINISHED;

	/* mark the placement execution as finished */
	if (succeeded)
	{
		placementExecution->executionState = PLACEMENT_EXECUTION_FINISHED;
	}
	else
	{
		if (ShouldMarkPlacementsInvalidOnFailure(execution))
		{
			ShardPlacement *shardPlacement = placementExecution->shardPlacement;

			/*
			 * We only set shard state if its current state is FILE_FINALIZED, which
			 * prevents overwriting shard state if it is already set at somewhere else.
			 */
			if (shardPlacement->shardState == FILE_FINALIZED)
			{
				UpdateShardPlacementState(shardPlacement->placementId, FILE_INACTIVE);
			}
		}

		placementExecution->executionState = PLACEMENT_EXECUTION_FAILED;
	}

	if (executionState != TASK_EXECUTION_NOT_FINISHED)
	{
		/*
		 * Task execution has already been finished, no need to continue the
		 * next placement.
		 */
		return;
	}

	/*
	 * Update unfinishedTaskCount only when state changes from not finished to
	 * finished or failed state.
	 */
	newExecutionState = TaskExecutionStateMachine(shardCommandExecution);
	if (newExecutionState == TASK_EXECUTION_FINISHED)
	{
		execution->unfinishedTaskCount--;
		return;
	}
	else if (newExecutionState == TASK_EXECUTION_FAILED)
	{
		execution->unfinishedTaskCount--;

		/*
		 * Even if a single task execution fails, there is no way to
		 * successfully finish the execution.
		 */
		execution->failed = true;
		return;
	}
	else
	{
		ScheduleNextPlacementExecution(placementExecution, succeeded);
	}
}


/*
 * ScheduleNextPlacementExecution is triggered if the query needs to be
 * executed on any or all placements in order and there is a placement on
 * which the execution has not happened yet. If so make that placement
 * ready-to-start by adding it to the appropriate queue.
 */
static void
ScheduleNextPlacementExecution(TaskPlacementExecution *placementExecution, bool succeeded)
{
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	PlacementExecutionOrder executionOrder = shardCommandExecution->executionOrder;

	if ((executionOrder == EXECUTION_ORDER_ANY && !succeeded) ||
		executionOrder == EXECUTION_ORDER_SEQUENTIAL)
	{
		TaskPlacementExecution *nextPlacementExecution = NULL;
		int placementExecutionCount = shardCommandExecution->placementExecutionCount;

		/* find a placement execution that is not yet marked as failed */
		do {
			int nextPlacementExecutionIndex =
				placementExecution->placementExecutionIndex + 1;

			if (nextPlacementExecutionIndex == placementExecutionCount)
			{
				/*
				 * TODO: We should not need to reset nextPlacementExecutionIndex.
				 * However, we're currently not handling failed worker pools
				 * and sessions very well. Thus, reset the execution index.
				 */
				nextPlacementExecutionIndex = 0;
			}

			/* if all tasks failed then we should already have errored out */
			Assert(nextPlacementExecutionIndex < placementExecutionCount);

			/* get the next placement in the planning order */
			nextPlacementExecution =
				shardCommandExecution->placementExecutions[nextPlacementExecutionIndex];

			if (nextPlacementExecution->executionState == PLACEMENT_EXECUTION_NOT_READY)
			{
				/* move the placement execution to the ready queue */
				PlacementExecutionReady(nextPlacementExecution);
			}
		} while (nextPlacementExecution->executionState == PLACEMENT_EXECUTION_FAILED);
	}
}


/*
 * ShouldMarkPlacementsInvalidOnFailure returns true if the failure
 * should trigger marking placements invalid.
 */
static bool
ShouldMarkPlacementsInvalidOnFailure(DistributedExecution *execution)
{
	if (!DistributedExecutionModifiesDatabase(execution) || execution->errorOnAnyFailure)
	{
		/*
		 * Failures that do not modify the database (e.g., mainly SELECTs) should
		 * never lead to invalid placement.
		 *
		 * Failures that lead throwing error, no need to mark any placement
		 * invalid.
		 */
		return false;
	}

	return true;
}


/*
 * PlacementExecutionReady adds a placement execution to the ready queue when
 * its dependent placement executions have finished.
 */
static void
PlacementExecutionReady(TaskPlacementExecution *placementExecution)
{
	WorkerPool *workerPool = placementExecution->workerPool;

	if (placementExecution->assignedSession != NULL)
	{
		WorkerSession *session = placementExecution->assignedSession;
		MultiConnection *connection = session->connection;
		RemoteTransaction *transaction = &(connection->remoteTransaction);
		RemoteTransactionState transactionState = transaction->transactionState;

		if (placementExecution->executionState == PLACEMENT_EXECUTION_NOT_READY)
		{
			/* remove from not-ready task queue */
			dlist_delete(&placementExecution->sessionPendingQueueNode);

			/* add to ready-to-start task queue */
			dlist_push_tail(&session->readyTaskQueue,
							&placementExecution->sessionReadyQueueNode);
		}

		if (transactionState == REMOTE_TRANS_INVALID ||
			transactionState == REMOTE_TRANS_STARTED)
		{
			/*
			 * If the connection is idle, wake it up by checking whether
			 * the connection is writeable.
			 */
			UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
		}
	}
	else
	{
		ListCell *sessionCell = NULL;

		if (placementExecution->executionState == PLACEMENT_EXECUTION_NOT_READY)
		{
			/* remove from not-ready task queue */
			dlist_delete(&placementExecution->workerPendingQueueNode);

			/* add to ready-to-start task queue */
			dlist_push_tail(&workerPool->readyTaskQueue,
							&placementExecution->workerReadyQueueNode);
		}

		workerPool->readyTaskCount++;

		/* wake up an idle connection by checking whether the connection is writeable */
		foreach(sessionCell, workerPool->sessionList)
		{
			WorkerSession *session = lfirst(sessionCell);
			MultiConnection *connection = session->connection;
			RemoteTransaction *transaction = &(connection->remoteTransaction);
			RemoteTransactionState transactionState = transaction->transactionState;

			if (transactionState == REMOTE_TRANS_INVALID ||
				transactionState == REMOTE_TRANS_STARTED)
			{
				UpdateConnectionWaitFlags(session,
										  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

				break;
			}
		}
	}

	/* update the state to ready for further processing */
	placementExecution->executionState = PLACEMENT_EXECUTION_READY;
}


/*
 * TaskExecutionStateMachine returns whether a shard command execution
 * finished or failed according to its execution order. If the task is
 * already finished, simply return the state. Else, calculate the state
 * and return it.
 */
static TaskExecutionState
TaskExecutionStateMachine(ShardCommandExecution *shardCommandExecution)
{
	PlacementExecutionOrder executionOrder = shardCommandExecution->executionOrder;
	int donePlacementCount = 0;
	int failedPlacementCount = 0;
	int placementCount = 0;
	int placementExecutionIndex = 0;
	int placementExecutionCount = shardCommandExecution->placementExecutionCount;
	TaskExecutionState currentTaskExecutionState = shardCommandExecution->executionState;

	if (currentTaskExecutionState != TASK_EXECUTION_NOT_FINISHED)
	{
		/* we've already calculated the state, simply return it */
		return currentTaskExecutionState;
	}

	for (; placementExecutionIndex < placementExecutionCount; placementExecutionIndex++)
	{
		TaskPlacementExecution *placementExecution =
			shardCommandExecution->placementExecutions[placementExecutionIndex];
		TaskPlacementExecutionState executionState = placementExecution->executionState;

		if (executionState == PLACEMENT_EXECUTION_FINISHED)
		{
			donePlacementCount++;
		}
		else if (executionState == PLACEMENT_EXECUTION_FAILED)
		{
			failedPlacementCount++;
		}

		placementCount++;
	}

	if (failedPlacementCount == placementCount)
	{
		currentTaskExecutionState = TASK_EXECUTION_FAILED;
	}
	else if (executionOrder == EXECUTION_ORDER_ANY && donePlacementCount > 0)
	{
		currentTaskExecutionState = TASK_EXECUTION_FINISHED;
	}
	else if (donePlacementCount + failedPlacementCount == placementCount)
	{
		currentTaskExecutionState = TASK_EXECUTION_FINISHED;
	}
	else
	{
		currentTaskExecutionState = TASK_EXECUTION_NOT_FINISHED;
	}

	shardCommandExecution->executionState = currentTaskExecutionState;

	return shardCommandExecution->executionState;
}


/*
 * BuildWaitEventSet creates a WaitEventSet for the given array of connections
 * which can be used to wait for any of the sockets to become read-ready or
 * write-ready.
 */
static WaitEventSet *
BuildWaitEventSet(List *sessionList)
{
	WaitEventSet *waitEventSet = NULL;
	ListCell *sessionCell = NULL;

	/* additional 2 is for postmaster and latch */
	int eventSetSize = list_length(sessionList) + 2;

	waitEventSet =
		CreateWaitEventSet(CurrentMemoryContext, eventSetSize);

	foreach(sessionCell, sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);
		MultiConnection *connection = session->connection;
		int socket = 0;

		if (connection->pgConn == NULL)
		{
			/* connection died earlier in the transaction */
			continue;
		}

		if (connection->waitFlags == 0)
		{
			/* not currently waiting for this connection */
			continue;
		}

		socket = PQsocket(connection->pgConn);
		if (socket == -1)
		{
			/* connection was closed */
			continue;
		}

		AddWaitEventToSet(waitEventSet, connection->waitFlags, socket, NULL,
						  (void *) session);
	}

	AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	return waitEventSet;
}

