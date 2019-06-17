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
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
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
	/* distributed query plan */
	DistributedPlan *plan;

	/* custom scan state */
	CitusScanState *scanState;

	/* list of workers involved in the execution */
	List *workerList;

	/* list of all connections used for distributed execution */
	List *sessionList;

	/*
	 * Flag to indiciate that the number of connections has changed
	 * and the WaitEventSet needs to be rebuilt.
	 */
	bool rebuildWaitEventSet;

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

	/* total number of rows received from shard commands */
	uint64 rowCount;

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

	/*
	 * Keep track of how many connections are ready for execution, in
	 * order to (efficiently) know whether more connections to the worker
	 * are needed.
	 */
	int idleConnectionCount;

	/* number of failed connections */
	int failedConnectionCount;

	/* number of failed connection attempts */
	int failedConnectionAttempts;

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
} WorkerPool;

struct TaskPlacementExecution;

/*
 * WorkerSession represents a session on a worker that can execute tasks
 * (sequentially).
 */
typedef struct WorkerSession
{
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

	/* job of which the task is part */
	Job *job;

	/* order in which the command should be replicated on replicas */
	PlacementExecutionOrder executionOrder;

	/* executions of the command on the placements of the shard */
	struct TaskPlacementExecution **placementExecutions;
	int placementExecutionCount;

	/*
	 * RETURNING results from other shard placements can be ignored
	 * after we got results from the first placements.
	 */
	bool gotResults;

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


/* local functions */
static DistributedExecution * CreateDistributedExecution(DistributedPlan *distributedPlan,
														 CitusScanState *scanState,
														 int targetPoolSize);
static void StartDistributedExecution(DistributedExecution *execution);
static void RunDistributedExecution(DistributedExecution *execution);
static void FinishDistributedExecution(DistributedExecution *execution);

static void AssignTasksToConnections(DistributedExecution *execution);
static PlacementExecutionOrder ExecutionOrderForTask(Job *job, Task *task);
static WorkerPool * FindOrCreateWorkerPool(DistributedExecution *execution,
										   WorkerNode *workerNode);
static WorkerSession * FindOrCreateWorkerSession(WorkerPool *workerPool,
												 MultiConnection *connection);
static void ManageWorkerPool(WorkerPool *workerPool);
static WaitEventSet * BuildWaitEventSet(List *sessionList);
static TaskPlacementExecution * PopPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopAssignedPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopUnassignedPlacementExecution(WorkerPool *workerPool);
static bool StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
											 WorkerSession *session);
static List * PlacementAccessListForTask(Task *task, ShardPlacement *taskPlacement);
static void ConnectionStateMachine(WorkerSession *session);
static void TransactionStateMachine(WorkerSession *session);
static bool CheckConnectionReady(MultiConnection *connection);
static bool ReceiveResults(WorkerSession *session, bool storeRows);
static void WorkerSessionFailed(WorkerSession *session);
static void WorkerPoolFailed(WorkerPool *workerPool);
static void PlacementExecutionDone(TaskPlacementExecution *placementExecution,
								   bool succeeded);
static void PlacementExecutionReady(TaskPlacementExecution *placementExecution);
static TaskExecutionState GetTaskExecutionState(
	ShardCommandExecution *shardCommandExecution);


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
		EState *executorState = scanState->customScanState.ss.ps.state;
		bool randomAccess = true;
		bool interTransactions = false;

		/* we are taking locks on partitions of partitioned tables */
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

		ExecuteSubPlans(distributedPlan);

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

		execution = CreateDistributedExecution(distributedPlan, scanState, 4);

		StartDistributedExecution(execution);
		RunDistributedExecution(execution);

		executorState->es_processed = execution->rowCount;

		FinishDistributedExecution(execution);

		if (IsTransactionBlock() || distributedPlan->operation != CMD_SELECT)
		{
			BeginOrContinueCoordinatedTransaction();
		}

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * CreateDistributedExecution creates a distributed execution data structure for
 * a distributed plan.
 */
DistributedExecution *
CreateDistributedExecution(DistributedPlan *distributedPlan,
						   CitusScanState *scanState, int targetPoolSize)
{
	DistributedExecution *execution = NULL;
	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;

	/* TODO: move to nicer place, add other conditions */
	if (IsTransactionBlock() || distributedPlan->operation != CMD_SELECT)
	{
		BeginOrContinueCoordinatedTransaction();
	}


	execution = (DistributedExecution *) palloc0(sizeof(DistributedExecution));
	execution->plan = distributedPlan;
	execution->scanState = scanState;

	execution->workerList = NIL;
	execution->sessionList = NIL;
	execution->targetPoolSize = targetPoolSize;

	execution->totalTaskCount = list_length(taskList);
	execution->unfinishedTaskCount = list_length(taskList);
	execution->rowCount = 0;

	execution->raiseInterrupts = true;
	execution->isTransaction = InCoordinatedTransaction();

	return execution;
}


/*
 * StartDistributedExecution opens connections for distributed execution and
 * assigns each task with shard placements that have previously been modified
 * in the current transaction to the connection that modified them.
 */
void
StartDistributedExecution(DistributedExecution *execution)
{
	AssignTasksToConnections(execution);
}


/*
 * FinishDistributedExecution cleans up resources associated with a
 * distributed execution. In particular, it releases connections and
 * clears their state.
 */
void
FinishDistributedExecution(DistributedExecution *execution)
{
	ListCell *sessionCell = NULL;

	/* always trigger wait event set in the first round */
	foreach(sessionCell, execution->sessionList)
	{
		WorkerSession *session = lfirst(sessionCell);
		MultiConnection *connection = session->connection;
    	RemoteTransaction *transaction = &(connection->remoteTransaction);
	    RemoteTransactionState transactionState = transaction->transactionState;

		UnclaimConnection(connection);

		if (connection->connectionState == MULTI_CONNECTION_CONNECTING)
		{
			ShutdownConnection(connection);
		}
		else if (transactionState == REMOTE_TRANS_CLEARING_RESULTS)
		{
			/* TODO: should we keep this in the state machine? or cancel? */
			ClearResults(connection, false);
		}

		/* TODO: can we be in SENT_COMMAND? */
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
	DistributedPlan *distributedPlan = execution->plan;
	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;
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
		shardCommandExecution->job = job;
		shardCommandExecution->executionOrder = ExecutionOrderForTask(job, task);
		shardCommandExecution->placementExecutions = (TaskPlacementExecution **)
			palloc0(placementExecutionCount * sizeof(TaskPlacementExecution *));
		shardCommandExecution->placementExecutionCount = placementExecutionCount;

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
				RemoteTransaction *transaction = &(connection->remoteTransaction);

				/*
				 * Note: We may get the same connection for multiple task placements.
				 * FindOrCreateWorkerSession ensures that we only have one session per
				 * connection.
				 */
				WorkerSession *session = FindOrCreateWorkerSession(workerPool,
																   connection);

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

				/*
				 * Not all parts of the code set connection state. For now we set
				 * it explicitly here.
				 */
				connection->connectionState = MULTI_CONNECTION_CONNECTED;

				/* always poll the connection in the first round */
				connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;

				/* keep track of how many connections are ready */
				transaction = &(connection->remoteTransaction);
				if (transaction->transactionState == REMOTE_TRANS_STARTED)
				{
					workerPool->idleConnectionCount++;
				}
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
 * ExecutionOrderForTask gives the appropriate execution order for a task.
 */
static PlacementExecutionOrder
ExecutionOrderForTask(Job *job, Task *task)
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
			Query *query = job->jobQuery;

			if (query->commandType == CMD_INSERT && !task->upsertQuery)
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

	foreach(sessionCell, workerPool->sessionList)
	{
		session = lfirst(sessionCell);

		if (session->connection == connection)
		{
			return session;
		}
	}

	session = (WorkerSession *) palloc0(sizeof(WorkerSession));
	session->connection = connection;
	session->workerPool = workerPool;
	dlist_init(&session->pendingTaskQueue);
	dlist_init(&session->readyTaskQueue);

	workerPool->sessionList = lappend(workerPool->sessionList, session);
	execution->sessionList = lappend(execution->sessionList, session);

	return session;
}


/*
 * RunDistributedExecution runs a distributed execution to completion. It
 * creates a wait event set to listen for events on any of the connections
 * and runs the connection state machine when a connection has an event.
 */
void
RunDistributedExecution(DistributedExecution *execution)
{
	WaitEventSet *waitEventSet = NULL;
	int workerCount = list_length(execution->workerList);

	/* allocate events for the maximum number of connections to avoid realloc */
	WaitEvent *events = palloc0(execution->totalTaskCount * workerCount *
								sizeof(WaitEvent));

	PG_TRY();
	{
		bool cancellationReceived = false;

		while (execution->unfinishedTaskCount > 0 && !cancellationReceived)
		{
			long timeout = 1000;
			int connectionCount = list_length(execution->sessionList);
			int eventCount = 0;
			int eventIndex = 0;
			ListCell *workerCell = NULL;

			foreach(workerCell, execution->workerList)
			{
				WorkerPool *workerPool = lfirst(workerCell);
				ManageWorkerPool(workerPool);
			}

			waitEventSet = BuildWaitEventSet(execution->sessionList);

			/* wait for I/O events */
#if (PG_VERSION_NUM >= 100000)
			eventCount = WaitEventSetWait(waitEventSet, timeout, events,
										  connectionCount + 2, WAIT_EVENT_CLIENT_READ);
#else
			eventCount = WaitEventSetWait(waitEventSet, timeout, events,
										  connectionCount + 2);
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

			FreeWaitEventSet(waitEventSet);
		}

		pfree(events);
	}
	PG_CATCH();
	{
		/* make sure the epoll file descriptor is always closed */
		if (waitEventSet != NULL)
		{
			FreeWaitEventSet(waitEventSet);
		}

		pfree(events);

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
	int currentConnectionCount = 0;
	int idleConnectionCount = workerPool->idleConnectionCount;
	int readyTaskCount = workerPool->readyTaskCount;
	int newConnectionCount = 0;
	int connectionIndex = 0;

	currentConnectionCount = list_length(workerPool->sessionList);
	if (currentConnectionCount >= targetPoolSize)
	{
		/* already reached the minimal pool size */
		return;
	}

	if (readyTaskCount - idleConnectionCount <= currentConnectionCount)
	{
		/* after assigning tasks to idle connections we don't need more connections */
		return;
	}

	/*
	 * Open enough connections to handle all tasks that are ready, but no more
	 * than the target pool size.
	 */
	newConnectionCount = Min(readyTaskCount - idleConnectionCount, targetPoolSize) -
						 currentConnectionCount;

	elog(DEBUG4, "opening %d new connections to %s:%d", newConnectionCount,
		 workerNode->workerName, workerNode->workerPort);

	for (connectionIndex = 0; connectionIndex < newConnectionCount; connectionIndex++)
	{
		MultiConnection *connection = NULL;
		int connectionFlags = SESSION_LIFESPAN;

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

		/* always poll the connection in the first round */
		connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;

		/* keep track of how many connections are ready */
		if (connection->remoteTransaction.transactionState == REMOTE_TRANS_STARTED)
		{
			workerPool->idleConnectionCount++;
		}

		/* create a session for the connection */
		FindOrCreateWorkerSession(workerPool, connection);
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
			case MULTI_CONNECTION_CONNECTING:
			{
				PostgresPollingStatusType pollMode;

				ConnStatusType status = PQstatus(connection->pgConn);
				if (status == CONNECTION_OK)
				{
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
					break;
				}
				else if (pollMode == PGRES_POLLING_READING)
				{
					connection->waitFlags = WL_SOCKET_READABLE;
					break;
				}
				else if (pollMode == PGRES_POLLING_WRITING)
				{
					connection->waitFlags = WL_SOCKET_WRITEABLE;
					break;
				}
				else
				{
					connection->waitFlags = WL_SOCKET_WRITEABLE;
					connection->connectionState = MULTI_CONNECTION_CONNECTED;
					break;
				}

				break;
			}

			case MULTI_CONNECTION_CONNECTED:
			{
				/* connection is ready, run the transaction state machine */
				TransactionStateMachine(session);
				break;
			}

			case MULTI_CONNECTION_FAILED:
			{
				MarkRemoteTransactionFailed(connection, false);

				/* a task has failed */
				if (execution->failed)
				{
					ReportConnectionError(connection, ERROR);
				}
				else
				{
					ReportConnectionError(connection, WARNING);
				}

				WorkerSessionFailed(session);

				workerPool->failedConnectionCount++;

				/* try next placement for all tasks on worker */
				/* TODO: what if we have some healthy connections? */
				/* TODO: setting */
				if (workerPool->failedConnectionCount == 2)
				{
					WorkerPoolFailed(workerPool);
				}

				/* remove the connection */
				UnclaimConnection(connection);
				ShutdownConnection(connection);

				/* remove connection from wait event set */
				execution->rebuildWaitEventSet = true;
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

		if (!CheckConnectionReady(connection))
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
					/* need to open a transaction block first */
					StartRemoteTransactionBegin(connection);

					transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
				}
				else
				{
					TaskPlacementExecution *placementExecution = NULL;

					/* connection is currently idle */
					workerPool->idleConnectionCount++;

					placementExecution = PopPlacementExecution(session);
					if (placementExecution == NULL)
					{
						/* no tasks are ready to be executed at the moment */
						break;
					}

					StartPlacementExecutionOnSession(placementExecution, session);
					transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				}

				connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
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

					session->currentTask = NULL;

					PlacementExecutionDone(placementExecution, succeeded);
				}

				/* connection is ready to use for executing commands */
				workerPool->idleConnectionCount++;

				/* connection needs to be writeable to send next command */
				connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;

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
					connection->waitFlags = WL_SOCKET_READABLE;
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
				bool storeRows = !shardCommandExecution->gotResults;

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
 * CheckConnectionReady returns true if the the connection is ready to
 * read or write, or false if it still has bytes to send/receive.
 */
static bool
CheckConnectionReady(MultiConnection *connection)
{
	int sendStatus = 0;

	ConnStatusType status = PQstatus(connection->pgConn);
	if (status == CONNECTION_BAD)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}

	/* try to send all pending data */
	sendStatus = PQflush(connection->pgConn);
	if (sendStatus == -1)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}
	else if (sendStatus == 1)
	{
		/* more data to send, wait for socket to become writable */
		connection->waitFlags |= WL_SOCKET_WRITEABLE;
		return false;
	}

	/* if reading fails, there's not much we can do */
	if (PQconsumeInput(connection->pgConn) == 0)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}

	if (PQisBusy(connection->pgConn))
	{
		/* did not get a full result, wait for socket to become readable */
		connection->waitFlags |= WL_SOCKET_READABLE;
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
	CitusScanState *scanState = execution->scanState;
	ParamListInfo paramListInfo =
		scanState->customScanState.ss.ps.state->es_param_list_info;

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
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection->pgConn);
	if (singleRowMode == 0)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
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

	placementAccessList = BuildPlacementSelectList(taskPlacement->groupId,
												   relationShardList);

	if (addAnchorAccess)
	{
		ShardPlacementAccess *placementAccess =
			CreatePlacementAccess(taskPlacement, accessType);

		placementAccessList = lappend(placementAccessList, placementAccess);
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
	CitusScanState *scanState = execution->scanState;
	TupleDesc tupleDescriptor =
		scanState->customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	Tuplestorestate *tupleStore = NULL;

	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"ReceiveResults",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	tupleStore = scanState->tuplestorestate;

	while (!PQisBusy(connection->pgConn))
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
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

			if (*currentAffectedTupleString != '\0')
			{
				scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
				Assert(currentAffectedTupleCount >= 0);
			}

			execution->rowCount += currentAffectedTupleCount;
			return true;
		}
		if (resultStatus == PGRES_TUPLES_OK)
		{
			execution->rowCount += PQntuples(result);
			return true;
		}
		else if (resultStatus != PGRES_SINGLE_TUPLE)
		{
			/* query failures are always hard errors */
			ReportResultError(connection, result, ERROR);
		}
		else if (!storeRows)
		{
			/* already receieved rows from executing on another shard placement */
			PQclear(result);
			continue;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);

		if (columnCount != expectedColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected number of columns from worker: %d, "
								   "expected %d",
								   columnCount, expectedColumnCount)));
		}

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

			execution->rowCount++;
		}

		PQclear(result);
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

	/* no more tasks to do after the pool failed */
	workerPool->readyTaskCount = 0;
}


/*
 * WorkerSessionFailed marks all placement executions scheduled on the
 * connection as failed.
 */
static void
WorkerSessionFailed(WorkerSession *session)
{
	bool succeeded = false;
	dlist_iter iter;

	dlist_foreach(iter, &session->pendingTaskQueue)
	{
		TaskPlacementExecution *placementExecution =
			dlist_container(TaskPlacementExecution, sessionPendingQueueNode, iter.cur);

		PlacementExecutionDone(placementExecution, succeeded);
	}

	dlist_foreach(iter, &session->readyTaskQueue)
	{
		TaskPlacementExecution *placementExecution =
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
	ShardCommandExecution *shardCommandExecution = NULL;
	TaskExecutionState executionState;
	PlacementExecutionOrder executionOrder;
	int placementExecutionCount = 0;

	TaskPlacementExecutionState currentPlacementExecutionState =
		placementExecution->executionState;

	/* mark the placement execution as finished */
	if (succeeded)
	{
		placementExecution->executionState = PLACEMENT_EXECUTION_FINISHED;
	}
	else
	{
		placementExecution->executionState = PLACEMENT_EXECUTION_FAILED;
	}

	shardCommandExecution = placementExecution->shardCommandExecution;
	executionOrder = shardCommandExecution->executionOrder;

	executionState = GetTaskExecutionState(shardCommandExecution);
	if (executionState == TASK_EXECUTION_FINISHED)
	{
		execution->unfinishedTaskCount--;
		return;
	}

	if (executionState == TASK_EXECUTION_FAILED)
	{
		execution->unfinishedTaskCount--;
		execution->failed = true;
		return;
	}

	placementExecutionCount = shardCommandExecution->placementExecutionCount;

	/*
	 * If the query needs to be executed on any or all placements in order
	 * and there is a placement left, then make that placement ready-to-start
	 * by adding it to the appropriate queue.
	 */
	if ((executionOrder == EXECUTION_ORDER_ANY ||
		 executionOrder == EXECUTION_ORDER_SEQUENTIAL) &&
		currentPlacementExecutionState == PLACEMENT_EXECUTION_RUNNING)
	{
		TaskPlacementExecution *nextPlacementExecution = NULL;

		/* find a placement execution that is not yet marked as failed */
		do
		{
			int nextPlacementExecutionIndex =
				placementExecution->placementExecutionIndex + 1;

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
		}
		while (nextPlacementExecution->executionState == PLACEMENT_EXECUTION_FAILED);
	}
}


/*
 * PlacementExecutionReady adds a placement execution to the ready queue when
 * its dependent placement executions have finished.
 */
static void
PlacementExecutionReady(TaskPlacementExecution *placementExecution)
{
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
			connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
		}
	}
	else
	{
		WorkerPool *workerPool = placementExecution->workerPool;
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
				connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
				break;
			}
		}
	}
}


/*
 * GetTaskExecutionState returns whether a shard command execution
 * finished or failed according to its execution order.
 */
static TaskExecutionState
GetTaskExecutionState(ShardCommandExecution *shardCommandExecution)
{
	PlacementExecutionOrder executionOrder = shardCommandExecution->executionOrder;
	int doneTaskCount = 0;
	int failedTaskCount = 0;
	int taskCount = 0;
	int placementExecutionIndex = 0;
	int placementExecutionCount = shardCommandExecution->placementExecutionCount;

	for (; placementExecutionIndex < placementExecutionCount; placementExecutionIndex++)
	{
		TaskPlacementExecution *placementExecution =
			shardCommandExecution->placementExecutions[placementExecutionIndex];
		TaskPlacementExecutionState executionState = placementExecution->executionState;

		if (executionState == PLACEMENT_EXECUTION_FINISHED)
		{
			doneTaskCount++;
		}
		else if (executionState == PLACEMENT_EXECUTION_FAILED)
		{
			failedTaskCount++;
		}

		taskCount++;
	}

	if (failedTaskCount == taskCount)
	{
		return TASK_EXECUTION_FAILED;
	}
	else if (executionOrder == EXECUTION_ORDER_ANY && doneTaskCount > 0)
	{
		return TASK_EXECUTION_FINISHED;
	}
	else if (doneTaskCount + failedTaskCount == taskCount)
	{
		return TASK_EXECUTION_FINISHED;
	}
	else
	{
		return TASK_EXECUTION_NOT_FINISHED;
	}
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
	int connectionCount = list_length(sessionList);
	ListCell *sessionCell = NULL;

	/* allocate# connections + 2 for the signal latch and postmaster death */
	waitEventSet = CreateWaitEventSet(CurrentMemoryContext, connectionCount + 2);

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
