/*-------------------------------------------------------------------------
 *
 * adaptive_executor.c
 *
 * The adaptive executor executes a list of tasks (queries on shards) over
 * a connection pool per worker node. The results of the queries, if any,
 * are written to a tuple store.
 *
 * The concepts in the executor are modelled in a set of structs:
 *
 * - DistributedExecution:
 *     Execution of a Task list over a set of WorkerPools.
 * - WorkerPool
 *     Pool of WorkerSessions for the same worker which opportunistically
 *     executes "unassigned" tasks from a queue.
 * - WorkerSession:
 *     Connection to a worker that is used to execute "assigned" tasks
 *     from a queue and may execute unassigned tasks from the WorkerPool.
 * - ShardCommandExecution:
 *     Execution of a Task across a list of placements.
 * - TaskPlacementExecution:
 *     Execution of a Task on a specific placement.
 *     Used in the WorkerPool and WorkerSession queues.
 *
 * Every connection pool (WorkerPool) and every connection (WorkerSession)
 * have a queue of tasks that are ready to execute (readyTaskQueue) and a
 * queue/set of pending tasks that may become ready later in the execution
 * (pendingTaskQueue). The tasks are wrapped in a ShardCommandExecution,
 * which keeps track of the state of execution and is referenced from a
 * TaskPlacementExecution, which is the data structure that is actually
 * added to the queues and describes the state of the execution of a task
 * on a particular worker node.
 *
 * When the task list is part of a bigger distributed transaction, the
 * shards that are accessed or modified by the task may have already been
 * accessed earlier in the transaction. We need to make sure we use the
 * same connection since it may hold relevant locks or have uncommitted
 * writes. In that case we "assign" the task to a connection by adding
 * it to the task queue of specific connection (in
 * AssignTasksToConnectionsOrWorkerPool). Otherwise we consider the task
 * unassigned and add it to the task queue of a worker pool, which means
 * that it can be executed over any connection in the pool.
 *
 * A task may be executed on multiple placements in case of a reference
 * table or a replicated distributed table. Depending on the type of
 * task, it may not be ready to be executed on a worker node immediately.
 * For instance, INSERTs on a reference table are executed serially across
 * placements to avoid deadlocks when concurrent INSERTs take conflicting
 * locks. At the beginning, only the "first" placement is ready to execute
 * and therefore added to the readyTaskQueue in the pool or connection.
 * The remaining placements are added to the pendingTaskQueue. Once
 * execution on the first placement is done the second placement moves
 * from pendingTaskQueue to readyTaskQueue. The same approach is used to
 * fail over read-only tasks to another placement.
 *
 * Once all the tasks are added to a queue, the main loop in
 * RunDistributedExecution repeatedly does the following:
 *
 * For each pool:
 * - ManageWorkPool evaluates whether to open additional connections
 *   based on the number unassigned tasks that are ready to execute
 *   and the targetPoolSize of the execution.
 *
 * Poll all connections:
 * - We use a WaitEventSet that contains all (non-failed) connections
 *   and is rebuilt whenever the set of active connections or any of
 *   their wait flags change.
 *
 *   We almost always check for WL_SOCKET_READABLE because a session
 *   can emit notices at any time during execution, but it will only
 *   wake up WaitEventSetWait when there are actual bytes to read.
 *
 *   We check for WL_SOCKET_WRITEABLE just after sending bytes in case
 *   there is not enough space in the TCP buffer. Since a socket is
 *   almost always writable we also use WL_SOCKET_WRITEABLE as a
 *   mechanism to wake up WaitEventSetWait for non-I/O events, e.g.
 *   when a task moves from pending to ready.
 *
 * For each connection that is ready:
 * - ConnectionStateMachine handles connection establishment and failure
 *   as well as command execution via TransactionStateMachine.
 *
 * When a connection is ready to execute a new task, it first checks its
 * own readyTaskQueue and otherwise takes a task from the worker pool's
 * readyTaskQueue (on a first-come-first-serve basis).
 *
 * In cases where the tasks finish quickly (e.g. <1ms), a single
 * connection will often be sufficient to finish all tasks. It is
 * therefore not necessary that all connections are established
 * successfully or open a transaction (which may be blocked by an
 * intermediate pgbouncer in transaction pooling mode). It is therefore
 * essential that we take a task from the queue only after opening a
 * transaction block.
 *
 * When a command on a worker finishes or the connection is lost, we call
 * PlacementExecutionDone, which then updates the state of the task
 * based on whether we need to run it on other placements. When a
 * connection fails or all connections to a worker fail, we also call
 * PlacementExecutionDone for all queued tasks to try the next placement
 * and, if necessary, mark shard placements as inactive. If a task fails
 * to execute on all placements, the execution fails and the distributed
 * transaction rolls back.
 *
 * For multi-row INSERTs, tasks are executed sequentially by
 * SequentialRunDistributedExecution instead of in parallel, which allows
 * a high degree of concurrency without high risk of deadlocks.
 * Conversely, multi-row UPDATE/DELETE/DDL commands take aggressive locks
 * which forbids concurrency, but allows parallelism without high risk
 * of deadlocks. Note that this is unrelated to SEQUENTIAL_CONNECTION,
 * which indicates that we should use at most one connection per node, but
 * can run tasks in parallel across nodes. This is used when there are
 * writes to a reference table that has foreign keys from a distributed
 * table.
 *
 * Execution finishes when all tasks are done, the query errors out, or
 * the user cancels the query.
 *
 *-------------------------------------------------------------------------
 */

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "lib/ilist.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "distributed/adaptive_executor.h"
#include "distributed/backend_data.h"
#include "distributed/cancel_utils.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/executor_util.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/param_utils.h"
#include "distributed/placement_access.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/resource_lock.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/stats/stat_counters.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_identifier.h"
#include "distributed/transaction_management.h"
#include "distributed/tuple_destination.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"

#define SLOW_START_DISABLED 0


/*
 * DistributedExecution represents the execution of a distributed query
 * plan.
 */
typedef struct DistributedExecution
{
	/* the corresponding distributed plan's modLevel */
	RowModifyLevel modLevel;

	/*
	 * remoteAndLocalTaskList contains all the tasks required to finish the
	 * execution. remoteTaskList contains all the tasks required to
	 * finish the remote execution. localTaskList contains all the
	 * local tasks required to finish the local execution.
	 *
	 * remoteAndLocalTaskList is the union of remoteTaskList and localTaskList.
	 */
	List *remoteAndLocalTaskList;
	List *remoteTaskList;
	List *localTaskList;

	/*
	 * If a task specific destination is not provided for a task, then use
	 * defaultTupleDest.
	 */
	TupleDestination *defaultTupleDest;

	/* Parameters for parameterized plans. Can be NULL. */
	ParamListInfo paramListInfo;

	/* list of workers involved in the execution */
	List *workerList;

	/* list of all connections used for distributed execution */
	List *sessionList;

	/*
	 * Flag to indiciate that the set of connections we are interested
	 * in has changed and waitEventSet needs to be rebuilt.
	 */
	bool rebuildWaitEventSet;

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
	 *
	 * Another reason for keeping these here is to cache a single
	 * WaitEventSet/WaitEvent within the execution pair until we
	 * need to rebuild the waitEvents.
	 */
	WaitEventSet *waitEventSet;
	WaitEvent *events;
	int eventSetSize;

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

	/* transactional properties of the current execution */
	TransactionProperties *transactionProperties;

	/* indicates whether distributed execution has failed */
	bool failed;

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

	/*
	 * The following fields are used while receiving results from remote nodes.
	 * We store this information here to avoid re-allocating it every time.
	 *
	 * columnArray field is reset/calculated per row, so might be useless for
	 * other contexts. The benefit of keeping it here is to avoid allocating
	 * the array over and over again.
	 */
	uint32 allocatedColumnCount;
	void **columnArray;
	StringInfoData *stringInfoDataArray;

	/*
	 * jobIdList contains all jobs in the job tree, this is used to
	 * do cleanup for repartition queries.
	 */
	List *jobIdList;

	/*
	 * Indicates whether we can execute tasks locally during distributed
	 * execution. In other words, this flag must be set to false when
	 * executing a command that we surely know that local execution would
	 * fail, such as CREATE INDEX CONCURRENTLY.
	 */
	bool localExecutionSupported;
} DistributedExecution;


/*
 * WorkerPoolFailureState indicates the current state of the
 * pool.
 */
typedef enum WorkerPoolFailureState
{
	/* safe to continue execution*/
	WORKER_POOL_NOT_FAILED,

	/* if a pool fails, the execution fails */
	WORKER_POOL_FAILED,

	/*
	 * The remote execution over the pool failed, but we failed over
	 * to the local execution and still finish the execution.
	 */
	WORKER_POOL_FAILED_OVER_TO_LOCAL
} WorkerPoolFailureState;

/*
 * WorkerPool represents a pool of sessions on the same worker.
 *
 * A WorkerPool has two queues containing the TaskPlacementExecutions that need
 * to be executed on the worker.
 *
 * TaskPlacementExecutions that are ready to execute are in readyTaskQueue.
 * TaskPlacementExecutions that may need to be executed once execution on
 * another worker finishes or fails are in pendingTaskQueue.
 *
 * In TransactionStateMachine, the sessions opportunistically take
 * TaskPlacementExecutions from the readyQueue when they are ready and have no
 * assigned tasks.
 *
 * We track connection timeouts per WorkerPool. When the first connection is
 * established we set the poolStartTime and if no connection can be established
 * before NodeConnectionTime, the WorkerPool fails. There is some specialised
 * logic in case citus.force_max_query_parallelization is enabled because we
 * may fail to establish a connection per placement after already establishing
 * some connections earlier in the execution.
 *
 * A WorkerPool fails if all connection attempts failed or all connections
 * are lost. In that case, all TaskPlacementExecutions in the queues are
 * marked as failed in PlacementExecutionDone, which typically causes the
 * task and therefore the distributed execution to fail. In case of a
 * replicated table or a SELECT on a reference table, the remaining placements
 * will be tried by moving them from a pendingTaskQueue to a readyTaskQueue.
 */
typedef struct WorkerPool
{
	/* distributed execution in which the worker participates */
	DistributedExecution *distributedExecution;

	/* worker node on which we have a pool of sessions */
	char *nodeName;
	int nodePort;

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
	 * connection and ready to start.
	 */
	dlist_head readyTaskQueue;
	int readyTaskCount;

	/*
	 * We keep this for enforcing the connection timeouts. In our definition, a pool
	 * starts when the first connection establishment starts.
	 */
	instr_time poolStartTime;

	/* indicates whether to check for the connection timeout */
	bool checkForPoolTimeout;

	/* last time we opened a connection */
	instr_time lastConnectionOpenTime;

	/* maximum number of connections we are allowed to open at once */
	uint32 maxNewConnectionsPerCycle;

	/*
	 * Set to true if the pool is to local node. We use this value to
	 * avoid re-calculating often.
	 */
	bool poolToLocalNode;

	/*
	 * This is only set in WorkerPoolFailed() function. Once a pool fails, we do not
	 * use it anymore.
	 */
	WorkerPoolFailureState failureState;

	/* execution statistics per pool, in microseconds */
	uint64 totalTaskExecutionTime;
	int totalExecutedTasks;
} WorkerPool;

struct TaskPlacementExecution;

/*
 * WorkerSession represents a session on a worker that can execute tasks
 * (sequentially) and is part of a WorkerPool.
 *
 * Each WorkerSession has two queues containing TaskPlacementExecutions that
 * need to be executed within this particular session because the session
 * accessed the same or co-located placements earlier in the transaction.
 *
 * TaskPlacementExecutions that are ready to execute are in readyTaskQueue.
 * TaskPlacementExecutions that may need to be executed once execution on
 * another worker finishes or fails are in pendingTaskQueue.
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

	/* index in the wait event set */
	int waitEventSetIndex;

	/* events reported by the latest call to WaitEventSetWait */
	int latestUnconsumedWaitEvents;

	/* for some restricted scenarios, we allow a single connection retry */
	bool connectionRetried;

	/* keep track of if the session has an active connection */
	bool sessionHasActiveConnection;
} WorkerSession;


/* GUC, determining whether Citus opens 1 connection per task */
bool ForceMaxQueryParallelization = false;
int MaxAdaptiveExecutorPoolSize = 16;
bool EnableBinaryProtocol = true;

/* GUC, number of ms to wait between opening connections to the same worker */
int ExecutorSlowStartInterval = 10;
bool EnableCostBasedConnectionEstablishment = true;
bool PreventIncompleteConnectionEstablishment = true;


/*
 * TaskExecutionState indicates whether or not a command on a shard
 * has finished, or whether it has failed.
 */
typedef enum TaskExecutionState
{
	TASK_EXECUTION_NOT_FINISHED,
	TASK_EXECUTION_FINISHED,
	TASK_EXECUTION_FAILED,
	TASK_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION
} TaskExecutionState;

/*
 * PlacementExecutionOrder indicates whether a command should be executed
 * on any replica, on all replicas sequentially (in order), or on all
 * replicas in parallel. In other words, EXECUTION_ORDER_ANY is used for
 * SELECTs, EXECUTION_ORDER_SEQUENTIAL/EXECUTION_ORDER_PARALLEL is used for
 * DML/DDL.
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

	/* cached AttInMetadata for task */
	AttInMetadata **attributeInputMetadata;

	/* indicates whether the attributeInputMetadata has binary or text
	 * encoding/decoding functions */
	bool binaryResults;

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

	TaskExecutionState executionState;

	/*
	 * Indicates whether given shard command can be executed locally on
	 * placements. Normally determined by DistributedExecution's same field.
	 */
	bool localExecutionSupported;
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
	PLACEMENT_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION,
	PLACEMENT_EXECUTION_FAILED
} TaskPlacementExecutionState;

/*
 * TaskPlacementExecution represents the execution of a command
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

	/*
	 * Task query can contain multiple queries. queryIndex tracks results of
	 * which query we are waiting for.
	 */
	uint32 queryIndex;

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

	/* execution time statistics for this placement execution */
	instr_time startTime;
	instr_time endTime;
} TaskPlacementExecution;

extern MemoryContext SubPlanExplainAnalyzeContext;

/* local functions */
static DistributedExecution * CreateDistributedExecution(RowModifyLevel modLevel,
														 List *taskList,
														 ParamListInfo paramListInfo,
														 int targetPoolSize,
														 TupleDestination *
														 defaultTupleDest,
														 TransactionProperties *
														 xactProperties,
														 List *jobIdList,
														 bool localExecutionSupported);
static TransactionProperties DecideTransactionPropertiesForTaskList(RowModifyLevel
																	modLevel,
																	List *taskList,
																	bool
																	exludeFromTransaction);
static void StartDistributedExecution(DistributedExecution *execution);
static void RunLocalExecution(CitusScanState *scanState, DistributedExecution *execution);
static void RunDistributedExecution(DistributedExecution *execution);
static void SequentialRunDistributedExecution(DistributedExecution *execution);
static void FinishDistributedExecution(DistributedExecution *execution);
static void CleanUpSessions(DistributedExecution *execution);

static bool DistributedExecutionModifiesDatabase(DistributedExecution *execution);
static void AssignTasksToConnectionsOrWorkerPool(DistributedExecution *execution);
static void UnclaimAllSessionConnections(List *sessionList);
static PlacementExecutionOrder ExecutionOrderForTask(RowModifyLevel modLevel, Task *task);
static WorkerPool * FindOrCreateWorkerPool(DistributedExecution *execution,
										   char *nodeName, int nodePort);
static WorkerSession * FindOrCreateWorkerSession(WorkerPool *workerPool,
												 MultiConnection *connection);
static void ManageWorkerPool(WorkerPool *workerPool);
static bool ShouldWaitForSlowStart(WorkerPool *workerPool);
static int CalculateNewConnectionCount(WorkerPool *workerPool);
static bool UsingExistingSessionsCheaperThanEstablishingNewConnections(int
																	   readyTaskCount,
																	   WorkerPool *
																	   workerPool);
static double AvgTaskExecutionTimeApproximation(WorkerPool *workerPool);
static double AvgConnectionEstablishmentTime(WorkerPool *workerPool);
static void OpenNewConnections(WorkerPool *workerPool, int newConnectionCount,
							   TransactionProperties *transactionProperties);
static void CheckConnectionTimeout(WorkerPool *workerPool);
static void MarkEstablishingSessionsTimedOut(WorkerPool *workerPool);
static int UsableConnectionCount(WorkerPool *workerPool);
static long NextEventTimeout(DistributedExecution *execution);
static WaitEventSet * BuildWaitEventSet(List *sessionList);
static void FreeExecutionWaitEvents(DistributedExecution *execution);
static void AddSessionToWaitEventSet(WorkerSession *session,
									 WaitEventSet *waitEventSet);
static void RebuildWaitEventSetFlags(WaitEventSet *waitEventSet, List *sessionList);
static TaskPlacementExecution * PopPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopAssignedPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopUnassignedPlacementExecution(WorkerPool *workerPool);
static bool StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
											 WorkerSession *session);
static bool SendNextQuery(TaskPlacementExecution *placementExecution,
						  WorkerSession *session);
static void ConnectionStateMachine(WorkerSession *session);
static bool HasUnfinishedTaskForSession(WorkerSession *session);
static void HandleMultiConnectionSuccess(WorkerSession *session, bool newConnection);
static bool HasAnyConnectionFailure(WorkerPool *workerPool);
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
static bool CanFailoverPlacementExecutionToLocalExecution(TaskPlacementExecution *
														  placementExecution);
static void PlacementExecutionReady(TaskPlacementExecution *placementExecution);
static TaskExecutionState TaskExecutionStateMachine(ShardCommandExecution *
													shardCommandExecution);
static int GetEventSetSize(List *sessionList);
static bool ProcessSessionsWithFailedWaitEventSetOperations(
	DistributedExecution *execution);
static bool HasIncompleteConnectionEstablishment(DistributedExecution *execution);
static void RebuildWaitEventSet(DistributedExecution *execution);
static void RebuildWaitEventSetForSessions(DistributedExecution *execution);
static void AddLatchWaitEventToExecution(DistributedExecution *execution);
static void ProcessWaitEvents(DistributedExecution *execution, WaitEvent *events, int
							  eventCount, bool *cancellationReceived);
static void RemoteSocketClosedForAnySession(DistributedExecution *execution);
static void ProcessWaitEventsForSocketClosed(WaitEvent *events, int eventCount);
static long MillisecondsBetweenTimestamps(instr_time startTime, instr_time endTime);
static uint64 MicrosecondsBetweenTimestamps(instr_time startTime, instr_time endTime);
static int WorkerPoolCompare(const void *lhsKey, const void *rhsKey);
static void SetAttributeInputMetadata(DistributedExecution *execution,
									  ShardCommandExecution *shardCommandExecution);
static ExecutionParams * CreateDefaultExecutionParams(RowModifyLevel modLevel,
													  List *taskList,
													  TupleDestination *tupleDest,
													  bool expectResults,
													  ParamListInfo paramListInfo);


/*
 * AdaptiveExecutorPreExecutorRun gets called right before postgres starts its executor
 * run. Given that the result of our subplans would be evaluated before the first call to
 * the exec function of our custom scan we make sure our subplans have executed before.
 */
void
AdaptiveExecutorPreExecutorRun(CitusScanState *scanState)
{
	if (scanState->finishedPreScan)
	{
		/*
		 * Cursors (and hence RETURN QUERY syntax in pl/pgsql functions)
		 * may trigger AdaptiveExecutorPreExecutorRun() on every fetch
		 * operation. Though, we should only execute PreScan once.
		 */
		return;
	}

	DistributedPlan *distributedPlan = scanState->distributedPlan;

	/*
	 * PostgreSQL takes locks on all partitions in the executor. It's not entirely
	 * clear why this is necessary (instead of locking the parent during DDL), but
	 * we do the same for consistency.
	 */
	LockPartitionsForDistributedPlan(distributedPlan);

	ExecuteSubPlans(distributedPlan, RequestedForExplainAnalyze(scanState));

	scanState->finishedPreScan = true;
}


/*
 * AdaptiveExecutor is called via CitusExecScan on the
 * first call of CitusExecScan. The function fills the tupleStore
 * of the input scanScate.
 */
TupleTableSlot *
AdaptiveExecutor(CitusScanState *scanState)
{
	TupleTableSlot *resultSlot = NULL;

	DistributedPlan *distributedPlan = scanState->distributedPlan;
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	bool randomAccess = true;
	bool interTransactions = false;
	int targetPoolSize = MaxAdaptiveExecutorPoolSize;
	List *jobIdList = NIL;

	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;

	/* we should only call this once before the scan finished */
	Assert(!scanState->finishedRemoteScan);

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "AdaptiveExecutor",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);


	/* Reset Task fields that are only valid for a single execution */
	ResetExplainAnalyzeData(taskList);

	scanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	TupleDestination *defaultTupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate, tupleDescriptor);

	bool localExecutionSupported = true;

	/*
	 * When running a distributed plan—either the root plan or a subplan’s
	 * distributed fragment—we need to know if we’re under EXPLAIN ANALYZE.
	 * Subplans can’t receive the EXPLAIN ANALYZE flag directly, so we use
	 * SubPlanExplainAnalyzeContext as a flag to indicate that context.
	 */
	if (RequestedForExplainAnalyze(scanState) || SubPlanExplainAnalyzeContext)
	{
		/*
		 * We use multiple queries per task in EXPLAIN ANALYZE which need to
		 * be part of the same transaction.
		 */
		UseCoordinatedTransaction();
		taskList = ExplainAnalyzeTaskList(taskList, defaultTupleDest, tupleDescriptor,
										  paramListInfo);

		/*
		 * Multiple queries per task is not supported with local execution. See the Assert in
		 * TupleDestDestReceiverReceive.
		 */
		localExecutionSupported = false;
	}

	bool hasDependentJobs = job->dependentJobList != NIL;
	if (hasDependentJobs)
	{
		/* jobs use intermediate results, which require a distributed transaction */
		UseCoordinatedTransaction();

		jobIdList = ExecuteDependentTasks(taskList, job);
	}

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		/* defer decision after ExecuteSubPlans() */
		targetPoolSize = 1;
	}

	bool excludeFromXact = false;

	TransactionProperties xactProperties = DecideTransactionPropertiesForTaskList(
		distributedPlan->modLevel, taskList, excludeFromXact);

	/*
	 * In some rare cases, we have prepared statements that pass a parameter
	 * and never used in the query, mark such parameters' type as Invalid(0),
	 * which will be used later in ExtractParametersFromParamList() to map them
	 * to a generic datatype. Skip for dynamic parameters.
	 */
	if (paramListInfo && !paramListInfo->paramFetch)
	{
		paramListInfo = copyParamList(paramListInfo);
		MarkUnreferencedExternParams((Node *) job->jobQuery, paramListInfo);
	}

	DistributedExecution *execution = CreateDistributedExecution(
		distributedPlan->modLevel,
		taskList,
		paramListInfo,
		targetPoolSize,
		defaultTupleDest,
		&xactProperties,
		jobIdList,
		localExecutionSupported);

	/*
	 * Make sure that we acquire the appropriate locks even if the local tasks
	 * are going to be executed with local execution.
	 */
	StartDistributedExecution(execution);

	if (ShouldRunTasksSequentially(execution->remoteTaskList))
	{
		SequentialRunDistributedExecution(execution);
	}
	else
	{
		RunDistributedExecution(execution);
	}

	/* execute tasks local to the node (if any) */
	if (list_length(execution->localTaskList) > 0)
	{
		/* now execute the local tasks */
		RunLocalExecution(scanState, execution);
	}

	CmdType commandType = job->jobQuery->commandType;
	if (commandType != CMD_SELECT)
	{
		executorState->es_processed = execution->rowsProcessed;
	}

	FinishDistributedExecution(execution);

	if (SortReturning && distributedPlan->expectResults && commandType != CMD_SELECT)
	{
		SortTupleStore(scanState);
	}

	MemoryContextSwitchTo(oldContext);

	return resultSlot;
}


/*
 * RunLocalExecution runs the localTaskList in the execution, fills the tuplestore
 * and sets the es_processed if necessary.
 *
 * It also sorts the tuplestore if there are no remote tasks remaining.
 */
static void
RunLocalExecution(CitusScanState *scanState, DistributedExecution *execution)
{
	EState *estate = ScanStateGetExecutorState(scanState);
	bool isUtilityCommand = false;
	uint64 rowsProcessed = ExecuteLocalTaskListExtended(execution->localTaskList,
														estate->es_param_list_info,
														scanState->distributedPlan,
														execution->defaultTupleDest,
														isUtilityCommand);

	execution->rowsProcessed += rowsProcessed;
}


/*
 * ExecuteUtilityTaskList is a wrapper around executing task
 * list for utility commands.
 */
uint64
ExecuteUtilityTaskList(List *utilityTaskList, bool localExecutionSupported)
{
	RowModifyLevel modLevel = ROW_MODIFY_NONE;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		modLevel, utilityTaskList, MaxAdaptiveExecutorPoolSize, localExecutionSupported
		);
	executionParams->xactProperties =
		DecideTransactionPropertiesForTaskList(modLevel, utilityTaskList, false);
	executionParams->isUtilityCommand = true;

	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteUtilityTaskListExtended is a wrapper around executing task
 * list for utility commands.
 */
uint64
ExecuteUtilityTaskListExtended(List *utilityTaskList, int poolSize,
							   bool localExecutionSupported)
{
	RowModifyLevel modLevel = ROW_MODIFY_NONE;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		modLevel, utilityTaskList, poolSize, localExecutionSupported
		);

	bool excludeFromXact = false;
	executionParams->xactProperties =
		DecideTransactionPropertiesForTaskList(modLevel, utilityTaskList,
											   excludeFromXact);
	executionParams->isUtilityCommand = true;

	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteTaskList is a proxy to ExecuteTaskListExtended
 * with defaults for some of the arguments.
 */
uint64
ExecuteTaskList(RowModifyLevel modLevel, List *taskList)
{
	bool localExecutionSupported = true;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		modLevel, taskList, MaxAdaptiveExecutorPoolSize, localExecutionSupported
		);

	bool excludeFromXact = false;
	executionParams->xactProperties = DecideTransactionPropertiesForTaskList(
		modLevel, taskList, excludeFromXact);

	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteTaskListOutsideTransaction is a proxy to ExecuteTaskListExtended
 * with defaults for some of the arguments.
 */
uint64
ExecuteTaskListOutsideTransaction(RowModifyLevel modLevel, List *taskList,
								  int targetPoolSize, List *jobIdList)
{
	/*
	 * As we are going to run the tasks outside transaction, we shouldn't use local execution.
	 * However, there is some problem when using local execution related to
	 * repartition joins, when we solve that problem, we can execute the tasks
	 * coming to this path with local execution. See PR:3711
	 */
	bool localExecutionSupported = false;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		modLevel, taskList, targetPoolSize, localExecutionSupported
		);

	executionParams->xactProperties = DecideTransactionPropertiesForTaskList(
		modLevel, taskList, true);
	return ExecuteTaskListExtended(executionParams);
}


/*
 * CreateDefaultExecutionParams returns execution params based on given (possibly null)
 * bind params (presumably from executor state) with defaults for some of the arguments.
 */
static ExecutionParams *
CreateDefaultExecutionParams(RowModifyLevel modLevel, List *taskList,
							 TupleDestination *tupleDest,
							 bool expectResults,
							 ParamListInfo paramListInfo)
{
	int targetPoolSize = MaxAdaptiveExecutorPoolSize;
	bool localExecutionSupported = true;
	ExecutionParams *executionParams = CreateBasicExecutionParams(
		modLevel, taskList, targetPoolSize, localExecutionSupported
		);

	executionParams->xactProperties = DecideTransactionPropertiesForTaskList(
		modLevel, taskList, false);
	executionParams->expectResults = expectResults;
	executionParams->tupleDestination = tupleDest;
	executionParams->paramListInfo = paramListInfo;

	return executionParams;
}


/*
 * ExecuteTaskListIntoTupleDestWithParam is a proxy to ExecuteTaskListExtended() which uses
 * bind params from executor state, and with defaults for some of the arguments.
 */
uint64
ExecuteTaskListIntoTupleDestWithParam(RowModifyLevel modLevel, List *taskList,
									  TupleDestination *tupleDest,
									  bool expectResults,
									  ParamListInfo paramListInfo)
{
	ExecutionParams *executionParams = CreateDefaultExecutionParams(modLevel, taskList,
																	tupleDest,
																	expectResults,
																	paramListInfo);
	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteTaskListIntoTupleDest is a proxy to ExecuteTaskListExtended() with defaults
 * for some of the arguments.
 */
uint64
ExecuteTaskListIntoTupleDest(RowModifyLevel modLevel, List *taskList,
							 TupleDestination *tupleDest,
							 bool expectResults)
{
	ParamListInfo paramListInfo = NULL;
	ExecutionParams *executionParams = CreateDefaultExecutionParams(modLevel, taskList,
																	tupleDest,
																	expectResults,
																	paramListInfo);
	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteTaskListExtended sets up the execution for given task list and
 * runs it.
 */
uint64
ExecuteTaskListExtended(ExecutionParams *executionParams)
{
	/* if there are no tasks to execute, we can return early */
	if (list_length(executionParams->taskList) == 0)
	{
		return 0;
	}

	uint64 locallyProcessedRows = 0;

	TupleDestination *defaultTupleDest = executionParams->tupleDestination;

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		executionParams->targetPoolSize = 1;
	}

	DistributedExecution *execution =
		CreateDistributedExecution(
			executionParams->modLevel, executionParams->taskList,
			executionParams->paramListInfo, executionParams->targetPoolSize,
			defaultTupleDest, &executionParams->xactProperties,
			executionParams->jobIdList, executionParams->localExecutionSupported);

	/*
	 * If current transaction accessed local placements and task list includes
	 * tasks that should be executed locally (accessing any of the local placements),
	 * then we should error out as it would cause inconsistencies across the
	 * remote connection and local execution.
	 */
	EnsureCompatibleLocalExecutionState(execution->remoteTaskList);

	/* run the remote execution */
	StartDistributedExecution(execution);
	RunDistributedExecution(execution);
	FinishDistributedExecution(execution);

	/* now, switch back to the local execution */
	if (executionParams->isUtilityCommand)
	{
		locallyProcessedRows += ExecuteLocalUtilityTaskList(execution->localTaskList);
	}
	else
	{
		locallyProcessedRows += ExecuteLocalTaskList(execution->localTaskList,
													 defaultTupleDest);
	}

	return execution->rowsProcessed + locallyProcessedRows;
}


/*
 * CreateBasicExecutionParams creates basic execution parameters with some common
 * fields.
 */
ExecutionParams *
CreateBasicExecutionParams(RowModifyLevel modLevel,
						   List *taskList,
						   int targetPoolSize,
						   bool localExecutionSupported)
{
	ExecutionParams *executionParams = palloc0(sizeof(ExecutionParams));
	executionParams->modLevel = modLevel;
	executionParams->taskList = taskList;
	executionParams->targetPoolSize = targetPoolSize;
	executionParams->localExecutionSupported = localExecutionSupported;

	executionParams->tupleDestination = CreateTupleDestNone();
	executionParams->expectResults = false;
	executionParams->isUtilityCommand = false;
	executionParams->jobIdList = NIL;
	executionParams->paramListInfo = NULL;

	return executionParams;
}


/*
 * CreateDistributedExecution creates a distributed execution data structure for
 * a distributed plan.
 */
static DistributedExecution *
CreateDistributedExecution(RowModifyLevel modLevel, List *taskList,
						   ParamListInfo paramListInfo,
						   int targetPoolSize, TupleDestination *defaultTupleDest,
						   TransactionProperties *xactProperties,
						   List *jobIdList, bool localExecutionSupported)
{
	DistributedExecution *execution =
		(DistributedExecution *) palloc0(sizeof(DistributedExecution));

	execution->modLevel = modLevel;
	execution->remoteAndLocalTaskList = taskList;
	execution->transactionProperties = xactProperties;

	/* we are going to calculate this values below */
	execution->localTaskList = NIL;
	execution->remoteTaskList = NIL;

	execution->paramListInfo = paramListInfo;
	execution->workerList = NIL;
	execution->sessionList = NIL;
	execution->targetPoolSize = targetPoolSize;
	execution->defaultTupleDest = defaultTupleDest;

	execution->rowsProcessed = 0;

	execution->raiseInterrupts = true;

	execution->rebuildWaitEventSet = false;
	execution->waitFlagsChanged = false;

	execution->jobIdList = jobIdList;

	execution->localExecutionSupported = localExecutionSupported;

	/*
	 * Since task can have multiple queries, we are not sure how many columns we should
	 * allocate for. We start with 16, and reallocate when we need more.
	 */
	execution->allocatedColumnCount = 16;
	execution->columnArray = palloc0(execution->allocatedColumnCount * sizeof(void *));
	if (EnableBinaryProtocol)
	{
		/*
		 * Initialize enough StringInfos for each column. These StringInfos
		 * (and thus the backing buffers) will be reused for each row.
		 * We will reference these StringInfos in the columnArray if the value
		 * is not NULL.
		 *
		 * NOTE: StringInfos are always grown in the memory context in which
		 * they were initially created. So appending in any memory context will
		 * result in bufferes that are still valid after removing that memory
		 * context.
		 */
		execution->stringInfoDataArray = palloc0(
			execution->allocatedColumnCount *
			sizeof(StringInfoData));
		for (int i = 0; i < execution->allocatedColumnCount; i++)
		{
			initStringInfo(&execution->stringInfoDataArray[i]);
		}
	}

	if (execution->localExecutionSupported &&
		ShouldExecuteTasksLocally(taskList))
	{
		bool readOnlyPlan = !TaskListModifiesDatabase(modLevel, taskList);
		ExtractLocalAndRemoteTasks(readOnlyPlan, taskList, &execution->localTaskList,
								   &execution->remoteTaskList);
	}
	else
	{
		/*
		 * Get a shallow copy of the list as we rely on remoteAndLocalTaskList
		 * across the execution.
		 */
		execution->remoteTaskList = list_copy(execution->remoteAndLocalTaskList);
	}

	execution->totalTaskCount = list_length(execution->remoteTaskList);
	execution->unfinishedTaskCount = list_length(execution->remoteTaskList);

	return execution;
}


/*
 * DecideTransactionPropertiesForTaskList decides whether to use remote transaction
 * blocks, whether to use 2PC for the given task list, and whether to error on any
 * failure.
 *
 * Since these decisions have specific dependencies on each other (e.g. 2PC implies
 * errorOnAnyFailure, but not the other way around) we keep them in the same place.
 */
static TransactionProperties
DecideTransactionPropertiesForTaskList(RowModifyLevel modLevel, List *taskList, bool
									   exludeFromTransaction)
{
	TransactionProperties xactProperties;

	/* ensure uninitialized padding doesn't escape the function */
	memset_struct_0(xactProperties);
	xactProperties.errorOnAnyFailure = false;
	xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_ALLOWED;
	xactProperties.requires2PC = false;

	if (taskList == NIL)
	{
		/* nothing to do, return defaults */
		return xactProperties;
	}

	if (exludeFromTransaction)
	{
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_DISALLOWED;
		return xactProperties;
	}

	if (TaskListCannotBeExecutedInTransaction(taskList))
	{
		/*
		 * We prefer to error on any failures for CREATE INDEX
		 * CONCURRENTLY or VACUUM//VACUUM ANALYZE (e.g., COMMIT_PROTOCOL_BARE).
		 */
		xactProperties.errorOnAnyFailure = true;
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_DISALLOWED;
		return xactProperties;
	}

	if (TaskListRequiresRollback(taskList))
	{
		/* transaction blocks are required if the task list needs to roll back */
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_REQUIRED;

		if (TaskListRequires2PC(taskList))
		{
			/*
			 * Although using two phase commit protocol is an independent decision than
			 * failing on any error, we prefer to couple them. Our motivation is that
			 * the failures are rare, and we prefer to avoid marking placements invalid
			 * in case of failures.
			 */
			xactProperties.errorOnAnyFailure = true;
			xactProperties.requires2PC = true;
		}
	}
	else if (InCoordinatedTransaction())
	{
		/*
		 * If we are already in a coordinated transaction then transaction blocks
		 * are required even if they are not strictly required for the current
		 * execution.
		 */
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_REQUIRED;
	}

	return xactProperties;
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
	TransactionProperties *xactProperties = execution->transactionProperties;

	if (xactProperties->useRemoteTransactionBlocks == TRANSACTION_BLOCKS_REQUIRED)
	{
		UseCoordinatedTransaction();
	}

	if (xactProperties->requires2PC)
	{
		Use2PCForCoordinatedTransaction();
	}

	/*
	 * Prevent unsafe concurrent modifications of replicated shards by taking
	 * locks.
	 *
	 * When modifying a reference tables in MX mode, we take the lock via RPC
	 * to the first worker in a transaction block, which activates a coordinated
	 * transaction. We need to do this before determining whether the execution
	 * should use transaction blocks (see below).
	 *
	 * We acquire the locks for both the remote and local tasks.
	 */
	AcquireExecutorShardLocksForExecution(execution->modLevel,
										  execution->remoteAndLocalTaskList);

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
		/*
		 * Record the access for both the local and remote tasks. The main goal
		 * is to make sure that Citus behaves consistently even if the local
		 * shards are moved away.
		 */
		RecordParallelRelationAccessForTaskList(execution->remoteAndLocalTaskList);
	}

	/* make sure we are not doing remote execution from within a task */
	if (execution->remoteTaskList != NIL)
	{
		bool isRemote = true;
		EnsureTaskExecutionAllowed(isRemote);
	}
}


/*
 *  DistributedExecutionModifiesDatabase returns true if the execution modifies the data
 *  or the schema.
 */
static bool
DistributedExecutionModifiesDatabase(DistributedExecution *execution)
{
	return TaskListModifiesDatabase(execution->modLevel,
									execution->remoteAndLocalTaskList);
}


/*
 * FinishDistributedExecution cleans up resources associated with a
 * distributed execution.
 */
static void
FinishDistributedExecution(DistributedExecution *execution)
{
	if (DistributedExecutionModifiesDatabase(execution))
	{
		/* prevent copying shards in same transaction */
		XactModificationLevel = XACT_MODIFICATION_DATA;
	}
}


/*
 * AssignTasksToConnectionsOrWorkerPool goes through the list of tasks to determine whether any
 * task placements need to be assigned to particular connections because of preceding
 * operations in the transaction. It then adds those connections to the pool and adds
 * the task placement executions to the assigned task queue of the connection.
 */
static void
AssignTasksToConnectionsOrWorkerPool(DistributedExecution *execution)
{
	RowModifyLevel modLevel = execution->modLevel;
	List *taskList = execution->remoteTaskList;

	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		bool placementExecutionReady = true;
		int placementExecutionIndex = 0;
		int placementExecutionCount = list_length(task->taskPlacementList);

		/*
		 * Execution of a command on a shard, which may have multiple replicas.
		 */
		ShardCommandExecution *shardCommandExecution =
			(ShardCommandExecution *) palloc0(sizeof(ShardCommandExecution));
		shardCommandExecution->task = task;
		shardCommandExecution->executionOrder = ExecutionOrderForTask(modLevel, task);
		shardCommandExecution->executionState = TASK_EXECUTION_NOT_FINISHED;
		shardCommandExecution->localExecutionSupported =
			execution->localExecutionSupported;
		shardCommandExecution->placementExecutions =
			(TaskPlacementExecution **) palloc0(placementExecutionCount *
												sizeof(TaskPlacementExecution *));
		shardCommandExecution->placementExecutionCount = placementExecutionCount;

		SetAttributeInputMetadata(execution, shardCommandExecution);
		ShardPlacement *taskPlacement = NULL;
		foreach_declared_ptr(taskPlacement, task->taskPlacementList)
		{
			int connectionFlags = 0;
			char *nodeName = NULL;
			int nodePort = 0;
			LookupTaskPlacementHostAndPort(taskPlacement, &nodeName, &nodePort);

			WorkerPool *workerPool = FindOrCreateWorkerPool(execution, nodeName,
															nodePort);

			/*
			 * Execution of a command on a shard placement, which may not always
			 * happen if the query is read-only and the shard has multiple placements.
			 */
			TaskPlacementExecution *placementExecution =
				(TaskPlacementExecution *) palloc0(sizeof(TaskPlacementExecution));
			placementExecution->shardCommandExecution = shardCommandExecution;
			placementExecution->shardPlacement = taskPlacement;
			placementExecution->workerPool = workerPool;
			placementExecution->placementExecutionIndex = placementExecutionIndex;
			placementExecution->queryIndex = 0;
			INSTR_TIME_SET_ZERO(placementExecution->startTime);
			INSTR_TIME_SET_ZERO(placementExecution->endTime);

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

			List *placementAccessList = PlacementAccessListForTask(task, taskPlacement);

			MultiConnection *connection = NULL;
			if (execution->transactionProperties->useRemoteTransactionBlocks !=
				TRANSACTION_BLOCKS_DISALLOWED)
			{
				/*
				 * Determine whether the task has to be assigned to a particular connection
				 * due to a preceding access to the placement in the same transaction.
				 */
				connection = GetConnectionIfPlacementAccessedInXact(
					connectionFlags,
					placementAccessList,
					NULL);
			}

			if (connection != NULL)
			{
				/*
				 * Note: We may get the same connection for multiple task placements.
				 * FindOrCreateWorkerSession ensures that we only have one session per
				 * connection.
				 */
				WorkerSession *session =
					FindOrCreateWorkerSession(workerPool, connection);

				ereport(DEBUG4, (errmsg("Session %ld (%s:%d) has an assigned task",
										session->sessionId, connection->hostname,
										connection->port)));

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
	 * We sort the workerList because adaptive connection management
	 * (e.g., OPTIONAL_CONNECTION) requires any concurrent executions
	 * to wait for the connections in the same order to prevent any
	 * starvation. If we don't sort, we might end up with:
	 *      Execution 1: Get connection for worker 1, wait for worker 2
	 *      Execution 2: Get connection for worker 2, wait for worker 1
	 *
	 *  and, none could proceed. Instead, we enforce every execution establish
	 *  the required connections to workers in the same order.
	 */
	execution->workerList = SortList(execution->workerList, WorkerPoolCompare);

	/*
	 * The executor claims connections exclusively to make sure that calls to
	 * StartNodeUserDatabaseConnection do not return the same connections.
	 *
	 * We need to do this after assigning tasks to connections because the same
	 * connection may be be returned multiple times by GetPlacementListConnectionIfCached.
	 */
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, execution->sessionList)
	{
		MultiConnection *connection = session->connection;

		ClaimConnectionExclusively(connection);
	}
}


/*
 * WorkerPoolCompare is based on WorkerNodeCompare function. The function
 * compares two worker nodes by their host name and port number.
 */
static int
WorkerPoolCompare(const void *lhsKey, const void *rhsKey)
{
	const WorkerPool *workerLhs = *(const WorkerPool **) lhsKey;
	const WorkerPool *workerRhs = *(const WorkerPool **) rhsKey;

	return NodeNamePortCompare(workerLhs->nodeName, workerRhs->nodeName,
							   workerLhs->nodePort, workerRhs->nodePort);
}


/*
 * SetAttributeInputMetadata sets attributeInputMetadata in
 * shardCommandExecution for all the queries that are part of its task.
 * This contains the deserialization functions for the tuples that will be
 * received. It also sets binaryResults when applicable.
 */
static void
SetAttributeInputMetadata(DistributedExecution *execution,
						  ShardCommandExecution *shardCommandExecution)
{
	TupleDestination *tupleDest = shardCommandExecution->task->tupleDest ?
								  shardCommandExecution->task->tupleDest :
								  execution->defaultTupleDest;
	uint32 queryCount = shardCommandExecution->task->queryCount;
	shardCommandExecution->attributeInputMetadata = palloc0(queryCount *
															sizeof(AttInMetadata *));

	for (uint32 queryIndex = 0; queryIndex < queryCount; queryIndex++)
	{
		AttInMetadata *attInMetadata = NULL;
		TupleDesc tupleDescriptor = tupleDest->tupleDescForQuery(tupleDest,
																 queryIndex);
		if (tupleDescriptor == NULL)
		{
			attInMetadata = NULL;
		}
		else if (EnableBinaryProtocol && CanUseBinaryCopyFormat(tupleDescriptor))
		{
			attInMetadata = TupleDescGetAttBinaryInMetadata(tupleDescriptor);
			shardCommandExecution->binaryResults = true;
		}
		else
		{
			attInMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
		}

		shardCommandExecution->attributeInputMetadata[queryIndex] = attInMetadata;
	}
}


/*
 * ExecutionOrderForTask gives the appropriate execution order for a task.
 */
static PlacementExecutionOrder
ExecutionOrderForTask(RowModifyLevel modLevel, Task *task)
{
	switch (task->taskType)
	{
		case READ_TASK:
		{
			return EXECUTION_ORDER_ANY;
		}

		case MODIFY_TASK:
		{
			/*
			 * For non-commutative modifications we take aggressive locks, so
			 * there is no risk of deadlock and we can run them in parallel.
			 * When the modification is commutative, we take no additional
			 * locks, so we take a conservative approach and execute sequentially
			 * to avoid deadlocks.
			 */
			if (modLevel < ROW_MODIFY_NONCOMMUTATIVE)
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
		case MAP_TASK:
		case MERGE_TASK:
		case MAP_OUTPUT_FETCH_TASK:
		case MERGE_FETCH_TASK:
		{
			return EXECUTION_ORDER_PARALLEL;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported task type %d in adaptive executor",
								   task->taskType)));
		}
	}
}


/*
 * FindOrCreateWorkerPool gets the pool of connections for a particular worker.
 */
static WorkerPool *
FindOrCreateWorkerPool(DistributedExecution *execution, char *nodeName, int nodePort)
{
	WorkerPool *workerPool = NULL;
	foreach_declared_ptr(workerPool, execution->workerList)
	{
		if (strncmp(nodeName, workerPool->nodeName, WORKER_LENGTH) == 0 &&
			nodePort == workerPool->nodePort)
		{
			return workerPool;
		}
	}

	workerPool = (WorkerPool *) palloc0(sizeof(WorkerPool));
	workerPool->nodeName = pstrdup(nodeName);
	workerPool->nodePort = nodePort;

	WorkerNode *workerNode = FindWorkerNode(nodeName, nodePort);
	if (workerNode)
	{
		workerPool->poolToLocalNode =
			workerNode->groupId == GetLocalGroupId();
	}

	/* "open" connections aggressively when there are cached connections */
	int nodeConnectionCount = MaxCachedConnectionsPerWorker;
	workerPool->maxNewConnectionsPerCycle = Max(1, nodeConnectionCount);

	dlist_init(&workerPool->pendingTaskQueue);
	dlist_init(&workerPool->readyTaskQueue);

	workerPool->distributedExecution = execution;

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
	static uint64 sessionId = 1;

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, workerPool->sessionList)
	{
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
	session->waitEventSetIndex = WAIT_EVENT_SET_INDEX_NOT_INITIALIZED;

	/* always detect closed sockets */
	UpdateConnectionWaitFlags(session, WL_SOCKET_CLOSED);

	dlist_init(&session->pendingTaskQueue);
	dlist_init(&session->readyTaskQueue);

	if (connection->connectionState == MULTI_CONNECTION_CONNECTED)
	{
		/* keep track of how many connections are ready */
		workerPool->activeConnectionCount++;
		workerPool->idleConnectionCount++;

		session->sessionHasActiveConnection = true;
	}

	workerPool->unusedConnectionCount++;

	/*
	 * Record the first connection establishment time to the pool. We need this
	 * to enforce NodeConnectionTimeout.
	 */
	if (list_length(workerPool->sessionList) == 0)
	{
		INSTR_TIME_SET_CURRENT(workerPool->poolStartTime);
		workerPool->checkForPoolTimeout = true;
	}

	workerPool->sessionList = lappend(workerPool->sessionList, session);
	execution->sessionList = lappend(execution->sessionList, session);

	return session;
}


/*
 * RemoteSocketClosedForNewSession is a helper function for detecting whether
 * the remote socket corresponding to the input session is closed. This is
 * mostly common there is a cached connection and remote server restarted
 * (due to failover or restart etc.).
 *
 * The function is not a generic function that can be called at the start of
 * the execution. The function is not generic because it does not check all
 * the events, even ignores cancellation events. Future callers of this
 * function should consider its limitations.
 */
static void
RemoteSocketClosedForAnySession(DistributedExecution *execution)
{
	if (!WaitEventSetCanReportClosed())
	{
		/* we cannot detect for this OS */
		return;
	}

	long timeout = 0;/* don't wait */

	int eventCount = WaitEventSetWait(execution->waitEventSet, timeout, execution->events,
									  execution->eventSetSize, WAIT_EVENT_CLIENT_READ);
	ProcessWaitEventsForSocketClosed(execution->events, eventCount);
}


/*
 * SequentialRunDistributedExecution gets a distributed execution and
 * executes each individual task in the execution sequentially, one
 * task at a time. See related function ShouldRunTasksSequentially()
 * for more detail on the definition of SequentialRun.
 */
static void
SequentialRunDistributedExecution(DistributedExecution *execution)
{
	List *taskList = execution->remoteTaskList;
	int connectionMode = MultiShardConnectionType;

	/*
	 * There are some implicit assumptions about this setting for the sequential
	 * executions, so make sure to set it.
	 */
	MultiShardConnectionType = SEQUENTIAL_CONNECTION;
	Task *taskToExecute = NULL;
	foreach_declared_ptr(taskToExecute, taskList)
	{
		execution->remoteAndLocalTaskList = list_make1(taskToExecute);
		execution->remoteTaskList = list_make1(taskToExecute);
		execution->totalTaskCount = 1;
		execution->unfinishedTaskCount = 1;

		CHECK_FOR_INTERRUPTS();

		if (IsHoldOffCancellationReceived())
		{
			break;
		}

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
	AssignTasksToConnectionsOrWorkerPool(execution);

	PG_TRY();
	{
		/* Preemptively step state machines in case of immediate errors */
		WorkerSession *session = NULL;
		foreach_declared_ptr(session, execution->sessionList)
		{
			ConnectionStateMachine(session);
		}

		bool cancellationReceived = false;

		/* always (re)build the wait event set the first time */
		execution->rebuildWaitEventSet = true;

		/*
		 * Iterate until all the tasks are finished. Once all the tasks
		 * are finished, ensure that all the connection initializations
		 * are also finished. Otherwise, those connections are terminated
		 * abruptly before they are established (or failed). Instead, we let
		 * the ConnectionStateMachine() to properly handle them.
		 *
		 * Note that we could have the connections that are not established
		 * as a side effect of slow-start algorithm. At the time the algorithm
		 * decides to establish new connections, the execution might have tasks
		 * to finish. But, the execution might finish before the new connections
		 * are established.
		 *
		 * Note that the rules explained above could be overriden by any
		 * cancellation to the query. In that case, we terminate the execution
		 * irrespective of the current status of the tasks or the connections.
		 */
		while (!cancellationReceived &&
			   (execution->unfinishedTaskCount > 0 ||
				HasIncompleteConnectionEstablishment(execution)))
		{
			WorkerPool *workerPool = NULL;
			foreach_declared_ptr(workerPool, execution->workerList)
			{
				ManageWorkerPool(workerPool);
			}

			bool skipWaitEvents = false;
			if (execution->remoteTaskList == NIL)
			{
				/*
				 * All the tasks are failed over to the local execution, no need
				 * to wait for any connection activity.
				 */
				continue;
			}
			else if (execution->rebuildWaitEventSet)
			{
				RebuildWaitEventSet(execution);

				skipWaitEvents =
					ProcessSessionsWithFailedWaitEventSetOperations(execution);
			}
			else if (execution->waitFlagsChanged)
			{
				RebuildWaitEventSetFlags(execution->waitEventSet, execution->sessionList);
				execution->waitFlagsChanged = false;

				skipWaitEvents =
					ProcessSessionsWithFailedWaitEventSetOperations(execution);
			}

			if (skipWaitEvents)
			{
				/*
				 * Some operation on the wait event set is failed, retry
				 * as we already removed the problematic connections.
				 */
				execution->rebuildWaitEventSet = true;

				continue;
			}

			/* wait for I/O events */
			long timeout = NextEventTimeout(execution);
			int eventCount =
				WaitEventSetWait(execution->waitEventSet, timeout, execution->events,
								 execution->eventSetSize, WAIT_EVENT_CLIENT_READ);

			ProcessWaitEvents(execution, execution->events, eventCount,
							  &cancellationReceived);
		}

		FreeExecutionWaitEvents(execution);

		CleanUpSessions(execution);
	}
	PG_CATCH();
	{
		/*
		 * We can still recover from error using ROLLBACK TO SAVEPOINT,
		 * unclaim all connections to allow that.
		 */
		UnclaimAllSessionConnections(execution->sessionList);

		FreeExecutionWaitEvents(execution);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ProcessSessionsWithFailedWaitEventSetOperations goes over the session list
 * and processes sessions with failed wait event set operations.
 *
 * Failed sessions are not going to generate any further events, so it is our
 * only chance to process the failure by calling into `ConnectionStateMachine`.
 *
 * The function returns true if any session failed.
 */
static bool
ProcessSessionsWithFailedWaitEventSetOperations(DistributedExecution *execution)
{
	bool foundFailedSession = false;
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, execution->sessionList)
	{
		if (session->waitEventSetIndex == WAIT_EVENT_SET_INDEX_FAILED)
		{
			/*
			 * We can only lost only already connected connections,
			 * others are regular failures.
			 */
			MultiConnection *connection = session->connection;
			if (connection->connectionState == MULTI_CONNECTION_CONNECTED)
			{
				connection->connectionState = MULTI_CONNECTION_LOST;
			}
			else
			{
				connection->connectionState = MULTI_CONNECTION_FAILED;
				IncrementStatCounterForMyDb(STAT_CONNECTION_ESTABLISHMENT_FAILED);
			}


			ConnectionStateMachine(session);

			session->waitEventSetIndex = WAIT_EVENT_SET_INDEX_NOT_INITIALIZED;

			foundFailedSession = true;
		}
	}

	return foundFailedSession;
}


/*
 * HasIncompleteConnectionEstablishment returns true if any of the connections
 * that has been initiated by the executor is in initialization stage.
 */
static bool
HasIncompleteConnectionEstablishment(DistributedExecution *execution)
{
	if (!PreventIncompleteConnectionEstablishment)
	{
		return false;
	}

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, execution->sessionList)
	{
		MultiConnection *connection = session->connection;
		if (connection->connectionState == MULTI_CONNECTION_INITIAL ||
			connection->connectionState == MULTI_CONNECTION_CONNECTING)
		{
			return true;
		}
	}

	return false;
}


/*
 * RebuildWaitEventSet updates the waitEventSet for the distributed execution.
 * This happens when the connection set for the distributed execution is changed,
 * which means that we need to update which connections we wait on for events.
 */
static void
RebuildWaitEventSet(DistributedExecution *execution)
{
	RebuildWaitEventSetForSessions(execution);

	AddLatchWaitEventToExecution(execution);
}


/*
 * AddLatchWaitEventToExecution is a helper function that adds the latch
 * wait event to the execution->waitEventSet. Note that this function assumes
 * that execution->waitEventSet has already allocated enough slot for latch
 * event.
 */
static void
AddLatchWaitEventToExecution(DistributedExecution *execution)
{
	CitusAddWaitEventSetToSet(execution->waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET,
							  MyLatch, NULL);
}


/*
 * RebuildWaitEventSetForSessions re-creates the waitEventSet for the
 * sessions involved in the distributed execution.
 *
 * Most of the time you need RebuildWaitEventSet() which also includes
 * adds the Latch wait event to the set.
 */
static void
RebuildWaitEventSetForSessions(DistributedExecution *execution)
{
	FreeExecutionWaitEvents(execution);

	execution->waitEventSet = BuildWaitEventSet(execution->sessionList);

	execution->eventSetSize = GetEventSetSize(execution->sessionList);
	execution->events = palloc0(execution->eventSetSize * sizeof(WaitEvent));

	CitusAddWaitEventSetToSet(execution->waitEventSet, WL_POSTMASTER_DEATH,
							  PGINVALID_SOCKET, NULL, NULL);

	execution->rebuildWaitEventSet = false;
	execution->waitFlagsChanged = false;
}


/*
 * ProcessWaitEvents processes the received events from connections.
 */
static void
ProcessWaitEvents(DistributedExecution *execution, WaitEvent *events, int eventCount,
				  bool *cancellationReceived)
{
	int eventIndex = 0;

	/* process I/O events */
	for (; eventIndex < eventCount; eventIndex++)
	{
		WaitEvent *event = &events[eventIndex];

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

			if (IsHoldOffCancellationReceived())
			{
				/*
				 * Break out of event loop immediately in case of cancellation.
				 * We cannot use "return" here inside a PG_TRY() block since
				 * then the exception stack won't be reset.
				 */
				*cancellationReceived = true;
			}

			continue;
		}

		WorkerSession *session = (WorkerSession *) event->user_data;
		session->latestUnconsumedWaitEvents = event->events;

		ConnectionStateMachine(session);
	}
}


/*
 * ProcessWaitEventsForSocketClosed mainly checks for WL_SOCKET_CLOSED event.
 * If WL_SOCKET_CLOSED is found, the function sets the underlying connection's
 * state as MULTI_CONNECTION_LOST.
 */
static void
ProcessWaitEventsForSocketClosed(WaitEvent *events, int eventCount)
{
	int eventIndex = 0;

	/* process I/O events */
	for (; eventIndex < eventCount; eventIndex++)
	{
		WaitEvent *event = &events[eventIndex];

		if (event->events & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		WorkerSession *session = (WorkerSession *) event->user_data;
		session->latestUnconsumedWaitEvents = event->events;

		if (session->latestUnconsumedWaitEvents & WL_SOCKET_CLOSED)
		{
			/* let the ConnectionStateMachine handle the rest */
			session->connection->connectionState = MULTI_CONNECTION_LOST;
		}
	}
}


/*
 * ManageWorkerPool ensures the worker pool has the appropriate number of connections
 * based on the number of pending tasks.
 */
static void
ManageWorkerPool(WorkerPool *workerPool)
{
	DistributedExecution *execution = workerPool->distributedExecution;

	/* we do not expand the pool further if there was any failure */
	if (HasAnyConnectionFailure(workerPool))
	{
		return;
	}

	/* we wait until a slow start interval has passed before expanding the pool */
	if (ShouldWaitForSlowStart(workerPool))
	{
		return;
	}

	int newConnectionCount = CalculateNewConnectionCount(workerPool);
	if (newConnectionCount <= 0)
	{
		return;
	}

	/* increase the open rate every cycle (like TCP slow start) */
	workerPool->maxNewConnectionsPerCycle += 1;

	OpenNewConnections(workerPool, newConnectionCount, execution->transactionProperties);

	/*
	 * Cannot establish new connections to the local host, most probably because the
	 * local node cannot accept new connections (e.g., hit max_connections). Switch
	 * the tasks to the local execution.
	 *
	 * We prefer initiatedConnectionCount over the new connection establishments happen
	 * in this iteration via OpenNewConnections(). The reason is that it is expected for
	 * OpenNewConnections() to not open any new connections as long as the connections
	 * are optional (e.g., the second or later connections in the pool). But, for
	 * initiatedConnectionCount to be zero, the connection to the local pool should have
	 * been failed.
	 */
	int initiatedConnectionCount = list_length(workerPool->sessionList);
	if (initiatedConnectionCount == 0)
	{
		/*
		 * Only the pools to the local node are allowed to have optional
		 * connections for the first connection. Hence, initiatedConnectionCount
		 * could only be zero for poolToLocalNode. For other pools, the connection
		 * manager would wait until it gets at least one connection.
		 */
		Assert(workerPool->poolToLocalNode);

		WorkerPoolFailed(workerPool);

		if (execution->failed)
		{
			const char *errHint =
				execution->localExecutionSupported ?
				"This command supports local execution. Consider enabling "
				"local execution using SET citus.enable_local_execution "
				"TO true;" :
				"Consider using a higher value for max_connections";

			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("the total number of connections on the "
								   "server is more than max_connections(%d)",
								   MaxConnections),
							errhint("%s", errHint)));
		}

		return;
	}

	INSTR_TIME_SET_CURRENT(workerPool->lastConnectionOpenTime);
}


/*
 * HasAnyConnectionFailure returns true if worker pool has failed,
 * or connection timed out or we have a failure in connections.
 */
static bool
HasAnyConnectionFailure(WorkerPool *workerPool)
{
	if (workerPool->failureState == WORKER_POOL_FAILED ||
		workerPool->failureState == WORKER_POOL_FAILED_OVER_TO_LOCAL)
	{
		/* connection pool failed */
		return true;
	}

	/* we might fail the execution or warn the user about connection timeouts */
	if (workerPool->checkForPoolTimeout)
	{
		CheckConnectionTimeout(workerPool);
	}

	int failedConnectionCount = workerPool->failedConnectionCount;
	if (failedConnectionCount >= 1)
	{
		/* do not attempt to open more connections after one failed */
		return true;
	}
	return false;
}


/*
 * ShouldWaitForSlowStart returns true if we should wait before
 * opening a new connection because of slow start algorithm.
 */
static bool
ShouldWaitForSlowStart(WorkerPool *workerPool)
{
	/* if we can use a connection per placement, we don't need to wait for slowstart */
	if (UseConnectionPerPlacement())
	{
		return false;
	}

	/* if slow start is disabled, we can open new connections */
	if (ExecutorSlowStartInterval == SLOW_START_DISABLED)
	{
		return false;
	}

	double milliSecondsPassedSince = MillisecondsPassedSince(
		workerPool->lastConnectionOpenTime);
	if (milliSecondsPassedSince < ExecutorSlowStartInterval)
	{
		return true;
	}

	/*
	 * Refrain from establishing new connections unless we have already
	 * finalized all the earlier connection attempts. This prevents unnecessary
	 * load on the remote nodes and emulates the TCP slow-start algorithm.
	 */
	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int finalizedConnectionCount =
		workerPool->activeConnectionCount + workerPool->failedConnectionCount;
	if (finalizedConnectionCount < initiatedConnectionCount)
	{
		return true;
	}

	return false;
}


/*
 * CalculateNewConnectionCount returns the amount of connections
 * that we can currently open.
 */
static int
CalculateNewConnectionCount(WorkerPool *workerPool)
{
	DistributedExecution *execution = workerPool->distributedExecution;

	int targetPoolSize = execution->targetPoolSize;
	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int activeConnectionCount PG_USED_FOR_ASSERTS_ONLY =
		workerPool->activeConnectionCount;
	int idleConnectionCount PG_USED_FOR_ASSERTS_ONLY =
		workerPool->idleConnectionCount;
	int readyTaskCount = workerPool->readyTaskCount;
	int newConnectionCount = 0;


	/* we should always have more (or equal) active connections than idle connections */
	Assert(activeConnectionCount >= idleConnectionCount);

	/* we should always have more (or equal) initiated connections than active connections */
	Assert(initiatedConnectionCount >= activeConnectionCount);

	/* we should never have less than 0 connections ever */
	Assert(activeConnectionCount >= 0 && idleConnectionCount >= 0);

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
		/* cannot open more than targetPoolSize connections */
		int maxNewConnectionCount = targetPoolSize - initiatedConnectionCount;

		/* total number of connections that are (almost) available for tasks */
		int usableConnectionCount = UsableConnectionCount(workerPool);

		/*
		 * Number of additional connections we would need to run all ready tasks in
		 * parallel.
		 */
		int newConnectionsForReadyTasks = Max(0, readyTaskCount - usableConnectionCount);

		/* If Slow start is enabled we need to update the maxNewConnection to the current cycle's maximum.*/
		if (ExecutorSlowStartInterval != SLOW_START_DISABLED)
		{
			maxNewConnectionCount = Min(workerPool->maxNewConnectionsPerCycle,
										maxNewConnectionCount);
		}

		/*
		 * Open enough connections to handle all tasks that are ready, but no more
		 * than the target pool size.
		 */
		newConnectionCount = Min(newConnectionsForReadyTasks, maxNewConnectionCount);
		if (EnableCostBasedConnectionEstablishment && newConnectionCount > 0 &&
			initiatedConnectionCount <= MaxCachedConnectionsPerWorker &&
			UsingExistingSessionsCheaperThanEstablishingNewConnections(
				readyTaskCount, workerPool))
		{
			/*
			 * Before giving the decision, we do one more check. If the cost of
			 * executing the remaining tasks over the existing sessions in the
			 * pool is cheaper than establishing new connections and executing
			 * the tasks over the new connections, we prefer the former.
			 *
			 * For cached connections we should ignore any optimizations as
			 * cached connections are almost free to get. In other words,
			 * as long as there are cached connections that the pool has
			 * not used yet, aggressively use these already established
			 * connections.
			 *
			 * Note that until MaxCachedConnectionsPerWorker has already been
			 * established within the session, we still need to establish
			 * the connections right now.
			 *
			 * Also remember that we are not trying to find the optimal number
			 * of connections for the remaining tasks here. Our goal is to prevent
			 * connection establishments that are absolutely unnecessary. In the
			 * future, we may improve the calculations below to find the optimal
			 * number of new connections required.
			 */
			return 0;
		}
	}

	return newConnectionCount;
}


/*
 * UsingExistingSessionsCheaperThanEstablishingNewConnections returns true if
 * using the already established connections takes less time compared to opening
 * new connections based on the current execution's stats.
 *
 * The function returns false if the current execution has not established any connections
 * or finished any tasks (e.g., no stats to act on).
 */
static bool
UsingExistingSessionsCheaperThanEstablishingNewConnections(int readyTaskCount,
														   WorkerPool *workerPool)
{
	int activeConnectionCount = workerPool->activeConnectionCount;
	if (workerPool->totalExecutedTasks < 1 || activeConnectionCount < 1)
	{
		/*
		 * The pool has not finished any connection establishment or
		 * task yet. So, we refrain from optimizing the execution.
		 */
		return false;
	}

	double avgTaskExecutionTime = AvgTaskExecutionTimeApproximation(workerPool);
	double avgConnectionEstablishmentTime = AvgConnectionEstablishmentTime(workerPool);

	/* we assume that we are halfway through the execution */
	double remainingTimeForActiveTaskExecutionsToFinish = avgTaskExecutionTime / 2;

	/*
	 * We use "newConnectionCount" as if it is the task count as
	 * we are only interested in this iteration of CalculateNewConnectionCount().
	 */
	double totalTimeToExecuteNewTasks = avgTaskExecutionTime * readyTaskCount;

	double estimatedExecutionTimeForNewTasks =
		floor(totalTimeToExecuteNewTasks / activeConnectionCount);

	/*
	 * First finish the already running tasks, and then use the connections
	 * to execute the new tasks.
	 */
	double costOfExecutingTheTasksOverExistingConnections =
		remainingTimeForActiveTaskExecutionsToFinish +
		estimatedExecutionTimeForNewTasks;

	/*
	 * For every task, the executor is supposed to establish one
	 * connection and then execute the task over the connection.
	 */
	double costOfExecutingTheTasksOverNewConnection =
		(avgTaskExecutionTime + avgConnectionEstablishmentTime);

	return (costOfExecutingTheTasksOverExistingConnections <=
			costOfExecutingTheTasksOverNewConnection);
}


/*
 * AvgTaskExecutionTimeApproximation returns the approximation of the average task
 * execution times on the workerPool.
 */
static double
AvgTaskExecutionTimeApproximation(WorkerPool *workerPool)
{
	uint64 totalTaskExecutionTime = workerPool->totalTaskExecutionTime;
	int taskCount = workerPool->totalExecutedTasks;

	instr_time now;
	INSTR_TIME_SET_CURRENT(now);

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, workerPool->sessionList)
	{
		/*
		 * Involve the tasks that are currently running. We do this to
		 * make sure that the execution responds with new connections
		 * quickly if the actively running tasks
		 */
		TaskPlacementExecution *placementExecution = session->currentTask;
		if (placementExecution != NULL &&
			placementExecution->executionState == PLACEMENT_EXECUTION_RUNNING)
		{
			uint64 durationInMicroSecs =
				MicrosecondsBetweenTimestamps(placementExecution->startTime, now);

			/*
			 * Our approximation is that we assume that the task execution is
			 * just in the halfway through.
			 */
			totalTaskExecutionTime += (2 * durationInMicroSecs);
			taskCount += 1;
		}
	}

	return taskCount == 0 ? 0 : ((double) totalTaskExecutionTime / taskCount);
}


/*
 * AvgConnectionEstablishmentTime calculates the average connection establishment times
 * for the input workerPool.
 */
static double
AvgConnectionEstablishmentTime(WorkerPool *workerPool)
{
	double totalTimeMicrosec = 0;
	int sessionCount = 0;

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, workerPool->sessionList)
	{
		MultiConnection *connection = session->connection;

		/*
		 * There could be MaxCachedConnectionsPerWorker connections that are
		 * already connected. Those connections might skew the average
		 * connection establishment times for the current execution. The reason
		 * is that they are established earlier and the connection establishment
		 * times might be different at the moment those connections are established.
		 */
		if (connection->connectionState == MULTI_CONNECTION_CONNECTED)
		{
			long connectionEstablishmentTime =
				MicrosecondsBetweenTimestamps(connection->connectionEstablishmentStart,
											  connection->connectionEstablishmentEnd);

			totalTimeMicrosec += connectionEstablishmentTime;
			++sessionCount;
		}
	}

	return (sessionCount == 0) ? 0 : (totalTimeMicrosec / sessionCount);
}


/*
 * OpenNewConnections opens the given amount of connections for the given workerPool.
 */
static void
OpenNewConnections(WorkerPool *workerPool, int newConnectionCount,
				   TransactionProperties *transactionProperties)
{
	ereport(DEBUG4, (errmsg("opening %d new connections to %s:%d", newConnectionCount,
							workerPool->nodeName, workerPool->nodePort)));

	List *newSessionsList = NIL;
	for (int connectionIndex = 0; connectionIndex < newConnectionCount; connectionIndex++)
	{
		/* experimental: just to see the perf benefits of caching connections */
		int connectionFlags = 0;

		if (transactionProperties->useRemoteTransactionBlocks ==
			TRANSACTION_BLOCKS_DISALLOWED)
		{
			connectionFlags |= OUTSIDE_TRANSACTION;
		}

		/*
		 * Enforce the requirements for adaptive connection management (a.k.a.,
		 * throttle connections if citus.max_shared_pool_size reached)
		 */
		int adaptiveConnectionManagementFlag =
			AdaptiveConnectionManagementFlag(workerPool->poolToLocalNode,
											 list_length(workerPool->sessionList));
		connectionFlags |= adaptiveConnectionManagementFlag;

		/* open a new connection to the worker */
		MultiConnection *connection = StartNodeUserDatabaseConnection(connectionFlags,
																	  workerPool->nodeName,
																	  workerPool->nodePort,
																	  NULL, NULL);
		if (!connection)
		{
			/* connection can only be NULL for optional connections */
			Assert((connectionFlags & OPTIONAL_CONNECTION));
			continue;
		}

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

		if (list_length(workerPool->sessionList) == 0)
		{
			/*
			 * The worker pool has just started to establish connections. We need to
			 * defer this initialization after StartNodeUserDatabaseConnection()
			 * because for non-optional connections, we have some logic to wait
			 * until a connection is allowed to be established.
			 */
			INSTR_TIME_SET_ZERO(workerPool->poolStartTime);
		}

		/* create a session for the connection */
		WorkerSession *session = FindOrCreateWorkerSession(workerPool, connection);
		newSessionsList = lappend(newSessionsList, session);
	}

	if (list_length(newSessionsList) == 0)
	{
		/* nothing to do as no new connections happened */
		return;
	}

	DistributedExecution *execution = workerPool->distributedExecution;


	/*
	 * Although not ideal, there is a slight difference in the implementations
	 * of PG15+ and others.
	 *
	 * Recreating the WaitEventSet even once is prohibitively expensive (almost
	 * ~7% overhead for select-only pgbench). For all versions, the aim is to
	 * be able to create the WaitEventSet only once after any new connections
	 * are added to the execution. That is the main reason behind the implementation
	 * differences.
	 *
	 * For pre-PG15 versions, we leave the waitEventSet recreation to the main
	 * execution loop. For PG15+, we do it right here.
	 *
	 * We require this difference because for PG15+, there is a new type of
	 * WaitEvent (WL_SOCKET_CLOSED). We can provide this new event at this point,
	 * and check RemoteSocketClosedForAnySession(). For earlier versions, we have
	 * to defer the rebuildWaitEventSet as there is no other event to waitFor
	 * at this point. We could have forced to re-build, but that would mean we try to
	 * create waitEventSet without any actual events. That has some other implications
	 * such that we have to avoid certain optimizations of WaitEventSet creation.
	 *
	 * Instead, we prefer this slight difference, which in effect has almost no
	 * difference, but doing things in different points in time.
	 */

	/* we added new connections, rebuild the waitEventSet */
	RebuildWaitEventSetForSessions(execution);

	/*
	 * If there are any closed sockets, mark connection lost such that
	 * we can re-connect.
	 */
	RemoteSocketClosedForAnySession(execution);

	/*
	 * For RemoteSocketClosedForAnySession() purposes, we explicitly skip
	 * the latch because we want to handle all cancellations to be caught
	 * on the main execution loop, not here. We mostly skip cancellations
	 * on RemoteSocketClosedForAnySession() for simplicity. Handling
	 * cancellations on the main execution loop much easier to break out
	 * of the execution.
	 */
	AddLatchWaitEventToExecution(execution);

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, newSessionsList)
	{
		/* immediately run the state machine to handle potential failure */
		ConnectionStateMachine(session);
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
	instr_time poolStartTime = workerPool->poolStartTime;
	instr_time now;
	INSTR_TIME_SET_CURRENT(now);

	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int activeConnectionCount = workerPool->activeConnectionCount;
	int requiredActiveConnectionCount = 1;

	if (initiatedConnectionCount == 0)
	{
		/* no connection has been planned for the pool yet */
		Assert(INSTR_TIME_IS_ZERO(poolStartTime));
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

	if (MillisecondsBetweenTimestamps(poolStartTime, now) >= NodeConnectionTimeout)
	{
		if (activeConnectionCount < requiredActiveConnectionCount)
		{
			int logLevel = WARNING;

			/*
			 * First fail the pool and create an opportunity to execute tasks
			 * over other pools when tasks have more than one placement to execute.
			 */
			WorkerPoolFailed(workerPool);

			if (workerPool->failureState == WORKER_POOL_FAILED_OVER_TO_LOCAL)
			{
				/*
				 *
				 * When the pool is failed over to local execution, warning
				 * the user just creates chatter as the executor is capable of
				 * finishing the execution.
				 */
				logLevel = DEBUG1;
			}
			else if (execution->transactionProperties->errorOnAnyFailure ||
					 execution->failed)
			{
				/*
				 * The enforcement is not always erroring out. For example, if a SELECT task
				 * has two different placements, we'd warn the user, fail the pool and continue
				 * with the next placement.
				 */
				logLevel = ERROR;
			}

			/*
			 * We hit the connection timeout. In that case, we should not let the
			 * connection establishment to continue because the execution logic
			 * pretends that failed sessions are not going to be used anymore.
			 *
			 * That's why we mark the connection as timed out to trigger the state
			 * changes in the executor, if we don't throw an error below.
			 */
			MarkEstablishingSessionsTimedOut(workerPool);

			ereport(logLevel, (errcode(ERRCODE_CONNECTION_FAILURE),
							   errmsg("could not establish any connections to the node "
									  "%s:%d after %u ms", workerPool->nodeName,
									  workerPool->nodePort,
									  NodeConnectionTimeout)));
		}
		else
		{
			/* stop interrupting WaitEventSetWait for timeouts */
			workerPool->checkForPoolTimeout = false;
		}
	}
}


/*
 * MarkEstablishingSessionsTimedOut goes over the sessions in the given
 * workerPool and marks them timed out. ConnectionStateMachine()
 * later cleans up the sessions.
 */
static void
MarkEstablishingSessionsTimedOut(WorkerPool *workerPool)
{
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, workerPool->sessionList)
	{
		MultiConnection *connection = session->connection;

		if (connection->connectionState == MULTI_CONNECTION_CONNECTING ||
			connection->connectionState == MULTI_CONNECTION_INITIAL)
		{
			connection->connectionState = MULTI_CONNECTION_TIMED_OUT;
			IncrementStatCounterForMyDb(STAT_CONNECTION_ESTABLISHMENT_FAILED);
		}
	}
}


/*
 * UsableConnectionCount returns the number of connections in the worker pool
 * that are (soon to be) usable for sending commands, this includes both idle
 * connections and connections that are still establishing.
 */
static int
UsableConnectionCount(WorkerPool *workerPool)
{
	int initiatedConnectionCount = list_length(workerPool->sessionList);
	int activeConnectionCount = workerPool->activeConnectionCount;
	int failedConnectionCount = workerPool->failedConnectionCount;
	int idleConnectionCount = workerPool->idleConnectionCount;

	/* connections that are still establishing will soon be available for tasks */
	int establishingConnectionCount =
		initiatedConnectionCount - activeConnectionCount - failedConnectionCount;

	int usableConnectionCount = idleConnectionCount + establishingConnectionCount;

	return usableConnectionCount;
}


/*
 * NextEventTimeout finds the earliest time at which we need to interrupt
 * WaitEventSetWait because of a timeout and returns the number of milliseconds
 * until that event with a minimum of 1ms and a maximum of 1000ms.
 *
 * This code may be sensitive to clock jumps, but only has the effect of waking
 * up WaitEventSetWait slightly earlier to later.
 */
static long
NextEventTimeout(DistributedExecution *execution)
{
	instr_time now;
	INSTR_TIME_SET_CURRENT(now);
	long eventTimeout = 1000; /* milliseconds */

	WorkerPool *workerPool = NULL;
	foreach_declared_ptr(workerPool, execution->workerList)
	{
		if (workerPool->failureState == WORKER_POOL_FAILED)
		{
			/* worker pool may have already timed out */
			continue;
		}

		if (!INSTR_TIME_IS_ZERO(workerPool->poolStartTime) &&
			workerPool->checkForPoolTimeout)
		{
			long timeSincePoolStartMs =
				MillisecondsBetweenTimestamps(workerPool->poolStartTime, now);

			/*
			 * This could go into the negative if the connection timeout just passed.
			 * In that case we want to wake up as soon as possible. Once the timeout
			 * has been processed, checkForPoolTimeout will be false so we will skip
			 * this check.
			 */
			long timeUntilConnectionTimeoutMs =
				NodeConnectionTimeout - timeSincePoolStartMs;

			if (timeUntilConnectionTimeoutMs < eventTimeout)
			{
				eventTimeout = timeUntilConnectionTimeoutMs;
			}
		}

		int initiatedConnectionCount = list_length(workerPool->sessionList);

		/*
		 * If there are connections to open we wait at most up to the end of the
		 * current slow start interval.
		 */
		if (workerPool->readyTaskCount > UsableConnectionCount(workerPool) &&
			initiatedConnectionCount < execution->targetPoolSize)
		{
			long timeSinceLastConnectMs =
				MillisecondsBetweenTimestamps(workerPool->lastConnectionOpenTime, now);
			long timeUntilSlowStartInterval =
				ExecutorSlowStartInterval - timeSinceLastConnectMs;

			if (timeUntilSlowStartInterval < eventTimeout)
			{
				eventTimeout = timeUntilSlowStartInterval;
			}
		}
	}

	return Max(1, eventTimeout);
}


/*
 * MillisecondsBetweenTimestamps is a helper to get the number of milliseconds
 * between timestamps when it is expected to be small enough to fit in a
 * long.
 */
static long
MillisecondsBetweenTimestamps(instr_time startTime, instr_time endTime)
{
	INSTR_TIME_SUBTRACT(endTime, startTime);
	return INSTR_TIME_GET_MILLISEC(endTime);
}


/*
 * MicrosecondsBetweenTimestamps is a helper to get the number of microseconds
 * between timestamps.
 */
static uint64
MicrosecondsBetweenTimestamps(instr_time startTime, instr_time endTime)
{
	INSTR_TIME_SUBTRACT(endTime, startTime);
	return INSTR_TIME_GET_MICROSEC(endTime);
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

			case MULTI_CONNECTION_TIMED_OUT:
			{
				/*
				 * When the connection timeout happens, the connection
				 * might still be able to successfuly established. However,
				 * the executor should not try to use this connection as
				 * the state machines might have already progressed and used
				 * new pools/sessions instead. That's why we terminate the
				 * connection, clear any state associated with it.
				 *
				 * Note that here we don't increment the failed connection
				 * stat counter because MarkEstablishingSessionsTimedOut()
				 * already did that.
				 */
				connection->connectionState = MULTI_CONNECTION_FAILED;
				break;
			}

			case MULTI_CONNECTION_CONNECTING:
			{
				ConnStatusType status = PQstatus(connection->pgConn);
				if (status == CONNECTION_OK)
				{
					/*
					 * Connection was already established, possibly a cached
					 * connection.
					 */
					bool newConnection = false;
					HandleMultiConnectionSuccess(session, newConnection);
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
					break;
				}
				else if (status == CONNECTION_BAD)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					IncrementStatCounterForMyDb(STAT_CONNECTION_ESTABLISHMENT_FAILED);
					break;
				}

				int beforePollSocket = PQsocket(connection->pgConn);
				PostgresPollingStatusType pollMode = PQconnectPoll(connection->pgConn);

				if (beforePollSocket != PQsocket(connection->pgConn))
				{
					/* rebuild the wait events if PQconnectPoll() changed the socket */
					execution->rebuildWaitEventSet = true;
				}

				if (pollMode == PGRES_POLLING_FAILED)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					IncrementStatCounterForMyDb(STAT_CONNECTION_ESTABLISHMENT_FAILED);
				}
				else if (pollMode == PGRES_POLLING_READING)
				{
					UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);

					/* we should have a valid socket */
					Assert(PQsocket(connection->pgConn) != -1);
				}
				else if (pollMode == PGRES_POLLING_WRITING)
				{
					UpdateConnectionWaitFlags(session, WL_SOCKET_WRITEABLE);

					/* we should have a valid socket */
					Assert(PQsocket(connection->pgConn) != -1);
				}
				else
				{
					/*
					 * Connection was not established befoore (!= CONNECTION_OK)
					 * but PQconnectPoll() did so now.
					 */
					bool newConnection = true;
					HandleMultiConnectionSuccess(session, newConnection);
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

					/* we should have a valid socket */
					Assert(PQsocket(connection->pgConn) != -1);
				}

				break;
			}

			case MULTI_CONNECTION_CONNECTED:
			{
				if (HasUnfinishedTaskForSession(session))
				{
					/*
					 * Connection is ready, and we have unfinished tasks.
					 * So, run the transaction state machine.
					 */
					TransactionStateMachine(session);
				}
				else
				{
					/*
					 * Connection is ready, but we don't have any unfinished
					 * tasks that this session can execute.
					 *
					 * Note that we can be in a situation where the executor
					 * decides to establish a connection, but not need to
					 * use it at the time the connection is established. This could
					 * happen when the earlier connections manages to finish all the
					 * tasks after this connection
					 *
					 * As no tasks are ready to be executed at the moment, we
					 * mark the socket readable to get any notices if exists.
					 */
					UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);
				}

				break;
			}

			case MULTI_CONNECTION_LOST:
			{
				/*
				 * If a connection is lost, we retry the connection for some
				 * very restricted scenarios. The main use case is to retry
				 * connection establishment when a cached connection is used
				 * in the executor while remote server has restarted / failedover
				 * etc.
				 *
				 * For simplicity, we only allow retrying connection establishment
				 * a single time.
				 *
				 * We can only retry connection when the remote transaction has
				 * not started over the connection. Otherwise, we'd have to deal
				 * with restoring the transaction state, which is beyond our
				 * purpose at this time.
				 */
				RemoteTransaction *transaction = &connection->remoteTransaction;
				if (!session->connectionRetried &&
					transaction->transactionState == REMOTE_TRANS_NOT_STARTED)
				{
					/*
					 * Try to connect again, we will reuse the same MultiConnection
					 * and keep it as claimed.
					 */
					RestartConnection(connection);

					/* socket have changed */
					execution->rebuildWaitEventSet = true;
					session->latestUnconsumedWaitEvents = 0;

					session->connectionRetried = true;

					break;
				}

				/*
				 * Here we don't increment the connection stat counter for failed
				 * connections because we don't track the connections that we could
				 * establish but lost later.
				 */
				connection->connectionState = MULTI_CONNECTION_FAILED;
				break;
			}

			case MULTI_CONNECTION_FAILED:
			{
				/* managed to connect, but connection was lost */
				if (session->sessionHasActiveConnection)
				{
					workerPool->activeConnectionCount--;

					if (session->currentTask == NULL)
					{
						/* this was an idle connection */
						workerPool->idleConnectionCount--;
					}

					session->sessionHasActiveConnection = false;
				}

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

				/*
				 * The execution may have failed as a result of WorkerSessionFailed
				 * or WorkerPoolFailed.
				 *
				 * Even if this execution has not failed -- but just a single session is
				 * failed -- and an earlier execution in this transaction which marked
				 * the remote transaction as critical, we should fail right away as the
				 * transaction will fail anyway on PREPARE/COMMIT time.
				 */
				RemoteTransaction *transaction = &connection->remoteTransaction;

				if (transaction->transactionCritical ||
					execution->failed ||
					(execution->transactionProperties->errorOnAnyFailure &&
					 workerPool->failureState != WORKER_POOL_FAILED_OVER_TO_LOCAL))
				{
					/* a task has failed due to this connection failure */
					ReportConnectionError(connection, ERROR);
				}
				else if (workerPool->activeConnectionCount > 0 ||
						 workerPool->failureState == WORKER_POOL_FAILED_OVER_TO_LOCAL)
				{
					/*
					 * We already have active connection(s) to the node, and the
					 * executor is capable of using those connections to successfully
					 * finish the execution. So, there is not much value in warning
					 * the user.
					 *
					 * Similarly when the pool is failed over to local execution, warning
					 * the user just creates chatter.
					 */
					ReportConnectionError(connection, DEBUG1);
				}
				else
				{
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
				execution->rebuildWaitEventSet = true;

				/*
				 * Reset the transaction state machine since CloseConnection()
				 * relies on it and even if we're not inside a distributed transaction
				 * we set the transaction state (e.g., REMOTE_TRANS_SENT_COMMAND).
				 */
				if (!connection->remoteTransaction.beginSent)
				{
					connection->remoteTransaction.transactionState =
						REMOTE_TRANS_NOT_STARTED;
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
 * HasUnfinishedTaskForSession gets a session and returns true if there
 * are any tasks that this session can execute.
 */
static bool
HasUnfinishedTaskForSession(WorkerSession *session)
{
	if (session->currentTask != NULL)
	{
		/* the session is executing a command right now */
		return true;
	}

	dlist_head *sessionReadyTaskQueue = &(session->readyTaskQueue);
	if (!dlist_is_empty(sessionReadyTaskQueue))
	{
		/* session has an assigned task, which is ready for execution */
		return true;
	}

	WorkerPool *workerPool = session->workerPool;
	dlist_head *poolReadyTaskQueue = &(workerPool->readyTaskQueue);
	if (!dlist_is_empty(poolReadyTaskQueue))
	{
		/*
		 * Pool has unassigned tasks that can be executed
		 * by the input session.
		 */
		return true;
	}

	return false;
}


/*
 * HandleMultiConnectionSuccess logs the established connection and updates
 * connection's state.
 */
static void
HandleMultiConnectionSuccess(WorkerSession *session, bool newConnection)
{
	MultiConnection *connection = session->connection;
	WorkerPool *workerPool = session->workerPool;

	MarkConnectionConnected(connection, newConnection);

	ereport(DEBUG4, (errmsg("established connection to %s:%d for "
							"session %ld in %ld microseconds",
							connection->hostname, connection->port,
							session->sessionId,
							MicrosecondsBetweenTimestamps(
								connection->connectionEstablishmentStart,
								connection->connectionEstablishmentEnd))));

	workerPool->activeConnectionCount++;
	workerPool->idleConnectionCount++;
	session->sessionHasActiveConnection = true;
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
 *          -- 2PC itself since it is a single shard modification
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
	DistributedExecution *execution = session->workerPool->distributedExecution;
	if (TransactionModifiedDistributedTable(execution) &&
		DistributedExecutionModifiesDatabase(execution) &&
		!ConnectionModifiedPlacement(session->connection))
	{
		/*
		 * We already did a modification, but not on the connection that we
		 * just opened, which means we're now going to make modifications
		 * over multiple connections. Activate 2PC!
		 */
		Use2PCForCoordinatedTransaction();
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
	 * We need to explicitly check for TRANSACTION_BLOCKS_REQUIRED due to
	 * citus.function_opens_transaction_block flag. When set to false, we
	 * should not be pretending that we're in a coordinated transaction even
	 * if XACT_MODIFICATION_DATA is set. That's why we implemented this workaround.
	 */
	return execution->transactionProperties->useRemoteTransactionBlocks ==
		   TRANSACTION_BLOCKS_REQUIRED &&
		   XactModificationLevel == XACT_MODIFICATION_DATA;
}


/*
 * TransactionStateMachine manages the execution of tasks over a connection.
 */
static void
TransactionStateMachine(WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	TransactionBlocksUsage useRemoteTransactionBlocks =
		execution->transactionProperties->useRemoteTransactionBlocks;

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
			case REMOTE_TRANS_NOT_STARTED:
			{
				if (useRemoteTransactionBlocks == TRANSACTION_BLOCKS_REQUIRED)
				{
					/* if we're expanding the nodes in a transaction, use 2PC */
					Activate2PCIfModifyingTransactionExpandsToNewNode(session);

					/* need to open a transaction block first */
					StartRemoteTransactionBegin(connection);

					transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
				}
				else
				{
					TaskPlacementExecution *placementExecution = PopPlacementExecution(
						session);
					if (placementExecution == NULL)
					{
						/*
						 * No tasks are ready to be executed at the moment. But we
						 * still mark the socket readable to get any notices if exists.
						 */
						UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);

						break;
					}

					bool placementExecutionStarted =
						StartPlacementExecutionOnSession(placementExecution, session);
					if (!placementExecutionStarted)
					{
						/* no need to continue, connection is lost */
						Assert(session->connection->connectionState ==
							   MULTI_CONNECTION_LOST);

						return;
					}

					transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				}

				UpdateConnectionWaitFlags(session,
										  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
				break;
			}

			case REMOTE_TRANS_SENT_BEGIN:
			case REMOTE_TRANS_CLEARING_RESULTS:
			{
				PGresult *result = PQgetResult(connection->pgConn);
				if (result != NULL)
				{
					if (!IsResponseOK(result))
					{
						/* query failures are always hard errors */
						ReportResultError(connection, result, ERROR);
					}

					PQclear(result);

					/* wake up WaitEventSetWait */
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

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

				if (transaction->beginSent)
				{
					transaction->transactionState = REMOTE_TRANS_STARTED;
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_NOT_STARTED;
				}
				break;
			}

			case REMOTE_TRANS_STARTED:
			{
				TaskPlacementExecution *placementExecution = PopPlacementExecution(
					session);
				if (placementExecution == NULL)
				{
					/* no tasks are ready to be executed at the moment */
					UpdateConnectionWaitFlags(session, WL_SOCKET_READABLE);
					break;
				}

				bool placementExecutionStarted =
					StartPlacementExecutionOnSession(placementExecution, session);
				if (!placementExecutionStarted)
				{
					/* no need to continue, connection is lost */
					Assert(session->connection->connectionState == MULTI_CONNECTION_LOST);

					return;
				}

				transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				break;
			}

			case REMOTE_TRANS_SENT_COMMAND:
			{
				TaskPlacementExecution *placementExecution = session->currentTask;
				if (placementExecution == NULL)
				{
					/*
					 * We have seen accounts in production where the placementExecution
					 * could inadvertently be not set. Investigation documented on
					 * https://github.com/citusdata/citus-enterprise/issues/493
					 * (due to sensitive data in the initial report it is not discussed
					 * in our community repository)
					 *
					 * Currently we don't have a reliable way of reproducing this issue.
					 * Erroring here seems to be a more desirable approach compared to a
					 * SEGFAULT on the dereference of placementExecution, with a possible
					 * crash recovery as a result.
					 */
					ereport(ERROR, (errmsg(
										"unable to recover from inconsistent state in "
										"the connection state machine on coordinator")));
				}

				ShardCommandExecution *shardCommandExecution =
					placementExecution->shardCommandExecution;
				Task *task = shardCommandExecution->task;

				/*
				 * In EXPLAIN ANALYZE we need to store results except for multiple placements,
				 * regardless of query type. In other cases, doing the same doesn't seem to have
				 * a drawback.
				 */
				bool storeRows = true;

				if (shardCommandExecution->gotResults)
				{
					/* already received results from another replica */
					storeRows = false;
				}
				else if (task->partiallyLocalOrRemote)
				{
					/*
					 * For the tasks that involves placements from both
					 * remote and local placments, such as modifications
					 * to reference tables, we store the rows during the
					 * local placement/execution.
					 */
					storeRows = false;
				}

				bool fetchDone = ReceiveResults(session, storeRows);
				if (!fetchDone)
				{
					break;
				}

				/* if this is a multi-query task, send the next query */
				if (placementExecution->queryIndex < task->queryCount)
				{
					bool querySent = SendNextQuery(placementExecution, session);
					if (!querySent)
					{
						/* no need to continue, connection is lost */
						Assert(session->connection->connectionState ==
							   MULTI_CONNECTION_LOST);

						return;
					}

					/*
					 * At this point the query might be just in pgconn buffers. We
					 * need to wait until it becomes writeable to actually send
					 * the query.
					 */
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE);

					transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;

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

	/* always detect closed sockets */
	connection->waitFlags = waitFlags | WL_SOCKET_CLOSED;

	/* without signalling the execution, the flag changes won't be reflected */
	execution->waitFlagsChanged = true;
}


/*
 * CheckConnectionReady returns true if the connection is ready to
 * read or write, or false if it still has bytes to send/receive.
 */
static bool
CheckConnectionReady(WorkerSession *session)
{
	MultiConnection *connection = session->connection;
	int waitFlags = WL_SOCKET_READABLE;
	bool connectionReady = false;

	ConnStatusType status = PQstatus(connection->pgConn);
	if (status == CONNECTION_BAD)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	if ((session->latestUnconsumedWaitEvents & WL_SOCKET_CLOSED) != 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	/* try to send all pending data */
	int sendStatus = PQflush(connection->pgConn);
	if (sendStatus == -1)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}
	else if (sendStatus == 1)
	{
		/* more data to send, wait for socket to become writable */
		waitFlags = waitFlags | WL_SOCKET_WRITEABLE;
	}

	if ((session->latestUnconsumedWaitEvents & WL_SOCKET_READABLE) != 0)
	{
		if (PQconsumeInput(connection->pgConn) == 0)
		{
			connection->connectionState = MULTI_CONNECTION_LOST;
			return false;
		}
	}

	if (!PQisBusy(connection->pgConn))
	{
		connectionReady = true;
	}

	UpdateConnectionWaitFlags(session, waitFlags);

	/* don't consume input redundantly if we cycle back into CheckConnectionReady */
	session->latestUnconsumedWaitEvents = 0;

	return connectionReady;
}


/*
 * PopPlacementExecution returns the next available assigned or unassigned
 * placement execution for the given session.
 */
static TaskPlacementExecution *
PopPlacementExecution(WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;

	TaskPlacementExecution *placementExecution = PopAssignedPlacementExecution(session);
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
	dlist_head *readyTaskQueue = &(session->readyTaskQueue);

	if (dlist_is_empty(readyTaskQueue))
	{
		return NULL;
	}

	TaskPlacementExecution *placementExecution = dlist_container(TaskPlacementExecution,
																 sessionReadyQueueNode,
																 dlist_pop_head_node(
																	 readyTaskQueue));

	return placementExecution;
}


/*
 * PopAssignedPlacementExecution finds an executable task from the queue of assigned tasks.
 */
static TaskPlacementExecution *
PopUnassignedPlacementExecution(WorkerPool *workerPool)
{
	dlist_head *readyTaskQueue = &(workerPool->readyTaskQueue);

	if (dlist_is_empty(readyTaskQueue))
	{
		return NULL;
	}

	TaskPlacementExecution *placementExecution = dlist_container(TaskPlacementExecution,
																 workerReadyQueueNode,
																 dlist_pop_head_node(
																	 readyTaskQueue));

	workerPool->readyTaskCount--;

	return placementExecution;
}


/*
 * StartPlacementExecutionOnSession gets a TaskPlacementExecution and
 * WorkerSession, the task's query is sent to the worker via the session.
 *
 * The function does some bookkeeping such as associating the placement
 * accesses with the connection and updating session's local variables. For
 * details read the comments in the function.
 *
 * The function returns true if the query is successfully sent over the
 * connection, otherwise false.
 */
static bool
StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
								 WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	MultiConnection *connection = session->connection;
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	Task *task = shardCommandExecution->task;
	ShardPlacement *taskPlacement = placementExecution->shardPlacement;
	List *placementAccessList = PlacementAccessListForTask(task, taskPlacement);

	if (execution->transactionProperties->useRemoteTransactionBlocks !=
		TRANSACTION_BLOCKS_DISALLOWED)
	{
		/*
		 * Make sure that subsequent commands on the same placement
		 * use the same connection.
		 */
		AssignPlacementListToConnection(placementAccessList, connection);
	}

	if (session->commandsSent == 0)
	{
		/* first time we send a command, consider the connection used (not unused) */
		workerPool->unusedConnectionCount--;
	}

	/* connection is going to be in use */
	workerPool->idleConnectionCount--;
	session->currentTask = placementExecution;
	placementExecution->executionState = PLACEMENT_EXECUTION_RUNNING;

	Assert(INSTR_TIME_IS_ZERO(placementExecution->startTime));

	/*
	 * The same TaskPlacementExecution can be used to have
	 * call SendNextQuery() several times if queryIndex is
	 * non-zero. Still, all are executed under the current
	 * placementExecution, so we can start the timer right
	 * now.
	 */
	INSTR_TIME_SET_CURRENT(placementExecution->startTime);

	bool querySent = SendNextQuery(placementExecution, session);
	if (querySent)
	{
		session->commandsSent++;

		if (workerPool->poolToLocalNode)
		{
			/*
			 * As we started remote execution to the local node,
			 * we cannot switch back to local execution as that
			 * would cause self-deadlocks and breaking
			 * read-your-own-writes consistency.
			 */
			SetLocalExecutionStatus(LOCAL_EXECUTION_DISABLED);
		}
	}

	return querySent;
}


/*
 * SendNextQuery sends the next query for placementExecution on the given
 * session.
 */
static bool
SendNextQuery(TaskPlacementExecution *placementExecution,
			  WorkerSession *session)
{
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	MultiConnection *connection = session->connection;
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	bool binaryResults = shardCommandExecution->binaryResults;
	Task *task = shardCommandExecution->task;
	ParamListInfo paramListInfo = execution->paramListInfo;
	int querySent = 0;
	uint32 queryIndex = placementExecution->queryIndex;

	Assert(queryIndex < task->queryCount);
	char *queryString = TaskQueryStringAtIndex(task, queryIndex);

	if (paramListInfo != NULL && !task->parametersInQueryStringResolved)
	{
		int parameterCount = paramListInfo->numParams;
		Oid *parameterTypes = NULL;
		const char **parameterValues = NULL;

		/* force evaluation of bound params */
		paramListInfo = copyParamList(paramListInfo);

		ExtractParametersForRemoteExecution(paramListInfo, &parameterTypes,
											&parameterValues);
		querySent = SendRemoteCommandParams(connection, queryString, parameterCount,
											parameterTypes, parameterValues,
											binaryResults);
	}
	else
	{
		/*
		 * We only need to use SendRemoteCommandParams when we desire
		 * binaryResults. One downside of SendRemoteCommandParams is that it
		 * only supports one query in the query string. In some cases we have
		 * more than one query. In those cases we already make sure before that
		 * binaryResults is false.
		 *
		 * XXX: It also seems that SendRemoteCommandParams does something
		 * strange/incorrectly with select statements. In
		 * isolation_select_vs_all.spec, when doing an s1-router-select in one
		 * session blocked an s2-ddl-create-index-concurrently in another.
		 */
		if (!binaryResults)
		{
			querySent = SendRemoteCommand(connection, queryString);
		}
		else
		{
			querySent = SendRemoteCommandParams(connection, queryString, 0, NULL, NULL,
												binaryResults);
		}
	}

	if (querySent == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	int singleRowMode = PQsetSingleRowMode(connection->pgConn);
	if (singleRowMode == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return false;
	}

	return true;
}


/*
 * ReceiveResults reads the result of a command or query and writes returned
 * rows to the tuple store of the scan state. It returns whether fetching results
 * were done. On failure, it throws an error.
 */
static bool
ReceiveResults(WorkerSession *session, bool storeRows)
{
	bool fetchDone = false;
	MultiConnection *connection = session->connection;
	WorkerPool *workerPool = session->workerPool;
	DistributedExecution *execution = workerPool->distributedExecution;
	TaskPlacementExecution *placementExecution = session->currentTask;
	ShardCommandExecution *shardCommandExecution =
		placementExecution->shardCommandExecution;
	Task *task = placementExecution->shardCommandExecution->task;
	TupleDestination *tupleDest = task->tupleDest ?
								  task->tupleDest :
								  execution->defaultTupleDest;

	/*
	 * We use this context while converting each row fetched from remote node
	 * into tuple. The context is reseted on every row, thus we create it at the
	 * start of the loop and reset on every iteration.
	 */
	MemoryContext rowContext = AllocSetContextCreate(CurrentMemoryContext,
													 "RowContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	while (!PQisBusy(connection->pgConn))
	{
		uint32 columnIndex = 0;
		uint32 rowsProcessed = 0;

		PGresult *result = PQgetResult(connection->pgConn);
		if (result == NULL)
		{
			/* no more results, break out of loop and free allocated memory */
			fetchDone = true;
			break;
		}

		ExecStatusType resultStatus = PQresultStatus(result);
		if (resultStatus == PGRES_COMMAND_OK)
		{
			char *currentAffectedTupleString = PQcmdTuples(result);
			int64 currentAffectedTupleCount = 0;

			/* if there are multiple replicas, make sure to consider only one */
			if (storeRows && *currentAffectedTupleString != '\0')
			{
				currentAffectedTupleCount = pg_strtoint64(currentAffectedTupleString);
				Assert(currentAffectedTupleCount >= 0);
				execution->rowsProcessed += currentAffectedTupleCount;
			}

			PQclear(result);

			/* task query might contain multiple queries, so fetch until we reach NULL */
			placementExecution->queryIndex++;
			continue;
		}
		else if (resultStatus == PGRES_TUPLES_OK)
		{
			/*
			 * We've already consumed all the tuples, no more results. Break out
			 * of loop and free allocated memory before returning.
			 */
			Assert(PQntuples(result) == 0);
			PQclear(result);

			/* task query might contain multiple queries, so fetch until we reach NULL */
			placementExecution->queryIndex++;
			continue;
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

		uint32 queryIndex = placementExecution->queryIndex;
		if (queryIndex >= task->queryCount)
		{
			ereport(ERROR, (errmsg("unexpected query index while processing"
								   " query results")));
		}

		TupleDesc tupleDescriptor = tupleDest->tupleDescForQuery(tupleDest, queryIndex);
		if (tupleDescriptor == NULL)
		{
			PQclear(result);
			continue;
		}

		rowsProcessed = PQntuples(result);
		uint32 columnCount = PQnfields(result);
		uint32 expectedColumnCount = tupleDescriptor->natts;

		if (columnCount != expectedColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected number of columns from worker: %d, "
								   "expected %d",
								   columnCount, expectedColumnCount)));
		}

		if (columnCount > execution->allocatedColumnCount)
		{
			pfree(execution->columnArray);
			int oldColumnCount = execution->allocatedColumnCount;
			execution->allocatedColumnCount = columnCount;
			execution->columnArray = palloc0(execution->allocatedColumnCount *
											 sizeof(void *));
			if (EnableBinaryProtocol)
			{
				/*
				 * Using repalloc here, to not throw away any previously
				 * created StringInfos.
				 */
				execution->stringInfoDataArray = repalloc(
					execution->stringInfoDataArray,
					execution->allocatedColumnCount *
					sizeof(StringInfoData));
				for (int i = oldColumnCount; i < columnCount; i++)
				{
					initStringInfo(&execution->stringInfoDataArray[i]);
				}
			}
		}

		void **columnArray = execution->columnArray;
		StringInfoData *stringInfoDataArray = execution->stringInfoDataArray;
		bool binaryResults = shardCommandExecution->binaryResults;

		/*
		 * stringInfoDataArray is NULL when EnableBinaryProtocol is false. So
		 * we make sure binaryResults is also false in that case. Otherwise we
		 * cannot store them anywhere.
		 */
		Assert(EnableBinaryProtocol || !binaryResults);

		for (uint32 rowIndex = 0; rowIndex < rowsProcessed; rowIndex++)
		{
			uint64 tupleLibpqSize = 0;

			/*
			 * Switch to a temporary memory context that we reset after each
			 * tuple. This protects us from any memory leaks that might be
			 * present in anything we do to parse a tuple.
			 */
			MemoryContext oldContext = MemoryContextSwitchTo(rowContext);

			memset(columnArray, 0, columnCount * sizeof(void *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					int valueLength = PQgetlength(result, rowIndex, columnIndex);
					char *value = PQgetvalue(result, rowIndex, columnIndex);
					if (binaryResults)
					{
						if (PQfformat(result, columnIndex) == 0)
						{
							ereport(ERROR, (errmsg("unexpected text result")));
						}
						resetStringInfo(&stringInfoDataArray[columnIndex]);
						appendBinaryStringInfo(&stringInfoDataArray[columnIndex],
											   value, valueLength);
						columnArray[columnIndex] = &stringInfoDataArray[columnIndex];
					}
					else
					{
						if (PQfformat(result, columnIndex) == 1)
						{
							ereport(ERROR, (errmsg("unexpected binary result")));
						}
						columnArray[columnIndex] = value;
					}

					tupleLibpqSize += valueLength;
				}
			}

			AttInMetadata *attInMetadata =
				shardCommandExecution->attributeInputMetadata[queryIndex];
			HeapTuple heapTuple;
			if (binaryResults)
			{
				heapTuple = BuildTupleFromBytes(attInMetadata,
												(fmStringInfo *) columnArray);
			}
			else
			{
				heapTuple = BuildTupleFromCStrings(attInMetadata,
												   (char **) columnArray);
			}

			MemoryContextSwitchTo(oldContext);

			tupleDest->putTuple(tupleDest, task,
								placementExecution->placementExecutionIndex, queryIndex,
								heapTuple, tupleLibpqSize);

			MemoryContextReset(rowContext);

			execution->rowsProcessed++;
		}

		PQclear(result);
	}

	/* the context is local to the function, so not needed anymore */
	MemoryContextDelete(rowContext);

	return fetchDone;
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

	/*
	 * A pool cannot fail multiple times, the necessary actions
	 * has already be taken, so bail out.
	 */
	if (workerPool->failureState == WORKER_POOL_FAILED ||
		workerPool->failureState == WORKER_POOL_FAILED_OVER_TO_LOCAL)
	{
		return;
	}

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

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, workerPool->sessionList)
	{
		WorkerSessionFailed(session);
	}

	/* we do not want more connections in this pool */
	workerPool->readyTaskCount = 0;
	if (workerPool->failureState != WORKER_POOL_FAILED_OVER_TO_LOCAL)
	{
		/* we prefer not to override WORKER_POOL_FAILED_OVER_TO_LOCAL */
		workerPool->failureState = WORKER_POOL_FAILED;
	}

	/*
	 * The reason is that when replication factor is > 1 and we are performing
	 * a SELECT, then we only establish connections for the specific placements
	 * that we will read from. However, when a worker pool fails, we will need
	 * to establish multiple new connection to other workers and the query
	 * can only succeed if all those connections are established.
	 */
	if (UseConnectionPerPlacement())
	{
		List *workerList = workerPool->distributedExecution->workerList;

		WorkerPool *pool = NULL;
		foreach_declared_ptr(pool, workerList)
		{
			/* failed pools or pools without any connection attempts ignored */
			if (pool->failureState == WORKER_POOL_FAILED ||
				INSTR_TIME_IS_ZERO(pool->poolStartTime))
			{
				continue;
			}

			/*
			 * This should give another NodeConnectionTimeout until all
			 * the necessary connections are established.
			 */
			INSTR_TIME_SET_CURRENT(pool->poolStartTime);
			pool->checkForPoolTimeout = true;
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
	bool failedPlacementExecutionIsOnPendingQueue = false;

	if (placementExecution->executionState == PLACEMENT_EXECUTION_FAILED)
	{
		/*
		 * We may mark placements as failed multiple times, but should only act
		 * the first time. Nor should we accept success after failure.
		 */
		return;
	}

	if (succeeded)
	{
		/* mark the placement execution as finished */
		placementExecution->executionState = PLACEMENT_EXECUTION_FINISHED;

		Assert(INSTR_TIME_IS_ZERO(placementExecution->endTime));
		INSTR_TIME_SET_CURRENT(placementExecution->endTime);
		uint64 durationMicrosecs =
			MicrosecondsBetweenTimestamps(placementExecution->startTime,
										  placementExecution->endTime);
		workerPool->totalTaskExecutionTime += durationMicrosecs;
		workerPool->totalExecutedTasks += 1;

		if (IsLoggableLevel(DEBUG4))
		{
			ereport(DEBUG4, (errmsg("task execution (%d) for placement (%ld) on anchor "
									"shard (%ld) finished in %ld microseconds on worker "
									"node %s:%d", shardCommandExecution->task->taskId,
									placementExecution->shardPlacement->placementId,
									shardCommandExecution->task->anchorShardId,
									durationMicrosecs, workerPool->nodeName,
									workerPool->nodePort)));
		}
	}
	else if (CanFailoverPlacementExecutionToLocalExecution(placementExecution))
	{
		/*
		 * The placement execution can be done over local execution, so it is a soft
		 * failure for now.
		 */
		placementExecution->executionState =
			PLACEMENT_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION;
	}
	else
	{
		if (placementExecution->executionState == PLACEMENT_EXECUTION_NOT_READY)
		{
			/*
			 * If the placement is in NOT_READY state, it means that the placement
			 * execution is assigned to the pending queue of a failed pool or
			 * session. So, we should not schedule the next placement execution based
			 * on this failure.
			 */
			failedPlacementExecutionIsOnPendingQueue = true;
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
	TaskExecutionState newExecutionState =
		TaskExecutionStateMachine(shardCommandExecution);
	if (newExecutionState == TASK_EXECUTION_FINISHED)
	{
		execution->unfinishedTaskCount--;
		return;
	}
	else if (newExecutionState == TASK_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION)
	{
		execution->unfinishedTaskCount--;

		/* move the task to the local execution */
		Task *task = shardCommandExecution->task;
		execution->localTaskList = lappend(execution->localTaskList, task);

		/* remove the task from the remote execution list */
		execution->remoteTaskList = list_delete_ptr(execution->remoteTaskList, task);

		/*
		 * As we decided to failover this task to local execution, we cannot
		 * allow remote execution to this pool during this distributedExecution.
		 */
		SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);
		workerPool->failureState = WORKER_POOL_FAILED_OVER_TO_LOCAL;

		ereport(DEBUG4, (errmsg("Task %d execution is failed over to local execution",
								task->taskId)));

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
	else if (!failedPlacementExecutionIsOnPendingQueue)
	{
		ScheduleNextPlacementExecution(placementExecution, succeeded);
	}
}


/*
 * CanFailoverPlacementExecutionToLocalExecution returns true if the input
 * TaskPlacementExecution can be fail overed to local execution. In other words,
 * the execution can be deferred to local execution.
 */
static bool
CanFailoverPlacementExecutionToLocalExecution(TaskPlacementExecution *placementExecution)
{
	if (!EnableLocalExecution)
	{
		/* the user explicitly disabled local execution */
		return false;
	}

	if (!placementExecution->shardCommandExecution->localExecutionSupported)
	{
		/* cannot execute given task locally */
		return false;
	}

	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_DISABLED)
	{
		/*
		 * If the current transaction accessed the local node over a connection
		 * then we can't use local execution because of visibility issues.
		 */
		return false;
	}

	WorkerPool *workerPool = placementExecution->workerPool;
	if (!workerPool->poolToLocalNode)
	{
		/* we can only fail over tasks to local execution for local pools */
		return false;
	}

	if (workerPool->activeConnectionCount > 0)
	{
		/*
		 * The pool has already active connections, the executor is capable
		 * of using those active connections. So, no need to failover
		 * to the local execution.
		 */
		return false;
	}

	if (placementExecution->assignedSession != NULL)
	{
		/*
		 * If the placement execution has been assigned to a specific session,
		 * it has to be executed over that session. Otherwise, it would cause
		 * self-deadlocks and break read-your-own-writes consistency.
		 */
		return false;
	}

	return true;
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

		/* find a placement execution that is not yet marked as failed */
		do {
			int nextPlacementExecutionIndex =
				placementExecution->placementExecutionIndex + 1;

			/*
			 * If all tasks failed then we should already have errored out.
			 * Still, be defensive and throw error instead of crashes.
			 */
			int placementExecutionCount = shardCommandExecution->placementExecutionCount;
			if (nextPlacementExecutionIndex >= placementExecutionCount)
			{
				WorkerPool *workerPool = placementExecution->workerPool;
				ereport(ERROR, (errmsg("execution cannot recover from multiple "
									   "connection failures. Last node failed "
									   "%s:%d", workerPool->nodeName,
									   workerPool->nodePort)));
			}

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

		if (transactionState == REMOTE_TRANS_NOT_STARTED ||
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
		WorkerSession *session = NULL;
		foreach_declared_ptr(session, workerPool->sessionList)
		{
			MultiConnection *connection = session->connection;
			RemoteTransaction *transaction = &(connection->remoteTransaction);
			RemoteTransactionState transactionState = transaction->transactionState;

			if (transactionState == REMOTE_TRANS_NOT_STARTED ||
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
	int failedOverPlacementCount = 0;
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
		else if (executionState == PLACEMENT_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION)
		{
			failedOverPlacementCount++;
		}

		placementCount++;
	}

	if (failedPlacementCount == placementCount)
	{
		currentTaskExecutionState = TASK_EXECUTION_FAILED;
	}
	else if (executionOrder != EXECUTION_ORDER_ANY && failedPlacementCount > 0)
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
	else if (failedOverPlacementCount + donePlacementCount + failedPlacementCount ==
			 placementCount)
	{
		/*
		 * For any given task, we could have 3 end states:
		 *  - "donePlacementCount" indicates the successful placement executions
		 *  - "failedPlacementCount" indicates the failed placement executions
		 *  - "failedOverPlacementCount" indicates the placement executions that
		 *     are failed when using remote execution due to connection errors,
		 *     but there is still a possibility of being successful via
		 *     local execution. So, for now they are considered as soft
		 *     errors.
		 */
		currentTaskExecutionState = TASK_EXECUTION_FAILOVER_TO_LOCAL_EXECUTION;
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
	/* additional 2 is for postmaster and latch */
	int eventSetSize = GetEventSetSize(sessionList);

	WaitEventSet *waitEventSet =
		CreateWaitEventSet(WaitEventSetTracker_compat, eventSetSize);

	WorkerSession *session = NULL;
	foreach_declared_ptr(session, sessionList)
	{
		AddSessionToWaitEventSet(session, waitEventSet);
	}

	return waitEventSet;
}


/*
 * FreeExecutionWaitEvents is a helper function that gets
 * a DistributedExecution and frees events and waitEventSet.
 */
static void
FreeExecutionWaitEvents(DistributedExecution *execution)
{
	if (execution->events != NULL)
	{
		pfree(execution->events);
		execution->events = NULL;
	}

	if (execution->waitEventSet != NULL)
	{
		FreeWaitEventSet(execution->waitEventSet);
		execution->waitEventSet = NULL;
	}
}


/*
 * AddSessionToWaitEventSet is a helper function which adds the session to
 * the waitEventSet. The function does certain checks before adding the session
 * to the waitEventSet.
 */
static void
AddSessionToWaitEventSet(WorkerSession *session, WaitEventSet *waitEventSet)
{
	MultiConnection *connection = session->connection;

	if (connection->pgConn == NULL)
	{
		/* connection died earlier in the transaction */
		return;
	}

	if (connection->waitFlags == 0)
	{
		/* not currently waiting for this connection */
		return;
	}

	int sock = PQsocket(connection->pgConn);
	if (sock == -1)
	{
		/* connection was closed */
		return;
	}

	int waitEventSetIndex =
		CitusAddWaitEventSetToSet(waitEventSet, connection->waitFlags, sock,
								  NULL, (void *) session);
	session->waitEventSetIndex = waitEventSetIndex;

	/*
	 * Inform failed to add to wait event set with a debug message as this
	 * is too detailed information for users.
	 */
	if (session->waitEventSetIndex == WAIT_EVENT_SET_INDEX_FAILED)
	{
		ereport(DEBUG1, (errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("Adding wait event for node %s:%d failed. "
								"The socket was: %d",
								session->workerPool->nodeName,
								session->workerPool->nodePort, sock)));
	}
}


/*
 * GetEventSetSize returns the event set size for a list of sessions.
 */
static int
GetEventSetSize(List *sessionList)
{
	/* additional 2 is for postmaster and latch */
	return list_length(sessionList) + 2;
}


/*
 * RebuildWaitEventSetFlags modifies the given waitEventSet with the wait flags
 * for connections in the sessionList.
 */
static void
RebuildWaitEventSetFlags(WaitEventSet *waitEventSet, List *sessionList)
{
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;
		int waitEventSetIndex = session->waitEventSetIndex;

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

		int sock = PQsocket(connection->pgConn);
		if (sock == -1)
		{
			/* connection was closed */
			continue;
		}

		bool success =
			CitusModifyWaitEvent(waitEventSet, waitEventSetIndex,
								 connection->waitFlags, NULL);
		if (!success)
		{
			ereport(DEBUG1, (errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("modifying wait event for node %s:%d failed. "
									"The wait event index was: %d",
									connection->hostname, connection->port,
									waitEventSetIndex)));

			session->waitEventSetIndex = WAIT_EVENT_SET_INDEX_FAILED;
		}
	}
}


/*
 * CleanUpSessions does any clean-up necessary for the session used
 * during the execution. We only reach the function after successfully
 * completing all the tasks and we expect no tasks are still in progress.
 */
static void
CleanUpSessions(DistributedExecution *execution)
{
	List *sessionList = execution->sessionList;

	/* we get to this function only after successful executions */
	Assert(!execution->failed && execution->unfinishedTaskCount == 0);

	/* always trigger wait event set in the first round */
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;

		ereport(DEBUG4, (errmsg("Total number of commands sent over the session %ld: %ld "
								"to node %s:%d", session->sessionId,
								session->commandsSent,
								connection->hostname, connection->port)));

		UnclaimConnection(connection);

		if (connection->connectionState == MULTI_CONNECTION_CONNECTING ||
			connection->connectionState == MULTI_CONNECTION_FAILED ||
			connection->connectionState == MULTI_CONNECTION_LOST ||
			connection->connectionState == MULTI_CONNECTION_TIMED_OUT)
		{
			/*
			 * We want the MultiConnection go away and not used in
			 * the subsequent executions.
			 *
			 * We cannot get MULTI_CONNECTION_LOST via the ConnectionStateMachine,
			 * but we might get it via the connection API and find us here before
			 * changing any states in the ConnectionStateMachine.
			 *
			 */
			CloseConnection(connection);
		}
		else if (connection->connectionState == MULTI_CONNECTION_CONNECTED)
		{
			RemoteTransaction *transaction = &(connection->remoteTransaction);
			RemoteTransactionState transactionState = transaction->transactionState;

			if (transactionState == REMOTE_TRANS_CLEARING_RESULTS)
			{
				/*
				 * We might have established the connection, and even sent BEGIN, but not
				 * get to the point where we assigned a task to this specific connection
				 * (because other connections in the pool already finished all the tasks).
				 */
				Assert(session->commandsSent == 0);

				ClearResults(connection, false);
			}
			else if (!(transactionState == REMOTE_TRANS_NOT_STARTED ||
					   transactionState == REMOTE_TRANS_STARTED))
			{
				/*
				 * We don't have to handle anything else. Note that the execution
				 * could only finish on connectionStates of MULTI_CONNECTION_CONNECTING,
				 * MULTI_CONNECTION_FAILED and MULTI_CONNECTION_CONNECTED. The first two
				 * are already handled above.
				 *
				 * When we're on MULTI_CONNECTION_CONNECTED, TransactionStateMachine
				 * ensures that all the necessary commands are successfully sent over
				 * the connection and everything is cleared up. Otherwise, we'd have been
				 * on MULTI_CONNECTION_FAILED state.
				 */
				ereport(WARNING, (errmsg("unexpected transaction state at the end of "
										 "execution: %d", transactionState)));
			}

			/* get ready for the next executions if we need use the same connection */
			connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
		}
		else
		{
			ereport(WARNING, (errmsg("unexpected connection state at the end of "
									 "execution: %d", connection->connectionState)));
		}
	}
}


/*
 * UnclaimAllSessionConnections unclaims all of the connections for the given
 * sessionList.
 */
static void
UnclaimAllSessionConnections(List *sessionList)
{
	WorkerSession *session = NULL;
	foreach_declared_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;

		UnclaimConnection(connection);
	}
}


/*
 * SetLocalForceMaxQueryParallelization is simply a C interface for setting
 * the following:
 *      SET LOCAL citus.force_max_query_parallelization TO on;
 */
void
SetLocalForceMaxQueryParallelization(void)
{
	set_config_option("citus.force_max_query_parallelization", "on",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}
