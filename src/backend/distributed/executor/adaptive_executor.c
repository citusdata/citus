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
 *     from a queue and may execute unasssigned tasks from the WorkerPool.
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

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "distributed/adaptive_executor.h"
#include "distributed/cancel_utils.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/placement_access.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/resource_lock.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/tuple_destination.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "lib/ilist.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

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
	 * connection and not ready to start.
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
} WorkerSession;


struct TaskPlacementExecution;

/* GUC, determining whether Citus opens 1 connection per task */
bool ForceMaxQueryParallelization = false;
int MaxAdaptiveExecutorPoolSize = 16;
bool EnableBinaryProtocol = false;

/* GUC, number of ms to wait between opening connections to the same worker */
int ExecutorSlowStartInterval = 10;


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
} TaskPlacementExecution;


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

static void LockPartitionsForDistributedPlan(DistributedPlan *distributedPlan);
static void AcquireExecutorShardLocksForExecution(DistributedExecution *execution);
static bool DistributedExecutionModifiesDatabase(DistributedExecution *execution);
static bool IsMultiShardModification(RowModifyLevel modLevel, List *taskList);
static bool TaskListModifiesDatabase(RowModifyLevel modLevel, List *taskList);
static bool DistributedExecutionRequiresRollback(List *taskList);
static bool TaskListRequires2PC(List *taskList);
static bool SelectForUpdateOnReferenceTable(List *taskList);
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
static void OpenNewConnections(WorkerPool *workerPool, int newConnectionCount,
							   TransactionProperties *transactionProperties);
static void CheckConnectionTimeout(WorkerPool *workerPool);
static void MarkEstablishingSessionsTimedOut(WorkerPool *workerPool);
static int UsableConnectionCount(WorkerPool *workerPool);
static long NextEventTimeout(DistributedExecution *execution);
static WaitEventSet * BuildWaitEventSet(List *sessionList);
static void RebuildWaitEventSetFlags(WaitEventSet *waitEventSet, List *sessionList);
static TaskPlacementExecution * PopPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopAssignedPlacementExecution(WorkerSession *session);
static TaskPlacementExecution * PopUnassignedPlacementExecution(WorkerPool *workerPool);
static bool StartPlacementExecutionOnSession(TaskPlacementExecution *placementExecution,
											 WorkerSession *session);
static bool SendNextQuery(TaskPlacementExecution *placementExecution,
						  WorkerSession *session);
static void ConnectionStateMachine(WorkerSession *session);
static void HandleMultiConnectionSuccess(WorkerSession *session);
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
static bool ShouldMarkPlacementsInvalidOnFailure(DistributedExecution *execution);
static void PlacementExecutionReady(TaskPlacementExecution *placementExecution);
static TaskExecutionState TaskExecutionStateMachine(ShardCommandExecution *
													shardCommandExecution);
static bool HasDependentJobs(Job *mainJob);
static void ExtractParametersForRemoteExecution(ParamListInfo paramListInfo,
												Oid **parameterTypes,
												const char ***parameterValues);
static int GetEventSetSize(List *sessionList);
static int RebuildWaitEventSet(DistributedExecution *execution);
static void ProcessWaitEvents(DistributedExecution *execution, WaitEvent *events, int
							  eventCount, bool *cancellationReceived);
static long MillisecondsBetweenTimestamps(instr_time startTime, instr_time endTime);
static HeapTuple BuildTupleFromBytes(AttInMetadata *attinmeta, fmStringInfo *values);
static AttInMetadata * TupleDescGetAttBinaryInMetadata(TupleDesc tupdesc);
static int WorkerPoolCompare(const void *lhsKey, const void *rhsKey);
static void SetAttributeInputMetadata(DistributedExecution *execution,
									  ShardCommandExecution *shardCommandExecution);

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

	ExecuteSubPlans(distributedPlan);

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

	/* Reset Task fields that are only valid for a single execution */
	ResetExplainAnalyzeData(taskList);

	scanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	TupleDestination *defaultTupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate, tupleDescriptor);

	if (RequestedForExplainAnalyze(scanState))
	{
		/*
		 * We use multiple queries per task in EXPLAIN ANALYZE which need to
		 * be part of the same transaction.
		 */
		UseCoordinatedTransaction();
		taskList = ExplainAnalyzeTaskList(taskList, defaultTupleDest, tupleDescriptor,
										  paramListInfo);
	}

	bool hasDependentJobs = HasDependentJobs(job);
	if (hasDependentJobs)
	{
		jobIdList = ExecuteDependentTasks(taskList, job);
	}

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		/* defer decision after ExecuteSubPlans() */
		targetPoolSize = 1;
	}

	TransactionProperties xactProperties = DecideTransactionPropertiesForTaskList(
		distributedPlan->modLevel, taskList,
		hasDependentJobs);

	bool localExecutionSupported = true;
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

	if (hasDependentJobs)
	{
		DoRepartitionCleanup(jobIdList);
	}

	if (SortReturning && distributedPlan->expectResults && commandType != CMD_SELECT)
	{
		SortTupleStore(scanState);
	}

	return resultSlot;
}


/*
 * HasDependentJobs returns true if there is any dependent job
 * for the mainjob(top level) job.
 */
static bool
HasDependentJobs(Job *mainJob)
{
	return list_length(mainJob->dependentJobList) > 0;
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
 * ExecuteTaskListIntoTupleStore is a proxy to ExecuteTaskListExtended() with defaults
 * for some of the arguments.
 */
uint64
ExecuteTaskListIntoTupleDest(RowModifyLevel modLevel, List *taskList,
							 TupleDestination *tupleDest,
							 bool expectResults)
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

	return ExecuteTaskListExtended(executionParams);
}


/*
 * ExecuteTaskListExtended sets up the execution for given task list and
 * runs it.
 */
uint64
ExecuteTaskListExtended(ExecutionParams *executionParams)
{
	ParamListInfo paramListInfo = NULL;
	uint64 locallyProcessedRows = 0;

	TupleDestination *defaultTupleDest = executionParams->tupleDestination;

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		executionParams->targetPoolSize = 1;
	}

	DistributedExecution *execution =
		CreateDistributedExecution(
			executionParams->modLevel, executionParams->taskList,
			paramListInfo, executionParams->targetPoolSize,
			defaultTupleDest, &executionParams->xactProperties,
			executionParams->jobIdList, executionParams->localExecutionSupported);

	/*
	 * If current transaction accessed local placements and task list includes
	 * tasks that should be executed locally (accessing any of the local placements),
	 * then we should error out as it would cause inconsistencies across the
	 * remote connection and local execution.
	 */
	List *remoteTaskList = execution->remoteTaskList;
	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_REQUIRED &&
		AnyTaskAccessesLocalNode(remoteTaskList))
	{
		ErrorIfTransactionAccessedPlacementsLocally();
	}

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

	if (localExecutionSupported && ShouldExecuteTasksLocally(taskList))
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

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		/*
		 * We prefer to error on any failures for CREATE INDEX
		 * CONCURRENTLY or VACUUM//VACUUM ANALYZE (e.g., COMMIT_PROTOCOL_BARE).
		 */
		xactProperties.errorOnAnyFailure = true;
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_DISALLOWED;
		return xactProperties;
	}

	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_REQUIRED)
	{
		/*
		 * In case localExecutionHappened, we force the executor to use 2PC.
		 * The primary motivation is that at this point we're definitely expanding
		 * the nodes participated in the transaction. And, by re-generating the
		 * remote task lists during local query execution, we might prevent the adaptive
		 * executor to kick-in 2PC (or even start coordinated transaction, that's why
		 * we prefer adding this check here instead of
		 * Activate2PCIfModifyingTransactionExpandsToNewNode()).
		 */
		xactProperties.errorOnAnyFailure = true;
		xactProperties.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_REQUIRED;
		xactProperties.requires2PC = true;
		return xactProperties;
	}

	if (DistributedExecutionRequiresRollback(taskList))
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
		else if (MultiShardCommitProtocol != COMMIT_PROTOCOL_2PC &&
				 IsMultiShardModification(modLevel, taskList))
		{
			/*
			 * Even if we're not using 2PC, we prefer to error out
			 * on any failures during multi shard modifications/DDLs.
			 */
			xactProperties.errorOnAnyFailure = true;
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
		CoordinatedTransactionUse2PC();
	}

	/*
	 * Prevent unsafe concurrent modifications of replicated shards by taking
	 * locks.
	 *
	 * When modifying a reference tables in MX mode, we take the lock via RPC
	 * to the first worker in a transaction block, which activates a coordinated
	 * transaction. We need to do this before determining whether the execution
	 * should use transaction blocks (see below).
	 */
	AcquireExecutorShardLocksForExecution(execution);

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
 *  DistributedPlanModifiesDatabase returns true if the plan modifies the data
 *  or the schema.
 */
bool
DistributedPlanModifiesDatabase(DistributedPlan *plan)
{
	return TaskListModifiesDatabase(plan->modLevel, plan->workerJob->taskList);
}


/*
 * IsMultiShardModification returns true if the task list is a modification
 * across shards.
 */
static bool
IsMultiShardModification(RowModifyLevel modLevel, List *taskList)
{
	return list_length(taskList) > 1 && TaskListModifiesDatabase(modLevel, taskList);
}


/*
 *  TaskListModifiesDatabase is a helper function for DistributedExecutionModifiesDatabase and
 *  DistributedPlanModifiesDatabase.
 */
static bool
TaskListModifiesDatabase(RowModifyLevel modLevel, List *taskList)
{
	if (modLevel > ROW_MODIFY_READONLY)
	{
		return true;
	}

	/*
	 * If we cannot decide by only checking the row modify level,
	 * we should look closer to the tasks.
	 */
	if (list_length(taskList) < 1)
	{
		/* is this ever possible? */
		return false;
	}

	Task *firstTask = (Task *) linitial(taskList);

	return !ReadOnlyTask(firstTask->taskType);
}


/*
 * DistributedExecutionRequiresRollback returns true if the distributed
 * execution should start a CoordinatedTransaction. In other words, if the
 * function returns true, the execution sends BEGIN; to every connection
 * involved in the distributed execution.
 */
static bool
DistributedExecutionRequiresRollback(List *taskList)
{
	int taskCount = list_length(taskList);

	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		return false;
	}

	if (taskCount == 0)
	{
		return false;
	}

	Task *task = (Task *) linitial(taskList);

	bool selectForUpdate = task->relationRowLockList != NIL;
	if (selectForUpdate)
	{
		/*
		 * Do not check SelectOpensTransactionBlock, always open transaction block
		 * if SELECT FOR UPDATE is executed inside a distributed transaction.
		 */
		return IsTransactionBlock();
	}

	if (ReadOnlyTask(task->taskType))
	{
		return SelectOpensTransactionBlock &&
			   IsTransactionBlock();
	}

	if (IsMultiStatementTransaction())
	{
		return true;
	}

	if (list_length(taskList) > 1)
	{
		return true;
	}

	if (list_length(task->taskPlacementList) > 1)
	{
		if (SingleShardCommitProtocol == COMMIT_PROTOCOL_2PC)
		{
			/*
			 * Adaptive executor opts to error out on queries if a placement is unhealthy,
			 * not marking the placement itself unhealthy in the process.
			 * Use 2PC to rollback placements before the unhealthy replica failed.
			 */
			return true;
		}

		/*
		 * Some tasks don't set replicationModel thus we only
		 * rely on the anchorShardId, not replicationModel.
		 *
		 * TODO: Do we ever need replicationModel in the Task structure?
		 * Can't we always rely on anchorShardId?
		 */
		if (task->anchorShardId != INVALID_SHARD_ID && ReferenceTableShardId(
				task->anchorShardId))
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
 * TaskListRequires2PC determines whether the given task list requires 2PC
 * because the tasks provided operates on a reference table or there are multiple
 * tasks and the commit protocol is 2PC.
 *
 * Note that we currently do not generate tasks lists that involves multiple different
 * tables, thus we only check the first task in the list for reference tables.
 */
static bool
TaskListRequires2PC(List *taskList)
{
	if (taskList == NIL)
	{
		return false;
	}

	Task *task = (Task *) linitial(taskList);
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
	uint64 anchorShardId = task->anchorShardId;
	if (anchorShardId != INVALID_SHARD_ID && ReferenceTableShardId(anchorShardId))
	{
		return true;
	}

	bool multipleTasks = list_length(taskList) > 1;
	if (!ReadOnlyTask(task->taskType) &&
		multipleTasks && MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		return true;
	}

	if (task->taskType == DDL_TASK)
	{
		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
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
	switch (taskType)
	{
		case READ_TASK:
		case MAP_OUTPUT_FETCH_TASK:
		case MAP_TASK:
		case MERGE_TASK:
		{
			return true;
		}

		default:
		{
			return false;
		}
	}
}


/*
 * SelectForUpdateOnReferenceTable returns true if the input task
 * contains a FOR UPDATE clause that locks any reference tables.
 */
static bool
SelectForUpdateOnReferenceTable(List *taskList)
{
	if (list_length(taskList) != 1)
	{
		/* we currently do not support SELECT FOR UPDATE on multi task queries */
		return false;
	}

	Task *task = (Task *) linitial(taskList);
	RelationRowLock *relationRowLock = NULL;
	foreach_ptr(relationRowLock, task->relationRowLockList)
	{
		Oid relationId = relationRowLock->relationId;

		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			return true;
		}
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
 * We do not take executor shard locks for utility commands such as
 * TRUNCATE because the table locks already prevent concurrent access.
 */
static void
AcquireExecutorShardLocksForExecution(DistributedExecution *execution)
{
	RowModifyLevel modLevel = execution->modLevel;

	/* acquire the locks for both the remote and local tasks */
	List *taskList = execution->remoteAndLocalTaskList;

	if (modLevel <= ROW_MODIFY_READONLY &&
		!SelectForUpdateOnReferenceTable(taskList))
	{
		/*
		 * Executor locks only apply to DML commands and SELECT FOR UPDATE queries
		 * touching reference tables.
		 */
		return;
	}

	/*
	 * When executing in sequential mode or only executing a single task, we
	 * do not need multi-shard locks.
	 */
	if (list_length(taskList) == 1 || ShouldRunTasksSequentially(taskList))
	{
		Task *task = NULL;
		foreach_ptr(task, taskList)
		{
			AcquireExecutorShardLocks(task, modLevel);
		}
	}
	else if (list_length(taskList) > 1)
	{
		AcquireExecutorMultiShardLocks(taskList);
	}
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
	foreach_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;

		ereport(DEBUG4, (errmsg("Total number of commands sent over the session %ld: %ld",
								session->sessionId, session->commandsSent)));

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
	foreach_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;

		UnclaimConnection(connection);
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
	foreach_ptr(task, taskList)
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
		shardCommandExecution->placementExecutions =
			(TaskPlacementExecution **) palloc0(placementExecutionCount *
												sizeof(TaskPlacementExecution *));
		shardCommandExecution->placementExecutionCount = placementExecutionCount;

		SetAttributeInputMetadata(execution, shardCommandExecution);
		ShardPlacement *taskPlacement = NULL;
		foreach_ptr(taskPlacement, task->taskPlacementList)
		{
			int connectionFlags = 0;
			char *nodeName = taskPlacement->nodeName;
			int nodePort = taskPlacement->nodePort;
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
	foreach_ptr(session, execution->sessionList)
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
		/*
		 * We only allow binary results when queryCount is 1, because we
		 * cannot use binary results with SendRemoteCommand. Which must be
		 * used if queryCount is larger than 1.
		 */
		else if (EnableBinaryProtocol && queryCount == 1 &&
				 CanUseBinaryCopyFormat(tupleDescriptor))
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
	foreach_ptr(workerPool, execution->workerList)
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
	foreach_ptr(session, workerPool->sessionList)
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
		INSTR_TIME_SET_CURRENT(workerPool->poolStartTime);
		workerPool->checkForPoolTimeout = true;
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
bool
ShouldRunTasksSequentially(List *taskList)
{
	if (list_length(taskList) < 2)
	{
		/* single task plans are already qualified as sequential by definition */
		return false;
	}

	/* all the tasks are the same, so we only look one */
	Task *initialTask = (Task *) linitial(taskList);
	if (initialTask->rowValuesLists != NIL)
	{
		/* found a multi-row INSERT */
		return true;
	}

	return false;
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
	foreach_ptr(taskToExecute, taskList)
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
	WaitEvent *events = NULL;

	AssignTasksToConnectionsOrWorkerPool(execution);

	PG_TRY();
	{
		/* Preemptively step state machines in case of immediate errors */
		WorkerSession *session = NULL;
		foreach_ptr(session, execution->sessionList)
		{
			ConnectionStateMachine(session);
		}

		bool cancellationReceived = false;

		int eventSetSize = GetEventSetSize(execution->sessionList);

		/* always (re)build the wait event set the first time */
		execution->rebuildWaitEventSet = true;

		while (execution->unfinishedTaskCount > 0 && !cancellationReceived)
		{
			WorkerPool *workerPool = NULL;
			foreach_ptr(workerPool, execution->workerList)
			{
				ManageWorkerPool(workerPool);
			}

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
				if (events != NULL)
				{
					/*
					 * The execution might take a while, so explicitly free at this point
					 * because we don't need anymore.
					 */
					pfree(events);
					events = NULL;
				}
				eventSetSize = RebuildWaitEventSet(execution);
				events = palloc0(eventSetSize * sizeof(WaitEvent));
			}
			else if (execution->waitFlagsChanged)
			{
				RebuildWaitEventSetFlags(execution->waitEventSet, execution->sessionList);
				execution->waitFlagsChanged = false;
			}

			/* wait for I/O events */
			long timeout = NextEventTimeout(execution);
			int eventCount = WaitEventSetWait(execution->waitEventSet, timeout, events,
											  eventSetSize, WAIT_EVENT_CLIENT_READ);
			ProcessWaitEvents(execution, events, eventCount, &cancellationReceived);
		}

		if (events != NULL)
		{
			pfree(events);
		}

		if (execution->waitEventSet != NULL)
		{
			FreeWaitEventSet(execution->waitEventSet);
			execution->waitEventSet = NULL;
		}

		CleanUpSessions(execution);
	}
	PG_CATCH();
	{
		/*
		 * We can still recover from error using ROLLBACK TO SAVEPOINT,
		 * unclaim all connections to allow that.
		 */
		UnclaimAllSessionConnections(execution->sessionList);

		/* do repartition cleanup if this is a repartition query*/
		if (list_length(execution->jobIdList) > 0)
		{
			DoRepartitionCleanup(execution->jobIdList);
		}

		if (execution->waitEventSet != NULL)
		{
			FreeWaitEventSet(execution->waitEventSet);
			execution->waitEventSet = NULL;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * RebuildWaitEventSet updates the waitEventSet for the distributed execution.
 * This happens when the connection set for the distributed execution is changed,
 * which means that we need to update which connections we wait on for events.
 * It returns the new event set size.
 */
static int
RebuildWaitEventSet(DistributedExecution *execution)
{
	if (execution->waitEventSet != NULL)
	{
		FreeWaitEventSet(execution->waitEventSet);
		execution->waitEventSet = NULL;
	}

	execution->waitEventSet = BuildWaitEventSet(execution->sessionList);
	execution->rebuildWaitEventSet = false;
	execution->waitFlagsChanged = false;

	return GetEventSetSize(execution->sessionList);
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
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg(
								"could not establish any connections to the node %s:%d "
								"when local execution is also disabled.",
								workerPool->nodeName,
								workerPool->nodePort),
							errhint("Enable local execution via SET "
									"citus.enable_local_execution TO true;")));
		}

		return;
	}

	INSTR_TIME_SET_CURRENT(workerPool->lastConnectionOpenTime);
	execution->rebuildWaitEventSet = true;
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
		if (newConnectionCount > 0)
		{
			/* increase the open rate every cycle (like TCP slow start) */
			workerPool->maxNewConnectionsPerCycle += 1;
		}
	}
	return newConnectionCount;
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
			 * defer this initilization after StartNodeUserDatabaseConnection()
			 * because for non-optional connections, we have some logic to wait
			 * until a connection is allowed to be established.
			 */
			INSTR_TIME_SET_ZERO(workerPool->poolStartTime);
		}

		/* create a session for the connection */
		WorkerSession *session = FindOrCreateWorkerSession(workerPool, connection);

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

			ereport(logLevel, (errcode(ERRCODE_CONNECTION_FAILURE),
							   errmsg("could not establish any connections to the node "
									  "%s:%d after %u ms", workerPool->nodeName,
									  workerPool->nodePort,
									  NodeConnectionTimeout)));

			/*
			 * We hit the connection timeout. In that case, we should not let the
			 * connection establishment to continue because the execution logic
			 * pretends that failed sessions are not going to be used anymore.
			 *
			 * That's why we mark the connection as timed out to trigger the state
			 * changes in the executor.
			 */
			MarkEstablishingSessionsTimedOut(workerPool);
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
	foreach_ptr(session, workerPool->sessionList)
	{
		MultiConnection *connection = session->connection;

		if (connection->connectionState == MULTI_CONNECTION_CONNECTING ||
			connection->connectionState == MULTI_CONNECTION_INITIAL)
		{
			connection->connectionState = MULTI_CONNECTION_TIMED_OUT;
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
	foreach_ptr(workerPool, execution->workerList)
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
				 */
				connection->connectionState = MULTI_CONNECTION_FAILED;
				break;
			}

			case MULTI_CONNECTION_CONNECTING:
			{
				ConnStatusType status = PQstatus(connection->pgConn);
				if (status == CONNECTION_OK)
				{
					HandleMultiConnectionSuccess(session);
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

					connection->connectionState = MULTI_CONNECTION_CONNECTED;
					break;
				}
				else if (status == CONNECTION_BAD)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
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
					HandleMultiConnectionSuccess(session);
					UpdateConnectionWaitFlags(session,
											  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

					connection->connectionState = MULTI_CONNECTION_CONNECTED;

					/* we should have a valid socket */
					Assert(PQsocket(connection->pgConn) != -1);
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

				if (session->currentTask == NULL)
				{
					/* this was an idle connection */
					workerPool->idleConnectionCount--;
				}

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

				/*
				 * The execution may have failed as a result of WorkerSessionFailed
				 * or WorkerPoolFailed.
				 */
				if (execution->failed ||
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
 * HandleMultiConnectionSuccess logs the established connection and updates connection's state.
 */
static void
HandleMultiConnectionSuccess(WorkerSession *session)
{
	MultiConnection *connection = session->connection;
	WorkerPool *workerPool = session->workerPool;

	ereport(DEBUG4, (errmsg("established connection to %s:%d for "
							"session %ld",
							connection->hostname, connection->port,
							session->sessionId)));

	workerPool->activeConnectionCount++;
	workerPool->idleConnectionCount++;
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
	if (MultiShardCommitProtocol != COMMIT_PROTOCOL_2PC)
	{
		/* we don't need 2PC, so no need to continue */
		return;
	}

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

	connection->waitFlags = waitFlags;

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
				scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
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
 * TupleDescGetAttBinaryInMetadata - Build an AttInMetadata structure based on
 * the supplied TupleDesc. AttInMetadata can be used in conjunction with
 * fmStringInfos containing binary encoded types to produce a properly formed
 * tuple.
 *
 * NOTE: This function is a copy of the PG function TupleDescGetAttInMetadata,
 * except that it uses getTypeBinaryInputInfo instead of getTypeInputInfo.
 */
static AttInMetadata *
TupleDescGetAttBinaryInMetadata(TupleDesc tupdesc)
{
	int natts = tupdesc->natts;
	int i;
	Oid atttypeid;
	Oid attinfuncid;

	AttInMetadata *attinmeta = (AttInMetadata *) palloc(sizeof(AttInMetadata));

	/* "Bless" the tupledesc so that we can make rowtype datums with it */
	attinmeta->tupdesc = BlessTupleDesc(tupdesc);

	/*
	 * Gather info needed later to call the "in" function for each attribute
	 */
	FmgrInfo *attinfuncinfo = (FmgrInfo *) palloc0(natts * sizeof(FmgrInfo));
	Oid *attioparams = (Oid *) palloc0(natts * sizeof(Oid));
	int32 *atttypmods = (int32 *) palloc0(natts * sizeof(int32));

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Ignore dropped attributes */
		if (!att->attisdropped)
		{
			atttypeid = att->atttypid;
			getTypeBinaryInputInfo(atttypeid, &attinfuncid, &attioparams[i]);
			fmgr_info(attinfuncid, &attinfuncinfo[i]);
			atttypmods[i] = att->atttypmod;
		}
	}
	attinmeta->attinfuncs = attinfuncinfo;
	attinmeta->attioparams = attioparams;
	attinmeta->atttypmods = atttypmods;

	return attinmeta;
}


/*
 * BuildTupleFromBytes - build a HeapTuple given user data in binary form.
 * values is an array of StringInfos, one for each attribute of the return
 * tuple. A NULL StringInfo pointer indicates we want to create a NULL field.
 *
 * NOTE: This function is a copy of the PG function BuildTupleFromCStrings,
 * except that it uses ReceiveFunctionCall instead of InputFunctionCall.
 */
static HeapTuple
BuildTupleFromBytes(AttInMetadata *attinmeta, fmStringInfo *values)
{
	TupleDesc tupdesc = attinmeta->tupdesc;
	int natts = tupdesc->natts;
	int i;

	Datum *dvalues = (Datum *) palloc(natts * sizeof(Datum));
	bool *nulls = (bool *) palloc(natts * sizeof(bool));

	/*
	 * Call the "in" function for each non-dropped attribute, even for nulls,
	 * to support domains.
	 */
	for (i = 0; i < natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			/* Non-dropped attributes */
			dvalues[i] = ReceiveFunctionCall(&attinmeta->attinfuncs[i],
											 values[i],
											 attinmeta->attioparams[i],
											 attinmeta->atttypmods[i]);
			if (values[i] != NULL)
			{
				nulls[i] = false;
			}
			else
			{
				nulls[i] = true;
			}
		}
		else
		{
			/* Handle dropped attributes by setting to NULL */
			dvalues[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	/*
	 * Form a tuple
	 */
	HeapTuple tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	/*
	 * Release locally palloc'd space.  XXX would probably be good to pfree
	 * values of pass-by-reference datums, as well.
	 */
	pfree(dvalues);
	pfree(nulls);

	return tuple;
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
	foreach_ptr(session, workerPool->sessionList)
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
		foreach_ptr(pool, workerList)
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
		if (ShouldMarkPlacementsInvalidOnFailure(execution))
		{
			ShardPlacement *shardPlacement = placementExecution->shardPlacement;

			/*
			 * We only set shard state if it currently is SHARD_STATE_ACTIVE, which
			 * prevents overwriting shard state if it was already set somewhere else.
			 */
			if (shardPlacement->shardState == SHARD_STATE_ACTIVE)
			{
				MarkShardPlacementInactive(shardPlacement);
			}
		}

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
		int placementExecutionCount PG_USED_FOR_ASSERTS_ONLY =
			shardCommandExecution->placementExecutionCount;

		/* find a placement execution that is not yet marked as failed */
		do {
			int nextPlacementExecutionIndex =
				placementExecution->placementExecutionIndex + 1;

			/*
			 * If all tasks failed then we should already have errored out.
			 * Still, be defensive and throw error instead of crashes.
			 */
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
 * ShouldMarkPlacementsInvalidOnFailure returns true if the failure
 * should trigger marking placements invalid.
 */
static bool
ShouldMarkPlacementsInvalidOnFailure(DistributedExecution *execution)
{
	if (!DistributedExecutionModifiesDatabase(execution) ||
		execution->transactionProperties->errorOnAnyFailure)
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
		foreach_ptr(session, workerPool->sessionList)
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
		CreateWaitEventSet(CurrentMemoryContext, eventSetSize);

	WorkerSession *session = NULL;
	foreach_ptr(session, sessionList)
	{
		MultiConnection *connection = session->connection;

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

		int waitEventSetIndex = AddWaitEventToSet(waitEventSet, connection->waitFlags,
												  sock, NULL, (void *) session);
		session->waitEventSetIndex = waitEventSetIndex;
	}

	AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	return waitEventSet;
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
	foreach_ptr(session, sessionList)
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

		ModifyWaitEvent(waitEventSet, waitEventSetIndex, connection->waitFlags, NULL);
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


/*
 * ExtractParametersForRemoteExecution extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * It changes oid of custom types to InvalidOid so that they are the same in workers
 * and coordinators.
 */
static void
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
	int parameterCount = paramListInfo->numParams;

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the coordinator and worker nodes. Therefore, the worker nodes can
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
