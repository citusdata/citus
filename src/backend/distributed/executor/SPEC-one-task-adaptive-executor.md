# Spec: One-Task Adaptive Executor

| Field        | Value                                 |
|--------------|---------------------------------------|
| Author       | Colm                                  |
| Branch       | `colm/one-task-adaptive-executor`     |
| Status       | Draft                                 |
| Date         | 2026-03-03                            |

---

## 1. Problem Statement

The Citus adaptive executor treats single-task queries (single-shard SELECTs, point INSERTs, single-shard UPDATE/DELETE) identically to multi-task queries. For the overwhelmingly common case of one task targeting one placement on one remote node, the executor still:

- Allocates the full `DistributedExecution` struct (with column arrays, wait-event sets, etc.)
- Creates `WorkerPool`, `WorkerSession`, `ShardCommandExecution`, `TaskPlacementExecution` objects
- Runs the full `ConnectionStateMachine` / `TransactionStateMachine` event loop
- Builds and tears down a `WaitEventSet` for a single socket
- Traverses task and placement queues designed for many-to-many fan-out

This per-query overhead is negligible for complex multi-shard queries but becomes significant at high QPS for simple point queries, which are Citus's most performance-sensitive workload.

In one sentence, the problem is that throughput of single-shard queries and OLTP workloads is negatively impacted by the overhead in the Citus Adaptive Executor. 

## 2. Goals

- [ ] Reduce per-query executor overhead for single-task, single-placement remote queries
- [ ] No behavioral change for multi-task queries (zero risk to existing functionality)
- [ ] No change to transaction semantics, error handling, or EXPLAIN output
- [ ] Measurable improvement in throughput (QPS) for simple point queries (target: at least 10%)

## 3. Non-Goals

- Optimizing multi-shard queries
- Changing the planner or fast-path router planner
- Changing connection caching / pooling behavior
- Changing INSERT...SELECT, MERGE, or repartition code paths

## 4. Background

### 4.1 Current Execution Flow
The current flow for all adaptive-executor queries, regardless of task count:

```
CitusExecScan()
  └─ AdaptiveExecutor(scanState)
       ├─ tuplestore_begin_heap()                    ← allocate result store
       ├─ CreateTupleStoreTupleDest()
       ├─ DecideTaskListTransactionProperties()      ← determine 2PC, txn blocks
       ├─ copyParamList() + MarkUnreferencedExternParams()
       ├─ CreateDistributedExecution()               ← allocate DistributedExecution
       │    ├─ palloc0 column arrays (16 cols)
       │    ├─ ShouldExecuteTasksLocally()           ← iterate placements
       │    └─ ExtractLocalAndRemoteTasks()
       ├─ StartDistributedExecution()                ← coordinated txn, shard locks
       ├─ RunDistributedExecution()                  ← THE EVENT LOOP
       │    ├─ AssignTasksToConnectionsOrWorkerPool()
       │    │    ├─ alloc ShardCommandExecution
       │    │    ├─ alloc TaskPlacementExecution per placement
       │    │    ├─ FindOrCreateWorkerPool()          ← linear scan + palloc
       │    │    └─ SortList(workerList)
       │    ├─ ManageWorkerPool()                    ← slow-start, open connections
       │    │    └─ StartNodeUserDatabaseConnection()
       │    ├─ BuildWaitEventSet()                   ← palloc + AddWaitEventToSet
       │    └─ while (unfinishedTaskCount > 0):
       │         WaitEventSetWait()
       │         ProcessWaitEvents()
       │         ConnectionStateMachine()
       │           └─ TransactionStateMachine()
       │                ├─ send BEGIN (if txn block)
       │                ├─ send query
       │                └─ ReceiveResults() → tuplestore
       ├─ FinishDistributedExecution()
       └─ SortTupleStore() (if RETURNING + ORDER BY)
```

### 4.2 Key Data Structures

| Structure                 | Allocated Per  | Purpose                              |
|---------------------------|----------------|--------------------------------------|
| `DistributedExecution`    | query          | Central execution state              |
| `WorkerPool`              | worker node    | Connection pool per node             |
| `WorkerSession`           | connection     | Per-connection state machine         |
| `ShardCommandExecution`   | task           | Per-task execution tracking          |
| `TaskPlacementExecution`  | placement      | Per-placement execution state        |
| `WaitEventSet`            | event loop     | I/O multiplexing (epoll/kqueue)      |

### 4.3 Overhead Analysis for Single-Task Queries

For a single-task, single-placement query (e.g. `SELECT * FROM t WHERE id = 42`):

1. **Struct allocation**: All 6 structs above are allocated, for exactly 1 task
2. **Event loop setup**: `WaitEventSet` built and torn down for 1 socket
3. **Worker pool management**: `FindOrCreateWorkerPool` does a linear scan of an empty list, allocates a pool, sorts a 1-element list
4. **State machine overhead**: `ConnectionStateMachine` + `TransactionStateMachine` run their full state progressions for 1 connection

### 4.4 Existing Fast Paths

- **Planner fast path** (`PlanFastPathDistributedStmt`): Speeds up planning for simple queries. Does not affect execution.
- **Local execution** (`local_executor.c`): Completely bypasses adaptive executor when the shard is on the coordinator. Uses direct PostgreSQL executor calls.
- **`fastPathRouterPlan` flag** on `DistributedPlan`: Set by the planner for single-table, single-partition-value queries. Used in `CitusBeginReadOnlyScan` and `CitusBeginModifyScan` to decide deferred pruning, but not used by the executor itself.

## 5. Proposed Design

### 5.1 High-Level Approach

The one task adaptive executor clones and refactors the AdapativeExecutor() function in adaptive_executor.c. The signature of the function is:
 
`TupleTableSlot *OneTaskAdaptiveExecutor(CitusScanState *scanState)`

The OneTaskAdaptiveExecutor is selected by the distributed planner, in `FinalizePlan()`. We will need to extend citus_custom_scan.c with a new function `CitusExecOneTaskScan()`, that is identical to `CitusExecScan()` except it calls `OneTaskAdaptiveExecutor()` instead of `AdaptiveExecutor()`. We need a new CustomExecMethods instance to be called `OneTaskAdaptiveExecutorCustomExecMethods`, with custom name "OneTaskAdaptiveExecutor" and `ExecCustomScan` method pointing to `CitusExecOneTaskScan()`. There must also be a new function `static Node* OneTaskAdaptiveExecutorCreateScan()`, that is similar to `AdaptiveExecutorCreateScan()` except that it uses `MULTI_ONE_TASK_ADAPTIVE_EXECUTOR` as the executorType and the custom scan state methods are pointing at `OneTaskAdaptiveExecutorCustomExecMethods`. This implies a new CustomScanMethods instance, let's call that `OneTaskAdaptiveExecutorCustomScanMethods`, and that must be registered by `RegisterCitusCustomScanMethods()`. 

The `MultiExecutorType` Enum in multi_server_executor.h needs a new value: `MULTI_ONE_TASK_ADAPTIVE_EXECUTOR`

The implementation of `OneTaskAdaptiveExecutor()` can be in `adaptive_executor.c`, because it may need to use existing functionality that `AdaptiveExecutor()` uses. 

`FinalizePlan()` in distributed_planner.c uses the one task adaptive executor if `JobExecutorType()` returns `MULTI_ONE_TASK_ADAPTIVE_EXECUTOR`; `JobExecutorType()` does so if the distributed plan's `fastPathRouterPlan` is true. 

### 5.2 Eligibility Criteria

**Eligibility criteria:**
- [ ] `distribtuedPlan->fastPathRouterPlan` is true
- [ ] Single placement for that task
- [ ] No dependent tasks / sub-plans that require special handling
- [ ] <!-- Add additional criteria as needed -->

**Detection point:**
Function `JobExecutorType()` detects and returns `MULTI_ONE_TASK_ADAPTIVE_EXECUTOR` if the given distributed plan has its `fastPathRouterPlan` turned on (i.e., is true). `FinalizePlan()` responds to that by using OneTaskAdaptiveExecutorCustomScanMethods for the scan's methods.

### 5.3 Optimized Execution Path

Instead of a DistributedExecution, the OneTaskAdaptiveExcecutor is driven by a OneTaskExecution:
```
Typedef struct OneTaskExec 
{
	RowModifyLevel modLevel;	
	Task *task; // the task for the execution
	TupleDestination *defaultTupleDest; // Used when task destination not spec
	ParamListInfo paramListInfo; // For paramd plans. Can be null.
	Worker *worker; // Worker involved; NULL => Local execution
	Session *session; // Connection; NULL => Local execution
	bool rebuildWaitEventSet; // connection we are interested in has changed
	bool waitFlagsChanged; // wait events we are interested in has changed

	allocatedColumnCount;
	columnArray;		// can be initialized directly from the shard query
} OneTaskExec;
```

We can also have data structures for single worker and single session to simplify execution:
```
Worker	// Session on a worker
{
	OneTaskExec *myExecution;
	char *nodeName;
	int nodePort;

	Session *session;
	bool isActive; // true => connection established; 

	TaskPlacementExecution *myTask;
	ENUM_TYPE	taskState; // not_assigned; conn_assigned; 

	bool workerToLocalNode; // true => worker is for local node

}	

Session
{
	Worker *myWorker;
	TaskPlacementExecution *myTask;
  // other fields as needed
}
```

```
CitusExecOneTaskScan()
  └─ OneTaskAdaptiveExecutor(scanState)
       ├─ tuplestore_begin_heap()                    ← allocate result store
       ├─ CreateTupleStoreTupleDest()
       ├─ DecideTaskTransactionProperties()      ← determine 2PC, txn blocks
       ├─ copyParamList() + MarkUnreferencedExternParams()
       ├─ CreateOneTaskExecution()               ← allocate one task DistributedExecution
       │    ├─ palloc0 column arrays according to the query results descriptor
       │    ├─ ShouldExecuteTaskLocally()           ← Determine if Task is local or remote
       │    
       
       If task is remote:
       ├─ StartOneTaskDistributedExecution()   ← coordinated txn, shard locks; refactor of StartDistributedExecution()
       ├─ RunOneTaskDistributedExecution()     ← Refactor of RunDistributedExecution() for one task
       │    ├─ AssignTaskToConnectionsOrWorkerPool()
       │    │    ├─ alloc ShardCommandExecution
       │    │    ├─ alloc TaskPlacementExecution if needed
       │    │    ├─ FindOrCreateWorker()   ← refactor of FindOrCreateWorkerPool() that assumes one task, one worker, one session
       │    │    
       │    ├─ ManageWorkerPool()                    ← slow-start, open connections
       │    │    └─ StartNodeUserDatabaseConnection()
       │    ├─ BuildWaitEventSet()                   ← palloc + AddWaitEventToSet
       │    └─ while (unfinishedTaskCount > 0):
       │         WaitEventSetWait()
       │         ProcessWaitEvents()
       │         ConnectionStateMachine()
       │           └─ TransactionStateMachine()
       │                ├─ send BEGIN (if txn block)
       │                ├─ send query
       │                └─ ReceiveResults() → tuplestore
       ├─ FinishDistributedExecution()
       
       If task is local:
       ├─ RunOneTaskLocalExecution()
       |  |─ Refactor of LocalExecution() for one task

       └─ SortTupleStore() (if RETURNING + ORDER BY)
```

### 5.4 Connection Handling

Connection handling should be equivalent to the connection handling code-path for a single task execution in AdaptiveExecutor, but without the overhead for multiple tasks, sessions and connections. There is at most one connection, if the shard is remote. If the execution is local, then there are no connections needed.

### 5.5 Transaction Semantics

Transaction semantics should be identicial to a single task execution in AdaptiveExecutor, without multi-task overhead.

### 5.6 Error Handling

Error handling should be identical to a single task execution in AdaptiveExecutor. If an error is hit, do not fall back to AdaptiveExecutor, just error out, with the same message used by AdaptiveExecutor.

### 5.7 Result Handling

Use a tuplestore, just like AdaptiveExecutor currently does. This may be enhanced to a result slot in the future, if the query returns provably one row, or if it is a modification statement without a RETURNING clause.

## 6. Affected Files

citus_custom_scan.h
citus_custom_scan.c
adaptive_executor.c
local_executor.c
distributed_planner.c

## 7. Testing Strategy

### 7.1 Correctness

- [ ] All existing regression tests pass without modification

### 7.2 Edge Cases

- [ ] Single-shard SELECT with parameters (prepared statement)
- [ ] Single-shard INSERT (fast-path + deferred pruning)
- [ ] Single-shard UPDATE/DELETE
- [ ] Query in an explicit transaction block (`BEGIN ... COMMIT`)
- [ ] Query with `force_delegation`
- [ ] Query during shard move / split (shard doesn't exist → reroute)
- [ ] Query with RETURNING clause
- [ ] EXPLAIN ANALYZE on optimized path
- [ ] Connection failure during optimized execution
- [ ] Fallback from optimized path to full executor

### 7.3 Performance

- [ ] Benchmark: pgbench / custom harness with simple point queries
- [ ] Metric: QPS at saturation, p99 latency
- [ ] Comparison: before vs. after on same hardware

## 8. Rollout & Risk

### 8.1 Feature Flag / GUC

The GUC `citus.enable_single_task_fast_path` enables or disables this feature. It is checked by `JobExecutorType()`

| GUC Name | Default | Description |
|----------|---------|-------------|
| <!-- e.g. `citus.enable_single_task_fast_path` --> | <!-- on --> | <!-- ... --> |

### 8.2 Risks

- [ ] Behavioral divergence between fast path and full executor
- [ ] Edge case where eligibility check is wrong (query uses fast path but shouldn't)
- [ ] <!-- ... -->

### 8.3 Rollback Plan

## 9. Future Work

- [ ] Avoid tuplestore for single-row results (direct slot return)
- [ ] Pre-allocated single-task execution state (avoid palloc per query)
- [ ] <!-- ... -->

## 10. Open Questions
