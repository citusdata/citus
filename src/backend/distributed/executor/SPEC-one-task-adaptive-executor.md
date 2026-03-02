# Spec: One-Task Adaptive Executor

## 1. Problem Statement

The adaptive executor (`adaptive_executor.c`, ~5000 lines) is designed to execute
an arbitrary list of tasks across multiple worker nodes over connection pools.
It manages complex state machines for multi-pool connection management, slow-start
connection opening, cost-based parallelism decisions, task queuing across sessions,
WaitEventSet construction, and per-pool failure tracking.

However, the **most common execution path is a single task on a single shard**:
point SELECTs, single-row INSERTs, single-shard UPDATEs/DELETEs, and single-shard
utility commands. This single-task path still traverses the full multi-task
machinery, paying for allocation and initialization of structures it never uses.

### Quantified Overhead for Single-Task Execution

For a single task targeting one placement on one worker, the current code path:

| Step | Cost | Lines |
|------|------|-------|
| Allocates `DistributedExecution` (39 fields) | `palloc0(sizeof(...))` ~320 bytes | L1173–1321 |
| Splits task list into local/remote | `ShouldExecuteTasksLocally` + `ExtractLocalAndRemoteTasks` + `list_copy` | L1231–1245 |
| Calls `DecideTaskListTransactionProperties` | Scans task list for 2PC, error policy | L1260–1327 |
| Pre-allocates 16-element `columnArray` and `StringInfoData` array | Two `palloc0` calls | L1203–1226 |
| `AssignTasksToConnectionsOrWorkerPool` | Allocates `ShardCommandExecution`, `TaskPlacementExecution`, `WorkerPool`, possibly `WorkerSession`; initializes 6 `dlist_head` queues, does `FindOrCreateWorkerPool` linear search, `SortList` on workerList | L1449–1680 |
| `ManageWorkerPool` | Evaluates slow-start, `CalculateNewConnectionCount`, cost-based estimation | L2221–2466 |
| `BuildWaitEventSet` + `RebuildWaitEventSetForSessions` | Allocates `WaitEventSet` + events array for 1 connection + latch + postmaster | L4775–4850 |
| `NextEventTimeout` | Iterates all pools for slow-start and connection timeout computation | L2880–2930 |
| Main loop condition checking | `HasIncompleteConnectionEstablishment` iterates all sessions every iteration | L2055–2075 |
| `SequentialRunDistributedExecution` check | `ShouldRunTasksSequentially()` evaluated even for single task | L876 |

**Goal**: Provide a fast path that bypasses all of this for the single-task case,
reducing per-query overhead (allocations, list operations, queue initialization)
while preserving correctness.

## 2. Design Goals

1. **Zero behavioral change** for any existing query — the fast path must produce
   identical results, error handling, and transactional behavior.
2. **Measurable latency reduction** on single-shard queries (target: 5–15%
   reduction in executor overhead for point queries).
3. **Minimal code duplication** — reuse existing connection management,
   `TransactionStateMachine`, result-receiving logic, and error handling.
4. **Clean fallback** — if the fast path detects it cannot handle a case at
   any point, it falls back to the existing `RunDistributedExecution`.
5. **GUC-gated** — a new GUC `citus.enable_single_task_fast_path` (default `true`)
   allows toggling the fast path on/off for comparative benchmarking and
   safe rollout.

## 3. GUC Definition

| Property | Value |
|----------|-------|
| Name | `citus.enable_single_task_fast_path` |
| Type | `bool` |
| Default | `true` |
| Context | `PGC_USERSET` (can be changed per-session, no reload required) |
| Description | `"When enabled, single-task queries bypass the full adaptive executor machinery for reduced overhead."` |
| Location | Registered in `shared_library_init.c` alongside existing Citus GUCs |

When set to `false`, the eligibility check (`CanUseSingleTaskFastPath`) short-circuits
to return `false`, forcing all queries through the existing `RunDistributedExecution`
path. This enables A/B benchmarking:

```sql
-- Compare latency with fast path enabled vs disabled
SET citus.enable_single_task_fast_path TO false;
-- run benchmark
SET citus.enable_single_task_fast_path TO true;
-- run benchmark again
```

## 4. Scope & Eligibility Criteria

The one-task fast path applies when **all** of the following hold:

| Criterion | Rationale |
|-----------|-----------|
| `citus.enable_single_task_fast_path` is `true` | GUC must be enabled |
| `list_length(taskList) == 1` | Only one task to execute |
| Task has exactly 1 placement (or `executionOrder == EXECUTION_ORDER_ANY` and we only need the first) | No replica write coordination needed |
| Task is **not** local (goes through remote execution) | Local execution already has its own fast path |
| `targetPoolSize >= 1` (not `SEQUENTIAL_CONNECTION` with 0) | Connection is available |
| No `EXPLAIN ANALYZE` wrapping (single query per task) | Multi-query per task is complex |
| Not inside `SequentialRunDistributedExecution` | That path already unwraps to single tasks but needs its loop |
| No dependent jobs (`jobIdList == NIL`) | Repartition cleanup is orthogonal |
| Task is not a multi-query task (`task->queryCount <= 1`) | Multi-query needs stateful tracking |

## 5. Architecture

### 5.1 New Function: `RunSingleTaskExecution`

```
                    ┌──────────────────────────┐
                    │ ExecuteTaskListExtended   │
                    │ / AdaptiveExecutor        │
                    └──────────┬───────────────┘
                               │
                    ┌──────────▼───────────────┐
                    │  IsSingleTaskEligible()   │
                    │  (eligibility check)      │
                    └───┬──────────────────┬───┘
                        │ yes              │ no
               ┌────────▼──────┐  ┌────────▼──────────┐
               │ RunSingleTask │  │ RunDistributed     │
               │ Execution()   │  │ Execution()        │
               │ (new fast     │  │ (existing path,    │
               │  path)        │  │  unchanged)        │
               └───────────────┘  └────────────────────┘
```

### 5.2 `RunSingleTaskExecution` Flow

```
RunSingleTaskExecution(execution)
  │
  ├─ 1. Extract the single Task and its first ShardPlacement
  │
  ├─ 2. Get or establish connection
  │     ├─ Check GetConnectionIfPlacementAccessedInXact()
  │     │   (reuse existing transactional connection)
  │     └─ Otherwise: StartNodeUserDatabaseConnection()
  │
  ├─ 3. Claim connection exclusively
  │     └─ ClaimConnectionExclusively(connection)
  │
  ├─ 4. Wait for connection to be ready (if async)
  │     └─ FinishConnectionEstablishment(connection)
  │        (simpler than WaitEventSet: poll single fd)
  │
  ├─ 5. Send BEGIN if needed (RemoteTransactionBeginIfNecessary)
  │
  ├─ 6. Send query
  │     └─ SendRemoteCommand / SendRemoteCommandParams
  │
  ├─ 7. Receive results
  │     └─ Loop: PQgetResult()
  │        ├─ Store tuples via TupleDestination
  │        └─ Accumulate rowsProcessed
  │
  ├─ 8. Handle errors (same as existing path)
  │     └─ ReportResultError / MarkFailedShardPlacements
  │
  ├─ 9. Unclaim connection
  │     └─ UnclaimConnection(connection)
  │
  └─ 10. Return
```

### 5.3 Struct Allocation Savings

Instead of allocating the full struct hierarchy (`DistributedExecution` →
`WorkerPool` → `WorkerSession` → `ShardCommandExecution` →
`TaskPlacementExecution`), the fast path works with:

- The existing `DistributedExecution` (already allocated by caller)
- A single `MultiConnection *` (obtained from connection cache or pool)
- The `Task *` directly from the task list
- The `ShardPlacement *` from the task's placement list

No `WorkerPool`, `WorkerSession`, `ShardCommandExecution`, or
`TaskPlacementExecution` is allocated.

### 5.4 Connection Handling

The fast path reuses the existing connection infrastructure:

1. **Transactional reuse**: `GetConnectionIfPlacementAccessedInXact()` — if
   this placement was previously accessed in the current transaction, we MUST
   use the same connection (for lock visibility and write consistency).

2. **New connection**: `StartNodeUserDatabaseConnection()` with appropriate
   flags (`OUTSIDE_TRANSACTION` if `excludeFromTransaction` is set).

3. **2PC activation**: Call `Activate2PCIfModifyingTransactionExpandsToNewNode()`
   when appropriate (writes spanning multiple nodes).

4. **Connection claiming**: Same `ClaimConnectionExclusively` / `UnclaimConnection`
   protocol as the existing path.

### 5.5 Integration Points

#### Entry from `AdaptiveExecutor` (scan path)

In the `AdaptiveExecutor` function, after `CreateDistributedExecution`, before
the `ShouldRunTasksSequentially` check:

```c
if (CanUseSingleTaskFastPath(execution))
{
    RunSingleTaskExecution(execution);
}
else if (ShouldRunTasksSequentially(execution->remoteTaskList))
{
    SequentialRunDistributedExecution(execution);
}
else
{
    RunDistributedExecution(execution);
}
```

#### Entry from `ExecuteTaskListExtended` (non-scan path)

In `ExecuteTaskListExtended`, after `CreateDistributedExecution`:

```c
StartDistributedExecution(execution);

if (CanUseSingleTaskFastPath(execution))
{
    RunSingleTaskExecution(execution);
}
else
{
    RunDistributedExecution(execution);
}

FinishDistributedExecution(execution);
```

## 6. Detailed Function Specifications

### 6.1 `CanUseSingleTaskFastPath`

```c
/*
 * CanUseSingleTaskFastPath checks whether a distributed execution
 * can use the optimized single-task path that bypasses pool management,
 * task queuing, and WaitEventSet construction.
 */
static bool
CanUseSingleTaskFastPath(DistributedExecution *execution)
{
    /* GUC gate */
    if (!EnableSingleTaskFastPath)
        return false;

    /* must be exactly one remote task, no local tasks */
    if (list_length(execution->remoteTaskList) != 1 ||
        list_length(execution->localTaskList) != 0)
        return false;

    Task *task = linitial(execution->remoteTaskList);

    /* must have exactly one placement, or be a read that only needs one */
    if (task->taskPlacementList == NIL)
        return false;

    /* multi-query tasks need the full state machine */
    if (task->queryCount > 1)
        return false;

    /* replicated writes need sequential/parallel placement execution */
    RowModifyLevel modLevel = execution->modLevel;
    if (modLevel > ROW_MODIFY_READONLY &&
        list_length(task->taskPlacementList) > 1)
        return false;

    return true;
}
```

### 6.2 `RunSingleTaskExecution`

```c
/*
 * RunSingleTaskExecution executes a single task on a single placement
 * without constructing WorkerPool/WorkerSession/ShardCommandExecution
 * structures or WaitEventSets.
 *
 * This is the fast path for the most common case: a single-shard query.
 * It directly obtains a connection, sends the query, and receives results.
 *
 * On any unexpected condition, it falls back to RunDistributedExecution.
 */
static void
RunSingleTaskExecution(DistributedExecution *execution)
```

**Pseudocode** (see §4.2 for the flow diagram):

1. Extract `Task *task` and `ShardPlacement *placement`.
2. Obtain connection via existing placement-transaction cache or open new.
3. If connection establishment fails, fall back to `RunDistributedExecution`.
4. Handle 2PC activation if this is a modifying transaction expanding to a new node.
5. Build and send the query string (respecting `paramListInfo` if present).
6. Loop on `PQgetResult`:
   - For each successful tuple row, pass to `execution->defaultTupleDest`.
   - Track `execution->rowsProcessed`.
7. On error from `PQresultStatus`:
   - If failover is possible (read-only, multiple placements exist), fall
     back to `RunDistributedExecution`.
   - Otherwise, report error via `ReportResultError`.
8. Unclaim connection on exit.

### 6.3 Result Receiving

Reuse the existing `StoreResult` / tuple destination infrastructure:

- `execution->defaultTupleDest->putTuple(...)` for each row
- Same binary/text protocol handling via `SubPlanResultDestReceiver` or
  `TupleStoreTupleDest`
- Same `columnArray` / `stringInfoDataArray` usage from `DistributedExecution`
  (already allocated by `CreateDistributedExecution`)

### 6.4 Error Handling

The fast path uses the same error semantics as the full executor:

- **Connection failure during establishment**: Fall back to full path (which
  can try other placements or fail gracefully)
- **Query execution error**: Same `ReportResultError` codepath
- **Cancellation**: Same `CHECK_FOR_INTERRUPTS()` placement as existing code
- **Shard marking**: Same `MarkFailedShardPlacements` if placement is
  unreachable

### 6.5 Transaction Integration

- `StartDistributedExecution` is called BEFORE the fast path (same as today) —
  this handles lock acquisition and distributed transaction setup.
- `FinishDistributedExecution` is called AFTER — this handles stats, cleanup.
- The fast path calls `RemoteTransactionBeginIfNecessary` if needed (same as
  `TransactionStateMachine` does internally).
- 2PC upgrade path remains: `Activate2PCIfModifyingTransactionExpandsToNewNode`.

## 7. Fallback Strategy

The fast path can bail out at multiple points and delegate to the full executor:

| Bail-out point | Condition | Action |
|----------------|-----------|--------|
| Eligibility check | Criteria in §3 not met | Never enters fast path |
| Connection establishment | `FinishConnectionEstablishment` fails | Call `RunDistributedExecution(execution)` |
| Query send | `SendRemoteCommand` fails | Call `RunDistributedExecution(execution)` |
| Result receive | Error result with failover possible | Call `RunDistributedExecution(execution)` |

**Important**: Once the fast path has sent a command successfully and begun
receiving results, it does NOT fall back — errors at that point are terminal
(same as the existing executor).

## 8. Files to Modify

| File | Change |
|------|--------|
| `src/backend/distributed/executor/adaptive_executor.c` | Add `bool EnableSingleTaskFastPath` GUC variable, `CanUseSingleTaskFastPath()`, `RunSingleTaskExecution()`. Modify `AdaptiveExecutor()` and `ExecuteTaskListExtended()` to call fast path when eligible. |
| `src/backend/distributed/shared_library_init.c` | Register `citus.enable_single_task_fast_path` GUC via `DefineCustomBoolVariable` |
| `src/include/distributed/adaptive_executor.h` | Declare `extern bool EnableSingleTaskFastPath` |

Optional future optimizations (out of scope for initial implementation):

| File | Change |
|------|--------|
| `adaptive_executor.c` | Reduce `CreateDistributedExecution` allocation for single-task case (lazy `columnArray` allocation) |
| `adaptive_executor.c` | Skip `SortList(workerList)` when only one worker |

## 9. Testing Strategy

### 9.1 Correctness Tests

The fast path should be **invisible** to users — no behavioral change. Therefore:

- All existing regression tests must pass without modification
- The fast path is exercised automatically on every single-shard query

### 9.2 Targeted Validation

Add assertions or optional logging (gated behind `DEBUG4` log level) to confirm
the fast path is taken:

```c
ereport(DEBUG4,
    (errmsg("using single-task fast path for task on %s:%d",
            placement->nodeName, placement->nodePort)));
```

### 9.3 Edge Cases to Test

| Case | Expected behavior |
|------|-------------------|
| Single-shard SELECT | Fast path taken |
| Single-shard INSERT (non-replicated) | Fast path taken |
| Single-shard UPDATE/DELETE | Fast path taken |
| Multi-shard SELECT | Full executor (multiple tasks) |
| Reference table INSERT (replicated) | Full executor (multi-placement write) |
| Single-shard SELECT on replicated table | Fast path taken (EXECUTION_ORDER_ANY, only need first) |
| Single-shard with EXPLAIN ANALYZE | Full executor (multi-query per task) |
| Single-shard in multi-statement transaction | Fast path taken (with connection reuse) |
| Connection failure on single placement | Falls back to full executor |
| Local execution eligible | Full executor (local task, not remote) |
| Prepared statement with params | Fast path taken (params forwarded) |
| `SET citus.enable_single_task_fast_path TO false` | Full executor for all queries |
| `SET citus.enable_single_task_fast_path TO true` mid-session | Fast path resumes for eligible queries |

### 9.4 Performance Benchmarking

Measure with `pgbench` custom script doing single-shard point lookups:

```sql
\set id random(1, 1000000)
SELECT * FROM distributed_table WHERE id = :id;
```

The GUC enables controlled A/B comparison on the same cluster:

```bash
# Baseline: fast path disabled
psql -c "ALTER SYSTEM SET citus.enable_single_task_fast_path TO false;"
psql -c "SELECT pg_reload_conf();"
pgbench -f point_lookup.sql -T 60 -c 16

# Test: fast path enabled (default)
psql -c "ALTER SYSTEM SET citus.enable_single_task_fast_path TO true;"
psql -c "SELECT pg_reload_conf();"
pgbench -f point_lookup.sql -T 60 -c 16
```

Expected improvement: 5–15% latency reduction on single-shard queries due to
reduced allocation and state machine overhead.

## 10. Rollout Plan

### Phase 1: Implementation (this branch)
- Implement `CanUseSingleTaskFastPath` and `RunSingleTaskExecution`
- Wire into both entry points (`AdaptiveExecutor`, `ExecuteTaskListExtended`)
- Pass full regression suite

### Phase 2: Validation
- Run benchmarks comparing before/after
- Stress test with concurrent single-shard + multi-shard workloads
- Verify no deadlocks, no connection leaks, no 2PC issues

### Phase 3: Merge
- Merge to main once benchmarks confirm improvement and tests pass

## 11. Non-Goals

- **Optimizing multi-task execution**: This spec only addresses single-task.
- **Changing the public API**: All existing API functions remain unchanged.
- **Removing any existing code**: The full executor path remains intact.
- **Optimizing local execution**: The local execution fast path is separate.
- **Connection pooling changes**: We reuse the existing connection infrastructure as-is.
