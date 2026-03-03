# Research Document: One-Task Adaptive Executor

**Date:** 2026-03-03
**Spec:** SPEC-one-task-adaptive-executor.md

---

## 1. Summary

This document captures findings from code research against the spec, identifies
areas where the spec is clear and actionable, flags ambiguities/risks, and provides
the code-level detail needed to begin implementation. Questions raised during
research have been resolved (see §12).

---

## 2. Spec Validation: Eligibility Criteria

### 2.1 `fastPathRouterPlan` as the Sole Eligibility Gate

The spec gates eligibility on `distributedPlan->fastPathRouterPlan == true`, detected
in `JobExecutorType()`.

**Finding — deferred pruning means task list may be empty at plan time:**
When `fastPathRouterPlan` is true, the task list is often `NIL` because pruning is
deferred (dist key is a parameter, or it's an INSERT). After regeneration in BeginScan:

- **SELECT/UPDATE/DELETE**: exactly 1 task, or 0 for a modification targeting a
  non-existent dist-key value (`NIL` task list)
- **Single-row INSERT**: exactly 1 task
- **Multi-row INSERT**: can produce **> 1 task** even when `fastPathRouterPlan` is true

**Resolution:** Multi-row INSERTs where `fastPathRouterPlan == true` are excluded
at the planner level. The eligibility check in `JobExecutorType()` will be:

```c
if (EnableSingleTaskFastPath
    && distributedPlan->fastPathRouterPlan
    && dependentJobCount == 0
    && !IsMultiRowInsert(job))
    return MULTI_EXECUTOR_ONE_TASK_ADAPTIVE;
```

The `IsMultiRowInsert` check must be determined — likely
`job->jobQuery->commandType == CMD_INSERT && contain_multiple_rows(...)` or checking
for `rowValuesLists` on the job. Exact mechanism TBD during implementation.

**Finding — 0-task edge case:**
A fast-path DELETE/UPDATE where the dist-key matches no shard produces
`taskList = NIL`. The current adaptive executor handles this gracefully (no-op).

**Resolution:** The one-task executor handles the 0-task case inline as a no-op
(empty result, `rowsProcessed = 0`). No fallback needed.

### 2.2 `JobExecutorType()` Detection Point

Located at `src/backend/distributed/executor/multi_server_executor.c` line 49.
Currently checks INSERT...SELECT / MERGE first, then repartition, then returns
`MULTI_EXECUTOR_ADAPTIVE` for everything else. It already detects
`list_length(job->taskList) <= 1` but only for debug logging.

**Finding — task list is empty at plan time for deferred-pruned queries.** The spec's
approach of checking `fastPathRouterPlan` (a planner-set flag) rather than
`list_length(taskList)` is correct, because the flag is available at plan time while
the real task count is not.

---

## 3. Spec Validation: Data Structures

### 3.1 `OneTaskExec` vs `DistributedExecution`

The spec's struct is reasonable but should include these additional fields found in
research:

| Field | Type | Purpose |
|---|---|---|
| `transactionProperties` | `TransactionProperties` | Needed for BEGIN/2PC decisions |
| `failed` | `bool` | Tracks execution failure |
| `rowsProcessed` | `uint64` | Aggregate row count for DML |
| `raiseInterrupts` | `bool` | Whether to raise interrupts during wait |

### 3.2 `Session` struct — Essential Fields

The simplified `Session` struct must explicitly include:

| Field | Type | Purpose |
|---|---|---|
| `connection` | `MultiConnection *` | The libpq connection handle |
| `currentTask` | `TaskPlacementExecution *` | Task being executed |
| `commandsSent` | `int` | Tracks transaction/query round-trips |

Note: Since we are using simple polling (see §4.4), `waitEventSetIndex` and
`latestUnconsumedWaitEvents` are NOT needed.

---

## 4. Spec Validation: Execution Path

### 4.1 State Machine Simplification

For a single-task SELECT (no transaction block):
```
ConnectionStateMachine: INITIAL → CONNECTING → CONNECTED
TransactionStateMachine: NOT_STARTED → SENT_COMMAND → CLEARING_RESULTS → done
```

For a single-task DML inside explicit BEGIN:
```
TransactionStateMachine: NOT_STARTED → send BEGIN → CLEARING_RESULTS → STARTED
  → SENT_COMMAND → CLEARING_RESULTS → STARTED (done)
```

The spec's flow correctly preserves this pattern. The simplification is in eliminating
pool/queue management, not changing state transitions.

### 4.2 Steps That Can Be Eliminated

- `SequentialRunDistributedExecution()` — single task, always returns false
- `FindOrCreateWorkerPool()` linear scan + sort — one worker
- `targetPoolSize` calculation — always 1
- `ManageWorkerPool()` slow-start — one connection
- `RecordParallelRelationAccessForTaskList()` — no parallel access
- Dependent jobs (`ExecuteDependentTasks`) — `dependentJobCount == 0` guaranteed
- `ForceMaxQueryParallelization` — single task
- EXPLAIN ANALYZE wrapping — falls back to normal executor (see §5)
- `BuildWaitEventSet` / `WaitEventSetWait` — replaced by simple polling (see §4.4)

### 4.3 Steps That MUST Be Preserved

| Step | Why |
|---|---|
| `tuplestore_begin_heap` | Results must be stored |
| `DecideTaskListTransactionProperties` | Correctness (BEGIN, 2PC) |
| `AcquireExecutorShardLocksForExecution` | Prevents stale reads during shard moves |
| `GetConnectionIfPlacementAccessedInXact` | Connection reuse in transactions (**critical**) |
| `AssignPlacementListToConnection` | Transaction affinity tracking |
| `SendNextQuery` (incl. params, binary) | Core functionality |
| `ReceiveResults` (single-row mode) | Core functionality |
| `FinishDistributedExecution` logic | Sets `XactModificationLevel` |
| `CleanUpSessions` (unclaim connection) | Resource management |
| `Activate2PCIfModifyingTransactionExpandsToNewNode` | Correctness in multi-statement txns |

### 4.4 I/O Wait Strategy: Simple Polling

**Decision:** Use direct `PQconsumeInput` + `PQisBusy` polling instead of
`BuildWaitEventSet` / `WaitEventSetWait`.

For a single connection, the event loop simplifies to:

```c
/* Send query */
SendNextQuery(session, task, ...);

/* Poll for results */
while (!fetchDone)
{
    int sock = PQsocket(connection->pgConn);
    if (PQisBusy(connection->pgConn))
    {
        /* Wait for the socket to be readable */
        int rc = WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE | WL_LATCH_SET,
                                   sock, timeout, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        if (rc & WL_SOCKET_READABLE)
            PQconsumeInput(connection->pgConn);
    }

    fetchDone = ReceiveResults(session, ...);
}
```

This eliminates all `WaitEventSet` allocation, rebuild, and teardown overhead. The
`WaitLatchOrSocket` call handles interrupt checking and postmaster death detection.

**Tradeoffs:**
- Simpler code, fewer allocations
- Different code path from multi-task executor (but isolated to 1-task case)
- `WaitLatchOrSocket` is the standard Postgres pattern for single-socket waits

---

## 5. EXPLAIN ANALYZE

**Decision:** The one-task executor does NOT handle EXPLAIN ANALYZE. When EXPLAIN
ANALYZE is requested, fall back to the normal adaptive executor.

**Implementation:** In `CitusExecOneTaskScan()`, check
`RequestedForExplainAnalyze(scanState)`. If true, call `AdaptiveExecutor(scanState)`
instead of `OneTaskAdaptiveExecutor(scanState)`.

`CitusExplainScan` is generic (doesn't check `executorType`) and can be shared
as-is for non-ANALYZE EXPLAIN output.

---

## 6. Stats and Query Tracking

### 6.1 Executor Type in Stats

**Decision:** Reuse `MULTI_EXECUTOR_ADAPTIVE` for statistics tracking. The
`CitusScanState.executorType` will be set to `MULTI_EXECUTOR_ADAPTIVE` (not a new
enum value) so that `CitusEndScan`'s partition key extraction and
`CitusQueryStatsExecutorsEntry` work unchanged.

**Implication:** A new enum value `MULTI_EXECUTOR_ONE_TASK_ADAPTIVE` is still needed
for the `FinalizePlan()` switch (to select the correct `CustomScanMethods`), but the
`CreateScan` function sets `scanState->executorType = MULTI_EXECUTOR_ADAPTIVE`.

This means:
- No changes to `CitusEndScan`, `CitusExecutorName`, or `query_stats.c`
- Stats are bucketed identically to the normal adaptive executor
- From a monitoring perspective, one-task queries appear as normal adaptive queries

### 6.2 Single-Shard Counter

`CitusExecOneTaskScan` must call
`IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD)` since by definition
it handles single-task plans only.

---

## 7. Connection Reuse in Transactions — CRITICAL

The one-task executor MUST call `GetConnectionIfPlacementAccessedInXact()` before
establishing a new connection. If bypassed:
- Second connection to same worker → self-deadlock for writes
- Lost read-your-writes consistency within `BEGIN...COMMIT`

The specific function calls that must be preserved:
1. `PlacementAccessListForTask(task)` — build access list
2. `GetConnectionIfPlacementAccessedInXact(accessList)` — check existing conn
3. If no conn: `StartNodeUserDatabaseConnection()` — open new
4. `AssignPlacementListToConnection(accessList, connection)` — track for future
5. `ClaimConnectionExclusively(connection)` — mark in-use
6. After execution: unclaim

---

## 8. Local Execution

For local single-task execution, call `ExecuteLocalTaskListExtended()` directly with
a 1-element task list. This function is already in `local_executor.c` and handles all
local execution details. No need to clone or refactor it.

---

## 9. `CitusBeginScan` / `PreExecScan` Sharing

**Confirmed:** `CitusBeginScan`, `CitusBeginReadOnlyScan`, and
`CitusBeginModifyScan` are fully generic. The one-task executor reuses
`CitusBeginScan` as its `BeginCustomScan` callback.

**Decision:** Reuse `CitusPreExecScan` as-is. For `fastPathRouterPlan == true`, there
are no subplans (`ExecuteSubPlans` is a no-op).
`LockPartitionsForDistributedPlan()` is still needed for partitioned tables.

---

## 10. Serialization / Prepared Statements

No serialization changes needed. The new `CustomScanMethods` needs a unique
`CustomName` string and registration via `RegisterCustomScanMethods()`. PostgreSQL
resolves the methods pointer from the `CustomName` during deserialization. No changes
to `citus_copyfuncs.c` or `citus_outfuncs.c`.

---

## 11. All Files Requiring Changes

| File | Change | Notes |
|---|---|---|
| `multi_server_executor.h` | Add enum `MULTI_EXECUTOR_ONE_TASK_ADAPTIVE = 4` | |
| `multi_server_executor.c` | Modify `JobExecutorType()` | Check `fastPathRouterPlan && GUC && !multiRowInsert` |
| `citus_custom_scan.h` | Declare `extern OneTaskAdaptiveExecutorCustomScanMethods` | |
| `citus_custom_scan.c` | Add `OneTaskAdaptiveExecutorCreateScan`, `CitusExecOneTaskScan`, new methods instances; update `IsCitusCustomState`, `RegisterCitusCustomScanMethods` | |
| `adaptive_executor.c` | Implement `OneTaskAdaptiveExecutor()` | Main new code; uses simple polling |
| `distributed_planner.c` | Add case in `FinalizePlan()` switch | |
| `shared_library_init.c` | Register GUC `citus.enable_single_task_fast_path` | |

Files that do NOT need changes (confirmed):
- `local_executor.c` — reuse `ExecuteLocalTaskListExtended` as-is
- `query_stats.c` — reusing `MULTI_EXECUTOR_ADAPTIVE` for stats
- `citus_copyfuncs.c` / `citus_outfuncs.c` — no new serialized fields
- `multi_explain.c` — `CitusExplainScan` is generic, shared as-is

---

## 12. Resolved Decisions Summary

| # | Question | Decision | Rationale |
|---|---|---|---|
| 1 | Multi-row INSERT handling | Exclude at planner level | Simplicity; multi-row INSERT is not the target workload |
| 2 | 0-task case | Handle inline as no-op | Simple; mirrors current executor behavior |
| 3 | Session struct fields | Explicitly list `connection`, `currentTask`, `commandsSent` | Clarity; `waitEventSetIndex` not needed with simple polling |
| 4 | EXPLAIN ANALYZE | Fall back to normal adaptive executor | Not perf-sensitive; avoids multi-query complexity |
| 5 | Executor type in stats | Reuse `MULTI_EXECUTOR_ADAPTIVE` | No stat/monitoring changes needed |
| 6 | `PreExecScan` | Reuse `CitusPreExecScan` as-is | Safe for partitioned tables; no-op for subplans |
| 7 | I/O wait strategy | Simple `PQconsumeInput`/`PQisBusy` + `WaitLatchOrSocket` polling | Eliminates WaitEventSet overhead for 1 socket |

---

## 13. Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| Connection reuse broken in txns | **High** | Must call `GetConnectionIfPlacementAccessedInXact`; see §7 |
| Multi-row INSERT routed to one-task executor | Medium | Planner-level exclusion in `JobExecutorType()` |
| 0-task plan causes crash | Low | Inline no-op handling; null-check task list |
| Partition locking skipped | Low | Reusing `CitusPreExecScan` preserves this |
| Simple polling misses edge cases vs WaitEventSet | Low | `WaitLatchOrSocket` handles interrupts + postmaster death |
| EXPLAIN ANALYZE produces wrong output | Low | Fallback to normal executor detected early |
