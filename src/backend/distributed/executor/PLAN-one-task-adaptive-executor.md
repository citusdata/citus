# Implementation Plan: One-Task Adaptive Executor

**Date:** 2026-03-03
**Spec:** SPEC-one-task-adaptive-executor.md
**Research:** RESEARCH-one-task-adaptive-executor.md

---

## TL;DR

Add a streamlined executor for single-task fast-path queries (`fastPathRouterPlan == true`)
that bypasses the adaptive executor's pool management, slow-start, and WaitEventSet
machinery. A new `CustomScanMethods`/`CustomExecMethods` pair routes eligible queries
to `OneTaskAdaptiveExecutor()`, which uses direct `WaitLatchOrSocket` polling on a
single connection. Falls back to the normal adaptive executor for EXPLAIN ANALYZE
(detected at exec time via `es_instrument`). Gated by GUC
`citus.enable_single_task_fast_path` (default: true).

---

## Steps

### Step 1 — Add enum value in `multi_server_executor.h`

In [src/include/distributed/multi_server_executor.h](src/include/distributed/multi_server_executor.h#L29-L34),
add `MULTI_EXECUTOR_ONE_TASK_ADAPTIVE = 4` to the `MultiExecutorType` enum after
`MULTI_EXECUTOR_NON_PUSHABLE_MERGE_QUERY = 3`.

### Step 2 — Declare GUC extern in `multi_server_executor.h`

In [src/include/distributed/multi_server_executor.h](src/include/distributed/multi_server_executor.h#L38-L42),
add `extern bool EnableSingleTaskFastPath;` alongside `EnableRepartitionJoins`.

### Step 3 — Add eligibility check in `JobExecutorType()`

In [src/backend/distributed/executor/multi_server_executor.c](src/backend/distributed/executor/multi_server_executor.c#L100-L108),
insert a new check **before** the final `return MULTI_EXECUTOR_ADAPTIVE` at line 107.
The check must:

1. Guard on `EnableSingleTaskFastPath` (GUC)
2. Test `distributedPlan->fastPathRouterPlan == true`
3. Verify `list_length(job->dependentJobList) == 0` (no dependent jobs)
4. Exclude multi-row INSERTs via `!IsMultiRowInsert(job->jobQuery)`
   (from `multi_router_planner.h`)
5. Return `MULTI_EXECUTOR_ONE_TASK_ADAPTIVE`

Also define `bool EnableSingleTaskFastPath = true;` at file scope. Add the
`#include "distributed/multi_router_planner.h"` if not already present.

### Step 4 — Declare new `CustomScanMethods` extern in `citus_custom_scan.h`

In [src/include/distributed/citus_custom_scan.h](src/include/distributed/citus_custom_scan.h#L36-L39),
add after the existing extern declarations:

`extern CustomScanMethods OneTaskAdaptiveExecutorCustomScanMethods;`

### Step 5 — Add `CustomScanMethods` instance in `citus_custom_scan.c`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L84-L103),
add after the existing `CustomScanMethods` instances (after line 103):

- `CustomName` = `"Citus One-Task Adaptive"`
- `CreateCustomScanState` = `OneTaskAdaptiveExecutorCreateScan`

### Step 6 — Add `CustomExecMethods` instance in `citus_custom_scan.c`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L108-L135),
add after the existing static `CustomExecMethods` instances (after line 135):

- `.CustomName` = `"OneTaskAdaptiveExecutorScan"`
- `.BeginCustomScan` = `CitusBeginScan` (reuse)
- `.ExecCustomScan` = `CitusExecOneTaskScan` (new)
- `.EndCustomScan` = `CitusEndScan` (reuse)
- `.ReScanCustomScan` = `CitusReScan` (reuse)
- `.ExplainCustomScan` = `CitusExplainScan` (reuse — shared for non-ANALYZE EXPLAIN)

### Step 7 — Implement `OneTaskAdaptiveExecutorCreateScan()`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L710-L726),
add the new function near `AdaptiveExecutorCreateScan()`. Clone the pattern:

- Allocate `CitusScanState` via `palloc0`
- Set `executorType = MULTI_EXECUTOR_ADAPTIVE` (reuse for stats — see Research §6.1)
- Set `methods = &OneTaskAdaptiveExecutorCustomExecMethods`
- Set `PreExecScan = &CitusPreExecScan` (reuse)
- Set `distributedPlan = GetDistributedPlan(scan)`
- Set `finishedPreScan = false`, `finishedRemoteScan = false`

Add the corresponding forward declaration near line 59.

### Step 8 — Implement `CitusExecOneTaskScan()`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L263-L288),
add near `CitusExecScan()`. The function:

1. Cast `node` to `CitusScanState *scanState`
2. If `!scanState->finishedRemoteScan`:
   a. Check `RequestedForExplainAnalyze(scanState)` — if true, call
      `AdaptiveExecutor(scanState)` (fallback). Otherwise call
      `OneTaskAdaptiveExecutor(scanState)`.
   b. Call `IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD)`
      (always single-shard by definition)
   c. Set `scanState->finishedRemoteScan = true`
3. Return `ReturnTupleFromTuplestore(scanState)`

Add the forward declaration near the other `static` declarations.

### Step 9 — Update `IsCitusCustomState()`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L141-L155),
add `css->methods == &OneTaskAdaptiveExecutorCustomExecMethods` to the disjunction
that checks `methods` pointers.

### Step 10 — Update `RegisterCitusCustomScanMethods()`

In [src/backend/distributed/executor/citus_custom_scan.c](src/backend/distributed/executor/citus_custom_scan.c#L164-L170),
add `RegisterCustomScanMethods(&OneTaskAdaptiveExecutorCustomScanMethods);`
after the existing registrations.

### Step 11 — Add case in `FinalizePlan()` switch

In [src/backend/distributed/planner/distributed_planner.c](src/backend/distributed/planner/distributed_planner.c#L1453-L1458),
add a new case in the `switch (executorType)` block:

```
case MULTI_EXECUTOR_ONE_TASK_ADAPTIVE:
    customScan->methods = &OneTaskAdaptiveExecutorCustomScanMethods;
    break;
```

Place it after `MULTI_EXECUTOR_ADAPTIVE`. Ensure the `#include` for
`citus_custom_scan.h` is present (it already is).

### Step 12 — Register GUC in `shared_library_init.c`

In [src/backend/distributed/shared_library_init.c](src/backend/distributed/shared_library_init.c#L1562-L1579),
insert a `DefineCustomBoolVariable` block between `enable_single_hash_repartition_joins`
(line 1562) and `enable_stat_counters` (line 1579) to maintain alphabetical order:

- Name: `"citus.enable_single_task_fast_path"`
- Description: `"Enables the optimized single-task executor for fast-path queries."`
- Variable: `&EnableSingleTaskFastPath`
- Default: `true`
- Context: `PGC_USERSET`
- Flags: `GUC_STANDARD`

### Step 13 — Implement `OneTaskAdaptiveExecutor()` in `adaptive_executor.c`

This is the main new function in
[src/backend/distributed/executor/adaptive_executor.c](src/backend/distributed/executor/adaptive_executor.c).
Declare it non-static (needs to be callable from `citus_custom_scan.c`).
Add an extern declaration in `adaptive_executor.h` or `citus_custom_scan.h`.

The function takes `CitusScanState *scanState` and follows this flow:

**13a — Setup (mirrors `AdaptiveExecutor()` lines 775–870):**

1. Create a `MemoryContext localContext` under `CurrentMemoryContext`
2. Extract `distributedPlan`, `workerJob`, `taskList` from `scanState`
3. Call `tuplestore_begin_heap(true, false, work_mem)` → store in
   `scanState->tuplestorestate`
4. Build `TupleDestination` via `CreateTupleStoreTupleDest()`
5. Call `DecideTaskListTransactionProperties(modLevel, taskList, excludeFromXact)`
   → get `TransactionProperties`
6. Copy params: `copyParamList(paramListInfo)` + `MarkUnreferencedExternParams()`

**13b — Transaction setup (mirrors `StartDistributedExecution()` lines 1335–1397):**

7. If `transactionProperties.useRemoteTransactionBlocks == TRANSACTION_BLOCKS_REQUIRED`:
   call `UseCoordinatedTransaction()`
8. If `transactionProperties.requires2PC`: call `Use2PCForCoordinatedTransaction()`
9. Call `AcquireExecutorShardLocksForExecution(modLevel, taskList)`

**13c — Local vs Remote decision:**

10. Call `ShouldExecuteTasksLocally(taskList)` + `ExtractLocalAndRemoteTasks()`
    to split into local and remote task lists.
11. If local tasks exist: call `ExecuteLocalTaskListExtended(localTaskList, ...)`
    directly with the tuplestore destination. Then skip to step 13h.

**13d — Remote: Connection acquisition (mirrors adaptive_executor.c lines 1493–1544):**

12. Get the single `Task *task` from `remoteTaskList`
    (handle 0-task case: skip to 13h with empty result)
13. Get the first `ShardPlacement *taskPlacement` from `task->taskPlacementList`
14. Build `placementAccessList` via `PlacementAccessListForTask(task, taskPlacement)`
15. Try `GetConnectionIfPlacementAccessedInXact(connectionFlags, placementAccessList, NULL)`
    — reuses existing connection in a transaction
16. If no connection: `StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort, NULL, NULL)`
17. `ClaimConnectionExclusively(connection)`
18. `AssignPlacementListToConnection(placementAccessList, connection)`
19. Call `Activate2PCIfModifyingTransactionExpandsToNewNode()` if needed
    (check `InCoordinatedTransaction() && !ConnectionModifiedPlacement()`)

**13e — Remote: Send query:**

20. Allocate a lightweight `WorkerSession` (or a new minimal struct) on the
    local context. Set `connection`, `currentTask` (wrap task as `TaskPlacementExecution`),
    `commandsSent = 0`.
21. Create a `TaskPlacementExecution` for the task+placement.
    The `SendNextQuery()` function takes a `TaskPlacementExecution *` and
    `WorkerSession *` — reuse these existing types to avoid duplicating `SendNextQuery`.
22. Call `SendNextQuery(placementExecution, session)` to dispatch the query.

**13f — Remote: Poll for results (simple polling loop — Research §4.4):**

23. Enter a poll loop:
    - `PQisBusy(connection->pgConn)` → if busy, call
      `WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE | WL_LATCH_SET, PQsocket(...), timeout, PG_WAIT_EXTENSION)`
    - `ResetLatch(MyLatch)`, `CHECK_FOR_INTERRUPTS()`
    - On readable: `PQconsumeInput(connection->pgConn)`
    - Call `ReceiveResults(session, storeRows=true)` — returns true when done
    - Loop until `ReceiveResults` signals completion
24. Handle errors: if `ReceiveResults` reports failure, call `ReportConnectionError()`
    or equivalent.

**13g — Remote: Cleanup:**

25. `UnclaimConnection(connection)` — return connection to pool

**13h — Finish:**

26. If DML (`DistributedExecutionModifiesDatabase`-equivalent):
    set `XactModificationLevel = XACT_MODIFICATION_DATA`
27. If `RETURNING` + `ORDER BY`: call `SortTupleStore(scanState)`
28. Switch back to original memory context; destroy `localContext`

**Key consideration:** Steps 13e–13f reuse the existing `SendNextQuery()` and
`ReceiveResults()` functions. These take `WorkerSession *` and
`TaskPlacementExecution *` arguments. The one-task executor must allocate minimal
instances of these structs with the required fields populated:

- `WorkerSession`: `connection`, `currentTask`, `commandsSent`, `workerPool` (may
  need a minimal `WorkerPool` with `distributedExecution` pointer for error handling)
- `TaskPlacementExecution`: wraps a `Task *` + `ShardPlacement *` + execution state

Alternatively, if the coupling to `WorkerSession`/`TaskPlacementExecution` is too
deep (referencing `workerPool->distributedExecution` chains in error paths), we
may need to **inline the send/receive logic** rather than reusing `SendNextQuery()`
and `ReceiveResults()`. This is a decision point during implementation — start by
trying to reuse, fall back to inlining if the struct web is too tangled.

### Step 14 — Declare `OneTaskAdaptiveExecutor` extern

Add `extern void OneTaskAdaptiveExecutor(CitusScanState *scanState);` to an
appropriate header. Options:

- [src/include/distributed/adaptive_executor.h](src/include/distributed/adaptive_executor.h)
  alongside `AdaptiveExecutor` — **preferred**, keeps executor declarations together.
- Or `citus_custom_scan.h` — less natural since execution is not a custom-scan concern.

---

## Verification

### Build

```bash
cd /workspaces/citus && make -j$(nproc) && make install
```

### Unit-level smoke test

```sql
-- With new GUC enabled (default)
SET citus.enable_single_task_fast_path = true;
CREATE TABLE t (id int, val text);
SELECT create_distributed_table('t', 'id');
INSERT INTO t VALUES (1, 'a');
SELECT * FROM t WHERE id = 1;
UPDATE t SET val = 'b' WHERE id = 1;
DELETE FROM t WHERE id = 1;
```

### EXPLAIN ANALYZE fallback

```sql
EXPLAIN ANALYZE SELECT * FROM t WHERE id = 1;
-- Should produce full EXPLAIN ANALYZE output (uses normal adaptive executor)

EXPLAIN SELECT * FROM t WHERE id = 1;
-- Should show "Citus One-Task Adaptive" as the custom scan name
```

### GUC disable path

```sql
SET citus.enable_single_task_fast_path = false;
SELECT * FROM t WHERE id = 1;
-- Should use normal adaptive executor (verify via EXPLAIN)
```

### Transaction / connection reuse

```sql
BEGIN;
INSERT INTO t VALUES (2, 'x');
SELECT * FROM t WHERE id = 2;  -- must see the insert (same connection)
UPDATE t SET val = 'y' WHERE id = 2;
COMMIT;
```

### Multi-row INSERT exclusion

```sql
INSERT INTO t VALUES (1, 'a'), (2, 'b');
-- Should NOT use one-task executor (verify via EXPLAIN)
```

### Existing test suite

```bash
make check
# Run the isolation and regression tests to confirm no regressions
```

---

## Decisions

- **Reuse `SendNextQuery`/`ReceiveResults` vs inline:** Start by reusing with
  minimal struct allocation. Fall back to inlining if `WorkerPool`/`DistributedExecution`
  coupling is prohibitive. (Implementation-time decision)
- **Stats executor type:** `MULTI_EXECUTOR_ADAPTIVE` for stats bucketing; new enum
  value only for `FinalizePlan()` dispatch.
- **EXPLAIN ANALYZE:** Exec-time fallback via `es_instrument` check; cannot detect
  at plan time.
- **GUC scope:** `PGC_USERSET` — per-session toggle without superuser requirement.
