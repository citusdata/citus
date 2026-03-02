# Implementation Plan: One-Task Adaptive Executor

Based on the spec and code research, this is the step-by-step implementation
plan organized into atomic, committable stages.

---

## Stage 1: GUC Plumbing

**Goal**: Add the `citus.enable_single_task_fast_path` GUC with no behavioral
effect yet. This is a safe, independently testable commit.

### 1a. Declare GUC variable in header

**File**: `src/include/distributed/adaptive_executor.h`

Add after the existing GUC externs (`EnableCostBasedConnectionEstablishment`,
`PreventIncompleteConnectionEstablishment`):

```c
extern bool EnableSingleTaskFastPath;
```

### 1b. Define GUC variable in adaptive_executor.c

**File**: `src/backend/distributed/executor/adaptive_executor.c`

Add after the existing GUC definitions (after `PreventIncompleteConnectionEstablishment`
around line 505):

```c
bool EnableSingleTaskFastPath = true;
```

### 1c. Register GUC in shared_library_init.c

**File**: `src/backend/distributed/shared_library_init.c`

Insert between `citus.enable_single_hash_repartition_joins` (L1562) and
`citus.enable_stat_counters` (L1573), maintaining alphabetical order:

```c
DefineCustomBoolVariable(
    "citus.enable_single_task_fast_path",
    gettext_noop("Enables the optimized single-task executor fast path."),
    gettext_noop("When enabled, single-task queries bypass the full adaptive "
                 "executor machinery (pool management, WaitEventSet, task "
                 "queuing) for reduced overhead on single-shard queries."),
    &EnableSingleTaskFastPath,
    true,
    PGC_USERSET,
    GUC_STANDARD,
    NULL, NULL, NULL);
```

### 1d. Verify

- Build succeeds
- `SHOW citus.enable_single_task_fast_path;` returns `on`
- `SET citus.enable_single_task_fast_path TO false;` works
- All existing tests pass (GUC is inert)

**Commit message**: `Add citus.enable_single_task_fast_path GUC (inert)`

---

## Stage 2: Eligibility Check Function

**Goal**: Add `CanUseSingleTaskFastPath()` that evaluates whether the fast path
can be used, plus a `DEBUG4` log message. Still no behavioral change — the
function exists but is not called from the main execution paths yet.

### 2a. Add forward declaration

**File**: `src/backend/distributed/executor/adaptive_executor.c`

Add in the `/* local functions */` block (around line 648), near other static
function declarations:

```c
static bool CanUseSingleTaskFastPath(DistributedExecution *execution);
static void RunSingleTaskExecution(DistributedExecution *execution);
```

### 2b. Implement CanUseSingleTaskFastPath

**File**: `src/backend/distributed/executor/adaptive_executor.c`

Place before `RunDistributedExecution` (around line 1855). The function checks:

1. `EnableSingleTaskFastPath` GUC is true
2. Exactly one remote task, zero local tasks
3. Task has non-empty placement list
4. `task->queryCount <= 1` (not multi-query)
5. Read-only OR single placement (replicated writes need full path)

```c
/*
 * CanUseSingleTaskFastPath checks whether a distributed execution can use
 * the optimized single-task path that bypasses pool management, task
 * queuing, and WaitEventSet construction.
 */
static bool
CanUseSingleTaskFastPath(DistributedExecution *execution)
{
    if (!EnableSingleTaskFastPath)
    {
        return false;
    }

    if (list_length(execution->remoteTaskList) != 1 ||
        list_length(execution->localTaskList) != 0)
    {
        return false;
    }

    Task *task = linitial(execution->remoteTaskList);

    if (task->taskPlacementList == NIL)
    {
        return false;
    }

    if (task->queryCount > 1)
    {
        return false;
    }

    /*
     * Replicated writes need sequential or parallel placement execution
     * coordination across placements. Only allow the fast path for:
     * - read-only queries (any single placement suffices)
     * - single-placement writes (no coordination needed)
     */
    if (execution->modLevel > ROW_MODIFY_READONLY &&
        list_length(task->taskPlacementList) > 1)
    {
        return false;
    }

    return true;
}
```

### 2c. Stub RunSingleTaskExecution

Add an empty stub that just falls back, so the forward declaration resolves:

```c
/*
 * RunSingleTaskExecution is the fast path for executing a single task on
 * a single placement. It bypasses WorkerPool/WorkerSession/WaitEventSet
 * overhead. Falls back to RunDistributedExecution on any issue.
 */
static void
RunSingleTaskExecution(DistributedExecution *execution)
{
    /* full implementation in next stage */
    RunDistributedExecution(execution);
}
```

**Commit message**: `Add CanUseSingleTaskFastPath eligibility check (not yet wired)`

---

## Stage 3: Wire the Fast Path into Entry Points

**Goal**: Call `CanUseSingleTaskFastPath` + `RunSingleTaskExecution` from the
two main entry points. Since the stub falls back to `RunDistributedExecution`,
this is still a behavioral no-op but validates the wiring.

### 3a. Wire into AdaptiveExecutor (scan path)

**File**: `src/backend/distributed/executor/adaptive_executor.c`
**Location**: ~line 876, replace:

```c
if (ShouldRunTasksSequentially(execution->remoteTaskList))
{
    SequentialRunDistributedExecution(execution);
}
else
{
    RunDistributedExecution(execution);
}
```

With:

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

### 3b. Wire into ExecuteTaskListExtended (non-scan path)

**File**: `src/backend/distributed/executor/adaptive_executor.c`
**Location**: ~line 1122, replace:

```c
StartDistributedExecution(execution);
RunDistributedExecution(execution);
FinishDistributedExecution(execution);
```

With:

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

### 3c. Verify

- Build succeeds
- All existing tests pass (stub falls back to full path)

**Commit message**: `Wire CanUseSingleTaskFastPath into both executor entry points`

---

## Stage 4: Implement RunSingleTaskExecution

**Goal**: Replace the stub with the full fast-path implementation. This is the
core change.

### 4a. Implementation

**File**: `src/backend/distributed/executor/adaptive_executor.c`

Replace the stub with the full function. The implementation follows this flow:

```
RunSingleTaskExecution(execution)
│
├─ Extract Task, ShardPlacement, TupleDestination
├─ Build placementAccessList via PlacementAccessListForTask()
│
├─ Get connection:
│  ├─ If TRANSACTION_BLOCKS != DISALLOWED:
│  │    connection = GetConnectionIfPlacementAccessedInXact(0, accessList, NULL)
│  ├─ If no existing connection:
│  │    connectionFlags = OPTIONAL_CONNECTION (+ OUTSIDE_TRANSACTION if needed)
│  │    connection = StartNodeUserDatabaseConnection(flags, node, port, NULL, NULL)
│  ├─ If connection is NULL: fall back to RunDistributedExecution
│  └─ FinishConnectionEstablishment(connection)
│     If connection failed: fall back to RunDistributedExecution
│
├─ ClaimConnectionExclusively(connection)
│
├─ Register placement with connection (if in transaction):
│  AssignPlacementListToConnection(accessList, connection)
│
├─ 2PC activation check (inline):
│  if TransactionModifiedDistributedTable(execution) &&
│     DistributedExecutionModifiesDatabase(execution) &&
│     !ConnectionModifiedPlacement(connection)
│  → Use2PCForCoordinatedTransaction()
│
├─ Begin remote transaction if needed:
│  RemoteTransactionBeginIfNecessary(connection)
│
├─ Determine attInMetadata and binaryResults:
│  TupleDesc td = tupleDest->tupleDescForQuery(tupleDest, 0)
│  if td != NULL && EnableBinaryProtocol && CanUseBinaryCopyFormat(td):
│    attInMetadata = TupleDescGetAttBinaryInMetadata(td)
│    binaryResults = true
│  else if td != NULL:
│    attInMetadata = TupleDescGetAttInMetadata(td)
│    binaryResults = false
│
├─ Send query:
│  queryString = TaskQueryStringAtIndex(task, 0)
│  if paramListInfo && !task->parametersInQueryStringResolved:
│    ExtractParametersForRemoteExecution(...)
│    SendRemoteCommandParams(connection, queryString, ...)
│  else if !binaryResults:
│    SendRemoteCommand(connection, queryString)
│  else:
│    SendRemoteCommandParams(connection, queryString, 0, NULL, NULL, true)
│  PQsetSingleRowMode(connection->pgConn)
│
├─ Disable local execution if target is local node:
│  if pool is to local node:
│    SetLocalExecutionStatus(LOCAL_EXECUTION_DISABLED)
│
├─ Receive results (synchronous, blocking PQgetResult loop):
│  Create RowContext memory context
│  while (true):
│    PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts)
│    if NULL: break
│    switch PQresultStatus(result):
│      PGRES_COMMAND_OK:
│        accumulate rowsProcessed from PQcmdTuples
│        PQclear, continue
│      PGRES_TUPLES_OK:
│        PQclear, continue
│      PGRES_SINGLE_TUPLE:
│        for each row, for each column:
│          build columnArray from PQgetvalue
│        BuildTupleFromCStrings/Bytes → putTuple
│        execution->rowsProcessed++
│        PQclear
│      error:
│        ReportResultError(connection, result, ERROR)
│  MemoryContextDelete(rowContext)
│
├─ Mark execution as complete:
│  execution->unfinishedTaskCount = 0
│
├─ UnclaimConnection(connection)
│
└─ Log DEBUG4 message
```

### Key design decisions in the implementation:

1. **Synchronous receive**: Use `GetRemoteCommandResult(connection, raiseInterrupts)`
   instead of `PQgetResult` + manual `PQisBusy` loop. This function already
   handles `CHECK_FOR_INTERRUPTS()` and blocking waits, avoiding the need for
   a WaitEventSet entirely.

2. **No fallback after query send**: Once the query is on the wire and
   `PQsetSingleRowMode` succeeds, we don't fall back. Errors are terminal
   (same as existing executor).

3. **Fallback before send**: If connection establishment fails, we cleanly fall
   back to `RunDistributedExecution(execution)` which will retry with full
   pool logic.

4. **2PC check is inlined**: The 3-condition check from
   `Activate2PCIfModifyingTransactionExpandsToNewNode` is replicated using
   `TransactionModifiedDistributedTable` (static, needs forwarding) and the
   two public functions. Since `TransactionModifiedDistributedTable` is static,
   we either inline its logic or make it available to `RunSingleTaskExecution`
   (both are in the same file, so just call it directly).

5. **Local node check**: Use `IsLoopbackAddress(placement->nodeName)` or the
   equivalent check that the existing code uses in `WorkerPool->poolToLocalNode`
   to disable local execution when routing to the local node over a remote
   connection.

### 4b. Verify

- Build succeeds
- Run existing regression tests
- Test with `SET citus.enable_single_task_fast_path TO false` to verify
  fallback still works

**Commit message**: `Implement RunSingleTaskExecution fast path for single-shard queries`

---

## Stage 5: Validation & Cleanup

**Goal**: Add DEBUG4 logging, verify correctness, and run the full test suite.

### 5a. Add DEBUG4 log in RunSingleTaskExecution

At the start of the successful path (after connection is obtained, before send):

```c
ereport(DEBUG4,
    (errmsg("using single-task fast path for task on %s:%d",
            placement->nodeName, placement->nodePort)));
```

### 5b. Full test suite run

```bash
cd /workspaces/citus && make -sj$(nproc) && make install
# Run the check-* regression targets
```

### 5c. Verify GUC toggle

```sql
SET client_min_messages TO debug4;
SET citus.enable_single_task_fast_path TO true;
SELECT * FROM distributed_table WHERE id = 1;
-- Should see: "using single-task fast path for task on ..."

SET citus.enable_single_task_fast_path TO false;
SELECT * FROM distributed_table WHERE id = 1;
-- Should NOT see the debug message
```

**Commit message**: `Add DEBUG4 logging for single-task fast path`

---

## Summary of Files Modified

| File | Stages | Changes |
|------|--------|---------|
| `src/include/distributed/adaptive_executor.h` | 1 | `extern bool EnableSingleTaskFastPath` |
| `src/backend/distributed/executor/adaptive_executor.c` | 1–5 | GUC variable, `CanUseSingleTaskFastPath()`, `RunSingleTaskExecution()`, wiring in `AdaptiveExecutor()` and `ExecuteTaskListExtended()` |
| `src/backend/distributed/shared_library_init.c` | 1 | `DefineCustomBoolVariable` registration |

## Functions Called by the Fast Path (all existing, no new utils needed)

| Function | Header | Purpose |
|----------|--------|---------|
| `PlacementAccessListForTask` | `placement_access.h` | Build access list for connection tracking |
| `GetConnectionIfPlacementAccessedInXact` | `placement_connection.h` | Reuse transactional connection |
| `StartNodeUserDatabaseConnection` | `connection_management.h` | Open new connection |
| `FinishConnectionEstablishment` | `connection_management.h` | Wait for single connection |
| `ClaimConnectionExclusively` | `connection_management.h` | Mark connection in-use |
| `UnclaimConnection` | `connection_management.h` | Release connection claim |
| `AssignPlacementListToConnection` | `placement_connection.h` | Bind placement to connection |
| `ConnectionModifiedPlacement` | `placement_connection.h` | Check if connection wrote placements |
| `RemoteTransactionBeginIfNecessary` | `remote_transaction.h` | Send BEGIN if needed |
| `Use2PCForCoordinatedTransaction` | `transaction_management.h` | Activate 2PC |
| `TaskQueryStringAtIndex` | `deparse_shard_query.h` | Get query string |
| `ExtractParametersForRemoteExecution` | `executor_util.h` | Extract params for remote send |
| `SendRemoteCommand` | `remote_commands.h` | Send query (PQsendQuery) |
| `SendRemoteCommandParams` | `remote_commands.h` | Send parameterized query |
| `GetRemoteCommandResult` | `remote_commands.h` | Blocking PQgetResult with interrupts |
| `ReportResultError` | `remote_commands.h` | Report query error |
| `TupleDescGetAttInMetadata` | PostgreSQL `funcapi.h` | Text attInMetadata |
| `TupleDescGetAttBinaryInMetadata` | `executor_util.h` | Binary attInMetadata |
| `CanUseBinaryCopyFormat` | `binary_protocol.h` | Check binary compatibility |
| `BuildTupleFromCStrings` | PostgreSQL `funcapi.h` | Build tuple from text |
| `BuildTupleFromBytes` | `executor_util.h` | Build tuple from binary |
| `SetLocalExecutionStatus` | `local_executor.h` | Disable local exec |

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Connection leak on error | Use PG_TRY/PG_CATCH to UnclaimConnection on any error |
| Missing transaction state setup | StartDistributedExecution called before fast path (same as today) |
| Incorrect 2PC activation | Same 3-condition check as existing code, just inlined |
| Binary protocol mismatch | Same attInMetadata setup logic as SetAttributeInputMetadata |
| Read-your-own-writes violation | GetConnectionIfPlacementAccessedInXact ensures same connection |
| Performance regression if check fails | CanUseSingleTaskFastPath is ~5 comparisons, negligible |
