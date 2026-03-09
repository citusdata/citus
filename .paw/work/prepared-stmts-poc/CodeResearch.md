---
date: 2026-03-09T00:00:00Z
git_commit: 58fd4596caf02d95070968117ab40b07a300d937
branch: colm/prepared-stmts-poc
repository: citusdata/citus
topic: "Prepared Statement Caching on Worker Connections"
tags: [research, codebase, adaptive-executor, connection-management, prepared-statements, fast-path]
status: complete
last_updated: 2026-03-09
---

# Research: Prepared Statement Caching on Worker Connections

## Research Question

Trace the code paths for fast-path prepared statement execution from coordinator to worker, identify where remote queries are dispatched via libpq, document connection management lifecycle, and locate all insertion/modification points needed to add PQprepare/PQexecPrepared support.

## Summary

The adaptive executor sends all remote shard queries as SQL text via `PQsendQuery` or `PQsendQueryParams` — there is zero use of `PQprepare`/`PQexecPrepared` anywhere in the codebase. The critical integration point is `SendNextQuery()` in `adaptive_executor.c:3885-3950`, where the executor decides between parameterised and plain-text sends. This function receives the fully deparsed query string, parameter info, and the `MultiConnection` handle — everything needed to substitute a `PQprepare`/`PQexecPrepared` call. Connections are cached per `(hostname, port, user, database)` via `ConnectionHash` in `connection_management.c`, with configurable limits (`MaxCachedConnectionsPerWorker=1`, `MaxCachedConnectionLifetime=10min`). A per-connection statement cache can be attached to `MultiConnection`, invalidated naturally when the connection drops.

## Documentation System

- **Framework**: Plain Markdown (no doc framework)
- **Docs Directory**: N/A (documentation is in-tree `.md` files and `src/backend/distributed/README.md`)
- **Navigation Config**: N/A
- **Style Conventions**: CHANGELOG uses `### citus vX.Y.Z (date) ###` with bullet entries. README is a high-level intro. CONTRIBUTING.md covers development workflow.
- **Build Command**: N/A for docs
- **Standard Files**: `README.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `STYLEGUIDE.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `DEVCONTAINER.md`, `EXTENSION_COMPATIBILITY.md` (all at repo root); `src/backend/distributed/README.md` (technical architecture)

## Verification Commands

- **Build Command**: `make -j$(nproc)` (requires `./configure` first; calls into `src/backend/` PGXS build)
- **Install Command**: `make install`
- **Test Command**: `make -C src/test/regress check` (runs regression test suite)
- **Lint/Style Command**: `make check-style` (runs `black`, `isort`, `citus_indent`)
- **CI Scripts**: `ci/check_gucs_are_alphabetically_sorted.sh` (enforces alphabetical GUC ordering in `shared_library_init.c`)

## Detailed Findings

### 1. Fast-Path Execution Flow

A prepared fast-path query follows this path from EXECUTE to worker:

#### 1a. Planning Phase: `distributed_planner()` → Fast-Path Detection

- `distributed_planner()` (`src/backend/distributed/planner/distributed_planner.c:155`) is the entry point for all Citus planning
- At line 190, it calls `FastPathRouterQuery(parse, &fastPathContext)` to detect eligible queries
- Fast-path eligible queries: single-table SELECT/UPDATE/DELETE with distribution key equality in WHERE, or any INSERT (`src/backend/distributed/planner/fast_path_router_planner.c:229-330`)
- When eligible, `PlanFastPathDistributedStmt()` is called at `distributed_planner.c:271`, which invokes `FastPathPlanner()` (`fast_path_router_planner.c:95`) — this skips `standard_planner()` entirely and produces a lightweight `PlannedStmt`
- The resulting `DistributedPlan` has `fastPathRouterPlan = true` and `deferredPruning = true` (shard selection deferred to execution time)

#### 1b. Prepared Statement Generic Plan Mechanism

- PostgreSQL re-plans prepared statements for the first 5 executions (custom plans), then may switch to a generic plan
- Citus handles this at `distributed_planner.c:836-863`: when planning fails due to unresolved params, `DissuadePlannerFromUsingPlan()` inflates the plan cost to `FLT_MAX/100000000` so PostgreSQL prefers custom plans
- Once PostgreSQL switches to a generic plan (execution 6+), the `DistributedPlan` is cached. The `numberOfTimesExecuted` field tracks this (`multi_physical_planner.h:486`)
- On subsequent executions, `distributed_planner()` is NOT called. Instead, execution enters directly through `CitusBeginScan()` with the cached plan

#### 1c. Execution Phase: `CitusBeginScan()` → Task Generation

- `CitusBeginScan()` (`src/backend/distributed/executor/citus_custom_scan.c:180`) is the `BeginCustomScan` callback for all Citus custom scans
- For read-only queries: `CitusBeginReadOnlyScan()` (`citus_custom_scan.c:290`)
  - Copies the cached distributed plan: `CopyDistributedPlanWithoutCache()` at line 316
  - Evaluates coordinator expressions: `ExecuteCoordinatorEvaluableExpressions()` at line 337
  - Performs deferred shard pruning: `RegenerateTaskForFasthPathQuery()` at line 345
- For modification queries: `CitusBeginModifyScan()` (`citus_custom_scan.c:376`)
  - Similar flow but adds function evaluation via `ModifyJobNeedsEvaluation()`
  - For fast-path modify: calls `RegenerateTaskForFasthPathQuery()` at line 424
  - For INSERT: calls `RegenerateTaskListForInsert()` at line 420

#### 1d. Shard Pruning: `RegenerateTaskForFasthPathQuery()`

- Defined at `citus_custom_scan.c:654`
- Calls `TargetShardIntervalForFastPathQuery()` to resolve the distribution key to a shard
- Calls `RelationShardListForShardIntervalList()` + `UpdateRelationToShardNames()` to rewrite table names to shard names in the query
- Calls `CreateTaskPlacementListForShardIntervals()` to build placement list
- Calls `GenerateSingleShardRouterTaskList()` to produce a single `Task` with shard-specific query
- Calls `RebuildQueryStrings()` (`deparse_shard_query.c:62`) to deparse the shard query into SQL text

#### 1e. Query Execution: `CitusExecScan()` → `AdaptiveExecutor()`

- `CitusExecScan()` (`citus_custom_scan.c:247`) calls `AdaptiveExecutor(scanState)` on first tuple fetch
- `AdaptiveExecutor()` (`adaptive_executor.c:775`):
  - Creates a `DistributedExecution` via `CreateDistributedExecution()` (line 870)
  - Calls `StartDistributedExecution()` (line 878) — sets up coordinated transaction, acquires shard locks
  - Calls `RunDistributedExecution()` (line 886) — THE MAIN EVENT LOOP
  - Calls `FinishDistributedExecution()` (line 903)

### 2. Remote Query Dispatch

#### 2a. The Event Loop: `RunDistributedExecution()`

- Defined at `adaptive_executor.c:1898`
- Calls `AssignTasksToConnectionsOrWorkerPool()` at line 1901 — maps tasks to connections or worker pools
- Main loop (lines 1930-2000): while `unfinishedTaskCount > 0`
  - `ManageWorkerPool()` for each pool — opens connections as needed
  - Rebuilds or updates `WaitEventSet` as needed
  - `WaitEventSetWait()` for I/O events
  - `ProcessWaitEvents()` → for each ready session, calls `ConnectionStateMachine()`

#### 2b. Connection State Machine → Transaction State Machine → Query Send

- `ConnectionStateMachine()` (`adaptive_executor.c:2960`) handles connection lifecycle
- When connection reaches `MULTI_CONNECTION_CONNECTED` and has tasks: calls `TransactionStateMachine()` (line 3072)
- `TransactionStateMachine()` (`adaptive_executor.c:3394`):
  - In `REMOTE_TRANS_NOT_STARTED` or `REMOTE_TRANS_STARTED` state: pops a `TaskPlacementExecution` and calls `StartPlacementExecutionOnSession()` (lines 3434, 3530)
  - `StartPlacementExecutionOnSession()` (`adaptive_executor.c:3823`) calls `SendNextQuery()` at line 3862

#### 2c. **THE KEY FUNCTION**: `SendNextQuery()` — Where Queries Go to Workers

- Defined at `adaptive_executor.c:3885`
- This is the single function where all remote shard queries are dispatched
- **Current flow**:
  1. Gets `queryString` from `TaskQueryStringAtIndex(task, queryIndex)` (line 3899)
  2. If `paramListInfo != NULL && !task->parametersInQueryStringResolved`:
     - Extracts parameters via `ExtractParametersForRemoteExecution()` (line 3910)
     - Sends via `SendRemoteCommandParams(connection, queryString, parameterCount, parameterTypes, parameterValues, binaryResults)` (line 3912)
  3. Else (no params OR params already resolved into query string):
     - If `!binaryResults`: sends via `SendRemoteCommand(connection, queryString)` (line 3932)
     - Else: sends via `SendRemoteCommandParams(connection, queryString, 0, NULL, NULL, binaryResults)` (line 3936)
  4. After sending: enables `PQsetSingleRowMode()` (line 3944)

#### 2d. The libpq Wrappers

- `SendRemoteCommandParams()` (`src/backend/distributed/connection/remote_commands.c:522`):
  - Wraps `PQsendQueryParams(pgConn, command, ...)` at line 541
  - Logs via `LogRemoteCommand()`
  - Validates connection state
- `SendRemoteCommand()` (`remote_commands.c:556`):
  - Wraps `PQsendQuery(pgConn, command)` at line 573
  - Same logging and validation

**Key observation**: `SendNextQuery()` already has the query string, parameter types/values, and the `MultiConnection`. This is the exact point where `PQprepare`/`PQexecPrepared` can be substituted.

### 3. Connection Management

#### 3a. Connection Hash Table: `ConnectionHash`

- Declared at `connection_management.c:52`: `HTAB *ConnectionHash = NULL`
- Initialized in `InitializeConnectionManagement()` at `connection_management.c:101`
- Key: `ConnectionHashKey` (`connection_management.h:240`):
  ```
  hostname[MAX_NODE_LENGTH], port (int32), user[NAMEDATALEN],
  database[NAMEDATALEN], replicationConnParam (bool)
  ```
- Entry: `ConnectionHashEntry` (`connection_management.h:249`):
  ```
  ConnectionHashKey key, dlist_head *connections, bool isValid
  ```
- Each entry contains a linked list of `MultiConnection` structs sharing the same `(host, port, user, db)` key

#### 3b. `MultiConnection` Structure

- Defined at `connection_management.h:162` (full struct 162-233)
- Key fields for this POC:
  - `pgConn` (line 177): underlying `PGconn *` — the libpq handle for `PQprepare`/`PQexecPrepared`
  - `hostname`, `port`, `user`, `database` (lines 166-169): identification fields
  - `connectionId` (line 172): unique connection identifier
  - `connectionState` (line 175): `MultiConnectionState` enum
  - `forceCloseAtTransactionEnd` (line 184): forces cleanup
  - `connectionEstablishmentStart` (line 199): for lifetime tracking
  - **NO existing field for statement cache** — this must be added

#### 3c. Connection Lifecycle

- **Acquisition**: `StartNodeUserDatabaseConnection()` (`connection_management.c:285`)
  - Looks up `ConnectionHash` by key at line 334
  - If `!(flags & FORCE_NEW_CONNECTION)`: calls `FindAvailableConnection()` to reuse a cached connection (line 362)
  - Otherwise: allocates new `MultiConnection`, calls `StartConnectionEstablishment()` (line 393)
- **Caching**: `AfterXactHostConnectionHandling()` (line ~1480)
  - After transaction end, iterates connections
  - `ShouldShutdownConnection()` (`connection_management.c:1533`) decides whether to keep:
    - Shuts down if: `cachedConnectionCount >= MaxCachedConnectionsPerWorker`, connection not OK, `forceCloseAtTransactionEnd`, lifetime exceeded (`MaxCachedConnectionLifetime`), Citus internal backend, etc.
    - Otherwise: resets transaction state, unclaims connection, increments `cachedConnectionCount`
- **Lifetime**: controlled by `MaxCachedConnectionLifetime` (default 10 min, GUC `citus.max_cached_connection_lifetime`)
- **Max per worker**: controlled by `MaxCachedConnectionsPerWorker` (default 1, GUC `citus.max_cached_conns_per_worker`)
- **Connection drop**: `ShutdownConnection()` closes the underlying `PGconn`, naturally invalidating any server-side prepared statements. Also called in `RestartConnection()` (`connection_management.c:1594`)

#### 3d. Connection Reuse Within Sessions

- Within a session (transaction or auto-commit), connections are reused for subsequent queries to the same worker
- The `WorkerSession` struct (`adaptive_executor.c:457`) holds a `MultiConnection *connection` at field line 465
- `FindOrCreateWorkerSession()` at `adaptive_executor.c:1764` ensures one session per connection per pool
- When a placement was previously accessed in the same transaction, `GetConnectionIfPlacementAccessedInXact()` returns the same connection (`adaptive_executor.c:1503`)

### 4. Prepared Statement Handling on Coordinator

#### 4a. PREPARE/EXECUTE Flow at Coordinator Level

- PostgreSQL handles PREPARE/EXECUTE internally — the prepared statement name is resolved by the parser/executor before Citus is involved
- When PostgreSQL executes PREPARE, it calls the planner (which invokes `distributed_planner()`)
- On EXECUTE, PostgreSQL either:
  - Re-plans (custom plan, first 5 executions): calls `distributed_planner()` again with bound params
  - Uses generic plan (execution 6+): skips planning entirely, enters `CitusBeginScan()` directly

#### 4b. Deferred Pruning for Prepared Statements

- For fast-path queries with parameterized distribution keys, `fastPathContext.distributionKeyHasParam = true` (`fast_path_router_planner.c:178`)
- The `Job.deferredPruning = true` flag defers shard selection to execution time
- At execution time, `CitusBeginScan()` → `CitusBeginReadOnlyScan()` / `CitusBeginModifyScan()`:
  - Evaluates expressions to resolve parameter values
  - Calls `RegenerateTaskForFasthPathQuery()` to pick the shard (lines 345, 424)
  - This produces the final `Task` with the concrete shard query string

#### 4c. `DistributedPlan.numberOfTimesExecuted`

- Field at `multi_physical_planner.h:486`
- Incremented in `CitusBeginScan()` at `citus_custom_scan.c:269`: `distributedPlan->numberOfTimesExecuted++`
- Tracks how many times this cached plan has been used
- Potentially useful for: deciding when to prepare on the remote side (e.g., prepare after N executions)

### 5. Parameter Handling

#### 5a. Parameter Flow Through the Pipeline

- Parameters enter via `EState.es_param_list_info` (standard PostgreSQL `ParamListInfo`)
- `AdaptiveExecutor()` captures them at `adaptive_executor.c:781`: `ParamListInfo paramListInfo = executorState->es_param_list_info`
- Stored in `DistributedExecution.paramListInfo` (line 873)
- Available in `SendNextQuery()` via `execution->paramListInfo` (line 3893)

#### 5b. Parameter Evaluation and Extraction

- In `CitusBeginReadOnlyScan()` / `CitusBeginModifyScan()`:
  - `ExecuteCoordinatorEvaluableExpressions()` evaluates functions/expressions (line 337/395)
  - Sets `workerJob->parametersInJobQueryResolved = true` — signals params are baked into query text
  - After this, the task's query string has concrete values, not parameter placeholders

- In `SendNextQuery()`:
  - Checks `task->parametersInQueryStringResolved` (line 3901)
  - If params NOT resolved: calls `ExtractParametersForRemoteExecution()` → `ExtractParametersFromParamList()` (`executor_util_params.c:27`)
  - `ExtractParametersFromParamList()` (`executor_util_params.c:42`):
    - Allocates `Oid[] parameterTypes` and `char*[] parameterValues` arrays
    - For each param: gets type OID (remaps custom types to 0 for worker inference), converts datum to text via `OidOutputFunctionCall()`
    - NULL params: preserved with their type OID

#### 5c. Key Implication for PQprepare/PQexecPrepared

- When `parametersInQueryStringResolved = true` (the common case for fast-path prepared stmts after generic plan):
  - Parameters are already embedded in the query text
  - `SendNextQuery()` takes the else branch, sending plain SQL
  - For PQprepare: the query text with `$1`, `$2` placeholders would be needed instead
- When `parametersInQueryStringResolved = false`:
  - Parameters are available as arrays (types + text values)
  - This maps directly to PQexecPrepared's parameter interface
- **Key decision**: The POC may need to either:
  - Intercept *before* `parametersInJobQueryResolved` is set to `true`, preserving param placeholders
  - OR reconstruct the parameterized query template from the original query

### 6. Task and Placement Structures

#### 6a. `Task` Structure (`multi_physical_planner.h:243-368`)

Key fields:
- `taskQuery` (line 253): `TaskQuery` union containing the query string/object (see `TaskQuery` at line 187)
- `queryCount` (line 261): usually 1 for fast-path queries
- `anchorShardId` (line 264): shard ID this task targets
- `taskPlacementList` (line 265): `List` of `ShardPlacement *` — where to execute
- `parametersInQueryStringResolved` (line 327): when true, params are in query text
- `partitionKeyValue` (line 365): the resolved partition key as `Const *`
- `colocationId` (line 366): colocation group

#### 6b. `ShardPlacement` Structure (`metadata_utility.h:86-107`)

Key fields:
- `placementId` (line 91): unique placement ID
- `shardId` (line 92): which shard
- `groupId` (line 94): placement group
- `nodeName` (line 97): worker hostname
- `nodePort` (line 98): worker port
- `nodeId` (line 99): worker node ID

#### 6c. `ShardCommandExecution` (`adaptive_executor.c:538-572`)

- Wraps a `Task` for execution across multiple placements
- `task` (line 541): the underlying `Task`
- `binaryResults` (line 547): whether to use binary protocol
- `placementExecutions` (line 553): array of `TaskPlacementExecution *`
- `executionOrder` (line 550): PARALLEL or SEQUENTIAL

#### 6d. `TaskPlacementExecution` (`adaptive_executor.c:594-631`)

- Wraps execution of a task on one specific placement
- `shardCommandExecution` (line 597): parent
- `shardPlacement` (line 600): which placement
- `workerPool` (line 609): target pool
- `assignedSession` (line 612): specific session (or NULL for pool assignment)
- `queryIndex` (line 607): tracks progress through multi-query tasks

#### 6e. Query Identity for Cache Key

For the statement cache key, these fields identify a unique query:
- `Task.anchorShardId`: the target shard
- The query string template (before parameter substitution): the SQL pattern
- Or equivalently: the `DistributedPlan.planId` + `Task.anchorShardId`

### 7. GUC Registration

#### 7a. Registration Location

- All Citus GUCs are registered in `RegisterCitusConfigVariables()` at `src/backend/distributed/shared_library_init.c:975`
- GUCs are **alphabetically ordered** by their `citus.*` name
- This is enforced by `ci/check_gucs_are_alphabetically_sorted.sh`

#### 7b. Boolean GUC Pattern (Template: `citus.enable_binary_protocol`)

```c
// In shared_library_init.c:1287
DefineCustomBoolVariable(
    "citus.enable_binary_protocol",
    gettext_noop("Enables communication between nodes using binary protocol when possible"),
    NULL,
    &EnableBinaryProtocol,
    true,                    // default value
    PGC_USERSET,            // context (user can SET)
    GUC_STANDARD,           // flags
    NULL, NULL, NULL);      // check, assign, show hooks
```

#### 7c. Steps to Add `citus.enable_prepared_statement_caching`

1. **Declare variable**: `bool EnablePreparedStatementCaching = false;` in the appropriate `.c` file (e.g., `adaptive_executor.c` or a new file)
2. **Declare extern**: `extern bool EnablePreparedStatementCaching;` in the appropriate `.h` file (e.g., `adaptive_executor.h`)
3. **Register GUC**: In `shared_library_init.c`, insert alphabetically between `citus.enable_non_colocated_router_query_pushdown` and `citus.enable_repartition_joins`:
   ```c
   DefineCustomBoolVariable(
       "citus.enable_prepared_statement_caching",
       gettext_noop("Enables caching prepared statement plans on worker connections"),
       NULL,
       &EnablePreparedStatementCaching,
       false,
       PGC_USERSET,
       GUC_STANDARD,
       NULL, NULL, NULL);
   ```
4. **Alphabetical position**: `enable_prepared_statement_caching` sorts between `enable_non_colocated_router_query_pushdown` (line ~1437) and `enable_repartition_joins` (line ~1455)

#### 7d. GUC Flag Options

- `PGC_USERSET`: users can SET within their session (appropriate for this feature)
- `GUC_STANDARD`: visible in `SHOW ALL` and `pg_settings`
- `GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE`: hides from `SHOW ALL` and `postgresql.conf.sample` (used for internal/experimental GUCs)

### 8. Documentation Infrastructure

- **CHANGELOG.md**: At repo root. Format: `### citus vX.Y.Z (date) ###` followed by `* Description (#PR_number)` bullets. New entries go at the top.
- **README.md**: High-level project description, build instructions, links. No feature-level docs.
- **src/backend/distributed/README.md**: Technical architecture doc. Contains detailed explanation of distributed planning and execution (e.g., line 311 discusses prepared statement plan caching and `CitusBeginScan()`)
- **Test files**: Regression tests in `src/test/regress/sql/`:
  - `prepared_statements_1.sql` through `prepared_statements_4.sql`
  - `prepared_statements_create_load.sql`
  - `fast_path_router_modify.sql`
  - `multi_router_planner_fast_path.sql`
  - `subquery_prepared_statements.sql`
  - `cte_prepared_modify.sql`
- **Expected outputs**: `src/test/regress/expected/` (matching `.out` files)

## Code References

### Core Execution Path
- `src/backend/distributed/planner/distributed_planner.c:155` — `distributed_planner()` entry point
- `src/backend/distributed/planner/distributed_planner.c:190` — `FastPathRouterQuery()` call
- `src/backend/distributed/planner/distributed_planner.c:271` — `PlanFastPathDistributedStmt()` for fast-path
- `src/backend/distributed/planner/fast_path_router_planner.c:95` — `FastPathPlanner()` definition
- `src/backend/distributed/planner/fast_path_router_planner.c:229` — `FastPathRouterQuery()` eligibility check
- `src/backend/distributed/executor/citus_custom_scan.c:180` — `CitusBeginScan()` entry point
- `src/backend/distributed/executor/citus_custom_scan.c:247` — `CitusExecScan()` → `AdaptiveExecutor()` call
- `src/backend/distributed/executor/citus_custom_scan.c:290` — `CitusBeginReadOnlyScan()` deferred pruning
- `src/backend/distributed/executor/citus_custom_scan.c:376` — `CitusBeginModifyScan()` modify path
- `src/backend/distributed/executor/citus_custom_scan.c:654` — `RegenerateTaskForFasthPathQuery()` shard pruning

### Adaptive Executor
- `src/backend/distributed/executor/adaptive_executor.c:775` — `AdaptiveExecutor()` main function
- `src/backend/distributed/executor/adaptive_executor.c:1169` — `CreateDistributedExecution()`
- `src/backend/distributed/executor/adaptive_executor.c:1426` — `AssignTasksToConnectionsOrWorkerPool()`
- `src/backend/distributed/executor/adaptive_executor.c:1898` — `RunDistributedExecution()` main event loop
- `src/backend/distributed/executor/adaptive_executor.c:2960` — `ConnectionStateMachine()`
- `src/backend/distributed/executor/adaptive_executor.c:3394` — `TransactionStateMachine()`
- `src/backend/distributed/executor/adaptive_executor.c:3823` — `StartPlacementExecutionOnSession()`
- **`src/backend/distributed/executor/adaptive_executor.c:3885`** — **`SendNextQuery()` — PRIMARY INTEGRATION POINT**
- `src/backend/distributed/executor/adaptive_executor.c:3944` — `PQsetSingleRowMode()` after send

### Remote Command Layer
- `src/backend/distributed/connection/remote_commands.c:522` — `SendRemoteCommandParams()` (wraps `PQsendQueryParams`)
- `src/backend/distributed/connection/remote_commands.c:541` — `PQsendQueryParams()` call
- `src/backend/distributed/connection/remote_commands.c:556` — `SendRemoteCommand()` (wraps `PQsendQuery`)
- `src/backend/distributed/connection/remote_commands.c:573` — `PQsendQuery()` call

### Connection Management
- `src/backend/distributed/connection/connection_management.c:52` — `ConnectionHash` declaration
- `src/backend/distributed/connection/connection_management.c:101` — `InitializeConnectionManagement()`
- `src/backend/distributed/connection/connection_management.c:285` — `StartNodeUserDatabaseConnection()`
- `src/backend/distributed/connection/connection_management.c:362` — `FindAvailableConnection()` (connection reuse)
- `src/backend/distributed/connection/connection_management.c:1533` — `ShouldShutdownConnection()` (cache eviction)

### Parameter Handling
- `src/backend/distributed/executor/executor_util_params.c:27` — `ExtractParametersForRemoteExecution()`
- `src/backend/distributed/executor/executor_util_params.c:42` — `ExtractParametersFromParamList()` (types/values extraction)

### GUC Registration
- `src/backend/distributed/shared_library_init.c:975` — `RegisterCitusConfigVariables()` start
- `src/backend/distributed/shared_library_init.c:1287` — `citus.enable_binary_protocol` (template for new bool GUC)
- `src/backend/distributed/shared_library_init.c:1389` — `citus.enable_fast_path_router_planner`

### Key Data Structures
- `src/include/distributed/connection_management.h:162` — `MultiConnection` struct
- `src/include/distributed/connection_management.h:240` — `ConnectionHashKey` struct
- `src/include/distributed/connection_management.h:249` — `ConnectionHashEntry` struct
- `src/include/distributed/multi_physical_planner.h:134` — `Job` struct
- `src/include/distributed/multi_physical_planner.h:187` — `TaskQuery` union
- `src/include/distributed/multi_physical_planner.h:243` — `Task` struct
- `src/include/distributed/multi_physical_planner.h:400` — `DistributedPlan` struct
- `src/include/distributed/metadata_utility.h:86` — `ShardPlacement` struct
- `src/include/distributed/citus_custom_scan.h:19` — `CitusScanState` struct
- `src/backend/distributed/executor/adaptive_executor.c:220-320` — `DistributedExecution` struct
- `src/backend/distributed/executor/adaptive_executor.c:370-440` — `WorkerPool` struct
- `src/backend/distributed/executor/adaptive_executor.c:457-497` — `WorkerSession` struct
- `src/backend/distributed/executor/adaptive_executor.c:538-572` — `ShardCommandExecution` struct
- `src/backend/distributed/executor/adaptive_executor.c:594-631` — `TaskPlacementExecution` struct

### Test Files
- `src/test/regress/sql/prepared_statements_1.sql` through `prepared_statements_4.sql`
- `src/test/regress/sql/fast_path_router_modify.sql`
- `src/test/regress/sql/multi_router_planner_fast_path.sql`

## Architecture Documentation

### Execution Pipeline

```
PREPARE stmt AS SELECT ... FROM dist_table WHERE dist_key = $1;
  └─► distributed_planner() → FastPathRouterQuery() → FastPathPlanner()
      └─► DistributedPlan { fastPathRouterPlan=true, deferredPruning=true }

EXECUTE stmt(42);  [executions 1-5: custom plan, re-plans each time]
  └─► distributed_planner() again with bound params

EXECUTE stmt(42);  [execution 6+: generic plan]
  └─► CitusBeginScan()
      └─► CitusBeginReadOnlyScan() / CitusBeginModifyScan()
          ├─ ExecuteCoordinatorEvaluableExpressions()  [resolve params]
          ├─ parametersInJobQueryResolved = true
          └─ RegenerateTaskForFasthPathQuery()
              ├─ TargetShardIntervalForFastPathQuery()  [pick shard]
              ├─ UpdateRelationToShardNames()            [rewrite table→shard names]
              └─ RebuildQueryStrings()                   [deparse to SQL text]
  └─► CitusExecScan()
      └─► AdaptiveExecutor()
          └─► RunDistributedExecution()
              └─► ConnectionStateMachine()
                  └─► TransactionStateMachine()
                      └─► StartPlacementExecutionOnSession()
                          └─► SendNextQuery()  ◄── INTEGRATION POINT
                              └─► SendRemoteCommand[Params]()
                                  └─► PQsendQuery[Params]()  ◄── REPLACE WITH PQprepare/PQexecPrepared
```

### Connection Lifecycle

```
Transaction start:
  └─► StartNodeUserDatabaseConnection()
      ├─ ConnectionHash lookup
      ├─ FindAvailableConnection() → returns cached connection (if available)
      └─ OR: allocate new MultiConnection → StartConnectionEstablishment()

During execution:
  └─► RunDistributedExecution()
      ├─ AssignTasksToConnectionsOrWorkerPool()
      ├─ ManageWorkerPool() → OpenNewConnections() if needed
      └─ ConnectionStateMachine() → sends queries via sessions

Transaction end:
  └─► AfterXactConnectionHandling()
      └─ AfterXactHostConnectionHandling()
          └─ For each connection:
              ├─ ShouldShutdownConnection()? → ShutdownConnection() + pfree
              └─ OR: ResetRemoteTransaction() + UnclaimConnection() → stays in cache
```

## Key Integration Points

### Primary Changes

1. **`SendNextQuery()` (`adaptive_executor.c:3885`)** — The main integration point. This function must be modified to:
   - Check if a prepared statement handle exists on this connection for this query
   - If yes: call a new wrapper around `PQsendQueryPrepared()` (async) or `PQexecPrepared()` instead of `SendRemoteCommand[Params]()`
   - If no: call `PQsendPrepare()` first to prepare the statement, then execute it
   - Note: `PQsetSingleRowMode()` at line 3944 must remain after the send

2. **`MultiConnection` struct (`connection_management.h:162`)** — Add a field for per-connection statement cache:
   - e.g., `HTAB *preparedStatementCache;` — hash table mapping query identity → prepared statement name
   - Initialize to NULL, allocate on first use

3. **New file: Statement cache module** — Implement the cache logic:
   - Cache key: query text template (or hash thereof) + shard ID
   - Cache value: prepared statement name (e.g., `"citus_ps_1"`, `"citus_ps_2"`, ...)
   - Cache size limit: 1,000 entries (as per spec)
   - LRU eviction or simple counter-based naming

4. **`remote_commands.c`** — Add new libpq wrapper functions:
   - `SendRemotePrepare(connection, stmtName, query, nParams, paramTypes)` — wraps `PQsendPrepare()`
   - `SendRemotePreparedCommand(connection, stmtName, nParams, paramValues, binaryResults)` — wraps `PQsendQueryPrepared()`

5. **New GUC** — `citus.enable_prepared_statement_caching`:
   - In `shared_library_init.c` (alphabetically between `enable_non_colocated_router_query_pushdown` and `enable_repartition_joins`)
   - Default: `false`

### Secondary Changes

6. **Connection cleanup** — In `ShutdownConnection()` or `CitusPQFinish()` in `connection_management.c`:
   - Free the `preparedStatementCache` hash table when connection closes
   - No need to send DEALLOCATE — server-side statements are cleaned up when connection drops

7. **Cache invalidation** — For POC scope (no DDL invalidation):
   - Cache naturally invalidates when connection drops (connection lifetime: `MaxCachedConnectionLifetime`)
   - No explicit invalidation mechanism needed for POC

### Parameter Handling Decision Point

8. **Parameter preservation** — The POC has two options for handling the parameter flow:
   - **Option A (Simpler)**: Work with the "params resolved" path — the query text has concrete values baked in. Use the query *text* as the cache key. This means each unique set of literal values produces a different prepared statement (less caching benefit).
   - **Option B (Correct)**: Intercept before `parametersInJobQueryResolved = true` is set. Preserve the parameterized query template (`$1`, `$2` placeholders) for the prepared statement. Use the parameterized template as cache key. Forward params via `PQexecPrepared()`. This gives proper N-execution caching. The intervention point would be in `CitusBeginReadOnlyScan()` (`citus_custom_scan.c:337-345`) and `CitusBeginModifyScan()` (`citus_custom_scan.c:390-424`) — specifically around `ExecuteCoordinatorEvaluableExpressions()` and the `parametersInJobQueryResolved = true` assignment.

## Open Questions

1. **Parameter template preservation**: The spec says to use `PQprepare`/`PQexecPrepared` with parameter forwarding. This implies Option B above. Needs investigation into whether the parameterized query text (with `$1`, `$2`) is still available at `SendNextQuery()` time, or if it must be reconstructed. The `Task.taskQuery.data.jobQueryReferenceForLazyDeparsing` field may hold the original parameterized query.

2. **Async prepare**: `PQsendPrepare()` is async; `PQprepare()` is sync. The adaptive executor is async throughout. The POC should use `PQsendPrepare()` to avoid blocking, but this means the prepare+execute sequence requires two round trips on first execution. Need to verify this works within the existing `TransactionStateMachine` flow.

3. **Statement name generation**: Needs a scheme that avoids collisions across multiple prepared statements within the same connection. A simple monotonic counter per connection (e.g., `citus_ps_%lu` with `connectionId * 1000000 + seqno`) would work.
