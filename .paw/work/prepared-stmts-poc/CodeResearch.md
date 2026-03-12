---
date: 2026-03-09T00:00:00Z
git_commit: 58fd4596caf02d95070968117ab40b07a300d937
branch: colm/prepared-stmts-poc
repository: citusdata/citus
topic: "Prepared Statement Caching on Worker Connections"
tags: [research, codebase, adaptive-executor, connection-management, prepared-statements, fast-path]
status: complete
last_updated: 2026-03-12
last_updated_note: "Added Phase 4 Cache-Hit Fast Path research (Q1-Q10)"
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

---

## Phase 4: Cache-Hit Fast Path

> **Date**: 2026-03-12 | **Commit**: a71df7c0431422c4b2fa92c6e1332629005065d6 | **Branch**: colm/prepared-stmts-poc

### Q1: CitusBeginReadOnlyScan — Current fast-path flow

Entry point: [CitusBeginReadOnlyScan](src/backend/distributed/executor/citus_custom_scan.c#L294) is called from `CitusBeginScan()` at [citus_custom_scan.c:221](src/backend/distributed/executor/citus_custom_scan.c#L221) for `CMD_SELECT` queries.

The full execution flow for a fast-path SELECT with caching enabled and `deferredPruning = true`:

```
CitusBeginReadOnlyScan()                          [citus_custom_scan.c:294]
  │
  ├─ (1) CopyDistributedPlanWithoutCache()        [citus_custom_scan.c:317]
  │      Deep-copies entire DistributedPlan + Job + jobQuery tree.
  │      COST: copyObject() on the full plan tree — the single most
  │      expensive operation in this path. Allocates new memory for
  │      every node in the plan tree.
  │
  ├─ (2) Save savedJobQueryForCaching              [citus_custom_scan.c:339-348]
  │      On first execution: copyObject(jobQuery) saved on original plan.
  │      Subsequent executions: reuse saved copy. One-time cost.
  │
  ├─ (3) ExecuteCoordinatorEvaluableExpressions()  [citus_custom_scan.c:361]
  │      Walks the jobQuery tree replacing Param nodes ($1,$2...) with
  │      Const nodes (resolved values from ParamListInfo).
  │      For SELECTs: evaluationMode = EVALUATE_PARAMS (params only).
  │      Defined at [citus_clauses.c:69](src/backend/distributed/utils/citus_clauses.c#L69).
  │      COST: full expression_tree_walker over jobQuery. Moderate.
  │
  ├─ (4) workerJob->parametersInJobQueryResolved = true  [citus_custom_scan.c:365]
  │
  ├─ (5) RegenerateTaskForFasthPathQuery()         [citus_custom_scan.c:368]
  │      Resolves distribution key → shard, rewrites table names to shard
  │      names in the query tree, builds task and placement list.
  │      COST: Several function calls (see Q3). This is the second most
  │      expensive step due to UpdateRelationToShardNames (tree walk)
  │      and GenerateSingleShardRouterTaskList (task allocation + setup).
  │
  └─ (6) Attach prepared stmt metadata to tasks     [citus_custom_scan.c:374-379]
         Sets task->preparedStatementPlanId and task->jobQueryForPrepare.
         COST: Negligible (pointer assignment per task).
```

**Steps skippable on cache hit**: Steps (1), (2), (3), and most of (5) can be skipped entirely. The minimum work needed on a cache hit is: resolve the distribution key value from `ParamListInfo` → find the shard interval → build a minimal Task with `anchorShardId`, `taskPlacementList`, `relationShardList`, and the prepared-statement metadata fields. Steps (4) and (6) are trivially cheap.

### Q2: CopyDistributedPlanWithoutCache — What does it deep-copy?

Defined at [citus_custom_scan.c:686](src/backend/distributed/executor/citus_custom_scan.c#L686).

```c
static DistributedPlan *
CopyDistributedPlanWithoutCache(DistributedPlan *originalDistributedPlan)
{
    List *localPlannedStatements =
        originalDistributedPlan->workerJob->localPlannedStatements;
    Query *savedJobQueryForCaching =
        originalDistributedPlan->workerJob->savedJobQueryForCaching;
    originalDistributedPlan->workerJob->localPlannedStatements = NIL;
    originalDistributedPlan->workerJob->savedJobQueryForCaching = NULL;

    DistributedPlan *distributedPlan = copyObject(originalDistributedPlan);

    /* set back the immutable/cached fields */
    originalDistributedPlan->workerJob->localPlannedStatements = localPlannedStatements;
    distributedPlan->workerJob->localPlannedStatements = localPlannedStatements;
    originalDistributedPlan->workerJob->savedJobQueryForCaching = savedJobQueryForCaching;

    return distributedPlan;
}
```

The `copyObject()` call deep-copies the entire `DistributedPlan` struct ([multi_physical_planner.h:415-513](src/include/distributed/multi_physical_planner.h#L415-L513)), which includes:

- All `DistributedPlan` fields: `planId`, `modLevel`, `expectResults`, `combineQuery`, `queryId`, `relationIdList`, `targetRelationId`, `subPlanList`, `usedSubPlanNodeList`, `fastPathRouterPlan`, `numberOfTimesExecuted`, `planningError`, etc.
- The entire `Job` struct ([multi_physical_planner.h:134-166](src/include/distributed/multi_physical_planner.h#L134-L166)): `jobId`, `jobQuery` (full Query tree), `taskList`, `dependentJobList`, `deferredPruning`, `partitionKeyValue`, `parametersInJobQueryResolved`, `colocationId`.
- The `jobQuery` is the most expensive part: it is a full PostgreSQL `Query` tree including `rtable`, `jointree`, `targetList`, and all quals.

**Excluded from deep copy** (set to NIL/NULL before `copyObject`, restored after):
- `localPlannedStatements` — shared across executions (immutable plans)
- `savedJobQueryForCaching` — lives on the original plan only

**Fields accessed by the adaptive executor from `DistributedPlan`**:
- `distributedPlan->modLevel` — at [adaptive_executor.c:867](src/backend/distributed/executor/adaptive_executor.c#L867) for `CreateDistributedExecution()`
- `distributedPlan->workerJob->taskList` — at [adaptive_executor.c:789](src/backend/distributed/executor/adaptive_executor.c#L789)
- `job->jobQuery->commandType` — at [adaptive_executor.c:899](src/backend/distributed/executor/adaptive_executor.c#L899) for `es_processed` check
- `job->jobQuery` — at [adaptive_executor.c:864](src/backend/distributed/executor/adaptive_executor.c#L864) for `MarkUnreferencedExternParams` (skipped when caching enabled)
- `job->dependentJobList` — at [adaptive_executor.c:830](src/backend/distributed/executor/adaptive_executor.c#L830)
- `distributedPlan->expectResults` — at [adaptive_executor.c:903](src/backend/distributed/executor/adaptive_executor.c#L903) for `SortReturning`

**Conclusion**: On a cache hit, we can avoid the deep copy entirely if we construct a minimal `DistributedPlan`/`Job` stub or re-use a lightweight execution path. The critical fields needed by the executor are `modLevel`, `taskList`, `job->jobQuery->commandType`, and `job->dependentJobList` (NIL for fast-path). The most expensive thing being deep-copied — the full `jobQuery` tree — is not needed on cache hits if we have a pre-built Task.

### Q3: RegenerateTaskForFasthPathQuery — Full dissection

Defined at [citus_custom_scan.c:746](src/backend/distributed/executor/citus_custom_scan.c#L746).

```c
static void
RegenerateTaskForFasthPathQuery(Job *workerJob)
{
    bool isMultiShardQuery = false;
    List *shardIntervalList =
        TargetShardIntervalForFastPathQuery(workerJob->jobQuery,
                                            &isMultiShardQuery, NULL,
                                            &workerJob->partitionKeyValue);
    // ... error check for multi-shard ...

    bool shardsPresent = false;
    List *relationShardList =
        RelationShardListForShardIntervalList(shardIntervalList, &shardsPresent);

    UpdateRelationToShardNames((Node *) workerJob->jobQuery, relationShardList);

    bool hasLocalRelation = false;
    List *placementList =
        CreateTaskPlacementListForShardIntervals(shardIntervalList, shardsPresent,
                                                 true, hasLocalRelation);
    uint64 shardId = INVALID_SHARD_ID;
    if (shardsPresent) { shardId = GetAnchorShardId(shardIntervalList); }

    bool isLocalTableModification = false;
    bool delayedFastPath = false;
    GenerateSingleShardRouterTaskList(workerJob, relationShardList, placementList,
                                      shardId, isLocalTableModification,
                                      delayedFastPath);
}
```

Step-by-step breakdown:

1. **`TargetShardIntervalForFastPathQuery()`** ([multi_router_planner.c:3107](src/backend/distributed/planner/multi_router_planner.c#L3107)): Called with `inputDistributionKeyValue = NULL` (third arg). Since parameters were already resolved to Consts by `ExecuteCoordinatorEvaluableExpressions`, it falls through to the `quals`-based path at [multi_router_planner.c:3163](src/backend/distributed/planner/multi_router_planner.c#L3163): calls `PruneShards()` to scan the WHERE clause for `dist_col = <Const>`. Internally calls `ExtractFirstCitusTableId(query)` at [multi_router_planner.c:3112](src/backend/distributed/planner/multi_router_planner.c#L3112) to get the relation OID from the query's rtable, then `GetCitusTableCacheEntry()` + `FindShardInterval()` for the hash lookup.

2. **`RelationShardListForShardIntervalList()`**: Builds a `List` of `RelationShard` structs mapping relation OID → shard ID. Lightweight allocation.

3. **`UpdateRelationToShardNames()`** ([deparse_shard_query.c:467](src/backend/distributed/planner/deparse_shard_query.c#L467)): Walks the *entire* query tree via `query_tree_walker`/`expression_tree_walker`, modifying `RangeTblEntry` names to include shard IDs (e.g., `dist_table` → `dist_table_102001`). This modifies the `jobQuery` in-place. **This is expensive** — full tree walk mutating RTEs.

4. **`CreateTaskPlacementListForShardIntervals()`**: Builds the placement list (which worker nodes hold the shard). Involves metadata lookups but is relatively cheap.

5. **`GenerateSingleShardRouterTaskList()`** ([multi_router_planner.c:2279](src/backend/distributed/planner/multi_router_planner.c#L2279)): Calls `SingleShardTaskList()` ([multi_router_planner.c:2418](src/backend/distributed/planner/multi_router_planner.c#L2418)) which:
   - Calls `CreateTask(READ_TASK)` — allocates a `Task` node
   - Calls `SetTaskQueryIfShouldLazyDeparse(task, query)` — stores the query reference for lazy deparsing (the query string is NOT generated yet)
   - Sets `task->anchorShardId`, `task->taskPlacementList`, `task->relationShardList`, `task->parametersInQueryStringResolved`, `task->partitionKeyValue`, `task->colocationId`
   - The actual query string deparse happens lazily in `TaskQueryStringAtIndex()` during `SendNextQuery()`

**What can be called cheaply on the fast path**: `TargetShardIntervalForFastPathQuery` CAN be called with an `inputDistributionKeyValue` Const (see Q4), which avoids the quals-based `PruneShards()` walk entirely. However it still needs `ExtractFirstCitusTableId(query)` — which requires the query's rtable. The entire `UpdateRelationToShardNames` tree walk and query deparsing can be skipped on a cache hit.

### Q4: TargetShardIntervalForFastPathQuery — Cheap shard lookup

Defined at [multi_router_planner.c:3107](src/backend/distributed/planner/multi_router_planner.c#L3107):

```c
List *
TargetShardIntervalForFastPathQuery(Query *query, bool *isMultiShardQuery,
                                    Const *inputDistributionKeyValue,
                                    Const **outputPartitionValueConst)
{
    Oid relationId = ExtractFirstCitusTableId(query);

    if (!HasDistributionKey(relationId)) { ... }

    if (inputDistributionKeyValue && !inputDistributionKeyValue->constisnull)
    {
        CitusTableCacheEntry *cache = GetCitusTableCacheEntry(relationId);
        // ... type coercion check ...
        ShardInterval *cachedShardInterval =
            FindShardInterval(inputDistributionKeyValue->constvalue, cache);
        // ... return shard interval ...
    }

    // ... fallback: quals-based PruneShards() ...
```

**When given a non-NULL `inputDistributionKeyValue`**, the function takes the fast path at [multi_router_planner.c:3122-3148](src/backend/distributed/planner/multi_router_planner.c#L3122-L3148):
1. `GetCitusTableCacheEntry(relationId)` — hash table lookup, very cheap (cached)
2. Optional type coercion via `TransformPartitionRestrictionValue()` — only if types mismatch (rare for prepared stmts)
3. `FindShardInterval(constvalue, cache)` at [shardinterval_utils.c:260](src/backend/distributed/utils/shardinterval_utils.c#L260) — hash function call + binary search. Very cheap.
4. `CopyShardInterval()` — small struct copy

**The problem**: The function always calls `ExtractFirstCitusTableId(query)` at line 3112, which requires the `Query` tree to extract the relation OID from `query->rtable`.

**For the fast path bypass**, we don't need `TargetShardIntervalForFastPathQuery` at all. We can directly call: 
1. `GetCitusTableCacheEntry(relationId)` — using a stored relation OID from the `DistributedPlan` (e.g., `distributedPlan->targetRelationId` for DML, or `linitial_oid(distributedPlan->relationIdList)` for SELECTs)
2. `FindShardInterval(constvalue, cache)` — with the resolved Const datum

**Going from ParamListInfo → shard interval**:
- Extract the distribution key parameter value from `ParamListInfo` (we know which param index corresponds to the distribution key)
- Create a Const node with the value
- Call `FindShardInterval(const->constvalue, cache)` directly

The `FastPathRestrictionContext` contains `distributionKeyHasParam = true` for parameterized fast-path queries, but does NOT record which param index holds the distribution key. However, the distribution key type info is available via `GetCitusTableCacheEntry(relationId)->partitionColumn`.

### Q5: AdaptiveExecutor and CreateDistributedExecution — Minimum state needed

#### AdaptiveExecutor() at [adaptive_executor.c:775](src/backend/distributed/executor/adaptive_executor.c#L775)

Reads from `DistributedPlan`/`Job`:
- `distributedPlan->workerJob` → `job`
- `job->taskList` — the primary input at [adaptive_executor.c:789](src/backend/distributed/executor/adaptive_executor.c#L789)
- `job->dependentJobList` — checked at [adaptive_executor.c:830](src/backend/distributed/executor/adaptive_executor.c#L830); NIL for fast-path queries
- `job->jobQuery` — used for:
  - `MarkUnreferencedExternParams()` at [line 864](src/backend/distributed/executor/adaptive_executor.c#L864) — **skipped when `EnablePreparedStatementCaching`** (guard at line 861)
  - `job->jobQuery->commandType` at [line 899](src/backend/distributed/executor/adaptive_executor.c#L899) — only to check `CMD_SELECT` for `es_processed`
- `distributedPlan->modLevel` — at [line 867](src/backend/distributed/executor/adaptive_executor.c#L867) for `CreateDistributedExecution()` and `DecideTaskListTransactionProperties()`
- `distributedPlan->expectResults` — at [line 903](src/backend/distributed/executor/adaptive_executor.c#L903) for `SortReturning` check

#### CreateDistributedExecution() at [adaptive_executor.c:1176](src/backend/distributed/executor/adaptive_executor.c#L1176)

Parameters: `modLevel`, `taskList`, `paramListInfo`, `targetPoolSize`, `defaultTupleDest`, `xactProperties`, `jobIdList`, `localExecutionSupported`.

Does NOT directly access `Job` or `DistributedPlan` — it only uses the passed-in values. The `taskList` is the critical input.

#### Task fields actually READ by the executor

In `AssignTasksToConnectionsOrWorkerPool()` at [adaptive_executor.c:1433](src/backend/distributed/executor/adaptive_executor.c#L1433):
- `task->taskPlacementList` — at [line 1443](src/backend/distributed/executor/adaptive_executor.c#L1443) for `placementExecutionCount` and iteration at [line 1462](src/backend/distributed/executor/adaptive_executor.c#L1462)
- `task->tupleDest` — at [line 1641](src/backend/distributed/executor/adaptive_executor.c#L1641) for `SetAttributeInputMetadata`
- `task->queryCount` — at [line 1644](src/backend/distributed/executor/adaptive_executor.c#L1644) for `SetAttributeInputMetadata`
- `task->taskType` — at [line 1678](src/backend/distributed/executor/adaptive_executor.c#L1678) for `ExecutionOrderForTask`

In `SendNextQuery()` at [adaptive_executor.c:3891](src/backend/distributed/executor/adaptive_executor.c#L3891):
- `task->queryCount` — at [line 3905](src/backend/distributed/executor/adaptive_executor.c#L3905) for Assert
- `task->preparedStatementPlanId` — at [line 3937](src/backend/distributed/executor/adaptive_executor.c#L3937) for cache lookup key
- `task->anchorShardId` — at [line 3938](src/backend/distributed/executor/adaptive_executor.c#L3938) for cache lookup key
- `task->jobQueryForPrepare` — at [line 3916](src/backend/distributed/executor/adaptive_executor.c#L3916) for cache-miss deparse, and at [line 3968](src/backend/distributed/executor/adaptive_executor.c#L3968) for `copyObject`
- `task->relationShardList` — at [line 3970](src/backend/distributed/executor/adaptive_executor.c#L3970) for `UpdateRelationToShardNames` on cache miss
- `task->parametersInQueryStringResolved` — at [line 4022](src/backend/distributed/executor/adaptive_executor.c#L4022) for plain SQL fallback path

In `ReceiveResults()`:
- `task->tupleDest` — at [line 4095](src/backend/distributed/executor/adaptive_executor.c#L4095)
- `task->totalReceivedTupleData` — accumulated during result processing

Post-execution:
- `task->anchorShardId` — at [line 4474](src/backend/distributed/executor/adaptive_executor.c#L4474)

#### Minimal "cache-hit Task" fields

A Task built from scratch via `CreateTask(READ_TASK)` needs these populated:
| Field | Value | Why |
|-------|-------|-----|
| `taskType` | `READ_TASK` | `ExecutionOrderForTask` |
| `anchorShardId` | resolved shard ID | cache lookup key, post-execution |
| `taskPlacementList` | placement list for shard | connection assignment |
| `queryCount` | `1` | `SetAttributeInputMetadata`, `SendNextQuery` |
| `preparedStatementPlanId` | from `distributedPlan->planId` | cache lookup key |
| `jobQueryForPrepare` | `savedJobQueryForCaching` | cache-miss deparse (fallback) |
| `relationShardList` | relation→shard mapping | cache-miss `UpdateRelationToShardNames` |
| `parametersInQueryStringResolved` | `true` | plain SQL fallback |
| `taskQuery` | lazy-deparse reference or NULL | only for cache-miss/plain-SQL fallback |
| `tupleDest` | `NULL` (uses default) | result routing |

**Conclusion**: Yes, we can build a Task from scratch with `CreateTask()` and populate only these fields. The `taskQuery` field is only needed for the non-caching fallback path.

### Q6: SendNextQuery — Current caching path

Defined at [adaptive_executor.c:3891](src/backend/distributed/executor/adaptive_executor.c#L3891).

#### Query string resolution

At [line 3906](src/backend/distributed/executor/adaptive_executor.c#L3906):
```c
char *queryString = TaskQueryStringAtIndex(task, queryIndex);
```
This is called **BEFORE** the caching check. `TaskQueryStringAtIndex()` at [deparse_shard_query.c:756](src/backend/distributed/planner/deparse_shard_query.c#L756) may trigger lazy deparsing via `TaskQueryString()` — meaning it can call `pg_get_query_def()` on the query tree if the query string hasn't been materialized yet. This is a **wasted deparse** when the caching path is taken.

#### Cache hit path (lines 3912-4011)

```
if (EnablePreparedStatementCaching && task->jobQueryForPrepare != NULL && paramListInfo != NULL)
{
    1. copyParamList(paramListInfo)                    — force param evaluation
    2. ExtractParametersForRemoteExecution()            — convert to text values
    3. Lazy-create preparedStatementCache if NULL
    4. PreparedStatementCacheLookup(planId, shardId)
    
    IF cache hit:
        5. SendRemotePreparedQuery(stmtName, params)   — PQsendQueryPrepared
    
    IF cache miss:
        6. PreparedStatementCacheInsert()
        7. copyObject(task->jobQueryForPrepare)         — copy query tree
        8. UpdateRelationToShardNames()                 — rewrite to shard names
        9. pg_get_query_def()                           — deparse to SQL
       10. SendRemotePrepare()                          — PQprepare (synchronous)
       11. Store template in cache entry
       12. SendRemotePreparedQuery(stmtName, params)    — PQsendQueryPrepared
}
```

#### Cache miss path

Falls through to `plain_sql:` label at [line 4019](src/backend/distributed/executor/adaptive_executor.c#L4019-L4060):
- If `paramListInfo != NULL && !task->parametersInQueryStringResolved`: sends via `SendRemoteCommandParams` with parameters
- Otherwise: sends via `SendRemoteCommand` (plain text) or `SendRemoteCommandParams` (for binary results)

#### Can we skip TaskQueryStringAtIndex?

**Yes.** The `queryString` variable is only used after the `plain_sql:` label. On the caching path, it is never referenced — the query string comes from the cache entry's `parameterizedQueryString` or is generated from `jobQueryForPrepare`. The call at line 3906 is **unconditionally evaluated before the caching check**, making it a wasted lazy-deparse on every cache hit.

**Proposed fix**: Move the `TaskQueryStringAtIndex` call to after the caching check, or guard it with `if (!EnablePreparedStatementCaching || task->jobQueryForPrepare == NULL)`.

#### jobQueryForPrepare handling

At [citus_custom_scan.c:377](src/backend/distributed/executor/citus_custom_scan.c#L377): `task->jobQueryForPrepare = savedJobQuery` — this is a pointer to `origJob->savedJobQueryForCaching`, which lives on the `originalDistributedPlan` (long-lived memory context). It is **not** copied per-execution; it is the original saved query with Param nodes intact.

On cache miss in `SendNextQuery()` at [line 3968](src/backend/distributed/executor/adaptive_executor.c#L3968): `copyObject(task->jobQueryForPrepare)` creates a working copy for shard-name substitution so the shared template stays unmodified.

### Q7: Task struct fields — What's needed for executor

Complete list of Task fields READ during `RunDistributedExecution` → `ManageWorkerPool` → `SendNextQuery` with file:line references:

| Field | Read at | Purpose |
|-------|---------|---------|
| `taskPlacementList` | [adaptive_executor.c:1443](src/backend/distributed/executor/adaptive_executor.c#L1443), [1462](src/backend/distributed/executor/adaptive_executor.c#L1462) | Connection assignment, placement execution count |
| `taskType` | [adaptive_executor.c:1678](src/backend/distributed/executor/adaptive_executor.c#L1678) | Execution order (sequential vs parallel) |
| `tupleDest` | [adaptive_executor.c:1641](src/backend/distributed/executor/adaptive_executor.c#L1641), [4095](src/backend/distributed/executor/adaptive_executor.c#L4095) | Tuple routing; NULL uses default |
| `queryCount` | [adaptive_executor.c:1644](src/backend/distributed/executor/adaptive_executor.c#L1644), [3605](src/backend/distributed/executor/adaptive_executor.c#L3605), [3905](src/backend/distributed/executor/adaptive_executor.c#L3905), [4172](src/backend/distributed/executor/adaptive_executor.c#L4172) | Query iteration, attribute metadata |
| `anchorShardId` | [adaptive_executor.c:3938](src/backend/distributed/executor/adaptive_executor.c#L3938), [4474](src/backend/distributed/executor/adaptive_executor.c#L4474) | Cache key, post-execution logging |
| `preparedStatementPlanId` | [adaptive_executor.c:3937](src/backend/distributed/executor/adaptive_executor.c#L3937) | Cache lookup key |
| `jobQueryForPrepare` | [adaptive_executor.c:3916](src/backend/distributed/executor/adaptive_executor.c#L3916), [3968](src/backend/distributed/executor/adaptive_executor.c#L3968) | Cache miss: deparse for PQprepare |
| `relationShardList` | [adaptive_executor.c:3970](src/backend/distributed/executor/adaptive_executor.c#L3970) | Cache miss: UpdateRelationToShardNames |
| `parametersInQueryStringResolved` | [adaptive_executor.c:4022](src/backend/distributed/executor/adaptive_executor.c#L4022) | Plain SQL path: determines param handling |
| `taskQuery` (.data) | [adaptive_executor.c:3906](src/backend/distributed/executor/adaptive_executor.c#L3906) via `TaskQueryStringAtIndex` | Plain SQL path: query string retrieval |
| `totalReceivedTupleData` | accumulated in `ReceiveResults()` | EXPLAIN ANALYZE stats |
| `partitionKeyValue` | not directly by executor, set by `SingleShardTaskList` | Passed through for metadata |
| `replicationModel` | not read by executor for READ_TASK | Only matters for `MODIFY_TASK` |

**Minimum for a cache-hit Task**: `taskType`, `anchorShardId`, `taskPlacementList`, `queryCount=1`, `preparedStatementPlanId`, `jobQueryForPrepare` (for fallback), `relationShardList` (for fallback), `parametersInQueryStringResolved=true`. The `taskQuery` field can be set to a lazy reference or left as `TASK_QUERY_NULL` if we ensure the caching path is always taken.

### Q8: FastPathRestrictionContext and plan-time data preservation

#### Definition at [distributed_planner.h:98-120](src/include/distributed/distributed_planner.h#L98-L120):

```c
typedef struct FastPathRestrictionContext
{
    bool fastPathRouterQuery;
    Const *distributionKeyValue;       // NULL when key is a Param
    bool distributionKeyHasParam;      // true when distKey = $N
    bool delayFastPathPlanning;
} FastPathRestrictionContext;
```

#### Where stored

Part of `PlannerRestrictionContext` at [distributed_planner.h:134](src/include/distributed/distributed_planner.h#L134):
```c
FastPathRestrictionContext *fastPathRestrictionContext;
```

This is a planner-time structure. It is allocated in the planner's `PlannerRestrictionContext` and is **NOT stored on the `DistributedPlan`**. It does not survive into execution time.

#### What IS available at execution time on DistributedPlan

- `distributedPlan->fastPathRouterPlan` (bool) — at [multi_physical_planner.h:496](src/include/distributed/multi_physical_planner.h#L496)
- `distributedPlan->targetRelationId` (Oid) — at [multi_physical_planner.h:444](src/include/distributed/multi_physical_planner.h#L444): set for DML targets
- `distributedPlan->relationIdList` (List of Oid) — at [multi_physical_planner.h:441](src/include/distributed/multi_physical_planner.h#L441): all accessed relations
- `workerJob->deferredPruning` (bool) — indicates params need resolving
- `workerJob->partitionKeyValue` (Const *) — NULL when deferred

#### Fields we need to preserve for the fast path

For the cache-hit bypass, we need to go from `ParamListInfo` → shard interval without the full query tree. Required plan-time data:

1. **Relation OID**: Available as `linitial_oid(distributedPlan->relationIdList)` for SELECT, or `distributedPlan->targetRelationId` for DML. From this we can get `GetCitusTableCacheEntry(relationId)`.
2. **Distribution column type info**: Available from `GetCitusTableCacheEntry(relationId)->partitionColumn` — this is cached metadata, not per-plan.
3. **Which Param index holds the distribution key**: **NOT currently stored**. The `FastPathRestrictionContext` knows `distributionKeyHasParam=true` but doesn't record the param index. At plan time, the code in `FastPathRouterQuery()` finds the distribution key restriction but doesn't preserve which `Param->paramid` it corresponds to.

**What needs to be added**: A new field on `DistributedPlan` or `Job` to store the distribution key's parameter index (e.g., `int distributionKeyParamId`). This would be set during planning when `FastPathRouterQuery()` identifies the `Param` node, and would allow the fast path to extract the correct value from `ParamListInfo` at execution time.

### Q9: savedJobQueryForCaching — Current optimization

#### Where set

**For SELECTs** — in `CitusBeginReadOnlyScan()` at [citus_custom_scan.c:341-348](src/backend/distributed/executor/citus_custom_scan.c#L341-L348):
```c
if (EnablePreparedStatementCaching)
{
    Job *origJob = originalDistributedPlan->workerJob;
    if (origJob->savedJobQueryForCaching == NULL)
    {
        MemoryContext oldCtx = MemoryContextSwitchTo(
            GetMemoryChunkContext(originalDistributedPlan));
        origJob->savedJobQueryForCaching = copyObject(jobQuery);
        MemoryContextSwitchTo(oldCtx);
    }
    savedJobQuery = origJob->savedJobQueryForCaching;
}
```

This is executed BEFORE `ExecuteCoordinatorEvaluableExpressions()` — so the saved query has **unevaluated Param nodes** (`$1`, `$2`, ...) still intact. Functions (for SELECT, no function evaluation happens anyway since `evaluationMode = EVALUATE_PARAMS` only). The copy is allocated in the original plan's memory context (long-lived, survives across executions).

**For DML** — in `CitusBeginModifyScan()` at [citus_custom_scan.c:453-457](src/backend/distributed/executor/citus_custom_scan.c#L453-L457):
```c
savedJobQuery = copyObject(jobQuery);
origJob->savedJobQueryForCaching = savedJobQuery;
```
Here, `ExecuteCoordinatorEvaluableFunctions()` has already been called — so the saved query has **evaluated functions** (e.g., `now()` resolved to a constant) but **unevaluated Param nodes** still intact. Note: for DML, this is refreshed every execution because volatile functions may produce different results.

#### Where used

At [citus_custom_scan.c:377](src/backend/distributed/executor/citus_custom_scan.c#L377) (SELECT) and [citus_custom_scan.c:497](src/backend/distributed/executor/citus_custom_scan.c#L497) (DML): assigned to `task->jobQueryForPrepare`.

#### Excluded from CopyDistributedPlanWithoutCache

At [citus_custom_scan.c:690-700](src/backend/distributed/executor/citus_custom_scan.c#L690-L700): set to NULL before `copyObject()`, then restored only on the original plan. The per-execution copy gets `savedJobQueryForCaching = NULL`. This avoids deep-copying the saved query on every execution.

#### Interaction with proposed fast path

The saved query is the **template for PQprepare** on cache miss. On a cache hit, `task->jobQueryForPrepare` is still set (as a fallback) but is never accessed — `SendNextQuery()` takes the cache-hit path directly to `SendRemotePreparedQuery()` without touching `jobQueryForPrepare`.

For the Phase 4 fast path, the `savedJobQueryForCaching` on the original plan is still needed for cache-miss fallback. But on a cache hit, we can set `task->jobQueryForPrepare` to the saved query pointer without any per-execution copy cost (it's already a pointer to the long-lived saved copy).

### Q10: Connection-level cache lookup feasibility

#### When is the connection assigned?

The connection is assigned inside `AssignTasksToConnectionsOrWorkerPool()` at [adaptive_executor.c:1433](src/backend/distributed/executor/adaptive_executor.c#L1433), which is called from `RunDistributedExecution()` at [adaptive_executor.c:1907](src/backend/distributed/executor/adaptive_executor.c#L1907). Specifically:

1. For tasks with prior transaction affinity: `GetConnectionIfPlacementAccessedInXact()` at [adaptive_executor.c:1519](src/backend/distributed/executor/adaptive_executor.c#L1519)
2. For regular tasks: connections are assigned lazily in `ManageWorkerPool()` → `OpenNewConnections()` → sessions pick up tasks from the `pendingTaskQueue`

The actual connection (`MultiConnection *`) is only known when `StartPlacementExecutionOnSession()` is called at [adaptive_executor.c:3821](src/backend/distributed/executor/adaptive_executor.c#L3821), which immediately calls `SendNextQuery()`.

#### Can we check the cache before entering the executor?

**No.** We cannot determine which specific `MultiConnection` will be used for a task until the connection is assigned inside the executor. For fast-path queries, there is typically one shard → one placement → one worker pool, but the specific cached connection from the `ConnectionHash` is selected by the connection management layer (`StartNodeUserDatabaseConnection` or `GetConnectionIfPlacementAccessedInXact`).

One theoretical approach: if we could predict the target connection (same worker, same session, likely same cached connection), we could look up the cache. But this requires reimplementing connection selection logic outside the executor, which is fragile and duplicative.

#### Minimum work from CitusBeginReadOnlyScan on cache hit

Even though we can't check the connection cache before the executor, we can still eliminate most of `CitusBeginReadOnlyScan`'s work. The minimum execution path is:

1. **Resolve distribution key param** → Const value (extract from `ParamListInfo` using stored param index)
2. **Find shard interval**: `GetCitusTableCacheEntry(relationId)` + `FindShardInterval(constvalue, cache)` — two hash lookups
3. **Get placement list**: `CreateTaskPlacementListForShardIntervals()` — metadata lookup
4. **Build relation-shard mapping**: `RelationShardListForShardIntervalList()` — lightweight list construction
5. **Build minimal Task**: `CreateTask(READ_TASK)` + populate the ~10 fields listed in Q5/Q7
6. **Set task on workerJob**: `workerJob->taskList = list_make1(task)`

**Skipped entirely**: `CopyDistributedPlanWithoutCache()`, `ExecuteCoordinatorEvaluableExpressions()`, `UpdateRelationToShardNames()`, `GenerateSingleShardRouterTaskList()`, lazy query string deparse.

The cache hit/miss determination still happens in `SendNextQuery()` inside the executor — but by then, all the expensive coordinator-side work has already been eliminated. On a true cache hit, `SendNextQuery()` goes directly to `SendRemotePreparedQuery()` (one libpq call). On a cache miss (first execution for this shard on this connection), the fallback path uses `task->jobQueryForPrepare` + `task->relationShardList` to construct the parameterized query template.

**Additional optimization for TaskQueryStringAtIndex**: The current code calls `TaskQueryStringAtIndex()` unconditionally at [adaptive_executor.c:3906](src/backend/distributed/executor/adaptive_executor.c#L3906) before checking the cache. For the fast path, if we don't set a `taskQuery` on the Task (or set it to `TASK_QUERY_NULL`), this would error. The fix is to either:
(a) Guard the call: skip `TaskQueryStringAtIndex` when `EnablePreparedStatementCaching && task->jobQueryForPrepare != NULL`, or
(b) Set a lazy-deparse reference so it only materializes if the plain SQL fallback is taken.

Option (a) is simpler and avoids the lazy deparse overhead entirely on cache hits.
