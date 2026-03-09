# Prepared Statement Caching POC — Implementation Plan

## Overview

Implement worker-side prepared statement plan caching for fast-path (single-shard) queries in Citus. When enabled, the coordinator uses `PQprepare`/`PQsendQueryPrepared` on worker connections instead of `PQsendQuery`/`PQsendQueryParams`, eliminating redundant parse/plan cycles on workers for repeated prepared statement executions.

## Current State Analysis

The adaptive executor sends all remote shard queries as SQL text via `PQsendQuery` or `PQsendQueryParams` — there is zero use of `PQprepare`/`PQexecPrepared` anywhere in the codebase ([adaptive_executor.c:3885-3950](src/backend/distributed/executor/adaptive_executor.c#L3885)).

For prepared statements (execution 6+, generic plan), the flow is:
1. `CitusBeginReadOnlyScan()`/`CitusBeginModifyScan()` evaluates all parameters into literal values via `ExecuteCoordinatorEvaluableExpressions()` ([citus_custom_scan.c:339](src/backend/distributed/executor/citus_custom_scan.c#L339))
2. Sets `parametersInJobQueryResolved = true` — signaling params are baked into query text
3. `RegenerateTaskForFasthPathQuery()` builds the shard-specific query with literal values
4. `SendNextQuery()` sends the literal-value SQL text via `PQsendQuery()`

The result: every execution sends the full query text with literal values, requiring the worker to parse and plan from scratch each time. There is no mechanism to reuse plans across executions.

Worker connections are cached per `(host, port, user, db)` in `ConnectionHash` ([connection_management.c:52](src/backend/distributed/connection/connection_management.c#L52)) with configurable lifetime (default 10 min). The `MultiConnection` struct ([connection_management.h:162](src/include/distributed/connection_management.h#L162)) manages these connections but has no field for a statement cache.

## Desired End State

When `citus.enable_prepared_statement_caching` is `ON` and a fast-path prepared statement enters the generic plan path (execution 6+):

- **First execution to a shard on a connection**: The coordinator sends `PQprepare()` with a parameterized query template (e.g., `SELECT ... FROM dist_table_102001 WHERE key = $1`) followed by `PQsendQueryPrepared()` with parameter values. The statement handle is cached per-connection.
- **Subsequent executions to the same shard on the same connection**: The coordinator skips the prepare step and directly sends `PQsendQueryPrepared()` with parameter values.
- **Connection drop/close**: All cached statement handles are invalidated naturally (per-connection lifecycle).
- **GUC disabled**: Behavior is identical to current Citus (SQL text sent every time).

Verification: A benchmark of 10,000 prepared fast-path query executions on a 3-node cluster shows measurable throughput improvement with caching enabled vs. disabled.

## What We're NOT Doing

- Multi-shard queries (queries touching more than one shard per execution)
- DDL invalidation of cached prepared statements
- Node addition/removal/rebalance invalidation
- Cross-connection prepared statement sharing
- Reference table or citus_local table queries
- External connection pooler compatibility (e.g., PgBouncer in transaction mode)
- Async prepare via `PQsendPrepare()` + state machine integration (the POC uses synchronous `PQprepare()` for first-time prepare; async optimization is a candidate)
- "Prepare after N executions" heuristic (prepare immediately on first encounter; optimization is a candidate)

## Phase Status

- [ ] **Phase 1: Statement Cache Infrastructure** — GUC toggle, per-connection cache data structure, MultiConnection integration, cleanup on connection close
- [ ] **Phase 2: Core Integration** — Parameterized query template preservation, `SendNextQuery()` prepare-or-execute logic, libpq wrapper functions
- [ ] **Phase 3: Tests & Documentation** — Regression tests, Docs.md, CHANGELOG

## Phase Candidates

- [ ] Async prepare integration: Replace synchronous `PQprepare()` with `PQsendPrepare()` + state machine changes for fully non-blocking first-execution path
- [ ] Prepare-after-N heuristic: Only prepare on worker after N executions (e.g., 3) to amortize prepare overhead for low-repeat queries

---

## Phase 1: Statement Cache Infrastructure

### Changes Required

- **`src/include/distributed/prepared_statement_cache.h`** (new file): Define the statement cache interface:
  - `PreparedStatementCacheEntry` struct: `{ char stmtName[64]; Oid *paramTypes; int paramCount; char *parameterizedQueryString; }` (the prepared statement handle on a connection; stores full query string for collision verification on cache hit)
  - Cache key: `{ uint32 queryHash; uint64 shardId; }` (32-bit hash via `hash_any()` of parameterized query template + target shard)
  - `PreparedStatementCacheCreate()` — allocate a new per-connection hash table
  - `PreparedStatementCacheLookup()` — look up a cache entry by key, verify `parameterizedQueryString` matches to guard against hash collisions
  - `PreparedStatementCacheInsert()` — insert a new entry, enforce 1,000-entry limit
  - `PreparedStatementCacheDestroy()` — free all memory
  - `extern bool EnablePreparedStatementCaching;` — GUC variable declaration

- **`src/backend/distributed/executor/prepared_statement_cache.c`** (new file): Implement the cache module:
  - Use PostgreSQL's `HTAB` (dynahash) for the cache, keyed by `(queryHash, shardId)`
  - On cache hit, compare `parameterizedQueryString` against stored string for collision safety; on mismatch, treat as miss and fall back to plain SQL for this query
  - Statement name counter per cache: `__citus_stmt_<N>` where N increments monotonically per connection
  - `PreparedStatementCacheInsert()` returns NULL when at 1,000-entry limit (stop caching, fall back to plain SQL)
  - Allocate cache in `TopMemoryContext` (connection-scoped, survives transactions)

- **`src/include/distributed/connection_management.h`**: Add `HTAB *preparedStatementCache` field to `MultiConnection` struct (after existing fields, around line 230)

- **`src/backend/distributed/connection/connection_management.c`**: 
  - In `StartNodeUserDatabaseConnection()`: initialize `preparedStatementCache = NULL` on new connections (lazy allocation)
  - In `ShutdownConnection()` / `CitusPQFinish()`: call `PreparedStatementCacheDestroy()` if non-NULL before closing the `PGconn`

- **`src/backend/distributed/shared_library_init.c`**: Register GUC `citus.enable_prepared_statement_caching`:
  - Insert alphabetically between `citus.enable_non_colocated_router_query_pushdown` and `citus.enable_repartition_joins`
  - Default: `false`, context: `PGC_USERSET`, flags: `GUC_STANDARD`
  - Variable: `EnablePreparedStatementCaching` (declared in `prepared_statement_cache.h`, defined in `prepared_statement_cache.c`)

- **`src/backend/distributed/Makefile`**: Add `executor/prepared_statement_cache.o` to the build

### Success Criteria

#### Automated Verification:
- [ ] `make -j$(nproc)` compiles without errors
- [ ] `SHOW citus.enable_prepared_statement_caching;` returns `off`
- [ ] `SET citus.enable_prepared_statement_caching = on;` succeeds
- [ ] `ci/check_gucs_are_alphabetically_sorted.sh` passes

#### Manual Verification:
- [ ] New GUC appears in `pg_settings` with correct description and default
- [ ] Creating and destroying connections does not leak memory

---

## Phase 2: Core Integration

### Changes Required

- **`src/include/distributed/citus_clauses.h`**: Add `EVALUATE_FUNCTIONS` to the `CoordinatorEvaluationMode` enum — evaluates function calls (volatile/stable) but leaves `Param` nodes untouched. This is needed so that for INSERT/UPDATE/DELETE, we can evaluate coordinator-side functions (`now()`, `nextval()`, etc.) while preserving parameter placeholders for the prepared statement template.

- **`src/backend/distributed/utils/citus_clauses.c`**: In `PartiallyEvaluateExpression()`, gate the `T_Param` evaluation: skip resolving Param nodes when mode is `EVALUATE_FUNCTIONS`. Update `ShouldEvaluateFunctions()` to return `true` for `EVALUATE_FUNCTIONS`. Add a new helper `ExecuteCoordinatorEvaluableFunctions(Query *query, PlanState *planState)` that calls `PartiallyEvaluateExpression` with `EVALUATE_FUNCTIONS` mode.

- **`src/include/distributed/multi_physical_planner.h`**: Add `char *parameterizedQueryString` field to the `Task` struct (around line 330, near `parametersInQueryStringResolved`) to hold the parameterized shard query template (with `$1`, `$2` placeholders)

- **`src/backend/distributed/executor/citus_custom_scan.c`**: In `CitusBeginReadOnlyScan()` (line ~310) and `CitusBeginModifyScan()` (line ~390), when `EnablePreparedStatementCaching && workerJob->deferredPruning`:

  **For SELECT** (`CitusBeginReadOnlyScan`):
  - Before `ExecuteCoordinatorEvaluableExpressions()`: save `savedJobQuery = copyObject(jobQuery)` (deep copy preserving Param nodes; safe because SELECTs use `EVALUATE_PARAMS` mode which only resolves params, no functions)
  - After `RegenerateTaskForFasthPathQuery()`: for each task in `workerJob->taskList`:
    - Apply `UpdateRelationToShardNames((Node *)savedJobQuery, task->relationShardList)` to substitute table→shard names in the saved copy
    - Deparse `savedJobQuery` via `pg_get_query_def()` to produce the parameterized shard query (e.g., `SELECT col FROM dist_table_102008 WHERE key = $1`)
    - Copy resulting string to `ExecutorState` memory context (survives until `SendNextQuery()`)
    - Assign to `task->parameterizedQueryString`

  **For INSERT/UPDATE/DELETE** (`CitusBeginModifyScan`):
  - Call `ExecuteCoordinatorEvaluableFunctions(jobQuery, planState)` first (evaluates functions but preserves Param nodes)
  - Save `savedJobQuery = copyObject(jobQuery)` (deep copy with functions evaluated, Param nodes intact)
  - Then call `PartiallyEvaluateExpression()` with `EVALUATE_PARAMS` on jobQuery to resolve remaining params (equivalent to the original `ExecuteCoordinatorEvaluableExpressions` flow)
  - Set `parametersInJobQueryResolved = true`
  - After `RegenerateTaskForFasthPathQuery()` / `RegenerateTaskListForInsert()`: same template construction as SELECT path above
  - Note: `CitusBeginModifyScan` uses a `localContext` memory context — ensure `parameterizedQueryString` is allocated in the `ExecutorState` context so it survives until `SendNextQuery()`

  `parametersInJobQueryResolved` is still set to `true` on the workerJob (normal flow), but the Task now carries the parameterized template alongside the resolved query string.

- **`src/backend/distributed/connection/remote_commands.c`**: Add two new libpq wrapper functions:
  - `SendRemotePrepare(MultiConnection *connection, const char *stmtName, const char *query, int nParams, Oid *paramTypes)` — wraps synchronous `PQprepare()`, returns success/failure, logs via `LogRemoteCommand()`
  - `SendRemotePreparedQuery(MultiConnection *connection, const char *stmtName, int nParams, const char **paramValues, bool binaryResults)` — wraps async `PQsendQueryPrepared()`, returns success/failure, logs via `LogRemoteCommand()`, consistent with existing `SendRemoteCommandParams()` pattern

- **`src/include/distributed/remote_commands.h`**: Declare the two new wrapper functions

- **`src/backend/distributed/executor/adaptive_executor.c`**: Modify `SendNextQuery()` (line ~3885) to add a third execution path when `EnablePreparedStatementCaching` is on and `task->parameterizedQueryString` is not NULL:
  1. Force-evaluate lazy params via `copyParamList(paramListInfo)` (required before `ExtractParametersForRemoteExecution` — matches existing pattern at line ~3906)
  2. Extract parameters via `ExtractParametersForRemoteExecution()` (reuse existing logic)
  3. Compute cache key from `hash_any(task->parameterizedQueryString, strlen(...))` + `task->anchorShardId`
  4. Look up in `connection->preparedStatementCache` (lazy-create cache if NULL)
  5. **Cache miss**: Call `SendRemotePrepare()` synchronously to prepare the statement. Insert into cache (with full `parameterizedQueryString` for collision verification). Then call `SendRemotePreparedQuery()` asynchronously.
  6. **Cache hit** (verified by string comparison): Call `SendRemotePreparedQuery()` asynchronously with the cached statement name.
  7. Continue with `PQsetSingleRowMode()` as before.
  
  The new path takes priority over the existing two branches. When the GUC is off or `parameterizedQueryString` is NULL, fall through to existing behavior unchanged.

### Success Criteria

#### Automated Verification:
- [ ] `make -j$(nproc)` compiles without errors
- [ ] Existing regression tests pass: `make -C src/test/regress check`

#### Manual Verification:
- [ ] With GUC ON: `PREPARE stmt AS SELECT * FROM dist_table WHERE key = $1; EXECUTE stmt(1); EXECUTE stmt(2); ... EXECUTE stmt(10);` — executions 7+ use cached prepared statements on workers (verify via `log_min_messages = DEBUG4` or adding logging to the new code path)
- [ ] With GUC OFF: same prepared statement series uses plain SQL text (current behavior)
- [ ] Executing with different shard targets reuses cache per-shard independently
- [ ] Losing and re-establishing a worker connection transparently re-prepares

---

## Phase 3: Tests & Documentation

### Changes Required

- **`src/test/regress/sql/prepared_statement_caching.sql`** (new test file): Regression tests:
  - Test 1: GUC toggle — verify default is OFF, SET to ON succeeds
  - Test 2: Basic caching — PREPARE a single-shard SELECT, EXECUTE 10 times with GUC ON, verify correct results
  - Test 3: Multi-shard-value — EXECUTE with different partition key values routing to different shards, verify each returns correct results
  - Test 4: INSERT/UPDATE/DELETE — verify prepared INSERTs, UPDATEs, DELETEs work with caching ON (including queries with `now()` and stable functions to verify coordinator-side evaluation)
  - Test 5: GUC OFF baseline — same queries with GUC OFF produce identical results
  - Test 6: Multiple prepared statements in same session — verify independent caching

- **`src/test/regress/expected/prepared_statement_caching.out`** (new file): Expected output for the regression test

- **`src/test/regress/citus_tests/run_test.py`** or equivalent test schedule: Register the new test (verify exact registration mechanism during implementation)

- **`.paw/work/prepared-stmts-poc/Docs.md`**: Technical reference (load `paw-docs-guidance`). Include:
  - Note about `MaxCachedConnectionsPerWorker` (default 1) affecting cache hit rates
  - Note about benchmarks being run manually outside implementation phases

- **`CHANGELOG.md`**: Add entry for the new feature under the development version section

Note: Benchmark execution for SC-001 and SC-002 (spec success criteria) is performed manually outside the implementation phases, using the existing benchmark infrastructure in `bench_results/`. The regression tests verify correctness, not performance.

### Success Criteria

#### Automated Verification:
- [ ] New test passes: `make -C src/test/regress check EXTRA_TESTS=prepared_statement_caching`
- [ ] All existing prepared statement tests still pass
- [ ] Full regression suite passes

#### Manual Verification:
- [ ] CHANGELOG entry follows project conventions
- [ ] Docs.md accurately describes the feature, its limitations, and verification approach

---

## References

- Spec: `.paw/work/prepared-stmts-poc/Spec.md`
- Research: `.paw/work/prepared-stmts-poc/SpecResearch.md`, `.paw/work/prepared-stmts-poc/CodeResearch.md`
- PostgreSQL PQprepare: https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQPREPARE
- PostgreSQL PQexecPrepared: https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQEXECPREPARED
