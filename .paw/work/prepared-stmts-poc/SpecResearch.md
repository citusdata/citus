---
date: 2026-03-06 12:00:00 UTC
git_commit: 3a1a34d2f
branch: colm/prepared-stmts-poc
repository: citus
topic: "Prepared Statements POC Spec Research"
tags: [research, specification]
status: complete
---

# Spec Research: Prepared Statements POC

## Summary

Citus currently sends all remote shard queries as SQL text via `PQsendQuery`/`PQsendQueryParams`, meaning every execution requires the worker to re-parse and re-plan the query. There is zero use of libpq's prepared statement protocol (`PQprepare`/`PQexecPrepared`) anywhere in the remote execution path. For fast-path queries (single-shard, distribution-key equality), the coordinator-side plan is cached after 6 executions via PostgreSQL's generic plan mechanism, but the worker side always receives fresh SQL text. Connection caching exists (per host/port/user/db, default 1 per worker, 10-minute lifetime) and persists within a session, providing a natural vehicle for statement handle reuse.

## Research Findings

### Question 1: How does Citus currently handle prepared statements from the user's perspective?

**Answer**: When a user PREPAREs and EXECUTEs a statement on a distributed table: (1) First 6 executions call the distributed planner to create a new DistributedPlan each time. (2) 7th+ execution: PostgreSQL caches the plan (if generic plan cost acceptable) and Citus planner is NOT called. (3) For fast-path queries with deferred pruning, shard pruning happens at execution time. (4) Remote shards receive deparsed SQL text, parsing and planning it fresh every time.

**Evidence**: src/backend/distributed/README.md documents "limited prepared statement support" — they functionally work but plans are only meaningfully cached in a few cases. citus_custom_scan.c tracks `numberOfTimesExecuted`.

**Implications**: The coordinator-side caching (after 6 executions) is adequate for fast-path queries. The worker-side re-planning is the core problem — every remote execution re-parses and re-plans.

### Question 2: What is a "fast-path" query in Citus?

**Answer**: Fast-path queries are trivial, performance-critical routable queries that skip `standard_planner()`. Criteria: (1) Single distributed table. (2) Equality condition on distribution column. (3) Allowed: GROUP BY, WINDOW, ORDER BY, HAVING, DISTINCT, LIMIT. (4) Prohibited: subqueries, CTEs, joins (except reference/local tables). (5) Types: SELECT, INSERT, UPDATE, DELETE.

**Evidence**: fast_path_router_planner.c implements `FastPathRouterQuery()` for eligibility and `FastPathPlanner()` which skips standard_planner.

**Implications**: Fast-path queries are the ideal POC scope — they target a single shard, making prepared statement caching straightforward (one statement per shard per connection).

### Question 3: How does Citus send queries to worker/remote shards?

**Answer**: Citus uses the extended query protocol primarily (`PQsendQueryParams` via `SendRemoteCommandParams()`) with fallback to simple protocol (`PQsendQuery` via `SendRemoteCommand()`). Queries are deparsed from DistributedPlan to SQL text via `RebuildQueryStrings()` before sending. There is zero use of `PQprepare`/`PQexecPrepared`.

**Evidence**: remote_commands.c contains only `SendRemoteCommand` and `SendRemoteCommandParams` wrappers. Grep for PQprepare/PQexecPrepared returns 0 matches.

**Implications**: POC must add `PQprepare`/`PQexecPrepared` capability and integrate it with the existing task execution pipeline.

### Question 4: How does Citus manage connections to worker nodes?

**Answer**: Connections are persistently cached per (host, port, user, database) tuple. Default: `MaxCachedConnectionsPerWorker = 1` per worker, `MaxCachedConnectionLifetime = 10 minutes`. Connections are lazy-established, cached across queries within a session, and cleaned up at transaction end. Transaction affinity ensures the same connection is reused for the same shard within a transaction.

**Evidence**: connection_management.c uses a `ConnectionHash` HTAB indexed by `ConnectionHashKey`. adaptive_executor.c documents WorkerPool maintaining per-worker connection lists.

**Implications**: Connection persistence within sessions enables statement handle reuse. Statement cache should be tied to connection identity. The 10-minute lifetime provides natural invalidation.

### Question 5: Is any remote caching done currently for repeated executions?

**Answer**: **No.** Workers always receive plain SQL text and parse/plan on every execution. The coordinator calls `RebuildQueryStrings()` on every execution, deparsing the query tree and sending the result as text. No statement handles or plan caches exist on the worker side.

**Evidence**: RebuildQueryStrings is called before every task execution. README.md confirms "limited prepared statement support."

**Implications**: Worker-side plan caching is completely absent — this is the entire opportunity for the POC.

### Question 6: Current fast-path prepared statement execution flow

**Answer**: The flow is:
1. EXECUTE → PostgreSQL ExecutorRun hook → `CitusExecutorRun()`
2. For 7th+ execution: use cached plan. For 1st-6th: distributed_planner → FastPathPlanner
3. `CitusBeginScan()` → deferred pruning → `TargetShardIntervalForFastPathQuery()` → determines shard
4. `RebuildQueryStrings()` — EVERY EXECUTION — deparses query, substitutes shard names, binds params
5. `AdaptiveExecutor()` → `ExecuteTaskListExtended()`
6. `GetConnection()` → `SendRemoteCommandParams(connection, taskQuery)` → PQsendQueryParams
7. `GetRemoteCommandResult()` → fetch results → return to coordinator

**Evidence**: citus_custom_scan.c, deparse_shard_query.c, adaptive_executor.c, remote_commands.c

**Implications**: Key intervention point is between steps 4 and 6 — after deparsing produces SQL text but before sending. POC can check if a prepared handle exists for this query on this connection, and use PQexecPrepared instead of PQsendQueryParams.

### Question 7: Does Citus use PQprepare/PQexecPrepared anywhere?

**Answer**: **No.** Zero matches in the entire distributed codebase.

**Evidence**: Grep search returns 0 matches. remote_commands.c only exports PQsendQuery and PQsendQueryParams wrappers.

**Implications**: POC must implement prepared statement protocol support from scratch.

### Question 8: PostgreSQL generic vs custom plan caching and Citus interaction

**Answer**: PostgreSQL caches plans after 5 executions (generic plan if cost acceptable). Citus integrates coordinator-side: distributed planner is called for first 6 executions, then plan cached. Worker-side: no integration — workers receive SQL text, so PostgreSQL's prepared statement cache on workers is never engaged.

**Evidence**: README.md documents the 6-execution threshold behavior. citus_custom_scan.c tracks `numberOfTimesExecuted`.

**Implications**: Coordinator-side caching is already adequate for fast-path queries. The POC only needs to add worker-side caching via the prepared statement protocol.

## Open Unknowns

- **Statement naming scheme**: How should prepared statements be named on workers? Need unique names per (query, shard) combination per connection. Likely solvable with hash-based naming.
- **Parameter type handling**: PQprepare requires OID arrays for parameter types. Need to determine how Citus tracks/resolves these — likely available from the query tree.

## User-Provided External Knowledge (Manual Fill)

- [ ] Are there any existing benchmarks or performance measurements for prepared statements on Citus clusters of varying sizes?
- [ ] Are there any known issues or prior attempts to address this problem?
