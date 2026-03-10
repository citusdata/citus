# Prepared Statement Caching POC

## Overview

Worker-side prepared statement plan caching for fast-path (single-shard) queries in Citus. When enabled via `citus.enable_prepared_statement_caching`, the coordinator uses `PQprepare`/`PQsendQueryPrepared` on worker connections instead of `PQsendQuery`/`PQsendQueryParams`, eliminating redundant parse/plan cycles on workers for repeated prepared statement executions.

Without this feature, every execution of a prepared statement sends full SQL text to workers, causing each worker to parse and plan from scratch each time — negating the primary benefit of prepared statements in distributed deployments.

## Architecture and Design

### How It Works

1. **First execution to a shard on a connection**: The coordinator sends a synchronous `PQprepare()` with a parameterized query template (e.g., `SELECT ... FROM dist_table_102001 WHERE key = $1`), followed by an async `PQsendQueryPrepared()` with parameter values. The statement handle is cached per-connection.

2. **Subsequent executions to the same shard**: The coordinator skips the prepare step and directly sends `PQsendQueryPrepared()` with parameter values — no parse/plan overhead on the worker.

3. **Connection drop/close**: All cached statement handles are invalidated naturally (per-connection lifecycle). The next execution transparently re-prepares.

### Key Components

- **Per-connection statement cache** (`PreparedStatementCacheEntry` in `prepared_statement_cache.h`): A hash table on each `MultiConnection`, keyed by `(planId, shardId)`. The `planId` uniquely identifies a cached generic plan; the `shardId` identifies the target shard.

- **Parameterized query preservation**: During `CitusBeginScan`, a deep copy of the job query is saved with `Param` nodes intact (before parameter resolution). This copy is used on cache miss to construct the `PQprepare()` template by substituting table→shard names and deparsing.

- **Coordinator-side function evaluation**: For INSERT/UPDATE/DELETE, functions like `now()` and `nextval()` are evaluated on the coordinator before the query template is built, via a new `EVALUATE_FUNCTIONS` mode in `citus_clauses.c`. Parameter placeholders (`$1`, `$2`, etc.) remain intact.

- **SendNextQuery integration**: A new code path at the top of `SendNextQuery()` in `adaptive_executor.c` checks `EnablePreparedStatementCaching && task->jobQueryForPrepare != NULL`. On cache hit, it sends `PQsendQueryPrepared`; on cache miss, it constructs the template, calls `PQprepare`, caches, then executes.

### Design Decisions

- **Synchronous `PQprepare()`**: The POC uses synchronous `PQprepare()` for first-time prepare, which blocks the adaptive executor's event loop for one network round-trip. Async `PQsendPrepare()` + state machine integration is a candidate for future optimization.

- **No eviction policy**: The per-connection cache has a hard limit of 1,000 statements. Once full, new statements fall back to plain SQL. No LRU or other eviction — acceptable for POC workloads.

- **Cache key simplicity**: Uses `(planId, shardId)` where `planId` is a monotonically incrementing counter. Production may need query-content hashing for plan invalidation/recreation scenarios.

## User Guide

### Enabling the Feature

```sql
SET citus.enable_prepared_statement_caching = on;
```

Default is `off`. Can be set per-session (`PGC_USERSET`).

### Basic Usage

```sql
-- Prepare a single-shard query
PREPARE my_query(int) AS SELECT * FROM dist_table WHERE id = $1;

-- First 5 executions use custom plans (PostgreSQL behavior)
EXECUTE my_query(1);
EXECUTE my_query(2);
-- ...

-- Execution 6+ uses generic plan → triggers worker-side caching
EXECUTE my_query(6);  -- PQprepare + PQsendQueryPrepared (cache miss)
EXECUTE my_query(7);  -- PQsendQueryPrepared only (cache hit)
```

Works with SELECT, INSERT, UPDATE, and DELETE on distributed tables with fast-path (single-shard) routing.

### Important Notes

- **`citus.max_cached_connections_per_worker`** (default 1): Controls how many worker connections are cached between transactions. With the default of 1, a single cached connection per worker retains its statement cache across transactions. Increasing this value means more connections may be cached, but the same connection must be reused for cache hits.

- **Connection lifetime**: `citus.max_cached_connection_lifetime` (default 10 min) controls how long cached connections survive. When a connection expires and a new one is established, statements are re-prepared transparently on first use.

- **Benchmarks**: Performance benchmarks are run manually outside the implementation workflow, using infrastructure in `bench_results/`. The regression tests verify correctness, not performance.

## Configuration Options

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `citus.enable_prepared_statement_caching` | bool | `off` | Enable worker-side prepared statement plan caching for fast-path queries |

## Testing

### How to Test

Run the regression test:
```bash
cd src/test/regress
python3 citus_tests/run_test.py prepared_statement_caching
```

The test covers: GUC toggle, basic SELECT caching, multi-shard-value routing, INSERT/UPDATE/DELETE with function evaluation, GUC OFF baseline, multiple concurrent prepared statements, and connection loss re-prepare.

### Edge Cases

- **Connection loss**: When a worker connection drops, the statement cache is destroyed with it. The next execution gets a new connection and transparently re-prepares. Tested via `citus.max_cached_connection_lifetime = '0s'`.
- **Cache full**: When a connection's cache reaches 1,000 statements, new statements fall back to plain SQL (no error, no eviction).
- **Multi-shard values**: Different parameter values routing to different shards each get their own cache entry. First execution to each shard incurs a prepare; subsequent executions are cache hits.

## Limitations and Future Work

### Current Limitations (POC)
- Fast-path (single-shard) queries only — multi-shard queries are unaffected
- No DDL invalidation of cached prepared statements
- No invalidation on node addition/removal/rebalance
- Synchronous `PQprepare()` blocks the event loop on first-time prepare per shard
- No cache eviction — hard limit of 1,000 statements per connection, then fallback to plain SQL
- No external connection pooler compatibility consideration (e.g., PgBouncer in transaction mode)

### Phase Candidates
- **Async prepare**: Replace `PQprepare()` with `PQsendPrepare()` + state machine changes
- **Prepare-after-N heuristic**: Only prepare on worker after N executions to amortize overhead
