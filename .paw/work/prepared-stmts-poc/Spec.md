# Feature Specification: Prepared Statements POC

**Branch**: colm/prepared-stmts-poc  |  **Created**: 2026-03-06  |  **Status**: Draft
**Input Brief**: Cache prepared statement plans on remote shards so distributed queries perform as well as local ones.

## Overview

Prepared statements in PostgreSQL allow applications to parse and plan a query once, then execute it multiple times with different parameter values — avoiding repeated parsing and planning overhead. In a single-node PostgreSQL deployment, this optimization works as designed: after a few executions, the plan is cached and subsequent executions skip parsing and planning entirely.

In Citus, however, this benefit degrades as the number of worker nodes increases. While the coordinator caches the distributed plan after several executions (leveraging PostgreSQL's generic plan mechanism), the actual shard queries sent to worker nodes are always transmitted as SQL text. Each worker parses and plans the query from scratch on every execution. For a cluster with N workers, this means N redundant parse/plan cycles per query execution — exactly the overhead that prepared statements are designed to eliminate.

This POC aims to demonstrate that prepared statements on distributed tables can be equally effective both locally and remotely. By enabling worker-side statement caching on shard connections, remote shards should cache query plans just as local shards do. The primary goal is to produce a working prototype that can be benchmarked to determine whether the performance improvement justifies pursuing a production-quality implementation.

To reach a benchmarkable prototype quickly, the POC deliberately narrows scope to fast-path queries (single-shard) and defers complex invalidation scenarios (DDL changes, node topology changes) that would be required for production but are not needed for performance measurement.

## Objectives

- Eliminate redundant parse/plan cycles on worker nodes for repeated prepared statement executions
- Achieve prepared statement performance on distributed tables comparable to single-node PostgreSQL
- Produce a benchmarkable prototype that measures the real-world performance delta
- Validate whether worker-side plan caching justifies investment in production-quality invalidation handling

## User Scenarios & Testing

### User Story P1 – Fast-Path Prepared Statement Performance

Narrative: A developer PREPAREs a single-shard SELECT/INSERT/UPDATE/DELETE on a distributed table and EXECUTEs it many times within one session. After the initial executions, remote shard queries should use cached plans rather than re-parsing and re-planning each time.

Independent Test: Compare execution latency of the 100th EXECUTE of a prepared single-shard SELECT against the same query run via unprepared repeated execution — the prepared version should show at least 15% lower latency.

Acceptance Scenarios:
1. Given a prepared SELECT with a distribution key parameter on a distributed table, When executed 100 times with different parameter values targeting the same shard, Then worker-side parse/plan overhead is incurred only on the first execution.
2. Given a prepared INSERT with a distribution key value, When executed repeatedly within the same session, Then the worker connection reuses the cached plan for all subsequent executions.
3. Given a prepared UPDATE/DELETE with a distribution key filter, When executed repeatedly, Then worker-side plan caching applies identically to SELECT.

### User Story P2 – Multi-Shard-Value Prepared Statement

Narrative: A developer EXECUTEs a prepared statement with different parameter values that route to different shards across multiple workers. Each worker caches the plan independently.

Independent Test: Execute a prepared statement 50 times with parameter values distributed across all shards — each worker's first execution parses/plans, subsequent executions to the same shard reuse the cached plan.

Acceptance Scenarios:
1. Given a prepared SELECT on a distributed table, When executed with parameter values routing to 3 different shards on 3 different workers, Then each worker independently caches the plan after its first execution.
2. Given a session with multiple prepared statements, When each targets different shards, Then plan caching works independently per statement per worker connection.

### User Story P3 – Benchmarkable Output

Narrative: A performance engineer runs a benchmark comparing prepared statement execution with and without worker-side plan caching to quantify the improvement.

Independent Test: Run a pgbench-style workload of 10,000 prepared statement executions on a 3-node cluster and compare throughput (TPS) with caching enabled vs disabled.

Acceptance Scenarios:
1. Given a configurable toggle for worker-side prepared statement caching, When the toggle is off, Then behavior matches current Citus (SQL text sent every time).
2. Given the toggle is on, When running a sustained prepared statement workload, Then throughput improves measurably compared to toggle-off.

### Edge Cases

- Connection to a worker drops and is re-established mid-session: prepared statements on that worker must be re-prepared on the new connection (handled by PostgreSQL's per-connection semantics).
- A prepared statement is executed with a parameter value that routes to a shard not previously targeted: the first execution to that shard incurs parse/plan; subsequent executions to the same shard are cached.
- Multiple distinct prepared statements in the same session: each maintains its own independent plan cache per worker connection.

## Requirements

### Functional Requirements

- FR-001: For fast-path prepared statements, cache and reuse query plans on worker connections instead of re-parsing and re-planning on every execution. (Stories: P1, P2)
- FR-002: Maintain a statement cache per worker connection, keyed by query identity and target shard. (Stories: P1, P2)
- FR-003: On first execution to a given shard, prepare the statement on the worker. On subsequent executions to the same shard on the same connection, reuse the cached plan with new parameters. (Stories: P1, P2)
- FR-004: Provide a runtime configuration toggle to enable/disable worker-side statement caching, defaulting to disabled. (Stories: P3)
- FR-005: When a worker connection is closed or lost, all associated cached statements are implicitly invalidated (per-connection lifecycle). (Stories: P1)

### Key Entities

- **Cached Statement**: A per-connection, per-query plan on a worker node that allows executing a previously planned query with new parameters without re-parsing.
- **Fast-Path Query**: A single-shard query with an equality condition on the distribution key that qualifies for Citus's fast-path planner.

### Cross-Cutting / Non-Functional

- The feature must not degrade performance for non-prepared-statement workloads or when the toggle is disabled.
- Per-connection cached statement count should be bounded (max 1,000 statements per connection) to prevent unbounded memory growth.

## Success Criteria

- SC-001: Prepared fast-path queries on a 3-node cluster achieve at least 80% of the throughput of equivalent single-node prepared queries for the same workload. (FR-001, FR-003)
- SC-002: A benchmark of 10,000 prepared statement executions shows at least 10% throughput improvement (TPS) with caching enabled vs disabled. (FR-001, FR-004)
- SC-003: When the GUC is disabled, query behavior and performance are identical to current Citus. (FR-004)
- SC-004: Losing and re-establishing a worker connection does not cause errors — statements are transparently re-prepared. (FR-005)

## Assumptions

- **Connection stability**: Worker connections persist for the duration of a session (default `MaxCachedConnectionLifetime = 10 minutes`), providing sufficient reuse window for cached statements.
- **DDL invariance**: No DDL changes occur during benchmarks. DDL invalidation is out of scope for this POC.
- **Topology invariance**: No node additions, removals, or shard rebalancing during benchmarks. Topology-change invalidation is out of scope.
- **Per-connection lifecycle**: PostgreSQL's per-connection prepared statement lifecycle is adequate — no cross-connection sharing needed.
- **Fast-path sufficiency**: Fast-path queries (single-shard, distribution-key equality) are representative of the performance-critical workload this optimization targets. (Per Research Finding #2: fast-path queries are implementation-simple and performance-critical, making them the ideal POC scope.)
- **Connection persistence**: Worker connections default to a 10-minute cache lifetime within a session, providing adequate window for cached statement reuse. (Per Research Finding #4.)

## Scope

In Scope:
- Fast-path (single-shard) prepared statement plan caching on workers
- SELECT, INSERT, UPDATE, DELETE on distributed tables
- GUC toggle for enabling/disabling the feature
- Benchmark comparison of throughput with and without caching

Out of Scope:
- Multi-shard queries (queries touching more than one shard per execution)
- DDL invalidation of cached prepared statements
- Node addition/removal/rebalance invalidation
- Cross-connection prepared statement sharing
- Reference table or citus_local table queries
- External connection pooler compatibility (e.g., PgBouncer in transaction mode)

## Design Decisions Deferred to Implementation

- **Statement naming scheme**: How cached statements are identified on workers (e.g., hash-based naming). To be resolved during code research.
- **Parameter type resolution**: How parameter types are extracted from the query tree at dispatch time. To be resolved during code research.

## Unresolved External Questions

- [ ] Are there existing benchmarks or performance measurements for prepared statements on multi-node Citus clusters?
- [ ] Are there known issues or prior attempts to address this problem in the Citus codebase?

## Dependencies

- Existing Citus connection management infrastructure (connection caching per worker)
- Existing fast-path planner and deferred pruning mechanism
- PostgreSQL's prepared statement protocol support on worker connections

## Risks & Mitigations

- **Performance improvement may be negligible**: If worker parse/plan is a small fraction of total query time (network latency dominates), the benefit may not justify the complexity. Mitigation: POC benchmarks on low-latency cluster to isolate the effect.
- **Statement cache memory growth**: Many distinct prepared statements could accumulate handles. Mitigation: Bound cache size per connection; rely on connection lifetime for natural cleanup.
- **Generic plan quality**: PostgreSQL's generic plans may be worse than custom plans for some queries (e.g., skewed data distributions). Mitigation: Use extended protocol parameters to allow worker-side custom plans.
- **Parameter type resolution**: Worker-side preparation requires parameter type information. Mitigation: Extract from the existing query tree parameter information during code research.

## References

- Research: .paw/work/prepared-stmts-poc/SpecResearch.md
- PostgreSQL Prepared Statements: https://www.postgresql.org/docs/current/sql-prepare.html
- Citus Distributed Planning: src/backend/distributed/README.md
