--
-- SINGLE_TASK_EXECUTION
--
-- Validates the single-task (one-task) adaptive executor:
--   1. Produces identical results to the full adaptive executor
--   2. Shows "Citus Single Task" in EXPLAIN plans
--   3. Handles single-row and multi-row results
--   4. Covers simple and complex fast-path queries
--
CREATE SCHEMA single_task_execution;
SET search_path TO single_task_execution;
SET citus.next_shard_id TO 99900000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- Create test tables
CREATE TABLE kv (key int PRIMARY KEY, value text, num int);
SELECT create_distributed_table('kv', 'key');

CREATE TABLE kv_multi (key int, seq int, payload text, PRIMARY KEY (key, seq));
SELECT create_distributed_table('kv_multi', 'key');

-- Load test data: single-row per key in kv, multiple rows per key in kv_multi
INSERT INTO kv SELECT g, 'val_' || g, g * 10 FROM generate_series(1, 100) g;
INSERT INTO kv_multi SELECT g / 10, g, repeat('x', g % 50 + 1) FROM generate_series(1, 200) g;

-- ============================================================
-- PART 1: EXPLAIN validation - confirm plan uses Single Task executor
-- ============================================================

-- Simple point SELECT
EXPLAIN (COSTS OFF) SELECT * FROM kv WHERE key = 1;

-- Point UPDATE
EXPLAIN (COSTS OFF) UPDATE kv SET value = 'updated' WHERE key = 1;

-- Point DELETE
EXPLAIN (COSTS OFF) DELETE FROM kv WHERE key = 1;

-- INSERT with ON CONFLICT
EXPLAIN (COSTS OFF) INSERT INTO kv (key, value, num) VALUES (1, 'x', 0)
  ON CONFLICT (key) DO UPDATE SET num = kv.num + 1;

-- SELECT with expression in target list
EXPLAIN (COSTS OFF) SELECT key, upper(value), num * 2 AS doubled FROM kv WHERE key = 42;

-- SELECT with LIMIT (still single-shard, should use single-task)
EXPLAIN (COSTS OFF) SELECT * FROM kv WHERE key = 5 LIMIT 1;

-- Multi-row per shard key: point lookup with multiple results
EXPLAIN (COSTS OFF) SELECT * FROM kv_multi WHERE key = 5 ORDER BY seq;

-- ============================================================
-- PART 2: Results equivalence - ON vs OFF must produce same results
-- ============================================================

-- Run queries with single-task ON and capture results
SET citus.enable_single_task_execution TO on;

-- Single-row point lookups
SELECT * FROM kv WHERE key = 1;
SELECT * FROM kv WHERE key = 50;
SELECT * FROM kv WHERE key = 99;

-- Point lookup with expressions
SELECT key, upper(value) AS uval, num + 100 AS shifted FROM kv WHERE key = 42;

-- Multi-row: all rows for a shard key value
SELECT * FROM kv_multi WHERE key = 5 ORDER BY seq;

-- Multi-row with aggregate
SELECT key, count(*), max(length(payload)) FROM kv_multi WHERE key = 10 GROUP BY key;

-- UPDATE RETURNING
UPDATE kv SET num = num + 1 WHERE key = 7 RETURNING key, num;

-- INSERT ON CONFLICT RETURNING
INSERT INTO kv (key, value, num) VALUES (7, 'conflict', 999)
  ON CONFLICT (key) DO UPDATE SET num = kv.num + 1
  RETURNING key, value, num;

-- DELETE RETURNING
DELETE FROM kv WHERE key = 100 RETURNING *;

-- Now run exact same queries with single-task OFF
SET citus.enable_single_task_execution TO off;

-- Single-row point lookups (same key values)
SELECT * FROM kv WHERE key = 1;
SELECT * FROM kv WHERE key = 50;
SELECT * FROM kv WHERE key = 99;

-- Point lookup with expressions
SELECT key, upper(value) AS uval, num + 100 AS shifted FROM kv WHERE key = 42;

-- Multi-row: all rows for a shard key value
SELECT * FROM kv_multi WHERE key = 5 ORDER BY seq;

-- Multi-row with aggregate
SELECT key, count(*), max(length(payload)) FROM kv_multi WHERE key = 10 GROUP BY key;

-- ============================================================
-- PART 3: Complex fast-path queries that still qualify
-- ============================================================
SET citus.enable_single_task_execution TO on;

-- Subquery in target list
SELECT key, (SELECT count(*) FROM kv_multi m WHERE m.key = kv.key) AS child_count
FROM kv WHERE key = 5;

-- CASE expression
SELECT key,
       CASE WHEN num > 500 THEN 'high' WHEN num > 200 THEN 'mid' ELSE 'low' END AS tier
FROM kv WHERE key = 50;

-- Coalesce and NULL handling
SELECT key, coalesce(value, 'missing') AS val, coalesce(NULL::int, num, 0) AS n
FROM kv WHERE key = 1;

-- Multiple conditions on same shard key (key = X AND ...)
SELECT * FROM kv WHERE key = 42 AND num > 100 AND value LIKE 'val%';

-- ORDER BY with LIMIT on multi-row table
SELECT * FROM kv_multi WHERE key = 5 ORDER BY seq DESC LIMIT 3;

-- FOR UPDATE (supported for replication_factor = 1)
SELECT * FROM kv WHERE key = 10 FOR UPDATE;

-- CTE that reads from same shard
WITH recent AS (
    SELECT * FROM kv_multi WHERE key = 8 ORDER BY seq DESC LIMIT 5
)
SELECT key, seq, length(payload) AS plen FROM recent ORDER BY seq;

-- ============================================================
-- PART 4: Queries that should NOT use single-task executor
-- ============================================================

-- Multi-shard query (no equality on distribution column)
EXPLAIN (COSTS OFF) SELECT count(*) FROM kv;

-- Range condition on distribution column
EXPLAIN (COSTS OFF) SELECT * FROM kv WHERE key > 50;

-- ============================================================
-- PART 5: GUC toggle - disabled means plans revert to full adaptive
-- ============================================================
SET citus.enable_single_task_execution TO off;

-- Same query that was Single Task before should now show "Citus Adaptive"
EXPLAIN (COSTS OFF) SELECT * FROM kv WHERE key = 1;
EXPLAIN (COSTS OFF) UPDATE kv SET value = 'x' WHERE key = 1;

-- ============================================================
-- PART 6: local-execution state must be preserved (read-your-writes)
-- Guard: when STE runs a task remotely against the local node it must set
-- LOCAL_EXECUTION_DISABLED, exactly like the adaptive executor. Otherwise a
-- local write followed by a forced-remote access in the same transaction
-- would silently return a stale value (read-your-writes violation) or
-- self-deadlock. With the guard in place, the REQUIRED -> DISABLED transition
-- raises the same protective error the adaptive executor raises.
-- ============================================================
SET citus.enable_single_task_execution TO on;

CREATE TABLE local_kv (key int PRIMARY KEY, val text);
SELECT citus_add_local_table_to_metadata('local_kv');
INSERT INTO local_kv VALUES (1, 'orig');

-- read case: must raise the protective error (NOT silently return 'orig')
BEGIN;
  UPDATE local_kv SET val = 'TX1' WHERE key = 1;
  SET LOCAL citus.enable_local_execution TO off;
  SELECT val FROM local_kv WHERE key = 1;
ROLLBACK;

-- write-after-write variant: must error, not self-deadlock
BEGIN;
  UPDATE local_kv SET val = 'TX1' WHERE key = 1;
  SET LOCAL citus.enable_local_execution TO off;
  UPDATE local_kv SET val = 'TX2' WHERE key = 1;
ROLLBACK;

-- positive parity: local write then local read in same txn sees fresh value
BEGIN;
  UPDATE local_kv SET val = 'TX1' WHERE key = 1;
  SELECT val FROM local_kv WHERE key = 1;
ROLLBACK;

DROP TABLE local_kv;

-- ============================================================
-- PART 7: eligibility exclusions - reference tables and multi-replica
-- distributed tables must stay on the full adaptive executor. STE only uses
-- the first placement, so choosing it for these would skip replica writes /
-- 2PC-to-all-placements. Confirm modifications plan as "Citus Adaptive".
-- ============================================================
SET citus.enable_single_task_execution TO on;

CREATE TABLE ref_kv (key int PRIMARY KEY, val text);
SELECT create_reference_table('ref_kv');
INSERT INTO ref_kv VALUES (1, 'orig');

SET citus.shard_replication_factor TO 2;
CREATE TABLE rep2 (key int PRIMARY KEY, val text);
SELECT create_distributed_table('rep2', 'key');
SET citus.shard_replication_factor TO 1;
INSERT INTO rep2 VALUES (1, 'orig');

-- reference-table modification must NOT use Single Task (writes all placements)
EXPLAIN (COSTS OFF) UPDATE ref_kv SET val = 'r2' WHERE key = 1;

-- multi-replica distributed modification must NOT use Single Task
EXPLAIN (COSTS OFF) UPDATE rep2 SET val = 'x2' WHERE key = 1;

DROP TABLE ref_kv, rep2;

-- ============================================================
-- PART 8: multi-node modification uses 2PC. Two single-shard writes to keys
-- on different shards within one transaction must commit atomically. STE
-- participates in the coordinated transaction / 2PC just like the adaptive
-- executor.
-- ============================================================
SET citus.enable_single_task_execution TO on;

-- atomic commit across shards
BEGIN;
  UPDATE kv SET value = 'tpc_a' WHERE key = 1;
  UPDATE kv SET value = 'tpc_b' WHERE key = 2;
COMMIT;
SELECT key, value FROM kv WHERE key IN (1, 2) ORDER BY key;

-- atomic rollback across shards
BEGIN;
  UPDATE kv SET value = 'rb_a' WHERE key = 1;
  UPDATE kv SET value = 'rb_b' WHERE key = 2;
ROLLBACK;
SELECT key, value FROM kv WHERE key IN (1, 2) ORDER BY key;

-- ============================================================
-- PART 9: dead cached connection reconnect - stat counter parity.
-- When a cached connection is remotely closed while idle, the next single-task
-- query must detect the dead connection (via the MSG_PEEK probe), reconnect,
-- and establish a fresh connection (connection_establishment_succeeded >= 1) to
-- return the correct result -- matching the adaptive executor. The exact
-- connection_reused count is not asserted: whether the dead cached connection
-- is still pooled when the reusing query runs (reused = 1) or was already
-- evicted (reused = 0) is timing-dependent and not the behavior under test.
-- ============================================================
SET citus.enable_stat_counters TO on;
SET citus.max_cached_conns_per_worker TO 1;

-- STE on: cache -> remote-close idle backend -> reuse must detect + reconnect.
-- pg_terminate_backend() is given a timeout so it waits for the backend to
-- actually exit; this guarantees the FATAL/close has been delivered to our
-- cached socket before the reusing query probes it, making the MSG_PEEK
-- detection deterministic (a bare pg_terminate_backend(pid) only signals and
-- returns, racing the probe).
SET citus.enable_single_task_execution TO on;
SELECT value FROM kv WHERE key = 1;
SELECT count(*) FROM run_command_on_workers($$
  SELECT count(pg_terminate_backend(pid, 10000)) FROM pg_stat_activity
  WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
    AND application_name LIKE 'citus%' AND state = 'idle' $$) WHERE success;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT connection_establishment_succeeded >= 1 AS established_ok
  FROM citus_stat_counters WHERE name = current_database();

-- adaptive executor: same scenario, must show identical parity
SET citus.enable_single_task_execution TO off;
SELECT value FROM kv WHERE key = 1;
SELECT count(*) FROM run_command_on_workers($$
  SELECT count(pg_terminate_backend(pid, 10000)) FROM pg_stat_activity
  WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
    AND application_name LIKE 'citus%' AND state = 'idle' $$) WHERE success;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT connection_establishment_succeeded >= 1 AS established_ok
  FROM citus_stat_counters WHERE name = current_database();

RESET citus.max_cached_conns_per_worker;
RESET citus.enable_stat_counters;

-- ============================================================
-- PART 10: stat-counter parity (success). A single-shard query must count
-- query_execution_single_shard = 1 whether it runs through the single-task
-- executor or the adaptive executor.
-- ============================================================
SET citus.enable_stat_counters TO on;

SET citus.enable_single_task_execution TO on;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT query_execution_single_shard = 1 AS single_shard_ok
  FROM citus_stat_counters WHERE name = current_database();

SET citus.enable_single_task_execution TO off;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT query_execution_single_shard = 1 AS single_shard_ok
  FROM citus_stat_counters WHERE name = current_database();

-- connection counters with cache disabled: each of the two queries establishes
-- a fresh connection (connection_establishment_succeeded >= 2), for both
-- executors. We assert only the establishment count, not reused = 0: a
-- just-closed uncached connection can occasionally still be observed as reused
-- before teardown, which is timing-dependent.
SET citus.max_cached_conns_per_worker TO 0;
-- flush any connection cached by the previous block so both executors start
-- from a clean (uncached) state
SELECT value FROM kv WHERE key = 1;

SET citus.enable_single_task_execution TO on;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT value FROM kv WHERE key = 1;
SELECT connection_establishment_succeeded >= 2 AS established_ok
  FROM citus_stat_counters WHERE name = current_database();

SET citus.enable_single_task_execution TO off;
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT value FROM kv WHERE key = 1;
SELECT value FROM kv WHERE key = 1;
SELECT connection_establishment_succeeded >= 2 AS established_ok
  FROM citus_stat_counters WHERE name = current_database();

RESET citus.max_cached_conns_per_worker;
RESET citus.enable_stat_counters;

-- ============================================================
-- PART 11: executor / connection GUC parity. STE must honor session GUCs the
-- same way the adaptive executor does: statement_timeout cancels the query,
-- SET LOCAL settings propagate to the worker, and text (non-binary) protocol
-- returns correct results.
-- ============================================================
SET citus.enable_single_task_execution TO on;

-- statement_timeout cancels the single-task query
SET statement_timeout TO '2s';
SELECT pg_sleep(10), key FROM kv WHERE key = 1;
RESET statement_timeout;

-- SET LOCAL timezone propagates to the worker (with propagate_set_commands
-- enabled the worker must report the pushed-down value, which is portable
-- across environments unlike the worker's ambient default timezone).
BEGIN;
  SET LOCAL citus.propagate_set_commands TO 'local';
  SET LOCAL timezone TO 'Asia/Tokyo';
  SELECT current_setting('timezone') FROM kv WHERE key = 1;
COMMIT;

-- text protocol returns correct results
SET citus.enable_binary_protocol TO off;
SELECT key, value FROM kv WHERE key = 42;
RESET citus.enable_binary_protocol;

-- same three with the adaptive executor
SET citus.enable_single_task_execution TO off;

SET statement_timeout TO '2s';
SELECT pg_sleep(10), key FROM kv WHERE key = 1;
RESET statement_timeout;

BEGIN;
  SET LOCAL citus.propagate_set_commands TO 'local';
  SET LOCAL timezone TO 'Asia/Tokyo';
  SELECT current_setting('timezone') FROM kv WHERE key = 1;
COMMIT;

SET citus.enable_binary_protocol TO off;
SELECT key, value FROM kv WHERE key = 42;
RESET citus.enable_binary_protocol;

-- ============================================================
-- PART 12: local / remote logging + enable_local_execution parity. STE splits
-- the task into local/remote lists, so it honors log_local_commands,
-- log_remote_commands and enable_local_execution. A citus-local table forces
-- the single task local; enable_local_execution=off forces it remote.
-- ============================================================
SET citus.enable_single_task_execution TO on;

CREATE TABLE ste_loc (a int primary key, b text);
SELECT citus_add_local_table_to_metadata('ste_loc');
INSERT INTO ste_loc VALUES (1, 'x');

-- local placement => "executing the command locally"
SET citus.log_local_commands TO on;
SET client_min_messages TO LOG;
SELECT * FROM ste_loc WHERE a = 1;
RESET client_min_messages;
RESET citus.log_local_commands;

-- enable_local_execution=off => same single task runs remote (loopback)
SET citus.enable_local_execution TO off;
SET citus.log_remote_commands TO on;
SET client_min_messages TO LOG;
SELECT * FROM ste_loc WHERE a = 1;
RESET client_min_messages;
RESET citus.log_remote_commands;
RESET citus.enable_local_execution;

DROP TABLE ste_loc;

-- ============================================================
-- Cleanup
-- ============================================================
SET citus.enable_single_task_execution TO on;
DROP SCHEMA single_task_execution CASCADE;
