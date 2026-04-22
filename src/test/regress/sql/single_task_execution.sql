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
-- Cleanup
-- ============================================================
SET citus.enable_single_task_execution TO on;
DROP SCHEMA single_task_execution CASCADE;
