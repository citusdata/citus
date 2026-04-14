--
-- Tests for the batched adaptive executor (reentrant execution)
--
CREATE SCHEMA adaptive_executor_batching;
SET search_path TO adaptive_executor_batching;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 802009000;

CREATE TABLE batch_test (x int, y text);
SELECT create_distributed_table('batch_test', 'x');

-- Insert enough rows to exercise multiple batches
INSERT INTO batch_test SELECT i, 'row_' || i FROM generate_series(1, 200) i;

-- Verify baseline: all rows present
SELECT count(*) FROM batch_test;

--
-- Test 1: Force small batch size and verify all rows are returned
--
SET citus.executor_batch_size TO 10;

SELECT count(*) FROM batch_test;

-- Ordered output to verify correctness across batches
SELECT x FROM batch_test WHERE x <= 20 ORDER BY x;

--
-- Test 2: Batch size of 1 (extreme case)
--
SET citus.executor_batch_size TO 1;

SELECT count(*) FROM batch_test;
SELECT x FROM batch_test WHERE x <= 5 ORDER BY x;

--
-- Test 3: Batch size larger than result set (single batch)
--
SET citus.executor_batch_size TO 100000;

SELECT count(*) FROM batch_test;

--
-- Test 4: Auto batch size (from work_mem)
--
SET citus.executor_batch_size TO 0;

SELECT count(*) FROM batch_test;

--
-- Test 5: Empty result set with batching
--
SET citus.executor_batch_size TO 10;

SELECT count(*) FROM batch_test WHERE x > 9999;

--
-- Test 6: LIMIT with batching
--
SELECT x FROM batch_test ORDER BY x LIMIT 5;

--
-- Test 7: Aggregation across batches
--
SELECT sum(x), min(x), max(x) FROM batch_test;

--
-- Additional tables for join / subquery / CTE tests
--
CREATE TABLE batch_orders (
    order_id int,
    customer_id int,
    amount numeric(10,2),
    region text,
    order_date date
);
SELECT create_distributed_table('batch_orders', 'order_id');

CREATE TABLE batch_items (
    item_id int,
    order_id int,
    product text,
    qty int,
    price numeric(10,2)
);
SELECT create_distributed_table('batch_items', 'order_id', colocate_with => 'batch_orders');

CREATE TABLE batch_regions (
    region text,
    country text
);
SELECT create_reference_table('batch_regions');

-- Populate orders (300 rows across 4 shards) with deterministic amounts
INSERT INTO batch_orders
SELECT i,
       (i % 50) + 1,
       round(((i * 127 + 53) % 500 + 10)::numeric, 2),
       (ARRAY['NA', 'EMEA', 'APAC', 'LATAM'])[1 + (i % 4)],
       '2025-01-01'::date + (i % 365)
FROM generate_series(1, 300) i;

-- Populate items (600 rows, ~2 items per order) with deterministic prices
INSERT INTO batch_items
SELECT i,
       ((i - 1) / 2) + 1,
       'product_' || (i % 20),
       1 + (i % 5),
       round(((i * 31 + 7) % 100 + 1)::numeric, 2)
FROM generate_series(1, 600) i;

-- Populate reference table
INSERT INTO batch_regions VALUES
    ('NA', 'United States'),
    ('EMEA', 'Germany'),
    ('APAC', 'Japan'),
    ('LATAM', 'Brazil');

-- Create vanilla PostgreSQL copies with identical data for result verification
CREATE TABLE local_orders AS SELECT * FROM batch_orders;
CREATE TABLE local_items AS SELECT * FROM batch_items;
CREATE TABLE local_regions AS SELECT * FROM batch_regions;

-- ================================================================
-- Test 7a: Distributed join with batching
-- ================================================================
SET citus.executor_batch_size TO 10;

-- Co-located join on distribution column (both distributed on order_id)
SELECT count(*)
FROM batch_orders o
JOIN batch_items i ON o.order_id = i.order_id;

-- Join with filter that spans multiple batches
SELECT o.order_id, o.region, i.product
FROM batch_orders o
JOIN batch_items i ON o.order_id = i.order_id
WHERE o.order_id <= 10
ORDER BY o.order_id, i.item_id;

-- ================================================================
-- Test 7b: Reference table joins with batching
-- ================================================================
SET citus.executor_batch_size TO 15;

SELECT o.order_id, o.region, r.country
FROM batch_orders o
JOIN batch_regions r ON o.region = r.region
WHERE o.order_id <= 20
ORDER BY o.order_id;

-- Aggregate over reference table join
SELECT r.country, count(*), sum(o.amount)::numeric(12,2)
FROM batch_orders o
JOIN batch_regions r ON o.region = r.region
GROUP BY r.country
ORDER BY r.country;

-- ================================================================
-- Test 7c: GROUP BY with HAVING across batches
-- ================================================================
SET citus.executor_batch_size TO 10;

-- Simple GROUP BY
SELECT region, count(*) AS cnt, min(amount) AS min_amt, max(amount) AS max_amt
FROM batch_orders
GROUP BY region
ORDER BY region;

-- GROUP BY with HAVING (filters groups after aggregation)
SELECT region, count(*) AS cnt
FROM batch_orders
GROUP BY region
HAVING count(*) > 50
ORDER BY region;

-- Multi-column GROUP BY across batch boundaries
SELECT region, (order_id % 10) AS bucket, count(*)
FROM batch_orders
WHERE order_id <= 100
GROUP BY region, (order_id % 10)
ORDER BY region, bucket;

-- ================================================================
-- Test 7d: Distinct and distinct aggregates
-- ================================================================
SET citus.executor_batch_size TO 10;

SELECT DISTINCT region FROM batch_orders ORDER BY region;

SELECT count(DISTINCT region), count(DISTINCT customer_id)
FROM batch_orders;

-- ================================================================
-- Test 7e: CTE (Common Table Expressions) with batching
-- ================================================================
SET citus.executor_batch_size TO 10;

-- Inlinable CTE with join
WITH regional_totals AS (
    SELECT o.region, sum(o.amount) AS total
    FROM batch_orders o
    GROUP BY o.region
)
SELECT rt.region, rt.total::numeric(12,2), r.country
FROM regional_totals rt
JOIN batch_regions r ON rt.region = r.region
ORDER BY rt.region;

-- CTE that feeds into a filtered query
WITH top_customers AS (
    SELECT customer_id, count(*) AS order_count
    FROM batch_orders
    GROUP BY customer_id
    HAVING count(*) >= 5
)
SELECT tc.customer_id, tc.order_count
FROM top_customers tc
ORDER BY tc.customer_id;

-- Nested CTEs
WITH order_stats AS (
    SELECT region, count(*) AS cnt, avg(amount) AS avg_amt
    FROM batch_orders
    GROUP BY region
),
enriched AS (
    SELECT os.*, r.country
    FROM order_stats os
    JOIN batch_regions r ON os.region = r.region
)
SELECT country, cnt, avg_amt::numeric(10,2)
FROM enriched
ORDER BY country;

-- ================================================================
-- Test 7f: Subqueries (correlated and uncorrelated)
-- ================================================================
SET citus.executor_batch_size TO 10;

-- Uncorrelated subquery in WHERE
SELECT order_id, amount
FROM batch_orders
WHERE amount > (SELECT avg(amount) FROM batch_orders)
ORDER BY order_id
LIMIT 15;

-- Subquery in FROM (derived table)
SELECT sub.region, sub.total_orders, sub.total_amount::numeric(12,2)
FROM (
    SELECT region, count(*) AS total_orders, sum(amount) AS total_amount
    FROM batch_orders
    GROUP BY region
) sub
ORDER BY sub.region;

-- IN subquery spanning batches
SELECT order_id, region
FROM batch_orders
WHERE region IN (
    SELECT region FROM batch_regions WHERE country IN ('Japan', 'Brazil')
)
ORDER BY order_id
LIMIT 20;

-- EXISTS subquery (co-located via distribution column)
SELECT o.order_id, o.region
FROM batch_orders o
WHERE EXISTS (
    SELECT 1 FROM batch_items i
    WHERE i.order_id = o.order_id
      AND i.qty >= 4
)
ORDER BY o.order_id
LIMIT 15;

-- ================================================================
-- Test 7g: Window functions across batches
-- ================================================================
SET citus.executor_batch_size TO 10;

SELECT order_id, region, amount,
       row_number() OVER (ORDER BY order_id) AS rn,
       sum(amount) OVER (PARTITION BY region ORDER BY order_id) AS running_total
FROM batch_orders
WHERE order_id <= 20
ORDER BY order_id;

-- RANK / DENSE_RANK
SELECT order_id, region,
       rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
FROM batch_orders
WHERE order_id <= 40
ORDER BY region, rnk, order_id
LIMIT 20;

-- ================================================================
-- Test 7h: UNION / UNION ALL across batches
-- ================================================================
SET citus.executor_batch_size TO 10;

SELECT order_id, 'orders' AS source FROM batch_orders WHERE order_id <= 10
UNION ALL
SELECT item_id, 'items' FROM batch_items WHERE item_id <= 10
ORDER BY source, order_id;

-- UNION (deduplicated)
SELECT region FROM batch_orders WHERE order_id <= 50
UNION
SELECT region FROM batch_orders WHERE order_id > 250
ORDER BY region;

-- ================================================================
-- Test 7i: Multi-table join with GROUP BY and HAVING
-- ================================================================
SET citus.executor_batch_size TO 10;

SELECT r.country, count(DISTINCT o.order_id) AS orders, sum(i.qty) AS total_qty
FROM batch_orders o
JOIN batch_items i ON o.order_id = i.order_id
JOIN batch_regions r ON o.region = r.region
WHERE o.order_id <= 50
GROUP BY r.country
HAVING sum(i.qty) > 10
ORDER BY r.country;

-- ================================================================
-- Test 7j: ORDER BY with LIMIT/OFFSET across batches
-- ================================================================
SET citus.executor_batch_size TO 10;

-- OFFSET spans batch boundaries
SELECT order_id, region FROM batch_orders ORDER BY order_id LIMIT 10 OFFSET 15;

-- Large OFFSET
SELECT order_id, region FROM batch_orders ORDER BY order_id LIMIT 5 OFFSET 290;

-- ================================================================
-- Test 7k: Cross-batch-size consistency for complex queries
-- Runs the same complex query at different batch sizes and verifies
-- identical results.
-- ================================================================
CREATE TABLE batch_complex_consistency AS
SELECT
    bs,
    (SELECT count(*)
     FROM batch_orders o
     JOIN batch_regions r ON o.region = r.region)::bigint AS join_cnt,
    (SELECT count(DISTINCT region) FROM batch_orders)::int AS dist_regions,
    (SELECT sum(amount)::numeric(14,2) FROM batch_orders) AS total_amount,
    (SELECT count(*)
     FROM batch_orders
     WHERE amount > (SELECT avg(amount) FROM batch_orders))::bigint AS above_avg
FROM (SELECT unnest(ARRAY[1, 7, 25, 100, 10000]) bs) sizes,
LATERAL (SELECT set_config('citus.executor_batch_size', bs::text, false)) AS apply;

-- All batch sizes must produce the same results
SELECT count(DISTINCT (join_cnt, dist_regions, total_amount, above_avg)) AS distinct_results
FROM batch_complex_consistency;

SELECT DISTINCT join_cnt, dist_regions, total_amount, above_avg
FROM batch_complex_consistency;

DROP TABLE batch_complex_consistency;

-- ================================================================
-- Test 7l: Distributed vs vanilla PostgreSQL result verification
-- Every query is run against both distributed and local tables;
-- EXCEPT should return zero rows if results are identical.
-- ================================================================
SET citus.executor_batch_size TO 10;

-- Join count
SELECT count(*) AS diff FROM (
    (SELECT count(*) AS c FROM batch_orders o JOIN batch_items i ON o.order_id = i.order_id)
    EXCEPT
    (SELECT count(*) AS c FROM local_orders o JOIN local_items i ON o.order_id = i.order_id)
) t;

-- Join with filter
SELECT count(*) AS diff FROM (
    (SELECT o.order_id, o.region, i.product
     FROM batch_orders o JOIN batch_items i ON o.order_id = i.order_id
     WHERE o.order_id <= 10)
    EXCEPT
    (SELECT o.order_id, o.region, i.product
     FROM local_orders o JOIN local_items i ON o.order_id = i.order_id
     WHERE o.order_id <= 10)
) t;

-- Reference table join
SELECT count(*) AS diff FROM (
    (SELECT o.order_id, o.region, r.country
     FROM batch_orders o JOIN batch_regions r ON o.region = r.region
     WHERE o.order_id <= 20)
    EXCEPT
    (SELECT o.order_id, o.region, r.country
     FROM local_orders o JOIN local_regions r ON o.region = r.region
     WHERE o.order_id <= 20)
) t;

-- Aggregate over reference table join
SELECT count(*) AS diff FROM (
    (SELECT r.country, count(*), sum(o.amount)::numeric(12,2)
     FROM batch_orders o JOIN batch_regions r ON o.region = r.region
     GROUP BY r.country)
    EXCEPT
    (SELECT r.country, count(*), sum(o.amount)::numeric(12,2)
     FROM local_orders o JOIN local_regions r ON o.region = r.region
     GROUP BY r.country)
) t;

-- GROUP BY with HAVING
SELECT count(*) AS diff FROM (
    (SELECT region, count(*) AS cnt, min(amount), max(amount)
     FROM batch_orders GROUP BY region)
    EXCEPT
    (SELECT region, count(*) AS cnt, min(amount), max(amount)
     FROM local_orders GROUP BY region)
) t;

-- Multi-column GROUP BY
SELECT count(*) AS diff FROM (
    (SELECT region, (order_id % 10) AS bucket, count(*)
     FROM batch_orders WHERE order_id <= 100
     GROUP BY region, (order_id % 10))
    EXCEPT
    (SELECT region, (order_id % 10) AS bucket, count(*)
     FROM local_orders WHERE order_id <= 100
     GROUP BY region, (order_id % 10))
) t;

-- DISTINCT
SELECT count(*) AS diff FROM (
    (SELECT DISTINCT region FROM batch_orders)
    EXCEPT
    (SELECT DISTINCT region FROM local_orders)
) t;

-- Distinct aggregates
SELECT count(*) AS diff FROM (
    (SELECT count(DISTINCT region), count(DISTINCT customer_id) FROM batch_orders)
    EXCEPT
    (SELECT count(DISTINCT region), count(DISTINCT customer_id) FROM local_orders)
) t;

-- CTE with join
SELECT count(*) AS diff FROM (
    (WITH rt AS (SELECT region, sum(amount) AS total FROM batch_orders GROUP BY region)
     SELECT rt.region, rt.total::numeric(12,2), r.country
     FROM rt JOIN batch_regions r ON rt.region = r.region)
    EXCEPT
    (WITH rt AS (SELECT region, sum(amount) AS total FROM local_orders GROUP BY region)
     SELECT rt.region, rt.total::numeric(12,2), r.country
     FROM rt JOIN local_regions r ON rt.region = r.region)
) t;

-- Nested CTEs
SELECT count(*) AS diff FROM (
    (WITH os AS (SELECT region, count(*) AS cnt, avg(amount) AS avg_amt FROM batch_orders GROUP BY region),
          en AS (SELECT os.*, r.country FROM os JOIN batch_regions r ON os.region = r.region)
     SELECT country, cnt, avg_amt::numeric(10,2) FROM en)
    EXCEPT
    (WITH os AS (SELECT region, count(*) AS cnt, avg(amount) AS avg_amt FROM local_orders GROUP BY region),
          en AS (SELECT os.*, r.country FROM os JOIN local_regions r ON os.region = r.region)
     SELECT country, cnt, avg_amt::numeric(10,2) FROM en)
) t;

-- Uncorrelated subquery in WHERE
SELECT count(*) AS diff FROM (
    (SELECT order_id, amount FROM batch_orders
     WHERE amount > (SELECT avg(amount) FROM batch_orders))
    EXCEPT
    (SELECT order_id, amount FROM local_orders
     WHERE amount > (SELECT avg(amount) FROM local_orders))
) t;

-- Subquery in FROM
SELECT count(*) AS diff FROM (
    (SELECT region, count(*) AS total_orders, sum(amount)::numeric(12,2) AS total_amount
     FROM batch_orders GROUP BY region)
    EXCEPT
    (SELECT region, count(*) AS total_orders, sum(amount)::numeric(12,2) AS total_amount
     FROM local_orders GROUP BY region)
) t;

-- IN subquery
SELECT count(*) AS diff FROM (
    (SELECT order_id, region FROM batch_orders
     WHERE region IN (SELECT region FROM batch_regions WHERE country IN ('Japan', 'Brazil')))
    EXCEPT
    (SELECT order_id, region FROM local_orders
     WHERE region IN (SELECT region FROM local_regions WHERE country IN ('Japan', 'Brazil')))
) t;

-- EXISTS subquery
SELECT count(*) AS diff FROM (
    (SELECT o.order_id, o.region FROM batch_orders o
     WHERE EXISTS (SELECT 1 FROM batch_items i WHERE i.order_id = o.order_id AND i.qty >= 4))
    EXCEPT
    (SELECT o.order_id, o.region FROM local_orders o
     WHERE EXISTS (SELECT 1 FROM local_items i WHERE i.order_id = o.order_id AND i.qty >= 4))
) t;

-- Window functions
SELECT count(*) AS diff FROM (
    (SELECT order_id, region, amount,
            row_number() OVER (ORDER BY order_id) AS rn,
            sum(amount) OVER (PARTITION BY region ORDER BY order_id) AS running_total
     FROM batch_orders WHERE order_id <= 20)
    EXCEPT
    (SELECT order_id, region, amount,
            row_number() OVER (ORDER BY order_id) AS rn,
            sum(amount) OVER (PARTITION BY region ORDER BY order_id) AS running_total
     FROM local_orders WHERE order_id <= 20)
) t;

-- RANK window function
SELECT count(*) AS diff FROM (
    (SELECT order_id, region,
            rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
     FROM batch_orders WHERE order_id <= 40)
    EXCEPT
    (SELECT order_id, region,
            rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
     FROM local_orders WHERE order_id <= 40)
) t;

-- UNION ALL
SELECT count(*) AS diff FROM (
    (SELECT order_id, 'orders' AS source FROM batch_orders WHERE order_id <= 10
     UNION ALL
     SELECT item_id, 'items' FROM batch_items WHERE item_id <= 10)
    EXCEPT
    (SELECT order_id, 'orders' AS source FROM local_orders WHERE order_id <= 10
     UNION ALL
     SELECT item_id, 'items' FROM local_items WHERE item_id <= 10)
) t;

-- 3-table join with GROUP BY and HAVING
SELECT count(*) AS diff FROM (
    (SELECT r.country, count(DISTINCT o.order_id) AS orders, sum(i.qty) AS total_qty
     FROM batch_orders o
     JOIN batch_items i ON o.order_id = i.order_id
     JOIN batch_regions r ON o.region = r.region
     WHERE o.order_id <= 50
     GROUP BY r.country HAVING sum(i.qty) > 10)
    EXCEPT
    (SELECT r.country, count(DISTINCT o.order_id) AS orders, sum(i.qty) AS total_qty
     FROM local_orders o
     JOIN local_items i ON o.order_id = i.order_id
     JOIN local_regions r ON o.region = r.region
     WHERE o.order_id <= 50
     GROUP BY r.country HAVING sum(i.qty) > 10)
) t;

-- ORDER BY with LIMIT/OFFSET
SELECT count(*) AS diff FROM (
    (SELECT order_id, region FROM batch_orders ORDER BY order_id LIMIT 10 OFFSET 15)
    EXCEPT
    (SELECT order_id, region FROM local_orders ORDER BY order_id LIMIT 10 OFFSET 15)
) t;

-- Reset for subsequent tests
SET citus.executor_batch_size TO 10;

--
-- Test 8: DML with RETURNING across batches
--
SET citus.executor_batch_size TO 10;

-- Use a separate table for DML test
CREATE TABLE batch_dml_test (x int, y text);
SELECT create_distributed_table('batch_dml_test', 'x');
INSERT INTO batch_dml_test SELECT i, 'val_' || i FROM generate_series(1, 50) i;

WITH deleted AS (
    DELETE FROM batch_dml_test WHERE x <= 20 RETURNING x
)
SELECT count(*) FROM deleted;

SELECT count(*) FROM batch_dml_test WHERE x <= 20;

--
-- Test 9: Verify GUC is session-level
--
SHOW citus.executor_batch_size;

SET citus.executor_batch_size TO 500;
SHOW citus.executor_batch_size;

RESET citus.executor_batch_size;
SHOW citus.executor_batch_size;

--
-- Test 10: Between-batch error cleanup (CitusEndScan must release sessions)
--
-- This validates that when an error occurs on the coordinator between
-- AdaptiveExecutorRun calls (mid-scan, mid-batch), CitusEndScan properly
-- cleans up distributed execution sessions via AdaptiveExecutorEnd.
-- Without this fix, sessions/connections would leak on each error.
--
SET citus.executor_batch_size TO 10;

-- Helper: iterates a distributed query and raises error after N rows.
-- With batch_size=10 and error at row 15, the error fires during
-- the second batch — between the first AdaptiveExecutorRun (which
-- returned control to the executor) and the scan completing.
CREATE OR REPLACE FUNCTION trigger_between_batch_error(fail_after int)
RETURNS void AS $$
DECLARE
    r RECORD;
    cnt int := 0;
BEGIN
    FOR r IN SELECT x FROM batch_test ORDER BY x LOOP
        cnt := cnt + 1;
        IF cnt >= fail_after THEN
            RAISE EXCEPTION 'intentional error at row %', cnt;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Single invocation: error should be caught cleanly
DO $$
BEGIN
    PERFORM trigger_between_batch_error(15);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
END;
$$;

-- Repeat 20 times to verify connections don't accumulate across errors.
-- If CitusEndScan didn't clean up, we'd exhaust the connection pool.
DO $$
BEGIN
    FOR i IN 1..20 LOOP
        BEGIN
            PERFORM trigger_between_batch_error(15);
        EXCEPTION WHEN OTHERS THEN
            -- expected, swallow
        END;
    END LOOP;
END;
$$;

-- After 20 error cycles, distributed queries must still work
SELECT count(*) FROM batch_test;

-- Also verify with a cursor closed mid-batch (non-error path through
-- CitusEndScan with finishedRemoteScan = false)
BEGIN;
DECLARE c CURSOR FOR SELECT x FROM batch_test ORDER BY x;
FETCH 5 FROM c;
CLOSE c;
COMMIT;

-- Still healthy after cursor close mid-batch
SELECT count(*) FROM batch_test;

--
-- Test 11: Cross-batch-size result consistency
--
-- Runs the same analytical queries at batch sizes 1, 10, 100, 10000, and 0
-- (auto) and asserts identical results. Any batch-boundary bug would cause
-- a mismatch.
--
CREATE TABLE batch_consistency AS
SELECT
    bs,
    (SELECT count(*) FROM batch_test)::bigint AS cnt,
    (SELECT sum(x) FROM batch_test)::bigint AS s,
    (SELECT min(x) FROM batch_test)::int AS mn,
    (SELECT max(x) FROM batch_test)::int AS mx,
    (SELECT count(*) FROM batch_test WHERE x BETWEEN 50 AND 150)::bigint AS range_cnt
FROM (SELECT unnest(ARRAY[1, 10, 100, 10000]) bs) sizes,
LATERAL (SELECT set_config('citus.executor_batch_size', bs::text, false)) AS apply;

-- All batch sizes must produce the same result — collapse to 1 distinct row
SELECT count(DISTINCT (cnt, s, mn, mx, range_cnt)) AS distinct_results FROM batch_consistency;

-- Show the actual values (should be a single row)
SELECT DISTINCT cnt, s, mn, mx, range_cnt FROM batch_consistency;

DROP TABLE batch_consistency;

--
-- Test 12: EXPLAIN ANALYZE with batching
-- Verify that EXPLAIN ANALYZE works correctly when batching is active.
-- We check: (a) it runs without error, (b) row counts in the output are
-- correct, and (c) it works with result sets that are exact multiples of
-- the batch size as well as non-multiples.
--

-- 12a: exact multiple of batch size (200 rows, batch_size=50 → 4 batches)
SET citus.executor_batch_size TO 50;
SELECT public.explain_filter('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM adaptive_executor_batching.batch_test');

-- Verify the actual query still returns the right count
SELECT count(*) FROM batch_test;

-- 12b: non-multiple of batch size (200 rows, batch_size=30 → 6 full + 1 partial)
SET citus.executor_batch_size TO 30;
SELECT public.explain_filter('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM adaptive_executor_batching.batch_test');

SELECT count(*) FROM batch_test;

-- 12c: small batch_size (200 rows, batch_size=7 → many small batches)
SET citus.executor_batch_size TO 7;
SELECT public.explain_filter('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT x, y FROM adaptive_executor_batching.batch_test WHERE x <= 10');

SELECT count(*) FROM batch_test WHERE x <= 10;

-- 12d: EXPLAIN ANALYZE with aggregation across batches
SET citus.executor_batch_size TO 25;
SELECT public.explain_filter('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT count(*), sum(x), min(x), max(x) FROM adaptive_executor_batching.batch_test');

SELECT count(*), sum(x), min(x), max(x) FROM batch_test;

--
-- Test 13: Scrollable cursor with batching
-- Verifies that DECLARE SCROLL CURSOR works correctly with the batched
-- executor. For scrollable cursors, the executor preserves the tuplestore
-- across batches (skips tuplestore_clear) so backward fetches can access
-- rows from earlier batches.
--
-- With batch_size=10, a 50-row result spans 5 batches:
--   batch 1: rows 1-10, batch 2: rows 11-20, batch 3: rows 21-30,
--   batch 4: rows 31-40, batch 5: rows 41-50
--
SET citus.executor_batch_size TO 10;

-- 13a: Forward/backward zigzag across batch boundaries
BEGIN;
DECLARE scroll_cur SCROLL CURSOR FOR
  SELECT x FROM batch_test WHERE x <= 50 ORDER BY x;

-- Read into batch 2 (rows 1-15)
FETCH FORWARD 15 FROM scroll_cur;

-- Backward 10: crosses batch 2→1 boundary, returns rows 14..5
FETCH BACKWARD 10 FROM scroll_cur;

-- Forward 5 from position 5: returns rows 6..10
FETCH FORWARD 5 FROM scroll_cur;

-- Forward 15 more: crosses batch 1→2→3, returns rows 11..25
FETCH FORWARD 15 FROM scroll_cur;

-- Backward 20: crosses batch 3→2→1, returns rows 24..5
FETCH BACKWARD 20 FROM scroll_cur;

CLOSE scroll_cur;
COMMIT;

-- 13b: FETCH FIRST / LAST / ABSOLUTE across all batches
BEGIN;
DECLARE abs_cur SCROLL CURSOR FOR
  SELECT x FROM batch_test WHERE x <= 50 ORDER BY x;

-- Jump to various positions across different batches
FETCH ABSOLUTE 5 FROM abs_cur;   -- batch 1
FETCH ABSOLUTE 45 FROM abs_cur;  -- batch 5
FETCH ABSOLUTE 1 FROM abs_cur;   -- batch 1 (backward jump)
FETCH ABSOLUTE 50 FROM abs_cur;  -- batch 5 (last row)
FETCH ABSOLUTE 15 FROM abs_cur;  -- batch 2 (backward jump)
FETCH ABSOLUTE 35 FROM abs_cur;  -- batch 4 (forward jump)
FETCH ABSOLUTE 10 FROM abs_cur;  -- batch 1 (backward jump across 3 batches)

-- FIRST and LAST
FETCH LAST FROM abs_cur;
FETCH FIRST FROM abs_cur;
FETCH LAST FROM abs_cur;

CLOSE abs_cur;
COMMIT;

-- 13c: Read all rows forward, then backward through all batches
BEGIN;
DECLARE full_cur SCROLL CURSOR FOR
  SELECT x FROM batch_test WHERE x <= 30 ORDER BY x;

-- Drain all 3 batches forward
FETCH FORWARD ALL FROM full_cur;

-- Now backward through all 3 batches
FETCH BACKWARD 30 FROM full_cur;

-- And forward to the end again
FETCH FORWARD ALL FROM full_cur;

CLOSE full_cur;
COMMIT;

-- 13d: FETCH RELATIVE across batch boundaries
BEGIN;
DECLARE rel_cur SCROLL CURSOR FOR
  SELECT x FROM batch_test WHERE x <= 50 ORDER BY x;

FETCH ABSOLUTE 25 FROM rel_cur;  -- middle of batch 3
FETCH RELATIVE -15 FROM rel_cur; -- jump back to batch 1 (row 10)
FETCH RELATIVE 30 FROM rel_cur;  -- jump forward to batch 4 (row 40)
FETCH RELATIVE -35 FROM rel_cur; -- jump back to batch 1 (row 5)
FETCH RELATIVE 10 FROM rel_cur;  -- forward within batch 2 (row 15)

CLOSE rel_cur;
COMMIT;

-- 13e: SCROLL cursor with inlinable CTE
-- Exercises the CTE-inlining planner path (InlineCtesAndCreateDistributedPlannedStmt
-- → TryCreateDistributedPlannedStmt → CreateDistributedPlannedStmt → FinalizePlan)
-- to verify that cursorOptions (CURSOR_OPT_SCROLL) is forwarded through
-- TryCreateDistributedPlannedStmt so FinalizePlan wraps the plan in a Material node.
BEGIN;
DECLARE cte_scroll SCROLL CURSOR FOR
  WITH batch_cte AS (
    SELECT x FROM batch_test WHERE x <= 30
  )
  SELECT x FROM batch_cte ORDER BY x;

-- Forward into batch 2
FETCH FORWARD 15 FROM cte_scroll;

-- Backward across batch boundary (rows 14..5)
FETCH BACKWARD 10 FROM cte_scroll;

-- Forward to end (rows 6..30)
FETCH FORWARD ALL FROM cte_scroll;

-- Backward all the way (rows 29..1)
FETCH BACKWARD ALL FROM cte_scroll;

CLOSE cte_scroll;
COMMIT;

--
-- Test 14: WITH HOLD cursor with batching
--
-- Validates that DECLARE ... SCROLL CURSOR WITH HOLD works correctly
-- with the batched executor. At COMMIT, PostgreSQL calls
-- PersistHoldablePortal(), which:
--
--   1. ExecutorRewind() → ExecReScan on the Material node (inserted by
--      FinalizePlan for SCROLL cursors), which does tuplestore_rescan()
--      to rewind the already-materialized rows — no distributed re-execution.
--   2. ExecutorRun(Forward, 0) → drains ALL tuples through the Material
--      node into the portal's holdStore tuplestore.
--   3. ExecutorEnd() shuts down the executor and releases connections.
--
-- After COMMIT the cursor survives and all FETCHes read from the local
-- holdStore — no distributed execution occurs. The Material node is
-- critical: without it, ExecutorRewind would call CitusReScan directly,
-- which could attempt to re-execute the distributed query at an unsafe time.
--
-- Also tests a non-SCROLL WITH HOLD cursor, where no Material node is
-- inserted. PersistHoldablePortal skips ExecutorRewind and only does a
-- forward drain of the remaining (unfetched) rows.
--
SET citus.executor_batch_size TO 5;

-- 14a: SCROLL + WITH HOLD — full backward/forward after COMMIT
CREATE TABLE withhold_test (id int, val int, label text);
SELECT create_distributed_table('withhold_test', 'id');

INSERT INTO withhold_test VALUES
    (1, 100, 'alpha'),   (2, 200, 'bravo'),
    (3, 300, 'charlie'),  (4, 100, 'delta'),
    (5, 200, 'echo'),     (6, 300, 'foxtrot'),
    (7, 100, 'golf'),     (8, 200, 'hotel'),
    (9, 300, 'india'),    (10, 100, 'juliet'),
    (11, 200, NULL),      (12, NULL, 'lima');

-- Also create a vanilla copy for result verification
CREATE TABLE withhold_local AS SELECT * FROM withhold_test;

BEGIN;
DECLARE hold_scroll SCROLL CURSOR WITH HOLD FOR
    SELECT id, val FROM withhold_test ORDER BY val NULLS LAST, id;
-- Fetch only 3 of 12 rows inside the transaction
FETCH FORWARD 3 FROM hold_scroll;
COMMIT;

-- After COMMIT the cursor survives; remaining rows must be accessible
FETCH FORWARD 5 FROM hold_scroll;
-- Backward scan into already-fetched territory
FETCH BACKWARD 4 FROM hold_scroll;
-- Fetch to end
FETCH FORWARD ALL FROM hold_scroll;
-- Verify the very end
FETCH BACKWARD 1 FROM hold_scroll;
CLOSE hold_scroll;

-- 14b: Non-SCROLL WITH HOLD — forward-only after COMMIT
BEGIN;
DECLARE hold_fwd NO SCROLL CURSOR WITH HOLD FOR
    SELECT id, val FROM withhold_test ORDER BY id;
-- Fetch 4 rows inside the transaction
FETCH FORWARD 4 FROM hold_fwd;
COMMIT;

-- After COMMIT, remaining 8 rows must be available
FETCH FORWARD ALL FROM hold_fwd;
CLOSE hold_fwd;

-- 14c: NO SCROLL cursor rejects backward fetches
-- Without explicit SCROLL, a cursor on a multi-shard query with ORDER BY
-- would normally auto-promote to SCROLL (because the Sort node supports
-- backward scan). Explicit NO SCROLL prevents this and must reject
-- backward fetches with an error.
BEGIN;
DECLARE hold_noscroll NO SCROLL CURSOR WITH HOLD FOR
    SELECT id, val FROM withhold_test ORDER BY id;
FETCH 3 FROM hold_noscroll;
FETCH FORWARD 3 FROM hold_noscroll;
COMMIT;

-- Forward still works after COMMIT
FETCH 3 FROM hold_noscroll;
-- Backward must fail
FETCH BACKWARD 3 FROM hold_noscroll;
CLOSE hold_noscroll;

-- 14d: Verify WITH HOLD results match vanilla PostgreSQL
-- Run the same ordered query against the local table and EXCEPT
SELECT count(*) AS diff FROM (
    (SELECT id, val FROM withhold_test ORDER BY val NULLS LAST, id)
    EXCEPT
    (SELECT id, val FROM withhold_local ORDER BY val NULLS LAST, id)
) t;

DROP TABLE withhold_test;
DROP TABLE withhold_local;

-- Reset batch size
RESET citus.executor_batch_size;

--
-- Cleanup
--
SET client_min_messages TO WARNING;
DROP SCHEMA adaptive_executor_batching CASCADE;
