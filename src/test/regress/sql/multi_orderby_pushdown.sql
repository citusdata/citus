--
-- MULTI_ORDERBY_PUSHDOWN
--
-- Coordinator-side k-way merge of pre-sorted worker results for
-- multi-shard SELECT ... ORDER BY queries, gated by
-- citus.enable_sorted_merge (planning-time, hidden experimental GUC).
--
-- MX verification: this test has been verified to pass with zero diffs
-- under check-base-mx (MX mode), confirming sorted merge works correctly
-- when any node in the cluster acts as coordinator.
--

--
-- SETUP_MULTI_ORDERBY_PUSHDOWN
--
-- Creates the test tables and data used by the sorted merge tests below.
--

SET citus.next_shard_id TO 960000;

-- =================================================================
-- Setup: create test tables
-- =================================================================

CREATE TABLE sorted_merge_test (
    id int,
    val text,
    num numeric,
    ts timestamptz DEFAULT now()
);
SELECT create_distributed_table('sorted_merge_test', 'id');

-- Insert 100 rows + NULLs + duplicates
INSERT INTO sorted_merge_test (id, val, num)
SELECT i, 'val_' || i, (i * 1.5)::numeric
FROM generate_series(1, 100) i;

INSERT INTO sorted_merge_test (id, val, num) VALUES (101, NULL, NULL);
INSERT INTO sorted_merge_test (id, val, num) VALUES (102, NULL, NULL);
INSERT INTO sorted_merge_test (id, val, num) VALUES (200, 'dup_a', 10.5);
INSERT INTO sorted_merge_test (id, val, num) VALUES (201, 'dup_b', 10.5);
INSERT INTO sorted_merge_test (id, val, num) VALUES (202, 'dup_c', 10.5);

-- Second table for join tests
CREATE TABLE sorted_merge_events (
    id int,
    event_type text,
    event_val int
);
SELECT create_distributed_table('sorted_merge_events', 'id');

INSERT INTO sorted_merge_events
SELECT i % 50 + 1, CASE WHEN i % 3 = 0 THEN 'click' WHEN i % 3 = 1 THEN 'view' ELSE 'buy' END, i
FROM generate_series(1, 200) i;
--
-- MULTI_SORTED_MERGE
--
-- Tests for the citus.enable_sorted_merge GUC and the sorted merge
-- planner eligibility logic. Verifies that enabling the GUC does not
-- introduce regressions for any query pattern.
--
-- MX verification: this test has been verified to pass with zero diffs
-- under check-base-mx (MX mode), confirming sorted merge works correctly
-- when any node in the cluster acts as coordinator.
--


-- =================================================================
-- 1. GUC basics
-- =================================================================

SHOW citus.enable_sorted_merge;

SET citus.enable_sorted_merge TO on;
SHOW citus.enable_sorted_merge;

SET citus.enable_sorted_merge TO off;

-- =================================================================
-- Category A: Eligibility — sort IS pushed to workers
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- A1: ORDER BY distribution column
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id');

-- A2: ORDER BY DESC
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id DESC');

-- A3: ORDER BY DESC NULLS LAST
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, num FROM sorted_merge_test ORDER BY num DESC NULLS LAST');

-- A4: ORDER BY non-distribution column
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY val');

-- A5: Multi-column ORDER BY
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id, val');

-- A6: Mixed directions
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val, num FROM sorted_merge_test ORDER BY id ASC, num DESC');

-- A7: GROUP BY dist_col ORDER BY dist_col
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id');

-- A8: WHERE clause + ORDER BY
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test WHERE num > 50 ORDER BY id');

-- A9: Expression in ORDER BY (non-aggregate)
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, num FROM sorted_merge_test ORDER BY id + 1');

-- A10: ORDER BY with LIMIT (existing pushdown, verify no regression)
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5');

-- =================================================================
-- Category B: Ineligibility — sort NOT pushed for merge
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- B1: ORDER BY count(*)
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY count(*)');

-- B2: ORDER BY avg(col)
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, avg(num) FROM sorted_merge_test GROUP BY id ORDER BY avg(num)');

-- B3: GROUP BY non-dist col, ORDER BY non-dist col
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT val, count(*) FROM sorted_merge_test GROUP BY val ORDER BY val');

-- B4: GROUP BY non-dist col, ORDER BY aggregate
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT val, count(*) FROM sorted_merge_test GROUP BY val ORDER BY count(*)');

-- =================================================================
-- Category C: Correctness — results match GUC off vs on
-- =================================================================

-- C1: Simple ORDER BY
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;

SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;

-- C2: ORDER BY DESC
SET citus.enable_sorted_merge TO off;
SELECT id FROM sorted_merge_test ORDER BY id DESC LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id FROM sorted_merge_test ORDER BY id DESC LIMIT 5;

-- C3: Multi-column ORDER BY
SET citus.enable_sorted_merge TO off;
SELECT id, num FROM sorted_merge_test ORDER BY num, id LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, num FROM sorted_merge_test ORDER BY num, id LIMIT 5;

-- C4: ORDER BY non-distribution column
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test WHERE val IS NOT NULL ORDER BY val LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test WHERE val IS NOT NULL ORDER BY val LIMIT 5;

-- C5: GROUP BY dist_col ORDER BY dist_col
SET citus.enable_sorted_merge TO off;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

-- C6: Mixed directions
SET citus.enable_sorted_merge TO off;
SELECT id, num FROM sorted_merge_test WHERE num IS NOT NULL ORDER BY id ASC, num DESC LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, num FROM sorted_merge_test WHERE num IS NOT NULL ORDER BY id ASC, num DESC LIMIT 5;

-- C7: WHERE + ORDER BY
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test WHERE num > 100 ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test WHERE num > 100 ORDER BY id LIMIT 5;

-- C8: Aggregates in SELECT, ORDER BY on dist_col (GROUP BY dist_col)
SET citus.enable_sorted_merge TO off;
SELECT id, count(*), sum(num), avg(num) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, count(*), sum(num), avg(num) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

-- =================================================================
-- Category D: Complex queries — regression guards
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- D1: Subquery in FROM with ORDER BY
SELECT * FROM (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 5
) sub ORDER BY id;

-- D2: CTE with ORDER BY
WITH top5 AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 5
)
SELECT * FROM top5 ORDER BY id;

-- D3: Co-located JOIN + ORDER BY
SELECT t.id, t.val, e.event_type
FROM sorted_merge_test t
JOIN sorted_merge_events e ON t.id = e.id
WHERE t.id <= 5
ORDER BY t.id, e.event_type
LIMIT 10;

-- D4: UNION ALL + ORDER BY
SELECT id, val FROM sorted_merge_test WHERE id <= 3
UNION ALL
SELECT id, val FROM sorted_merge_test WHERE id BETWEEN 98 AND 100
ORDER BY id;

-- D5: DISTINCT + ORDER BY
SELECT DISTINCT id FROM sorted_merge_test WHERE id <= 10 ORDER BY id;

-- D6: DISTINCT ON + ORDER BY
SELECT DISTINCT ON (id) id, val, num
FROM sorted_merge_test
WHERE id <= 5
ORDER BY id, num DESC;

-- D7: EXISTS subquery + ORDER BY
SELECT id, val FROM sorted_merge_test t
WHERE EXISTS (SELECT 1 FROM sorted_merge_events e WHERE e.id = t.id)
ORDER BY id LIMIT 5;

-- D8: IN subquery + ORDER BY
SELECT id, val FROM sorted_merge_test
WHERE id IN (SELECT id FROM sorted_merge_events WHERE event_type = 'click')
ORDER BY id LIMIT 5;

-- D9: Multiple aggregates, GROUP BY dist_col, ORDER BY dist_col
SELECT id, count(*), sum(num), avg(num), min(val), max(val)
FROM sorted_merge_test
GROUP BY id
ORDER BY id
LIMIT 5;

-- D10: CASE expression in SELECT + ORDER BY
SELECT id,
       CASE WHEN num > 75 THEN 'high' WHEN num > 25 THEN 'mid' ELSE 'low' END as bucket
FROM sorted_merge_test
WHERE num IS NOT NULL
ORDER BY id
LIMIT 10;

-- D11: NULL values ordering
SELECT id, num FROM sorted_merge_test ORDER BY num NULLS FIRST, id LIMIT 5;
SELECT id, num FROM sorted_merge_test ORDER BY num NULLS LAST, id LIMIT 5;
SELECT id, num FROM sorted_merge_test ORDER BY num DESC NULLS FIRST, id LIMIT 5;
SELECT id, num FROM sorted_merge_test ORDER BY num DESC NULLS LAST, id DESC LIMIT 5;

-- D12: Large OFFSET
SELECT id FROM sorted_merge_test ORDER BY id OFFSET 100 LIMIT 5;

-- D13: ORDER BY ordinal position
SELECT id, val FROM sorted_merge_test ORDER BY 2, 1 LIMIT 5;

-- =================================================================
-- Category E: Edge cases
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- E1: Empty result set
SELECT id FROM sorted_merge_test WHERE id < 0 ORDER BY id;

-- E2: Single row (may go through router planner)
SELECT id, val FROM sorted_merge_test WHERE id = 42 ORDER BY id;

-- E3: All rows with same sort value
SELECT id, num FROM sorted_merge_test WHERE num = 10.5 ORDER BY num, id;

-- E4: Wide sort key (4 columns)
SELECT id, val, num FROM sorted_merge_test
WHERE id <= 5
ORDER BY num, val, id
LIMIT 5;

-- E5: Zero-task defensive path
-- CreatePerTaskDispatchDest handles taskCount=0 gracefully (returns a no-op
-- destination). This cannot be triggered via normal SQL because distributed
-- tables always have at least one shard. The closest we can test is an
-- empty-result query through the sorted merge path to verify no crash.
SELECT id FROM sorted_merge_test WHERE false ORDER BY id;

-- =================================================================
-- Category F: Existing LIMIT pushdown stability
-- =================================================================

-- F1: Simple LIMIT + ORDER BY: plan unchanged between GUC off and on
SET citus.enable_sorted_merge TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5');

SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5');

-- F2: GROUP BY dist_col + ORDER BY + LIMIT
SET citus.enable_sorted_merge TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5');

SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5');

-- F3: ORDER BY aggregate + LIMIT (not eligible for merge)
SET citus.enable_sorted_merge TO off;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC, id LIMIT 5;

SET citus.enable_sorted_merge TO on;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC, id LIMIT 5;

-- =================================================================
-- Category G: Phase 4 — Sort elision and advanced scenarios
-- =================================================================

-- G1: Sort elision verification — coordinator Sort node absent
SET citus.enable_sorted_merge TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id');

SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id');

-- G2a: PREPARE with merge ON, EXECUTE after turning OFF
-- Plan-time decision is baked in — cached plan must still merge correctly.
-- Execute 6+ times to trigger PostgreSQL's generic plan caching, then
-- verify the plan shape is preserved after toggling the GUC.
SET citus.enable_sorted_merge TO on;
PREPARE merge_on_stmt AS SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;
EXECUTE merge_on_stmt;
EXECUTE merge_on_stmt;
EXECUTE merge_on_stmt;
EXECUTE merge_on_stmt;
EXECUTE merge_on_stmt;
EXECUTE merge_on_stmt;
-- Verify plan shape after caching — no Sort above CustomScan
EXPLAIN (COSTS OFF) EXECUTE merge_on_stmt;
SET citus.enable_sorted_merge TO off;
-- Cached plan retains the sorted merge decision from planning time
EXECUTE merge_on_stmt;
EXPLAIN (COSTS OFF) EXECUTE merge_on_stmt;
DEALLOCATE merge_on_stmt;

-- G2b: PREPARE with merge OFF, EXECUTE after turning ON
-- Cached plan has Sort node — must still return sorted results.
SET citus.enable_sorted_merge TO off;
PREPARE merge_off_stmt AS SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;
EXECUTE merge_off_stmt;
EXECUTE merge_off_stmt;
EXECUTE merge_off_stmt;
EXECUTE merge_off_stmt;
EXECUTE merge_off_stmt;
EXECUTE merge_off_stmt;
-- Verify plan shape after caching — Sort above CustomScan
EXPLAIN (COSTS OFF) EXECUTE merge_off_stmt;
SET citus.enable_sorted_merge TO on;
-- Cached plan retains the non-merge decision from planning time
EXECUTE merge_off_stmt;
EXPLAIN (COSTS OFF) EXECUTE merge_off_stmt;
DEALLOCATE merge_off_stmt;

-- G3: Cursor with backward scan (non-SCROLL)
-- Streaming sorted merge is forward-only; backward fetch must error.
-- Use ROLLBACK because the failed FETCH BACKWARD aborts the transaction.
SET citus.enable_sorted_merge TO on;
BEGIN;
DECLARE sorted_cursor CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 3 FROM sorted_cursor;
FETCH BACKWARD 1 FROM sorted_cursor;
ROLLBACK;

-- G3b: SCROLL cursor with backward scan
SET citus.enable_sorted_merge TO on;
BEGIN;
DECLARE sorted_scroll_cursor SCROLL CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 3 FROM sorted_scroll_cursor;
FETCH BACKWARD 1 FROM sorted_scroll_cursor;
FETCH 2 FROM sorted_scroll_cursor;
CLOSE sorted_scroll_cursor;
COMMIT;

-- G4: EXPLAIN ANALYZE (sorted merge skipped for EXPLAIN ANALYZE)
SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5');

-- G5: ORDER BY aggregate + LIMIT — crash regression test
-- Previously caused SIGSEGV when sorted merge was enabled because
-- aggregate ORDER BY was erroneously tagged as merge-eligible.
SET citus.enable_sorted_merge TO on;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC, id LIMIT 3;

-- G6: Small work_mem with many tasks (32 shards)
SET citus.enable_sorted_merge TO on;
SET work_mem TO '64kB';
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 10;
RESET work_mem;

-- G7: max_intermediate_result_size with CTE subplan
SET citus.enable_sorted_merge TO on;
SET citus.max_intermediate_result_size TO '4kB';
WITH cte AS (SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 50)
SELECT * FROM cte ORDER BY id LIMIT 5;
RESET citus.max_intermediate_result_size;

-- =================================================================
-- Category H: Subplan + Sorted Merge interactions
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- H1: CTE subplan with simple ORDER BY — eligible for sorted merge
-- The CTE becomes a subplan; its DistributedPlan may have useSortedMerge=true
WITH ordered_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id
)
SELECT * FROM ordered_cte ORDER BY id LIMIT 5;

-- H2: Multiple CTEs — one eligible (ORDER BY col), one ineligible (ORDER BY agg)
WITH eligible_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
),
ineligible_cte AS (
    SELECT id, count(*) as cnt FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC, id LIMIT 15
)
SELECT e.id, e.val, i.cnt
FROM eligible_cte e JOIN ineligible_cte i ON e.id = i.id
ORDER BY e.id;

-- H3: CTE subplan feeding outer ORDER BY — both levels may merge independently
WITH top_ids AS (
    SELECT id FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT t.id, t.val
FROM sorted_merge_test t
JOIN top_ids ON t.id = top_ids.id
ORDER BY t.id
LIMIT 10;

-- H4: Subquery in WHERE with ORDER BY + LIMIT — becomes subplan with merge
SELECT id, val FROM sorted_merge_test
WHERE id IN (
    SELECT id FROM sorted_merge_events ORDER BY id LIMIT 10
)
ORDER BY id
LIMIT 5;

-- H5: CTE subplan with max_intermediate_result_size enforcement
-- Tests that EnsureIntermediateSizeLimitNotExceeded works through per-task dispatch
SET citus.max_intermediate_result_size TO '4kB';
WITH small_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM small_cte ORDER BY id LIMIT 5;
RESET citus.max_intermediate_result_size;

-- H6: Cross-join subplan with non-aggregate ORDER BY (crash regression variant)
-- Similar pattern to subquery_complex_target_list but without aggregate ORDER BY
SELECT foo.id, bar.id as bar_id
FROM
    (SELECT id FROM sorted_merge_test ORDER BY id LIMIT 3) as foo,
    (SELECT id FROM sorted_merge_events ORDER BY id LIMIT 3) as bar
ORDER BY foo.id, bar.id
LIMIT 5;

-- H7: CTE correctness comparison — GUC off vs on must produce identical results
SET citus.enable_sorted_merge TO off;
WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5;

-- =================================================================
-- Category H EXPLAIN: Query plans for subplan + sorted merge
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- H1 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH ordered_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id
)
SELECT * FROM ordered_cte ORDER BY id LIMIT 5');

-- H2 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH eligible_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
),
ineligible_cte AS (
    SELECT id, count(*) as cnt FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC, id LIMIT 15
)
SELECT e.id, e.val, i.cnt
FROM eligible_cte e JOIN ineligible_cte i ON e.id = i.id
ORDER BY e.id');

-- H3 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH top_ids AS (
    SELECT id FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT t.id, t.val
FROM sorted_merge_test t
JOIN top_ids ON t.id = top_ids.id
ORDER BY t.id
LIMIT 10');

-- H4 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test
WHERE id IN (
    SELECT id FROM sorted_merge_events ORDER BY id LIMIT 10
)
ORDER BY id
LIMIT 5');

-- H5 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH small_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM small_cte ORDER BY id LIMIT 5');

-- H6 EXPLAIN
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT foo.id, bar.id as bar_id
FROM
    (SELECT id FROM sorted_merge_test ORDER BY id LIMIT 3) as foo,
    (SELECT id FROM sorted_merge_events ORDER BY id LIMIT 3) as bar
ORDER BY foo.id, bar.id
LIMIT 5');

-- H7 EXPLAIN — GUC off vs on
SET citus.enable_sorted_merge TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5');

SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5');

-- =================================================================
-- Category I: Distributed Transactions
-- =================================================================
-- Verify sorted merge correctness within multi-statement transactions
-- where data is modified before the sorted-merge SELECT.

SET citus.enable_sorted_merge TO on;

-- I1: INSERT then SELECT within a transaction
BEGIN;
INSERT INTO sorted_merge_test (id, val, num) VALUES (900, 'txn_insert', 900.0);
SELECT id, val FROM sorted_merge_test WHERE id >= 900 ORDER BY id;
ROLLBACK;

-- I2: UPDATE then SELECT within a transaction
BEGIN;
UPDATE sorted_merge_test SET val = 'updated' WHERE id = 1;
SELECT id, val FROM sorted_merge_test WHERE id <= 3 ORDER BY id;
ROLLBACK;

-- I3: DELETE then SELECT within a transaction
BEGIN;
DELETE FROM sorted_merge_test WHERE id <= 5;
SELECT id, val FROM sorted_merge_test WHERE id <= 10 ORDER BY id;
ROLLBACK;

-- I4: INSERT + UPDATE + SELECT with multi-column ORDER BY
BEGIN;
INSERT INTO sorted_merge_test (id, val, num) VALUES (901, 'txn_a', 1.0);
INSERT INTO sorted_merge_test (id, val, num) VALUES (902, 'txn_b', 2.0);
INSERT INTO sorted_merge_test (id, val, num) VALUES (903, 'txn_c', 3.0);
UPDATE sorted_merge_test SET num = 999.0 WHERE id = 901;
SELECT id, val, num FROM sorted_merge_test WHERE id >= 900 ORDER BY num, id;
ROLLBACK;

-- I5: Compare results with GUC off vs on in a transaction
BEGIN;
INSERT INTO sorted_merge_test (id, val, num) VALUES (910, 'cmp_a', 10.0);
INSERT INTO sorted_merge_test (id, val, num) VALUES (911, 'cmp_b', 20.0);
INSERT INTO sorted_merge_test (id, val, num) VALUES (912, 'cmp_c', 30.0);
SET LOCAL citus.enable_sorted_merge TO off;
SELECT id, val, num FROM sorted_merge_test WHERE id >= 910 ORDER BY id;
SET LOCAL citus.enable_sorted_merge TO on;
SELECT id, val, num FROM sorted_merge_test WHERE id >= 910 ORDER BY id;
ROLLBACK;

-- I6: DELETE + aggregate in SELECT with ORDER BY
BEGIN;
DELETE FROM sorted_merge_test WHERE id > 100 AND id < 200;
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;
ROLLBACK;

-- =================================================================
-- Category J: Coordinator expression evaluation exclusion
-- =================================================================
-- Verify that queries with ORDER BY on expressions that need coordinator-side
-- evaluation are correctly excluded from sorted merge (or handled correctly).

SET citus.enable_sorted_merge TO on;

-- J1: ORDER BY expression on aggregate result (ordinal reference)
-- The ORDER BY references position 2 which is an aggregate — sorted merge
-- must NOT be used because aggregates are rewritten between worker/coordinator.
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, sum(num) AS total FROM sorted_merge_test GROUP BY id ORDER BY 2 LIMIT 5');

-- J2: ORDER BY expression wrapping an aggregate
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, sum(num) + 1 AS total_plus FROM sorted_merge_test GROUP BY id ORDER BY sum(num) + 1 LIMIT 5');

-- J3: ORDER BY a non-aggregate expression that can be pushed to workers
-- This should be eligible for sorted merge — the expression is evaluated
-- on the worker side and sort order is preserved.
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id + 0');

-- J4: ORDER BY with CASE expression (no aggregates) — eligible
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY CASE WHEN id < 50 THEN 0 ELSE 1 END, id');

-- J5: ORDER BY on an expression that mixes aggregate and non-aggregate
-- Should be ineligible because the expression contains an aggregate.
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id + count(*)');

-- J6: Correctness comparison — expression ORDER BY, GUC off vs on
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test ORDER BY id + 0 LIMIT 5;
SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test ORDER BY id + 0 LIMIT 5;

-- -----------------------------------------------------------------
-- J7–J12: Additional pushable expressions (no aggregates)
-- -----------------------------------------------------------------

SET citus.enable_sorted_merge TO on;

-- J7: ORDER BY function call on column
SELECT id, val FROM sorted_merge_test ORDER BY upper(val) LIMIT 5;

-- J8: ORDER BY COALESCE
SELECT id, num FROM sorted_merge_test ORDER BY COALESCE(num, 0) LIMIT 5;

-- J9: ORDER BY negation
SELECT id, num FROM sorted_merge_test ORDER BY -num LIMIT 5;

-- J10: ORDER BY concatenation
SELECT id, val FROM sorted_merge_test ORDER BY val || '_suffix' LIMIT 5;

-- J11: ORDER BY mathematical function (abs distance)
SELECT id, num FROM sorted_merge_test ORDER BY abs(num - 25), id LIMIT 5;

-- J12: ORDER BY expression not in SELECT list
SELECT id FROM sorted_merge_test ORDER BY num + 1 LIMIT 5;

-- J13: ORDER BY expression referencing multiple columns
SELECT id, val FROM sorted_merge_test ORDER BY id * num LIMIT 5;

-- J14: ORDER BY with type cast
SELECT id, num FROM sorted_merge_test ORDER BY num::int LIMIT 5;

-- J15: ORDER BY with subexpression in SELECT and different expression in ORDER BY
SELECT id, num + 1 as n1 FROM sorted_merge_test ORDER BY num + 2 LIMIT 5;

-- J16: ORDER BY column alias
SELECT id, num * 2 as doubled FROM sorted_merge_test ORDER BY doubled LIMIT 5;

-- -----------------------------------------------------------------
-- J17–J21: Correctness — GUC off vs on for expression ORDER BY
-- -----------------------------------------------------------------

-- J17: function call
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test ORDER BY upper(val) LIMIT 5;
SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test ORDER BY upper(val) LIMIT 5;

-- J18: CASE expression
SET citus.enable_sorted_merge TO off;
SELECT id, CASE WHEN num > 50 THEN 'high' ELSE 'low' END as cat
FROM sorted_merge_test ORDER BY CASE WHEN num > 50 THEN 'high' ELSE 'low' END, id LIMIT 10;
SET citus.enable_sorted_merge TO on;
SELECT id, CASE WHEN num > 50 THEN 'high' ELSE 'low' END as cat
FROM sorted_merge_test ORDER BY CASE WHEN num > 50 THEN 'high' ELSE 'low' END, id LIMIT 10;

-- J19: COALESCE
SET citus.enable_sorted_merge TO off;
SELECT id, num FROM sorted_merge_test ORDER BY COALESCE(num, 0), id LIMIT 5;
SET citus.enable_sorted_merge TO on;
SELECT id, num FROM sorted_merge_test ORDER BY COALESCE(num, 0), id LIMIT 5;

-- J20: abs() distance function
SET citus.enable_sorted_merge TO off;
SELECT id, num FROM sorted_merge_test ORDER BY abs(num - 25), id LIMIT 5;
SET citus.enable_sorted_merge TO on;
SELECT id, num FROM sorted_merge_test ORDER BY abs(num - 25), id LIMIT 5;

-- -----------------------------------------------------------------
-- J21–J22: More ineligibility — aggregate inside expressions
-- -----------------------------------------------------------------

SET citus.enable_sorted_merge TO on;

-- J21: ORDER BY CASE wrapping an aggregate
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY CASE WHEN count(*) > 1 THEN 0 ELSE 1 END, id LIMIT 5');

-- J22: ORDER BY aggregate expression (sum + 1) — correctness
SET citus.enable_sorted_merge TO off;
SELECT id, sum(num) + 1 as s FROM sorted_merge_test GROUP BY id ORDER BY sum(num) + 1 LIMIT 5;
SET citus.enable_sorted_merge TO on;
SELECT id, sum(num) + 1 as s FROM sorted_merge_test GROUP BY id ORDER BY sum(num) + 1 LIMIT 5;

-- -----------------------------------------------------------------
-- J23–J24: EXPLAIN plans for pushable expression patterns
-- -----------------------------------------------------------------

SET citus.enable_sorted_merge TO on;

-- J23: Does function-call ORDER BY get pushed to workers?
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY upper(val) LIMIT 5');

-- J24: ORDER BY expression not in SELECT list — pushed to workers?
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY num + 1 LIMIT 5');

-- =================================================================
-- Category K: Index-based sort avoidance
-- =================================================================
-- When an index exists on the ORDER BY column, PostgreSQL's worker-side
-- planner should choose an Index Scan instead of Sort + Seq Scan, making
-- the worker-side sort essentially free. This is the best-case scenario
-- for sorted merge: zero worker sort cost + zero coordinator sort cost.
--
-- We disable enable_seqscan to force the worker planner to prefer the
-- index, since the test table is small enough that Seq Scan + Sort
-- would otherwise be cheaper.

CREATE INDEX sorted_merge_test_id_idx ON sorted_merge_test(id);

-- Use a transaction with SET LOCAL to propagate enable_seqscan=off to workers,
-- forcing the worker planner to use the index instead of Seq Scan + Sort.
SET citus.propagate_set_commands TO 'local';

-- K1: EXPLAIN with index — worker uses Index Scan, no Sort node
SET citus.enable_sorted_merge TO on;
BEGIN;
SET LOCAL enable_seqscan TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id');
COMMIT;

-- K2: Correctness with index — GUC off vs on
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 5;
SET LOCAL citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 5;
COMMIT;

-- K3: Multi-column index
CREATE INDEX sorted_merge_test_num_id_idx ON sorted_merge_test(num, id);

SET citus.enable_sorted_merge TO on;
BEGIN;
SET LOCAL enable_seqscan TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, num FROM sorted_merge_test ORDER BY num, id');
COMMIT;

-- K4: Correctness with multi-column index — GUC off vs on
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL citus.enable_sorted_merge TO off;
SELECT id, num FROM sorted_merge_test ORDER BY num, id LIMIT 5;
SET LOCAL citus.enable_sorted_merge TO on;
SELECT id, num FROM sorted_merge_test ORDER BY num, id LIMIT 5;
COMMIT;

-- K5: DESC ordering with index
SET citus.enable_sorted_merge TO on;
BEGIN;
SET LOCAL enable_seqscan TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY id DESC');
COMMIT;

RESET citus.propagate_set_commands;
DROP INDEX sorted_merge_test_id_idx;
DROP INDEX sorted_merge_test_num_id_idx;

-- =================================================================
-- Category L: Volatile and stable functions in ORDER BY
-- Tests that ORDER BY with functions works correctly with sorted merge.
-- Volatile functions (random, clock_timestamp, timeofday) are pushed
-- to workers as computed columns — sorted merge uses the materialized
-- worker values, which is semantically equivalent to coordinator Sort.
-- =================================================================

-- L1: STABLE function — now() in expression with column
-- now() returns the same value on all workers within a transaction,
-- so the merge is globally consistent. Sorted merge should be used.
SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY now() - ts, id');

-- L2: VOLATILE function — random() in ORDER BY
-- random() is pushed to workers as worker_column_3; each worker sorts
-- by its own random values. The merge interleaves using materialized
-- values — semantically equivalent to coordinator Sort on worker_column_3.
-- Test plan shape only (result is non-deterministic).
SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY random(), id');

-- L3: VOLATILE function — clock_timestamp() in ORDER BY
-- Same mechanics as random(): pushed to workers, sorted locally, merged.
SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY clock_timestamp(), id');

-- L4: nextval() in ORDER BY with sorted merge ON — expected ERROR
-- nextval() cannot be pushed to workers (CanPushDownExpression blocks it).
-- The sort clause references a target entry missing from the worker target
-- list, causing a plan-time error. This is a pre-existing Citus limitation.
CREATE SEQUENCE sorted_merge_test_seq;
SET citus.enable_sorted_merge TO on;
SELECT id, val FROM sorted_merge_test ORDER BY nextval('sorted_merge_test_seq');

-- L4b: nextval() in ORDER BY with sorted merge OFF but LIMIT present
-- Same error — demonstrates this is NOT a sorted merge regression.
SET citus.enable_sorted_merge TO off;
SELECT id, val FROM sorted_merge_test ORDER BY nextval('sorted_merge_test_seq') LIMIT 5;
DROP SEQUENCE sorted_merge_test_seq;

-- L5: STABLE function alone (constant-fold case)
-- current_timestamp is constant-folded by the planner; the sort key
-- effectively becomes just 'id'. Sorted merge should be used.
SET citus.enable_sorted_merge TO on;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id, val FROM sorted_merge_test ORDER BY current_timestamp, id');

SET citus.enable_sorted_merge TO off;

-- =================================================================
-- Category L6: EXPLAIN ANALYZE + sorted merge
--
-- Verify that sorted merge works correctly when the EXPLAIN ANALYZE
-- code path is active.  We test two mechanisms:
--
-- 1. Plain EXPLAIN ANALYZE: verifies plan structure (no coordinator
--    Sort node, "Custom Scan (Citus Sorted Merge Adaptive)" visible).
--
-- 2. auto_explain with log_analyze: triggers the same executor code
--    path (es_instrument != 0 → RequestedForExplainAnalyze() = true)
--    but returns actual data rows.  This directly validates that the
--    k-way merge produces correctly sorted output under the EXPLAIN
--    ANALYZE path — if the merge were skipped, the rows would be
--    visibly unsorted.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- Verify EXPLAIN ANALYZE plan structure: no Sort node at coordinator
-- level, "Custom Scan (Citus Sorted Merge Adaptive)" visible, and
-- "actual rows" confirms full execution through the merge path.
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id');

-- Load auto_explain to trigger the EXPLAIN ANALYZE executor path
-- while returning real data rows.  auto_explain sets es_instrument,
-- which makes RequestedForExplainAnalyze() return true — the same
-- condition as a real EXPLAIN ANALYZE.
LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_analyze TO true;

-- ASC sort under auto_explain: these SELECTs go through the EXPLAIN
-- ANALYZE code path but return actual data.  If the merge were
-- skipped, rows would arrive in arbitrary worker order.
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 10;

-- DESC sort under auto_explain
SELECT id FROM sorted_merge_test ORDER BY id DESC LIMIT 10;

-- Multi-column sort under auto_explain
SELECT id, val FROM sorted_merge_test ORDER BY id, val LIMIT 10;

-- Single-column sort on num (non-distribution column, has NULLs)
SELECT num FROM sorted_merge_test ORDER BY num LIMIT 10;

-- Multi-column sort with num as first column
SELECT num, id FROM sorted_merge_test ORDER BY num, id LIMIT 10;

-- Multi-column sort with num DESC as first column, id ASC
SELECT num, id FROM sorted_merge_test ORDER BY num DESC, id LIMIT 10;

-- Disable auto_explain
SET auto_explain.log_min_duration = -1;
SET auto_explain.log_analyze TO false;

-- Contrast: sorted merge OFF shows a Sort node at coordinator level.
SET citus.enable_sorted_merge TO off;
SELECT public.explain_filter('EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT id FROM sorted_merge_test ORDER BY id');

SET citus.enable_sorted_merge TO off;

-- =================================================================
-- Category M: Additional cursor backward-scan coverage (Phase B / T13)
--
-- The streaming adapter's "if (unlikely(!forward))" guard in
-- FetchNextScanTuple() is defensive — the planner drops
-- CUSTOMPATH_SUPPORT_BACKWARD_SCAN for sorted-merge plans and inserts
-- a Material node above SCROLL cursors, so PostgreSQL's portal layer
-- intercepts FETCH BACKWARD on non-SCROLL cursors before reaching us.
-- These tests document the resulting user-visible behavior.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- M1: FETCH PRIOR on a non-SCROLL cursor must also error
BEGIN;
DECLARE prior_cursor CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 2 FROM prior_cursor;
FETCH PRIOR FROM prior_cursor;
ROLLBACK;

-- M2: FETCH ABSOLUTE 0 (rewind to start) on non-SCROLL must error
BEGIN;
DECLARE abs_cursor CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 2 FROM abs_cursor;
FETCH ABSOLUTE 0 FROM abs_cursor;
ROLLBACK;

-- M3: SCROLL cursor with FETCH PRIOR / FETCH ABSOLUTE — Material node
-- above the CustomScan serves the backward fetches transparently.
BEGIN;
DECLARE scroll_cur SCROLL CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 3 FROM scroll_cur;
FETCH PRIOR FROM scroll_cur;
FETCH ABSOLUTE 1 FROM scroll_cur;
FETCH LAST FROM scroll_cur;
CLOSE scroll_cur;
COMMIT;

-- =================================================================
-- Category N: SortedMergeAdapterRescan() reachability (Phase B / T17)
--
-- This category preserves the SQL shape that DOES drive PG's
-- ExecutorRewind path on a sorted-merge plan: a SCROLL CURSOR WITH
-- HOLD that is partially fetched, then committed.
--
-- IMPORTANT — in production this query does *not* actually invoke
-- SortedMergeAdapterRescan().  Citus' planner inserts a Material
-- node above sorted-merge SCROLL plans via materialize_finished_plan()
-- in distributed_planner.c, and Material's ExecReScan rewinds its own
-- tuplestore without descending to the Citus custom scan.  See
-- rescan_experiments.md for the full investigation.
--
-- We keep the test here for two reasons:
--   1. It documents the SQL shape that exercises the SCROLL HOLD code
--      path in PG, which is the only PG entry point that calls
--      ExecutorRewind() on a portal plan tree.
--   2. If the Material insertion is ever changed (or a future PG
--      version routes WITH HOLD differently), this test will start
--      driving SortedMergeAdapterRescan() and any breakage there will
--      surface in the regression diff.
--
-- The expected behavior here is forward-only: FETCH 3 returns the
-- first three rows; COMMIT persists the cursor (Material absorbs the
-- rescan); FETCH 3 returns the next three rows from the holdStore.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- N1: SCROLL CURSOR WITH HOLD + partial fetch + COMMIT.
-- This is the only SQL pattern that puts PG on the path to invoking
-- our rescan callback (PersistHoldablePortal calls ExecutorRewind
-- iff CURSOR_OPT_SCROLL is set).  Material above us absorbs the
-- rescan in production; the test still exercises the BeginScan ->
-- forward drain -> Material persistence -> Free lifecycle and
-- documents the exact shape that the patch-out experiment showed
-- can drive SortedMergeAdapterRescan when Material is removed.
BEGIN;
DECLARE rescan_cur SCROLL CURSOR WITH HOLD FOR
    SELECT id FROM sorted_merge_test ORDER BY id LIMIT 10;
FETCH 3 FROM rescan_cur;
COMMIT;
FETCH 3 FROM rescan_cur;
CLOSE rescan_cur;

-- N1-EXPLAIN: Plan shape for the SCROLL HOLD source query.  The
-- "Materialize" node above the Custom Scan is the absorber that
-- prevents SortedMergeAdapterRescan from firing during COMMIT.  When
-- this node disappears (e.g. the materialize_finished_plan call is
-- removed for testing), the rescan callback fires as designed.
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    DECLARE rescan_cur SCROLL CURSOR WITH HOLD FOR
        SELECT id FROM sorted_merge_test ORDER BY id LIMIT 10');

-- =================================================================
-- Category O: work_mem stress (Phase B / T18)
--
-- Per-task tuplestores have a 64 kB floor; with many tasks the
-- aggregate budget can exceed work_mem.  These tests force per-task
-- spill-to-disk (work_mem = 64 kB → each store gets exactly the
-- floor) and verify correctness under memory pressure.
--
-- We surface the DEBUG2 message emitted by AssignPerTaskDispatchDests()
-- so the test output includes the per-task / aggregate / session
-- work_mem report.  We use SET LOCAL inside a transaction so the
-- message level scrubs back to its default afterwards.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- O1: First and last rows must match the unstressed sort.  Both
-- queries activate sorted merge (Merge Method line in EXPLAIN below).
-- The DEBUG2 line printed before each result confirms the per-task
-- budget actually drops to the 64 kB floor under tight work_mem.
BEGIN;
SET LOCAL work_mem TO '64kB';
SET LOCAL client_min_messages TO DEBUG2;
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5;
SELECT id FROM sorted_merge_test ORDER BY id DESC LIMIT 5;
COMMIT;

-- O1-EXPLAIN: Plans for the LIMIT 5 ASC/DESC cases under tight
-- work_mem — confirms LIMIT pushdown is unaffected by the memory
-- setting and that "Custom Scan (Citus Sorted Merge Adaptive)" still
-- appears.
BEGIN;
SET LOCAL work_mem TO '64kB';
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5');
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id FROM sorted_merge_test ORDER BY id DESC LIMIT 5');
COMMIT;

-- =================================================================
-- Category P: Window functions and COLLATE (Phase B / T21)
--
-- Window functions over distributed tables and explicit collations
-- in ORDER BY both must propagate correctly to worker queries.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- P1: row_number() with global window — global windows are NOT
-- pushable, so this disables sorted merge by the planner eligibility
-- gate.  Test confirms result correctness.
SELECT id, row_number() OVER (ORDER BY id) AS rn
FROM sorted_merge_test
WHERE id <= 5
ORDER BY id;

-- P2: EXPLAIN — for a global window, the coordinator Sort + WindowAgg
-- sits above a non-merging Citus scan (the plan node is "Custom Scan
-- (Citus Adaptive)", not "Custom Scan (Citus Sorted Merge Adaptive)").
-- This documents the planner's correct rejection of merge eligibility
-- when a non-pushable window is present.
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM sorted_merge_test
    ORDER BY id LIMIT 5');

-- P2b: row_number() with PARTITION BY on the distribution column —
-- this is a pushable window and sorted merge SHOULD activate.
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id, row_number() OVER (PARTITION BY id ORDER BY val) AS rn
    FROM sorted_merge_test
    ORDER BY id LIMIT 5');

-- P3: Explicit COLLATE "C" in ORDER BY — collation must propagate to
-- the worker SortSupport so the on-coordinator k-way merge agrees
-- with the worker-side sort order.
SELECT val
FROM sorted_merge_test
WHERE val IS NOT NULL
ORDER BY val COLLATE "C"
LIMIT 5;

-- P4: COLLATE "C" combined with multi-column ORDER BY
SELECT id, val
FROM sorted_merge_test
WHERE val IS NOT NULL
ORDER BY val COLLATE "C", id
LIMIT 5;

-- =================================================================
-- Category Q: onurctirtir review checklist (Phase B / B5)
--
-- Coverage for queries called out in the top-level review:
-- INSERT…SELECT…ORDER BY…LIMIT, router queries, dropped columns,
-- zero-output-column SELECT, LIMIT 0, single-shard table.
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- Q1: INSERT … SELECT … ORDER BY … LIMIT
-- Sorted merge is plan-time; the SELECT subplan should still produce
-- correctly ordered tuples for the INSERT to consume.
CREATE TABLE sorted_merge_target (id int, val text);
SELECT create_distributed_table('sorted_merge_target', 'id');

INSERT INTO sorted_merge_target
SELECT id, val FROM sorted_merge_test
WHERE id IS NOT NULL
ORDER BY id, val
LIMIT 5;

SELECT id, val FROM sorted_merge_target ORDER BY id, val;
DROP TABLE sorted_merge_target;

-- Q2: Router (single-shard) query — should bypass sorted merge.
-- The SELECT runs on a single shard, so there is no k-way merge and
-- EXPLAIN must NOT show "Custom Scan (Citus Sorted Merge Adaptive)".
SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id, val FROM sorted_merge_test
    WHERE id = 1
    ORDER BY id, val LIMIT 5');

SELECT id, val FROM sorted_merge_test
WHERE id = 1
ORDER BY id, val LIMIT 5;

-- Q3: Tables with dropped columns — important regression case.
-- After ALTER TABLE DROP COLUMN, attribute numbers in the worker
-- output must still align with SortedMergeKey.attno.
CREATE TABLE sorted_merge_dropcol (
    drop_a int,
    id int,
    drop_b text,
    val text,
    drop_c numeric
);
SELECT create_distributed_table('sorted_merge_dropcol', 'id');

INSERT INTO sorted_merge_dropcol (drop_a, id, drop_b, val, drop_c)
SELECT i * 10, i, 'pre_' || i, 'val_' || i, (i * 0.5)::numeric
FROM generate_series(1, 20) i;

ALTER TABLE sorted_merge_dropcol DROP COLUMN drop_a;
ALTER TABLE sorted_merge_dropcol DROP COLUMN drop_b;
ALTER TABLE sorted_merge_dropcol DROP COLUMN drop_c;

SELECT id, val FROM sorted_merge_dropcol ORDER BY id LIMIT 5;
SELECT id, val FROM sorted_merge_dropcol ORDER BY val, id LIMIT 5;

SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id, val FROM sorted_merge_dropcol ORDER BY id LIMIT 5');

DROP TABLE sorted_merge_dropcol;

-- Q4: Zero-output-column SELECT — Postgres allows SELECT FROM t
-- (no target list), and so does Citus.  Sorted merge must not crash.
SELECT FROM sorted_merge_test ORDER BY id LIMIT 5;

-- Q5: LIMIT 0 — empty result set, exercises the perTaskStoreCount==0
-- early return in FinalizeSortedMerge.
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 0;
SELECT id, val, num FROM sorted_merge_test ORDER BY id, val LIMIT 0;

-- Q6: Single-shard distributed table — only one task to merge, so the
-- plan is effectively a passthrough; verifies no crash and correct
-- output for the K=1 corner case.
CREATE TABLE sorted_merge_single (id int, val text);
SET citus.shard_count TO 1;
SELECT create_distributed_table('sorted_merge_single', 'id');
RESET citus.shard_count;

INSERT INTO sorted_merge_single
SELECT i, 'v_' || i FROM generate_series(1, 10) i;

SELECT id, val FROM sorted_merge_single ORDER BY id, val;
SELECT id FROM sorted_merge_single ORDER BY id LIMIT 3;

SELECT public.explain_filter('EXPLAIN (COSTS OFF, VERBOSE OFF)
    SELECT id, val FROM sorted_merge_single ORDER BY id, val');

DROP TABLE sorted_merge_single;

-- Q7: Append-distributed table after TRUNCATE — the planner can produce
-- a sorted-merge plan with an empty task list when all shards are
-- pruned at execution time.  CreatePerTaskDispatchDests must still
-- install a valid (empty) mergeAdapter so the scan returns 0 rows
-- instead of dereferencing NULL.  Regression test for the post-TRUNCATE
-- crash that previously segfaulted on PG16, PG17, and PG18.
--
-- Note: TRUNCATE on an append-distributed table actually *drops* the
-- shards rather than merely emptying them, so the post-TRUNCATE SELECT
-- below runs with zero remote tasks (i.e. an empty task list reaches
-- CreatePerTaskDispatchDests), which is precisely the scenario this
-- test exercises.
CREATE TABLE sorted_merge_append (id int, val text);
SELECT create_distributed_table('sorted_merge_append', 'id', 'append');
SELECT 1 FROM master_create_empty_shard('sorted_merge_append');

INSERT INTO sorted_merge_append VALUES (0, 'a'), (1, 'b');
SELECT id, val FROM sorted_merge_append ORDER BY id, val;

TRUNCATE sorted_merge_append;
SELECT id, val FROM sorted_merge_append ORDER BY id, val;

DROP TABLE sorted_merge_append;

-- Q8: Error during query execution must not poison subsequent
-- executions of the same prepared statement.  PostgreSQL switches a
-- prepared statement to its cached generic plan after the 5th custom
-- execution (G2a uses the same warmup pattern), so we EXECUTE the
-- statement six times before forcing a timeout, then EXECUTE again
-- afterward.  This proves both that:
--   (a) the per-task stores / merge adapter were cleaned up correctly
--       when the cached plan errored mid-flight, and
--   (b) the cached generic plan itself remains usable.
--
-- The pg_sleep in the WHERE clause makes each row arrival visible to
-- statement_timeout; LIMIT 3 keeps the warmup output compact while
-- still exercising the per-task → merge → result-slot pipeline.
PREPARE sorted_merge_ps AS
    SELECT id, val FROM sorted_merge_test
    WHERE pg_sleep(0.02) IS NOT NULL OR pg_sleep(0.02) IS NULL
    ORDER BY id LIMIT 3;

-- Warmup: 6 executions trigger PG's generic-plan caching.
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;

-- Confirm the cached plan is a sorted-merge custom scan (no Sort
-- above it).
EXPLAIN (COSTS OFF) EXECUTE sorted_merge_ps;

-- Force a timeout mid-execution against the now-cached generic plan.
-- 1ms is far shorter than the first pg_sleep(0.02 s) so the cancel
-- fires reliably as soon as the first row is processed.
SET statement_timeout = '1ms';
DO $$
BEGIN
    EXECUTE 'EXECUTE sorted_merge_ps';
EXCEPTION
    WHEN query_canceled THEN
        RAISE NOTICE 'expected timeout (canceling statement due to statement timeout)';
    WHEN OTHERS THEN
        RAISE NOTICE 'expected timeout (%)', SQLERRM;
END$$;
RESET statement_timeout;

-- Subsequent executions must reuse the cached generic plan and
-- produce identical rows, proving error cleanup is correct.
EXECUTE sorted_merge_ps;
EXECUTE sorted_merge_ps;

DEALLOCATE sorted_merge_ps;

SET citus.enable_sorted_merge TO off;

-- =================================================================
-- Cleanup
-- =================================================================

DROP TABLE sorted_merge_test;
DROP TABLE sorted_merge_events;
