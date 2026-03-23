--
-- MULTI_SORTED_MERGE
--
-- Tests for the citus.enable_sorted_merge GUC and the sorted merge
-- planner eligibility logic. Verifies that enabling the GUC does not
-- introduce regressions for any query pattern.
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
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test ORDER BY id;

-- A2: ORDER BY DESC
EXPLAIN (COSTS OFF)
SELECT id FROM sorted_merge_test ORDER BY id DESC;

-- A3: ORDER BY DESC NULLS LAST
EXPLAIN (COSTS OFF)
SELECT id, num FROM sorted_merge_test ORDER BY num DESC NULLS LAST;

-- A4: ORDER BY non-distribution column
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test ORDER BY val;

-- A5: Multi-column ORDER BY
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test ORDER BY id, val;

-- A6: Mixed directions
EXPLAIN (COSTS OFF)
SELECT id, val, num FROM sorted_merge_test ORDER BY id ASC, num DESC;

-- A7: GROUP BY dist_col ORDER BY dist_col
EXPLAIN (COSTS OFF)
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id;

-- A8: WHERE clause + ORDER BY
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test WHERE num > 50 ORDER BY id;

-- A9: Expression in ORDER BY (non-aggregate)
EXPLAIN (COSTS OFF)
SELECT id, num FROM sorted_merge_test ORDER BY id + 1;

-- A10: ORDER BY with LIMIT (existing pushdown, verify no regression)
EXPLAIN (COSTS OFF)
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5;

-- =================================================================
-- Category B: Ineligibility — sort NOT pushed for merge
-- =================================================================

SET citus.enable_sorted_merge TO on;

-- B1: ORDER BY count(*)
EXPLAIN (COSTS OFF)
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY count(*);

-- B2: ORDER BY avg(col)
EXPLAIN (COSTS OFF)
SELECT id, avg(num) FROM sorted_merge_test GROUP BY id ORDER BY avg(num);

-- B3: GROUP BY non-dist col, ORDER BY non-dist col
EXPLAIN (COSTS OFF)
SELECT val, count(*) FROM sorted_merge_test GROUP BY val ORDER BY val;

-- B4: GROUP BY non-dist col, ORDER BY aggregate
EXPLAIN (COSTS OFF)
SELECT val, count(*) FROM sorted_merge_test GROUP BY val ORDER BY count(*);

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
EXPLAIN (COSTS OFF)
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
EXPLAIN (COSTS OFF)
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5;

-- F2: GROUP BY dist_col + ORDER BY + LIMIT
SET citus.enable_sorted_merge TO off;
EXPLAIN (COSTS OFF)
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
EXPLAIN (COSTS OFF)
SELECT id, count(*) FROM sorted_merge_test GROUP BY id ORDER BY id LIMIT 5;

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
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test ORDER BY id;

SET citus.enable_sorted_merge TO on;
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test ORDER BY id;

-- G2a: PREPARE with merge ON, EXECUTE after turning OFF
-- Plan-time decision is baked in — cached plan must still merge correctly
SET citus.enable_sorted_merge TO on;
PREPARE merge_on_stmt AS SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;
EXECUTE merge_on_stmt;
SET citus.enable_sorted_merge TO off;
EXECUTE merge_on_stmt;
DEALLOCATE merge_on_stmt;

-- G2b: PREPARE with merge OFF, EXECUTE after turning ON
-- Cached plan has Sort node — must still return sorted results
SET citus.enable_sorted_merge TO off;
PREPARE merge_off_stmt AS SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 10;
EXECUTE merge_off_stmt;
SET citus.enable_sorted_merge TO on;
EXECUTE merge_off_stmt;
DEALLOCATE merge_off_stmt;

-- G3: Cursor with backward scan
SET citus.enable_sorted_merge TO on;
BEGIN;
DECLARE sorted_cursor CURSOR FOR SELECT id FROM sorted_merge_test ORDER BY id;
FETCH 3 FROM sorted_cursor;
FETCH BACKWARD 1 FROM sorted_cursor;
FETCH 2 FROM sorted_cursor;
CLOSE sorted_cursor;
COMMIT;

-- G4: EXPLAIN ANALYZE (sorted merge skipped for EXPLAIN ANALYZE)
SET citus.enable_sorted_merge TO on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id FROM sorted_merge_test ORDER BY id LIMIT 5;

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
SELECT * FROM ordered_cte LIMIT 5;

-- H2: Multiple CTEs — one eligible (ORDER BY col), one ineligible (ORDER BY agg)
WITH eligible_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
),
ineligible_cte AS (
    SELECT id, count(*) as cnt FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC LIMIT 15
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
EXPLAIN (COSTS OFF)
WITH ordered_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id
)
SELECT * FROM ordered_cte LIMIT 5;

-- H2 EXPLAIN
EXPLAIN (COSTS OFF)
WITH eligible_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
),
ineligible_cte AS (
    SELECT id, count(*) as cnt FROM sorted_merge_test GROUP BY id ORDER BY count(*) DESC LIMIT 15
)
SELECT e.id, e.val, i.cnt
FROM eligible_cte e JOIN ineligible_cte i ON e.id = i.id
ORDER BY e.id;

-- H3 EXPLAIN
EXPLAIN (COSTS OFF)
WITH top_ids AS (
    SELECT id FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT t.id, t.val
FROM sorted_merge_test t
JOIN top_ids ON t.id = top_ids.id
ORDER BY t.id
LIMIT 10;

-- H4 EXPLAIN
EXPLAIN (COSTS OFF)
SELECT id, val FROM sorted_merge_test
WHERE id IN (
    SELECT id FROM sorted_merge_events ORDER BY id LIMIT 10
)
ORDER BY id
LIMIT 5;

-- H5 EXPLAIN
EXPLAIN (COSTS OFF)
WITH small_cte AS (
    SELECT id, val FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM small_cte ORDER BY id LIMIT 5;

-- H6 EXPLAIN
EXPLAIN (COSTS OFF)
SELECT foo.id, bar.id as bar_id
FROM
    (SELECT id FROM sorted_merge_test ORDER BY id LIMIT 3) as foo,
    (SELECT id FROM sorted_merge_events ORDER BY id LIMIT 3) as bar
ORDER BY foo.id, bar.id
LIMIT 5;

-- H7 EXPLAIN — GUC off vs on
SET citus.enable_sorted_merge TO off;
EXPLAIN (COSTS OFF)
WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5;

SET citus.enable_sorted_merge TO on;
EXPLAIN (COSTS OFF)
WITH cte AS (
    SELECT id, val, num FROM sorted_merge_test ORDER BY id LIMIT 20
)
SELECT * FROM cte WHERE num > 10 ORDER BY id LIMIT 5;

-- =================================================================
-- Cleanup
-- =================================================================

SET citus.enable_sorted_merge TO off;
DROP TABLE sorted_merge_test;
DROP TABLE sorted_merge_events;
