-- ===================================================================
-- Reproduction test for GitHub issue #8137:
-- Segfault in Citus during AppendCopyRowData / intermediate results
-- (memcpy invalid length in textsend)
--
-- The crash occurs when a CTE referenced multiple times in a UNION ALL
-- query forces Citus to materialize it as an intermediate result.
-- The backtrace shows:
--   AdaptiveExecutorPreExecutorRun → ExecuteSubPlans
--     → ExecutePlanIntoDestReceiver → CitusExecutorRun (nested)
--       → RemoteFileDestReceiverReceive → AppendCopyRowData → textsend
--         → appendBinaryStringInfo with invalid length (~168MB)
--
-- The corrupted data contains EXPLAIN ANALYZE output text from
-- "Function Scan on read_intermediate_result", suggesting memory
-- context mismanagement during intermediate result serialization.
-- ===================================================================

DROP TABLE if exists main_table;
DROP SCHEMA if exists issue_8137 CASCADE;

CREATE SCHEMA IF NOT EXISTS issue_8137;
SET search_path TO issue_8137;
SET citus.shard_count TO 24;
SET citus.shard_replication_factor TO 1;

-- Create the table matching that reported in the issue.
CREATE TABLE main_table (
    org_id uuid NOT NULL,
    category_id uuid NOT NULL,
    status integer,
    created_at timestamp without time zone,
    modified_at timestamp without time zone,
    event_timestamp timestamp without time zone,
    score_percentage real,
    metric_value integer,
    completed_at timestamp without time zone
);

-- Distribute by org_id 
--SELECT create_distributed_table('main_table', 'org_id');

-- Distribute by category_id 
 SELECT create_distributed_table('main_table', 'category_id');

-- Populate with data for multiple org_ids to have data across shards.
-- Use deterministic,  values (no random()) for reproducible results.
INSERT INTO main_table (
    org_id, category_id, status,
    created_at, modified_at, event_timestamp,
    score_percentage, metric_value, completed_at
)
SELECT
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid,
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbb' || lpad(((i % 10)::text), 3, '0'))::uuid,
    (i % 5),
    '2024-01-01'::timestamp + (i || ' minutes')::interval,
    '2024-01-01'::timestamp + (i || ' minutes')::interval,
    '2024-01-01'::timestamp + (i || ' hours')::interval,
    ((i * 7) % 100)::real,
    ((i * 13) % 1000),
    '2024-01-01'::timestamp + (i || ' hours')::interval + '30 minutes'::interval
FROM generate_series(1, 500) AS s(i);

INSERT INTO main_table (
    org_id, category_id, status,
    created_at, modified_at, event_timestamp,
    score_percentage, metric_value, completed_at
)
SELECT
    'cccccccc-cccc-cccc-cccc-cccccccccccc'::uuid,
    ('dddddddd-dddd-dddd-dddd-ddddddddd' || lpad(((i % 10)::text), 3, '0'))::uuid,
    (i % 3),
    '2024-06-01'::timestamp + (i || ' minutes')::interval,
    '2024-06-01'::timestamp + (i || ' minutes')::interval,
    '2024-06-01'::timestamp + (i || ' hours')::interval,
    ((i * 11) % 100)::real,
    ((i * 17) % 500),
    '2024-06-01'::timestamp + (i || ' hours')::interval + '15 minutes'::interval
FROM generate_series(1, 500) AS s(i);

INSERT INTO main_table (
    org_id, category_id, status,
    created_at, modified_at, event_timestamp,
    score_percentage, metric_value, completed_at
)
SELECT
    gen_random_uuid(),
    gen_random_uuid(),
    (i % 7),
    '2025-06-01'::timestamp + (i || ' minutes')::interval,
    '2025-06-01'::timestamp + (i || ' minutes')::interval,
    '2025-06-01'::timestamp + (i || ' hours')::interval,
    ((i * 11) % 100)::real,
    ((i * 17) % 500),
    '2025-06-01'::timestamp + (i || ' hours')::interval + '15 minutes'::interval
FROM generate_series(1, 10000000) AS s(i);

-- ===================================================================
-- Test 1: Router-plan path (single org_id = distribution key)
--
-- When org_id is a constant matching the distribution key, the entire
-- query including CTEs is pushed down to a single shard. This is the
-- fast path that does NOT trigger intermediate result materialization.
-- ===================================================================
WITH all_records AS (
    SELECT
        t.metric_value AS data0,
        CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
        t.category_id AS col0
    FROM main_table t
    WHERE t.org_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid
      AND (t.event_timestamp BETWEEN '2024-01-01'::timestamp AND '2024-12-31'::timestamp)
),
category_aggregation AS (
    SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
    FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
),
overall_summary AS (
    SELECT
        COALESCE(SUM(data0), 0),
        COALESCE(ROUND(AVG(data1)), 0),
        '00000000-0000-0000-0000-000000000000'::uuid
    FROM all_records
)
SELECT * FROM category_aggregation
UNION ALL
SELECT * FROM overall_summary;


-- ===================================================================
-- Test 2: Multi-shard path forcing intermediate result materialization
--
-- By removing the org_id filter, the CTE scans multiple shards.
-- Because the CTE is referenced twice (by category_aggregation and
-- overall_summary), Citus MUST materialize it as an intermediate
-- result and broadcast it. This exercises the exact crash path:
--   ExecuteSubPlans → RemoteFileDestReceiverReceive → AppendCopyRowData
-- ===================================================================
WITH all_records AS MATERIALIZED (
    SELECT
        t.metric_value AS data0,
        CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
        t.category_id AS col0
    FROM main_table t
    WHERE t.event_timestamp BETWEEN '2024-01-01'::timestamp AND '2024-12-31'::timestamp
),
category_aggregation AS (
    SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
    FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
),
overall_summary AS (
    SELECT
        COALESCE(SUM(data0), 0),
        COALESCE(ROUND(AVG(data1)), 0),
        '00000000-0000-0000-0000-000000000000'::uuid
    FROM all_records
)
SELECT * FROM category_aggregation
UNION ALL
SELECT * FROM overall_summary;


-- ===================================================================
-- Test 3: Prepared statement with multiple executions
--
-- The reporter used parameterized queries ($1..$10). After ~5
-- executions PostgreSQL switches to a generic plan where the
-- distribution key ($1) is unknown, potentially forcing multi-shard
-- execution and intermediate result materialization.
-- ===================================================================
PREPARE issue_8137_query(uuid, timestamp, timestamp, int, int, int, int, numeric, numeric, uuid) AS
WITH all_records AS (
    SELECT
        t.metric_value AS data0,
        CASE WHEN t.metric_value > $5 THEN $6 ELSE $7 END AS data1,
        t.category_id AS col0
    FROM main_table t
    WHERE t.org_id = $1
      AND (t.event_timestamp BETWEEN $2 AND $3)
),
category_aggregation AS (
    SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
    FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT $4
),
overall_summary AS (
    SELECT
        COALESCE(SUM(data0), $8),
        COALESCE(ROUND(AVG(data1)), $9),
        $10::uuid
    FROM all_records
)
SELECT * FROM category_aggregation
UNION ALL
SELECT * FROM overall_summary;

-- Execute 7 times to cross the generic plan threshold (default: 5)
EXECUTE issue_8137_query(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '2024-01-01', '2024-12-31',
    10, 500, 1, 0, 0, 0, '00000000-0000-0000-0000-000000000000');
EXECUTE issue_8137_query(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '2024-01-01', '2024-06-30',
    5, 300, 1, 0, 0, 0, '11111111-1111-1111-1111-111111111111');
EXECUTE issue_8137_query(
    'cccccccc-cccc-cccc-cccc-cccccccccccc', '2024-06-01', '2024-12-31',
    10, 250, 1, 0, 0, 0, '22222222-2222-2222-2222-222222222222');
EXECUTE issue_8137_query(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '2024-01-01', '2024-12-31',
    10, 500, 1, 0, 0, 0, '00000000-0000-0000-0000-000000000000');
EXECUTE issue_8137_query(
    'cccccccc-cccc-cccc-cccc-cccccccccccc', '2024-06-01', '2024-12-31',
    5, 100, 1, 0, 0, 0, '33333333-3333-3333-3333-333333333333');
-- 6th+ should use generic plan
EXECUTE issue_8137_query(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '2024-01-01', '2024-12-31',
    10, 500, 1, 0, 0, 0, '00000000-0000-0000-0000-000000000000');
EXECUTE issue_8137_query(
    'cccccccc-cccc-cccc-cccc-cccccccccccc', '2024-06-01', '2024-12-31',
    10, 250, 1, 0, 0, 0, '44444444-4444-4444-4444-444444444444');

DEALLOCATE issue_8137_query;


-- ===================================================================
-- Test 4: EXPLAIN ANALYZE on the intermediate-result path
--
-- The corrupted data in the crash backtrace contained EXPLAIN ANALYZE
-- text ("actual rows=1 loops=1 ... Function Scan on read_intermediate_result"),
-- suggesting es_instrument / SubPlanExplainAnalyzeContext interaction.
-- ===================================================================
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
WITH all_records AS MATERIALIZED (
    SELECT
        t.metric_value AS data0,
        CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
        t.category_id AS col0
    FROM main_table t
    WHERE t.event_timestamp BETWEEN '2024-01-01'::timestamp AND '2024-12-31'::timestamp
),
category_aggregation AS (
    SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
    FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
),
overall_summary AS (
    SELECT
        COALESCE(SUM(data0), 0),
        COALESCE(ROUND(AVG(data1)), 0),
        '00000000-0000-0000-0000-000000000000'::uuid
    FROM all_records
)
SELECT * FROM category_aggregation
UNION ALL
SELECT * FROM overall_summary;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
WITH all_records AS (
    SELECT
        t.metric_value AS data0,
        CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
        t.category_id AS col0
    FROM main_table t
    WHERE t.org_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid
      AND (t.event_timestamp BETWEEN '2024-01-01'::timestamp AND '2024-12-31'::timestamp)
),
category_aggregation AS (
    SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
    FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
),
overall_summary AS (
    SELECT
        COALESCE(SUM(data0), 0),
        COALESCE(ROUND(AVG(data1)), 0),
        '00000000-0000-0000-0000-000000000000'::uuid
    FROM all_records
)
SELECT * FROM category_aggregation
UNION ALL
SELECT * FROM overall_summary;

-- ===================================================================
-- Test 5: Repeated execution in a loop (increases chance of
-- hitting timing-dependent memory corruption)
-- ===================================================================
DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 1..50 LOOP
        PERFORM * FROM (
            WITH all_records AS MATERIALIZED (
                SELECT
                    t.metric_value AS data0,
                    CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
                    t.category_id AS col0
                FROM issue_8137.main_table t
                WHERE t.org_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid
                  AND (t.event_timestamp BETWEEN '2024-01-01'::timestamp AND '2024-12-31'::timestamp)
            ),
            category_aggregation AS (
                SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
                FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
            ),
            overall_summary AS (
                SELECT
                    COALESCE(SUM(data0), 0),
                    COALESCE(ROUND(AVG(data1)), 0),
                    '00000000-0000-0000-0000-000000000000'::uuid
                FROM all_records
            )
            SELECT * FROM category_aggregation
            UNION ALL
            SELECT * FROM overall_summary
        ) sub;
    END LOOP;
END;
$$;


-- ===================================================================
-- Test 6: EXPLAIN ANALYZE in a loop (exercises the SubPlanExplainAnalyzeContext
-- code path repeatedly to stress memory context management)
-- ===================================================================
DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 1..10 LOOP
        EXECUTE '
            EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
            WITH all_records AS MATERIALIZED (
                SELECT t.metric_value AS data0,
                       CASE WHEN t.metric_value > 500 THEN 1 ELSE 0 END AS data1,
                       t.category_id AS col0
                FROM issue_8137.main_table t
                WHERE t.org_id = ''aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa''::uuid
                  AND (t.event_timestamp BETWEEN ''2024-01-01''::timestamp AND ''2024-12-31''::timestamp)
            ),
            category_aggregation AS (
                SELECT SUM(data0) AS agg0, ROUND(AVG(data1)) AS agg1, col0
                FROM all_records GROUP BY col0 ORDER BY agg0 DESC LIMIT 10
            ),
            overall_summary AS (
                SELECT COALESCE(SUM(data0), 0), COALESCE(ROUND(AVG(data1)), 0),
                       ''00000000-0000-0000-0000-000000000000''::uuid
                FROM all_records
            )
            SELECT * FROM category_aggregation
            UNION ALL
            SELECT * FROM overall_summary';
    END LOOP;
END;
$$;


-- ===================================================================
-- Cleanup
-- ===================================================================
DROP TABLE main_table;
DROP SCHEMA issue_8137 CASCADE;
