--
-- SETUP_MULTI_ORDERBY_PUSHDOWN
--
-- Creates the test tables and data used by multi_orderby_pushdown.sql
-- and its variants (e.g., multi_orderby_pushdown_streaming.sql).
-- This file is meant to be included via \i from test files that need
-- these tables.
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
