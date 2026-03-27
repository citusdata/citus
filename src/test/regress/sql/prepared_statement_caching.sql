--
-- PREPARED_STATEMENT_CACHING
--
-- Tests for citus.enable_prepared_statement_caching, which enables
-- worker-side prepared statement plan caching for fast-path queries.
--

CREATE SCHEMA prepared_stmt_caching;
SET search_path TO prepared_stmt_caching;

-- Create test tables
CREATE TABLE dist_table (
    key int PRIMARY KEY,
    value int,
    label text
);
SELECT create_distributed_table('dist_table', 'key');

-- Insert base data
INSERT INTO dist_table SELECT i, i * 10, 'label-' || i FROM generate_series(1, 20) i;

CREATE TABLE dist_table_ts (
    key int PRIMARY KEY,
    value int,
    created_at timestamptz DEFAULT now()
);
SELECT create_distributed_table('dist_table_ts', 'key');

-- ============================================================
-- Test 1: GUC toggle — verify default is OFF, SET to ON succeeds
-- ============================================================

SHOW citus.enable_prepared_statement_caching;
SET citus.enable_prepared_statement_caching = on;
SHOW citus.enable_prepared_statement_caching;
SET citus.enable_prepared_statement_caching = off;
SHOW citus.enable_prepared_statement_caching;

-- ============================================================
-- Test 2: Basic caching — PREPARE a single-shard SELECT, EXECUTE
--         10 times with GUC ON, verify correct results
-- ============================================================

SET citus.enable_prepared_statement_caching = on;
set citus.max_cached_connection_lifetime to '60min';
SET search_path TO prepared_stmt_caching;

PREPARE cached_select(int) AS
    SELECT key, value FROM dist_table WHERE key = $1;

-- Execute 10 times to ensure generic plan path and cache hit path
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);
EXECUTE cached_select(1);

DEALLOCATE cached_select;

-- ============================================================
-- Test 3: Multi-shard-value — EXECUTE with different partition
--         key values routing to different shards
-- ============================================================

PREPARE cached_multi_shard(int) AS
    SELECT key, value FROM dist_table WHERE key = $1;

-- Different keys likely route to different shards
EXECUTE cached_multi_shard(1);
EXECUTE cached_multi_shard(2);
EXECUTE cached_multi_shard(3);
EXECUTE cached_multi_shard(4);
EXECUTE cached_multi_shard(5);
EXECUTE cached_multi_shard(6);
EXECUTE cached_multi_shard(7);
EXECUTE cached_multi_shard(8);
EXECUTE cached_multi_shard(9);
EXECUTE cached_multi_shard(10);

DEALLOCATE cached_multi_shard;

-- ============================================================
-- Test 4: INSERT/UPDATE/DELETE with caching ON
--         Include now() to verify coordinator-side function
--         evaluation still works.
--         Single-row INSERT, UPDATE, and DELETE all use
--         the cached prepared statement path.
-- ============================================================

-- INSERT (cached via deparse_shard_query path)
PREPARE cached_insert(int, int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, $2);

EXECUTE cached_insert(100, 1000);
EXECUTE cached_insert(101, 1010);
EXECUTE cached_insert(102, 1020);
EXECUTE cached_insert(103, 1030);
EXECUTE cached_insert(104, 1040);
EXECUTE cached_insert(105, 1050);
EXECUTE cached_insert(106, 1060);

EXECUTE cached_insert(107, 1070);
EXECUTE cached_insert(108, 1080);
EXECUTE cached_insert(109, 1090);
EXECUTE cached_insert(110, 1100);

-- Verify inserts
SELECT key, value FROM dist_table_ts WHERE key >= 100 ORDER BY key;

DEALLOCATE cached_insert;

-- UPDATE
PREPARE cached_update(int, int) AS
    UPDATE dist_table SET value = $2 WHERE key = $1;

EXECUTE cached_update(1, 100);
EXECUTE cached_update(2, 200);
EXECUTE cached_update(3, 300);
EXECUTE cached_update(4, 400);
EXECUTE cached_update(5, 500);
EXECUTE cached_update(6, 600);
EXECUTE cached_update(7, 700);

SELECT key, value FROM dist_table WHERE key <= 7 ORDER BY key;

DEALLOCATE cached_update;

-- DELETE
PREPARE cached_delete(int) AS
    DELETE FROM dist_table WHERE key = $1;

EXECUTE cached_delete(18);
EXECUTE cached_delete(19);
EXECUTE cached_delete(20);
EXECUTE cached_delete(18);
EXECUTE cached_delete(19);
EXECUTE cached_delete(20);
EXECUTE cached_delete(18);

-- Verify deletes
SELECT count(*) FROM dist_table WHERE key >= 18;

DEALLOCATE cached_delete;

-- INSERT with now() function evaluation
PREPARE cached_insert_ts(int) AS
    INSERT INTO dist_table_ts (key, value, created_at) VALUES ($1, $1 * 10, now());

EXECUTE cached_insert_ts(200);
EXECUTE cached_insert_ts(201);
EXECUTE cached_insert_ts(202);
EXECUTE cached_insert_ts(203);
EXECUTE cached_insert_ts(204);
EXECUTE cached_insert_ts(205);
EXECUTE cached_insert_ts(206);

-- Verify that each row has created_at populated (functions were evaluated)
SELECT key, value, created_at IS NOT NULL AS has_ts FROM dist_table_ts
    WHERE key >= 200 ORDER BY key;

DEALLOCATE cached_insert_ts;

-- ============================================================
-- Test 5: GUC OFF baseline — same queries produce identical results
-- ============================================================

SET citus.enable_prepared_statement_caching = off;

PREPARE uncached_select(int) AS
    SELECT key, value FROM dist_table WHERE key = $1;

EXECUTE uncached_select(1);
EXECUTE uncached_select(2);
EXECUTE uncached_select(3);
EXECUTE uncached_select(4);
EXECUTE uncached_select(5);
EXECUTE uncached_select(6);
EXECUTE uncached_select(7);
EXECUTE uncached_select(8);
EXECUTE uncached_select(9);
EXECUTE uncached_select(10);

DEALLOCATE uncached_select;

-- ============================================================
-- Test 6: Multiple prepared statements in same session
-- ============================================================

SET citus.enable_prepared_statement_caching = on;

PREPARE stmt_a(int) AS SELECT key, value FROM dist_table WHERE key = $1;
PREPARE stmt_b(int) AS SELECT key, label FROM dist_table WHERE key = $1;
PREPARE stmt_c(int, int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, $2);

-- Interleave executions to verify independent caching
EXECUTE stmt_a(1);
EXECUTE stmt_b(1);
EXECUTE stmt_a(2);
EXECUTE stmt_b(2);
EXECUTE stmt_a(3);
EXECUTE stmt_b(3);
EXECUTE stmt_a(4);
EXECUTE stmt_b(4);
EXECUTE stmt_a(5);
EXECUTE stmt_b(5);
EXECUTE stmt_a(6);
EXECUTE stmt_b(6);
EXECUTE stmt_a(7);
EXECUTE stmt_b(7);
EXECUTE stmt_c(300, 3000);
EXECUTE stmt_c(301, 3010);
EXECUTE stmt_c(302, 3020);
EXECUTE stmt_c(303, 3030);
EXECUTE stmt_c(304, 3040);
EXECUTE stmt_c(305, 3050);
EXECUTE stmt_c(306, 3060);

-- Verify inserts from stmt_c
SELECT key, value FROM dist_table_ts WHERE key >= 300 ORDER BY key;

DEALLOCATE stmt_a;
DEALLOCATE stmt_b;
DEALLOCATE stmt_c;

-- ============================================================
-- Test 7: Connection loss re-prepare — force worker connection
--         close, verify subsequent EXECUTE still works
-- ============================================================

SET citus.enable_prepared_statement_caching = on;

PREPARE reconnect_test(int) AS
    SELECT key, value FROM dist_table WHERE key = $1;

-- Execute enough times to get into generic plan + cache hit
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);

-- Force all cached connections to be dropped by setting lifetime to 0
SET citus.max_cached_connection_lifetime TO '0s';

-- The next execution should get a new connection, re-prepare, and succeed
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(2);
EXECUTE reconnect_test(3);

-- Restore default
RESET citus.max_cached_connection_lifetime;

DEALLOCATE reconnect_test;

-- ============================================================
-- Cleanup
-- ============================================================

SET citus.enable_prepared_statement_caching = off;
DROP SCHEMA prepared_stmt_caching CASCADE;
