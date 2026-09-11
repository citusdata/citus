--
-- PREPARED_STATEMENT_CACHING
--
-- Tests for citus.enable_prepared_statement_caching, which enables
-- worker-side prepared statement plan caching for fast-path queries.
--

CREATE SCHEMA prepared_stmt_caching;
SET search_path TO prepared_stmt_caching;

-- Test 8 prints shard names in EXPLAIN output, so pin the shard ids
SET citus.next_shard_id TO 105000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- the regression suite runs with citus.stat_tenants_track = 'ALL', which the
-- cache declines in order to keep per-execution tenant attribution
SET citus.stat_tenants_track TO 'none';

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

-- INSERT ... ON CONFLICT DO UPDATE (upsert). The qualified reference to the
-- target table in DO UPDATE must resolve to the shard alias on the worker.
PREPARE cached_upsert(int, int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, $2)
    ON CONFLICT (key) DO UPDATE SET value = dist_table_ts.value + EXCLUDED.value;

EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(500, 1);
EXECUTE cached_upsert(501, 5);
EXECUTE cached_upsert(501, 5);

-- key 500 accumulated 6 increments, key 501 accumulated 2
SELECT key, value FROM dist_table_ts WHERE key >= 500 AND key < 600 ORDER BY key;

DEALLOCATE cached_upsert;

-- Multi-row INSERT is not cacheable: each shard's task carries only its own
-- subset of VALUES rows, so the rows must not be duplicated across shards.
PREPARE cached_multirow(int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, 1), ($1 + 1, 2);

EXECUTE cached_multirow(600);
EXECUTE cached_multirow(602);
EXECUTE cached_multirow(604);
EXECUTE cached_multirow(606);
EXECUTE cached_multirow(608);
EXECUTE cached_multirow(610);

-- exactly 12 rows, one per key, no duplicates
SELECT count(*) AS row_count, count(DISTINCT key) AS distinct_keys
    FROM dist_table_ts WHERE key >= 600;

DEALLOCATE cached_multirow;

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
SELECT key, value FROM dist_table_ts WHERE key >= 300 AND key < 400 ORDER BY key;

DEALLOCATE stmt_a;
DEALLOCATE stmt_b;
DEALLOCATE stmt_c;

-- ============================================================
-- Test 7: Connection loss re-prepare — force worker connection
--         close, verify the statement is re-prepared on the new
--         connection. Results alone cannot show this (see Test 13),
--         so assert the wire protocol: the new connection must be
--         sent the parameterized SQL before a statement name.
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

-- The lifetime is only applied when the connection is released at end of
-- transaction, so the first execution below still reuses the prepared
-- connection and the second lands on a fresh one. Both use the same key,
-- so a re-prepare can only be caused by the new connection.
SET citus.log_remote_commands TO on;
EXECUTE reconnect_test(1);
EXECUTE reconnect_test(1);
SET citus.log_remote_commands TO off;

EXECUTE reconnect_test(2);
EXECUTE reconnect_test(3);

-- Restore default
RESET citus.max_cached_connection_lifetime;

DEALLOCATE reconnect_test;

-- ============================================================
-- Test 8: EXPLAIN a cached fast-path statement. EXPLAIN builds the
--         shard query from the saved template rather than from the
--         executor's cache path, and the result is retained on the
--         task, so it must outlive the deparse.
-- ============================================================

SET citus.enable_prepared_statement_caching = on;

PREPARE explain_select(int) AS
    SELECT key, value FROM dist_table WHERE key = $1;

-- reach the generic plan and the cache-hit fast path first
EXECUTE explain_select(1);
EXECUTE explain_select(1);
EXECUTE explain_select(1);
EXECUTE explain_select(1);
EXECUTE explain_select(1);
EXECUTE explain_select(1);
EXECUTE explain_select(1);

EXPLAIN (COSTS OFF) EXECUTE explain_select(1);

-- the statement must still execute correctly afterwards
EXECUTE explain_select(1);

DEALLOCATE explain_select;

-- INSERT takes the other deparse branch
PREPARE explain_insert(int, int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, $2);

EXECUTE explain_insert(700, 7000);
EXECUTE explain_insert(701, 7010);
EXECUTE explain_insert(702, 7020);
EXECUTE explain_insert(703, 7030);
EXECUTE explain_insert(704, 7040);
EXECUTE explain_insert(705, 7050);
EXECUTE explain_insert(706, 7060);

EXPLAIN (COSTS OFF) EXECUTE explain_insert(707, 7070);

-- EXPLAIN without ANALYZE must not have inserted key 707
SELECT count(*) AS inserted_rows FROM dist_table_ts WHERE key >= 700 AND key < 800;

DEALLOCATE explain_insert;

-- An upsert is what actually needs the insert alias: the DO UPDATE reference to
-- the target table has to resolve against the shard alias, not the shard name.
PREPARE explain_upsert(int, int) AS
    INSERT INTO dist_table_ts (key, value) VALUES ($1, $2)
    ON CONFLICT (key) DO UPDATE SET value = dist_table_ts.value + EXCLUDED.value;

EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);
EXECUTE explain_upsert(800, 1);

EXPLAIN (COSTS OFF) EXECUTE explain_upsert(800, 1);

-- seven increments, and EXPLAIN must not have added an eighth
SELECT key, value FROM dist_table_ts WHERE key = 800;

DEALLOCATE explain_upsert;

-- ============================================================
-- Test 9: citus.task_assignment_policy must still apply on the
--         cache-hit fast path, which builds its task directly
--         instead of going through GenerateSingleShardRouterTaskList().
-- ============================================================

-- returns 'shardId@port' for the placement the task was assigned to
CREATE OR REPLACE FUNCTION parse_explain_output(in qry text, in table_name text, out r text)
RETURNS SETOF TEXT AS $$
DECLARE
       portOfTheTask text;
       shardOfTheTask text;
begin
  for r in execute qry loop
    IF r LIKE '%port%' THEN
      portOfTheTask = substring(r, '([0-9]{1,10})');
    END IF;

    IF r LIKE '%' || table_name || '%' THEN
      shardOfTheTask = substring(r, '([0-9]{5,10})');
    END IF;

  end loop;
  return QUERY SELECT shardOfTheTask || '@' || portOfTheTask;
end; $$ language plpgsql;

-- round-robin only has something to choose between when shards are replicated
SET citus.shard_replication_factor TO 2;
CREATE TABLE replicated_table (key int PRIMARY KEY, value int);
SELECT create_distributed_table('replicated_table', 'key');
SET citus.shard_replication_factor TO 1;

INSERT INTO replicated_table SELECT i, i * 10 FROM generate_series(1, 20) i;

SET citus.task_assignment_policy TO 'round-robin';
SET citus.explain_distributed_queries TO on;

PREPARE round_robin_select(int) AS
    SELECT value FROM replicated_table WHERE key = $1;

-- reach the generic plan so that later executions take the fast path
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);
EXECUTE round_robin_select(1);

CREATE TEMPORARY TABLE explain_outputs (value text);

INSERT INTO explain_outputs
    SELECT parse_explain_output('EXPLAIN EXECUTE round_robin_select(1)', 'replicated_table');
INSERT INTO explain_outputs
    SELECT parse_explain_output('EXPLAIN EXECUTE round_robin_select(1)', 'replicated_table');

-- outside a transaction round-robin must alternate placements, so the fast
-- path has to reach both nodes rather than pinning to the first placement
SELECT count(DISTINCT value) FROM explain_outputs;

DROP TABLE explain_outputs;
DEALLOCATE round_robin_select;
RESET citus.task_assignment_policy;
RESET citus.explain_distributed_queries;

-- ============================================================
-- Test 10: the distribution key parameter need not have the same type
--          as the distribution column. A single-row INSERT records the
--          Param with implicit coercions stripped, so $1 here is int4
--          while the column is numeric.
-- ============================================================

CREATE TABLE numeric_dist (key numeric PRIMARY KEY, value int);
SELECT create_distributed_table('numeric_dist', 'key');

PREPARE numeric_insert(int) AS
    INSERT INTO numeric_dist (key, value) VALUES ($1, $1 * 10);

EXECUTE numeric_insert(1);
EXECUTE numeric_insert(2);
EXECUTE numeric_insert(3);
EXECUTE numeric_insert(4);
EXECUTE numeric_insert(5);
EXECUTE numeric_insert(6);
EXECUTE numeric_insert(7);
EXECUTE numeric_insert(8);
EXECUTE numeric_insert(9);
EXECUTE numeric_insert(10);

SELECT count(*) AS total FROM numeric_dist;

-- rows written on the fast path must be reachable by a router lookup, which
-- only holds if the value was hashed as numeric rather than as int4
SELECT key, value FROM numeric_dist WHERE key = 8;
SELECT key, value FROM numeric_dist WHERE key = 9;
SELECT key, value FROM numeric_dist WHERE key = 10;

DEALLOCATE numeric_insert;

-- ============================================================
-- Test 11: DML whose expressions must be evaluated on the coordinator
--          cannot be cached. The template is copied before evaluation,
--          so a cached statement would evaluate nextval() on the worker,
--          where the sequence does not exist.
-- ============================================================

CREATE SEQUENCE coord_eval_seq;
CREATE TABLE coord_eval (key int PRIMARY KEY, seq_value bigint);
SELECT create_distributed_table('coord_eval', 'key');

PREPARE coord_eval_insert(int) AS
    INSERT INTO coord_eval (key, seq_value) VALUES ($1, nextval('coord_eval_seq'));

EXECUTE coord_eval_insert(1);
EXECUTE coord_eval_insert(2);
EXECUTE coord_eval_insert(3);
EXECUTE coord_eval_insert(4);
EXECUTE coord_eval_insert(5);
EXECUTE coord_eval_insert(6);
EXECUTE coord_eval_insert(7);
EXECUTE coord_eval_insert(8);

-- values come from the coordinator's sequence, in execution order
SELECT key, seq_value FROM coord_eval ORDER BY key;

DEALLOCATE coord_eval_insert;

-- UPDATE reaches the deferred-pruning router path, which must carry the same
-- coordinator-evaluation flag
ALTER TABLE coord_eval ADD COLUMN ts timestamptz;

PREPARE coord_eval_update(int) AS
    UPDATE coord_eval SET ts = now() WHERE key = $1;

BEGIN;
EXECUTE coord_eval_update(1);
EXECUTE coord_eval_update(2);
EXECUTE coord_eval_update(3);
EXECUTE coord_eval_update(4);
EXECUTE coord_eval_update(5);
EXECUTE coord_eval_update(6);
EXECUTE coord_eval_update(7);
EXECUTE coord_eval_update(8);

-- a coordinator-evaluated now() is one value for the whole transaction;
-- evaluated on the workers it would be one value per node
SELECT count(DISTINCT ts) AS distinct_timestamps FROM coord_eval;
COMMIT;

DEALLOCATE coord_eval_update;

-- ============================================================
-- Test 12: a prepared statement may carry parameters the query never
--          uses. Their types still have to be normalized, or the worker
--          cannot infer a type for a parameter absent from the SQL.
-- ============================================================

CREATE TYPE unused_param_type AS (a int);

PREPARE unused_param(int, unused_param_type) AS
    SELECT key, value FROM dist_table WHERE key = $1;

EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');
EXECUTE unused_param(1, '(1)');

DEALLOCATE unused_param;

-- ============================================================
-- Test 13: assert the cache is actually engaged. Every other test
--          here checks only query results, which are identical
--          whether or not the feature is active, so observe the
--          wire protocol instead: PQprepare logs the parameterized
--          SQL, PQsendQueryPrepared logs the statement name.
-- ============================================================

CREATE TABLE observe_cache (key int PRIMARY KEY, value int);
SELECT create_distributed_table('observe_cache', 'key');
INSERT INTO observe_cache SELECT i, i FROM generate_series(1, 20) i;

PREPARE observe_select(int) AS
    SELECT value FROM observe_cache WHERE key = $1;

-- reach the generic plan before enabling logging; the cache is populated on
-- the second use of the generic plan, so this needs one more than the
-- five executions it takes to get there
EXECUTE observe_select(1);
EXECUTE observe_select(1);
EXECUTE observe_select(1);
EXECUTE observe_select(1);
EXECUTE observe_select(1);
EXECUTE observe_select(1);
EXECUTE observe_select(1);

SET citus.log_remote_commands TO on;

-- key 1's shard is already prepared on this connection, so this reuses
-- the statement by name rather than sending SQL
EXECUTE observe_select(1);

-- an unprepared shard prepares first, then executes by name
EXECUTE observe_select(3);
EXECUTE observe_select(3);

SET citus.log_remote_commands TO off;

DEALLOCATE observe_select;

-- ============================================================
-- Test 14: SELECT ... FOR UPDATE is fast-path eligible, so the
--          cached task must carry its row locks. Without them the
--          executor treats it as an ordinary read and, with
--          select_opens_transaction_block off, opens no remote
--          transaction — releasing the row lock at statement end
--          rather than at the end of the enclosing transaction.
-- ============================================================

SET citus.select_opens_transaction_block TO off;

PREPARE lock_row(int) AS
    SELECT value FROM observe_cache WHERE key = $1 FOR UPDATE;

-- reach the generic plan before enabling logging
EXECUTE lock_row(5);
EXECUTE lock_row(5);
EXECUTE lock_row(5);
EXECUTE lock_row(5);
EXECUTE lock_row(5);
EXECUTE lock_row(5);
EXECUTE lock_row(5);

-- the worker must still be given a transaction to hold the row lock in
SET citus.log_remote_commands TO on;
BEGIN;
EXECUTE lock_row(5);
COMMIT;
SET citus.log_remote_commands TO off;

RESET citus.select_opens_transaction_block;
DEALLOCATE lock_row;

-- ============================================================
-- Test 15: the same wire-protocol assertion for the remote DML
--          paths. INSERT and UPDATE/DELETE reach the cache through
--          their own eligibility and task-building branches, and a
--          silent fallback in either is invisible in query results.
-- ============================================================

-- no primary key, so the same shard can be hit repeatedly by INSERT
CREATE TABLE observe_dml (key int, value int);
SELECT create_distributed_table('observe_dml', 'key');

PREPARE observe_insert(int, int) AS
    INSERT INTO observe_dml (key, value) VALUES ($1, $2);

-- reach the generic plan and populate the cache before enabling logging
EXECUTE observe_insert(1, 1);
EXECUTE observe_insert(1, 2);
EXECUTE observe_insert(1, 3);
EXECUTE observe_insert(1, 4);
EXECUTE observe_insert(1, 5);
EXECUTE observe_insert(1, 6);
EXECUTE observe_insert(1, 7);

SET citus.log_remote_commands TO on;

-- key 1's shard is already prepared on this connection, so this reuses
-- the statement by name rather than sending SQL
EXECUTE observe_insert(1, 8);

-- an unprepared shard prepares first, then executes by name
EXECUTE observe_insert(3, 9);
EXECUTE observe_insert(3, 10);

SET citus.log_remote_commands TO off;

PREPARE observe_update(int, int) AS
    UPDATE observe_dml SET value = $2 WHERE key = $1;

EXECUTE observe_update(1, 11);
EXECUTE observe_update(1, 12);
EXECUTE observe_update(1, 13);
EXECUTE observe_update(1, 14);
EXECUTE observe_update(1, 15);
EXECUTE observe_update(1, 16);
EXECUTE observe_update(1, 17);

SET citus.log_remote_commands TO on;
EXECUTE observe_update(1, 18);
SET citus.log_remote_commands TO off;

PREPARE observe_delete(int) AS
    DELETE FROM observe_dml WHERE key = $1;

EXECUTE observe_delete(3);
EXECUTE observe_delete(3);
EXECUTE observe_delete(3);
EXECUTE observe_delete(3);
EXECUTE observe_delete(3);
EXECUTE observe_delete(3);
EXECUTE observe_delete(3);

SET citus.log_remote_commands TO on;
EXECUTE observe_delete(3);
SET citus.log_remote_commands TO off;

SELECT key, value FROM observe_dml ORDER BY key, value;

DEALLOCATE observe_insert;
DEALLOCATE observe_update;
DEALLOCATE observe_delete;

-- ============================================================
-- Cleanup
-- ============================================================

SET citus.enable_prepared_statement_caching = off;
DROP SCHEMA prepared_stmt_caching CASCADE;
