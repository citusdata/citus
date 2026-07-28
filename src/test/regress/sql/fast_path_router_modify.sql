
CREATE SCHEMA fast_path_router_modify;
SET search_path TO fast_path_router_modify;

SET citus.next_shard_id TO 1840000;

-- all the tests in this file is intended for testing fast-path
-- router planner, so we're explicitly enabling itin this file.
-- We've bunch of other tests that triggers non-fast-path-router
-- planner (note this is already true by default)
SET citus.enable_fast_path_router_planner TO true;

SET citus.shard_replication_factor TO 1;
CREATE TABLE modify_fast_path(key int, value_1 int, value_2 text);
SELECT create_distributed_table('modify_fast_path', 'key');

SET citus.shard_replication_factor TO 2;
CREATE TABLE modify_fast_path_replication_2(key int, value_1 int, value_2 text);
SELECT create_distributed_table('modify_fast_path_replication_2', 'key');

CREATE TABLE modify_fast_path_reference(key int, value_1 int, value_2 text);
SELECT create_reference_table('modify_fast_path_reference');


-- Pre-propagate the plpgsql extension to workers via Citus's function
-- distribution machinery, before enabling DEBUG-level output. Otherwise the
-- first distributed CREATE FUNCTION below would emit worker-side "extension
-- plpgsql already exists" notices that only appear when this test runs against
-- a setup where plpgsql wasn't registered in pg_dist_object by an earlier
-- test in the schedule (e.g. the flakyness detector's minimal schedule).
SET client_min_messages TO WARNING;
CREATE OR REPLACE FUNCTION _propagate_plpgsql() RETURNS void
    LANGUAGE plpgsql AS $$BEGIN END$$;
DROP FUNCTION _propagate_plpgsql();
RESET client_min_messages;

-- show the output
SET client_min_messages TO DEBUG;

-- very simple queries goes through fast-path planning
DELETE FROM modify_fast_path WHERE key = 1;
UPDATE modify_fast_path SET value_1 = 1 WHERE key = 1;
UPDATE modify_fast_path SET value_1 = value_1 + 1 WHERE key = 1;
UPDATE modify_fast_path SET value_1 = value_1 + value_2::int WHERE key = 1;
DELETE FROM modify_fast_path WHERE value_1 = 15 AND (key = 1 AND value_2 = 'citus');
DELETE FROM modify_fast_path WHERE key = 1 and FALSE;

-- UPDATE may include complex target entries
UPDATE modify_fast_path SET value_1 = value_1 + 12 * value_1 WHERE key = 1;
UPDATE modify_fast_path SET value_1 = abs(-19) WHERE key = 1;

-- cannot go through fast-path because there are multiple keys
DELETE FROM modify_fast_path WHERE key = 1 AND key = 2;
DELETE FROM modify_fast_path WHERE key = 1 AND (key = 2 AND value_1 = 15);

-- cannot go through fast-path because key is not on the top level
DELETE FROM modify_fast_path WHERE value_1 = 15 OR (key = 1 AND value_2 = 'citus');
DELETE FROM modify_fast_path WHERE value_1 = 15 AND (key = 1 OR value_2 = 'citus');

-- goes through fast-path planning even if the key is updated to the same value
UPDATE modify_fast_path SET key = 1 WHERE key = 1;
UPDATE modify_fast_path SET key = 1::float WHERE key = 1;

-- cannot support if key changes
UPDATE modify_fast_path SET key = 2 WHERE key = 1;
UPDATE modify_fast_path SET key = 2::numeric WHERE key = 1;

-- returning is supported via fast-path
INSERT INTO modify_fast_path (key, value_1) VALUES (1,1);
DELETE FROM modify_fast_path WHERE key = 1 RETURNING *;
INSERT INTO modify_fast_path (key, value_1) VALUES (2,1) RETURNING value_1, key;
DELETE FROM modify_fast_path WHERE key = 2 RETURNING value_1 * 15, value_1::numeric * 16;

-- still, non-immutable functions are not supported
INSERT INTO modify_fast_path (key, value_1) VALUES (2,1) RETURNING value_1, random() * key;

-- modifying ctes are not supported via fast-path
WITH t1 AS (DELETE FROM modify_fast_path WHERE key = 1), t2 AS  (SELECT * FROM modify_fast_path) SELECT * FROM t2;

-- for update/share is supported via fast-path when replication factor = 1 or reference table
SELECT * FROM modify_fast_path WHERE key = 1 FOR UPDATE;
SELECT * FROM modify_fast_path WHERE key = 1 FOR SHARE;
SELECT * FROM modify_fast_path_reference WHERE key = 1 FOR UPDATE;
SELECT * FROM modify_fast_path_reference WHERE key = 1 FOR SHARE;

-- for update/share is not supported via fast-path wen replication factor > 1
SELECT * FROM modify_fast_path_replication_2 WHERE key = 1 FOR UPDATE;
SELECT * FROM modify_fast_path_replication_2 WHERE key = 1 FOR SHARE;

-- very simple queries on reference tables goes through fast-path planning
DELETE FROM modify_fast_path_reference WHERE key = 1;
UPDATE modify_fast_path_reference SET value_1 = 1 WHERE key = 1;
UPDATE modify_fast_path_reference SET value_1 = value_1 + 1 WHERE key = 1;
UPDATE modify_fast_path_reference SET value_1 = value_1 + value_2::int WHERE key = 1;


-- joins are not supported via fast-path
UPDATE modify_fast_path
	SET value_1 = 1
	FROM modify_fast_path_reference
	WHERE
		modify_fast_path.key = modify_fast_path_reference.key AND
		modify_fast_path.key  = 1 AND
		modify_fast_path_reference.key = 1;

PREPARE p1 (int, int, int) AS
	UPDATE modify_fast_path SET value_1 = value_1 + $1 WHERE key = $2 AND value_1 = $3;
EXECUTE p1(1,1,1);
EXECUTE p1(2,2,2);
EXECUTE p1(3,3,3);
EXECUTE p1(4,4,4);
EXECUTE p1(5,5,5);
EXECUTE p1(6,6,6);
EXECUTE p1(7,7,7);

CREATE FUNCTION modify_fast_path_plpsql(int, int) RETURNS void as $$
BEGIN
	DELETE FROM modify_fast_path WHERE key = $1 AND value_1 = $2;
END;
$$ LANGUAGE plpgsql;

SELECT modify_fast_path_plpsql(1,1);
SELECT modify_fast_path_plpsql(2,2);
SELECT modify_fast_path_plpsql(3,3);
SELECT modify_fast_path_plpsql(4,4);
SELECT modify_fast_path_plpsql(5,5);
SELECT modify_fast_path_plpsql(6,6);
SELECT modify_fast_path_plpsql(6,6);

-- prepared statements with zero shard
PREPARE prepared_zero_shard_select(int) AS SELECT count(*) FROM modify_fast_path WHERE key = $1 AND false;
PREPARE prepared_zero_shard_update(int) AS UPDATE modify_fast_path SET value_1 = 1 WHERE key = $1 AND false;
SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
EXECUTE prepared_zero_shard_select(1);
EXECUTE prepared_zero_shard_select(2);
EXECUTE prepared_zero_shard_select(3);
EXECUTE prepared_zero_shard_select(4);
EXECUTE prepared_zero_shard_select(5);
EXECUTE prepared_zero_shard_select(6);
EXECUTE prepared_zero_shard_select(7);

EXECUTE prepared_zero_shard_update(1);
EXECUTE prepared_zero_shard_update(2);
EXECUTE prepared_zero_shard_update(3);
EXECUTE prepared_zero_shard_update(4);
EXECUTE prepared_zero_shard_update(5);
EXECUTE prepared_zero_shard_update(6);
EXECUTE prepared_zero_shard_update(7);

-- same test with fast-path disabled
SET citus.enable_fast_path_router_planner TO FALSE;

EXECUTE prepared_zero_shard_select(1);
EXECUTE prepared_zero_shard_select(2);

EXECUTE prepared_zero_shard_update(1);
EXECUTE prepared_zero_shard_update(2);

DEALLOCATE prepared_zero_shard_select;
DEALLOCATE prepared_zero_shard_update;

PREPARE prepared_zero_shard_select(int) AS SELECT count(*) FROM modify_fast_path WHERE key = $1 AND false;
PREPARE prepared_zero_shard_update(int) AS UPDATE modify_fast_path SET value_1 = 1 WHERE key = $1 AND false;

EXECUTE prepared_zero_shard_select(1);
EXECUTE prepared_zero_shard_select(2);
EXECUTE prepared_zero_shard_select(3);
EXECUTE prepared_zero_shard_select(4);
EXECUTE prepared_zero_shard_select(5);
EXECUTE prepared_zero_shard_select(6);
EXECUTE prepared_zero_shard_select(7);

EXECUTE prepared_zero_shard_update(1);
EXECUTE prepared_zero_shard_update(2);
EXECUTE prepared_zero_shard_update(3);
EXECUTE prepared_zero_shard_update(4);
EXECUTE prepared_zero_shard_update(5);
EXECUTE prepared_zero_shard_update(6);
EXECUTE prepared_zero_shard_update(7);

-- same test with fast-path disabled
-- reset back to the original value, in case any new test comes after
RESET citus.enable_fast_path_router_planner;

RESET client_min_messages;
RESET citus.log_remote_commands;

-- Regression coverage for the delayed-fast-path zero-shard modification
-- case. When the WHERE clause combines a distribution-key equality with
-- a statically-unsatisfiable sub-predicate (`AND false`, `AND 1 = 0`, or
-- when the fast-path fallback triggers e.g. via bigint-key-vs-int-literal
-- type mismatch) shard pruning legitimately yields zero shards. Prior to
-- the fix, CheckAndBuildDelayedFastPathPlan tripped
-- `Assert(list_length(tasks) == 1)`; the fix routes these queries through
-- the same "empty task list is a silent no-op" contract already used by
-- the non-fast-path UPDATE/DELETE route.
CREATE TABLE modify_fast_path_bigint(dist_key bigint, val int);
SELECT create_distributed_table('modify_fast_path_bigint', 'dist_key');
INSERT INTO modify_fast_path_bigint VALUES (7, 100), (8, 200);

SET client_min_messages TO DEBUG2;

-- bigint-key vs int-literal: DistKeyInSimpleOpExpression bails out on the
-- type mismatch so fastPathContext->distributionKeyValue is NULL, then
-- TargetShardIntervalForFastPathQuery falls back to PruneShards which
-- short-circuits on ContainsFalseClause -> zero shards -> empty task list.
UPDATE modify_fast_path_bigint SET val = val WHERE dist_key = 7 AND false;
DELETE FROM modify_fast_path_bigint WHERE dist_key = 7 AND false;
UPDATE modify_fast_path_bigint SET val = val WHERE dist_key = 7 AND 1 = 0;

-- Same shape but on an int-keyed table (matching type). Plan shape must
-- remain the pre-fix fast-path single-shard dispatch that lets PG
-- evaluate AND FALSE at the shard.
UPDATE modify_fast_path SET value_1 = value_1 WHERE key = 1 AND false;
UPDATE modify_fast_path SET value_1 = value_1 WHERE key = 1 AND 1 = 0;
DELETE FROM modify_fast_path WHERE key = 1 AND (value_2 IS NULL AND false);

-- Standalone RETURNING variant: client sees an empty result set (no rows),
-- distinct from the plain UPDATE/DELETE "UPDATE 0" client shape.
UPDATE modify_fast_path_bigint SET val = val WHERE dist_key = 7 AND false
    RETURNING dist_key;
DELETE FROM modify_fast_path_bigint WHERE dist_key = 7 AND false
    RETURNING dist_key;

-- Explicit transaction block: the no-op must not abort the surrounding
-- transaction; subsequent statements proceed normally.
BEGIN;
UPDATE modify_fast_path_bigint SET val = val WHERE dist_key = 7 AND false;
DELETE FROM modify_fast_path_bigint WHERE dist_key = 7 AND false;
SELECT 1 AS still_alive;
COMMIT;

-- Sanity: the satisfiable happy-paths for both key types still resolve
-- to fast-path single-shard dispatch (must show "query has a single
-- distribution column value: N").
UPDATE modify_fast_path_bigint SET val = val + 1 WHERE dist_key = 7;
UPDATE modify_fast_path SET value_1 = value_1 + 1 WHERE key = 1;

-- Sanity: the zero-shard rows returned zero rows affected.
SET client_min_messages TO WARNING;
SELECT dist_key, val FROM modify_fast_path_bigint ORDER BY dist_key;
SET client_min_messages TO DEBUG2;

-- Plain SELECT with AND false must still work (the Phase 2 gate must not
-- fire here; only modifying-CTE SELECTs enter that transform).
SELECT count(*) FROM modify_fast_path_bigint WHERE dist_key = 7 AND false;
SELECT count(*) FROM modify_fast_path WHERE key = 1 AND false;

RESET client_min_messages;

DROP SCHEMA fast_path_router_modify CASCADE;
