CREATE SCHEMA "statistics'TestTarget";
SET search_path TO "statistics'TestTarget";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE TABLE t1 (a int, b int);
CREATE STATISTICS s1 ON a,b FROM t1;
CREATE STATISTICS s2 ON a,b FROM t1;
CREATE STATISTICS s3 ON a,b FROM t1;
CREATE STATISTICS s4 ON a,b FROM t1;

-- test altering stats target
-- test alter target before distribution
ALTER STATISTICS s1 SET STATISTICS 3;
-- since max value for target is 10000, this will automatically be lowered
ALTER STATISTICS s2 SET STATISTICS 999999;

SELECT create_distributed_table('t1', 'b');

-- test alter target before distribution
ALTER STATISTICS s3 SET STATISTICS 46;

\c - - - :worker_1_port
-- for stxstattarget, re-interpret -1 as null to avoid adding another test output for pg < 17
-- Changed stxstattarget in pg_statistic_ext to use nullable representation, removing explicit -1 for default statistics target in PostgreSQL 17.
-- https://github.com/postgres/postgres/commit/012460ee93c304fbc7220e5b55d9d0577fc766ab
SELECT
    nullif(stxstattarget, -1) AS stxstattarget,
    stxrelid::regclass
FROM pg_statistic_ext
WHERE stxnamespace IN (
    SELECT oid
    FROM pg_namespace
    WHERE nspname IN ('statistics''TestTarget')
)
AND stxname SIMILAR TO '%\_\d+'
ORDER BY
    nullif(stxstattarget, -1) IS NULL DESC,  -- Make sure null values are handled consistently
    nullif(stxstattarget, -1) NULLS FIRST,   -- Use NULLS FIRST to ensure consistent placement of nulls
    stxrelid::regclass ASC;

\c - - - :master_port
-- the first one should log a notice that says statistics object does not exist
ALTER STATISTICS IF EXISTS stats_that_doesnt_exists SET STATISTICS 0;
-- these three should error out as ALTER STATISTICS syntax doesn't support these with IF EXISTS clause
-- if output of any of these three changes, we should support them and update the test output here
ALTER STATISTICS IF EXISTS stats_that_doesnt_exists RENAME TO this_should_error_out;
ALTER STATISTICS IF EXISTS stats_that_doesnt_exists OWNER TO CURRENT_USER;
ALTER STATISTICS IF EXISTS stats_that_doesnt_exists SET SCHEMA "statistics'Test";
SET client_min_messages TO WARNING;
DROP SCHEMA "statistics'TestTarget" CASCADE;
