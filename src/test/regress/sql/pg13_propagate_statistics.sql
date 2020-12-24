SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 12 AS server_version_above_twelve
\gset
\if :server_version_above_twelve
\else
\q
\endif

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
SELECT stxstattarget, stxrelid::regclass
FROM pg_statistic_ext
WHERE stxnamespace IN (
	SELECT oid
	FROM pg_namespace
	WHERE nspname IN ('statistics''TestTarget')
)
ORDER BY stxstattarget, stxrelid::regclass ASC;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA "statistics'TestTarget" CASCADE;
