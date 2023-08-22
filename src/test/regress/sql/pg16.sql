--
-- PG16
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
\gset
\if :server_version_ge_16
\else
\q
\endif

CREATE SCHEMA pg16;
SET search_path TO pg16;
SET citus.next_shard_id TO 950000;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;

-- test the new vacuum and analyze options
-- Relevant PG commits:
-- https://github.com/postgres/postgres/commit/1cbbee03385763b066ae3961fc61f2cd01a0d0d7
-- https://github.com/postgres/postgres/commit/4211fbd8413b26e0abedbe4338aa7cda2cd469b4
-- https://github.com/postgres/postgres/commit/a46a7011b27188af526047a111969f257aaf4db8

CREATE TABLE t1 (a int);
SELECT create_distributed_table('t1','a');
SET citus.log_remote_commands TO ON;

VACUUM (PROCESS_MAIN FALSE) t1;
VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) t1;
VACUUM (PROCESS_MAIN TRUE) t1;
VACUUM (PROCESS_MAIN FALSE, FULL) t1;
VACUUM (SKIP_DATABASE_STATS) t1;
VACUUM (ONLY_DATABASE_STATS) t1;
VACUUM (BUFFER_USAGE_LIMIT '512 kB') t1;
VACUUM (BUFFER_USAGE_LIMIT 0) t1;
VACUUM (BUFFER_USAGE_LIMIT 16777220) t1;
VACUUM (BUFFER_USAGE_LIMIT -1) t1;
VACUUM (BUFFER_USAGE_LIMIT 'test') t1;
ANALYZE (BUFFER_USAGE_LIMIT '512 kB') t1;
ANALYZE (BUFFER_USAGE_LIMIT 0) t1;

SET citus.log_remote_commands TO OFF;

-- only verifying it works and not printing log
-- remote commands because it can be flaky
VACUUM (ONLY_DATABASE_STATS);

-- Proper error when creating statistics without a name on a Citus table
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/624aa2a13bd02dd584bb0995c883b5b93b2152df

CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

CREATE STATISTICS (dependencies) ON a, b FROM test_stats;
CREATE STATISTICS (ndistinct, dependencies) on a, b from test_stats;
CREATE STATISTICS (ndistinct, dependencies, mcv) on a, b from test_stats;

\set VERBOSITY terse
SET client_min_messages TO ERROR;
DROP SCHEMA pg16 CASCADE;
