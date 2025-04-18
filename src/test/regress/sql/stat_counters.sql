-- Setup another Citus cluster before setting up the tests for "regression" cluster
\c postgres - - :worker_1_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;

\c postgres - - :worker_2_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;

\c postgres - - :master_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- back to the "regression" database on coordinator that we usually use during tests
\c regression - - -

CREATE SCHEMA stat_counters;
SET search_path TO stat_counters;

SET citus.next_shard_id to 1970000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SET client_min_messages TO NOTICE;

-- make sure it's disabled first
SET citus.enable_stat_counters TO false;

-- verify that the UDFs don't do anything when NULL input is provided
SELECT citus_stat_counters(null);
SELECT citus_stat_counters_reset(null);

-- Verify that we fetch stats for all databases by default (because we pass 0).
--
-- We have 3 databases that at least one backend has connected to so far, i.e., either when
-- initializing the test cluster via our perl script or during the actual tests. So we should
-- get 3 rows here.
SELECT COUNT(*) = 3 FROM citus_stat_counters();

-- same as above, but we pass 0 explicitly
SELECT COUNT(*) = 3 FROM citus_stat_counters(0);

-- However, citus_stat_counters lists all the databases that currently exist,
-- so we should get 5 rows here.
SELECT COUNT(*) = 5 FROM citus_stat_counters;

-- Verify that providing an oid that doesn't correspond to any database
-- returns an empty set. We know that "SELECT MAX(oid)+1 FROM pg_database"
-- is definitely not a valid database oid.
SELECT COUNT(*) = 0 FROM (SELECT citus_stat_counters((MAX(oid)::integer+1)::oid) FROM pg_database) q;

-- This is the first test in multi_1_schedule that calls citus_stat_counters_reset(), so one
-- can could have reset the stats before us. So, here we can test that stats_reset column is
-- NULL for that databases that citus_stat_counters_reset() was certainly not called for.
SELECT stats_reset IS NULL FROM citus_stat_counters WHERE name IN ('template0', 'template1');

-- Even more, calling citus_stat_counters_reset() for a database that no one has connected
-- so far is simply a no-op.
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = 'template0';
SELECT stats_reset IS NULL FROM citus_stat_counters WHERE name = 'template0';

-- but this is not true otherwise
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT stats_reset IS NOT NULL FROM citus_stat_counters WHERE name = current_database();

-- multi_1_schedule has this test in an individual line, so there cannot be any other backends
-- -except Citus maintenance daemon- that can update the stat counters other than us. We also
-- know that Citus maintenance daemon cannot update query related stats.
--
-- So, no one could have incremented query related stats so far.
SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0 FROM citus_stat_counters;

-- Even further, for the databases that don't have Citus extension installed,
-- we should get 0 for other stats too.
--
-- For the databases that have Citus extension installed, we might or might not
-- get 0 for connection related stats, depending on whether the Citus maintenance
-- daemon has done any work so far, so we don't check them.
SELECT connection_establishment_succeeded = 0,
       connection_establishment_failed = 0,
       connection_reused = 0
FROM (
    SELECT * FROM citus_stat_counters WHERE name NOT IN ('regression', 'stat_counters_test_db')
) q;

CREATE TABLE dist_table (a int, b int);
SELECT create_distributed_table('dist_table', 'a');

-- no single shard queries yet, so it's set to 0
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- normally this should increment query_execution_single_shard counter, but the GUC is disabled
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

SET citus.enable_stat_counters TO true;

-- increment query_execution_single_shard counter
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 1 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- increment query_execution_single_shard counter
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 1 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- verify that we can reset the stats for a specific database
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- increment counters a bit
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table;
SELECT * FROM dist_table;

-- Close the current connection and open a new one to make sure that
-- backends save their stats before exiting.
\c - - - -

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

-- these will be ineffecitve because the GUC is disabled
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;

-- Verify that we can observe the counters incremented before the GUC was
-- disabled, even when the GUC is disabled.
SELECT query_execution_single_shard = 3, query_execution_multi_shard = 2
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

SET citus.enable_stat_counters TO true;

-- increment the counters a bit more
SELECT * FROM stat_counters.dist_table WHERE a = 1;
SELECT * FROM stat_counters.dist_table;

SET citus.force_max_query_parallelization TO ON;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
RESET citus.force_max_query_parallelization;

-- (*1) For the last two queries, we forced opening as many connections as
-- possible. So, we should expect connection_establishment_succeeded to be
-- incremented by some value closer to 32 shards * 2 queries = 64. However,
-- it might not be that high if the shard queries complete very quickly. So,
-- heuristically, we check that it's at least 50 to avoid making the test
-- flaky.
SELECT query_execution_single_shard = 4, query_execution_multi_shard = 5, connection_establishment_succeeded >= 50
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- We can even see the counter values for "regression" database from
-- other databases that has Citus installed.
\c stat_counters_test_db - - -

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

SELECT query_execution_single_shard = 4, query_execution_multi_shard = 5
FROM (SELECT (pg_catalog.citus_stat_counters(oid)).* FROM pg_database WHERE datname = 'regression') q;

-- repeat some of the tests from a worker node
\c regression - - :worker_1_port

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

SET client_min_messages TO NOTICE;

SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

-- no one could have incremented query related stats so far
SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0 FROM citus_stat_counters;

SET citus.enable_stat_counters TO true;

SELECT * FROM stat_counters.dist_table WHERE a = 1;
SELECT * FROM stat_counters.dist_table WHERE a = 1;

SET citus.force_max_query_parallelization TO ON;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
RESET citus.force_max_query_parallelization;

-- As in (*1), we don't directly compare connection_establishment_succeeded
-- with 3 * 32 = 96 but with something smaller.
SELECT query_execution_single_shard = 2, query_execution_multi_shard = 3, connection_establishment_succeeded >= 80
FROM citus_stat_counters WHERE name = current_database();

SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0 FROM citus_stat_counters;

SELECT stats_reset into saved_stats_reset_t1 FROM citus_stat_counters WHERE name = current_database();
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT stats_reset into saved_stats_reset_t2 FROM citus_stat_counters WHERE name = current_database();

-- check that that the latter is greater than the former
SELECT t1.stats_reset < t2.stats_reset FROM saved_stats_reset_t1 t1, saved_stats_reset_t2 t2;

DROP TABLE saved_stats_reset_t1, saved_stats_reset_t2;

\c regression - - -

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

SET search_path TO stat_counters;
SET citus.next_shard_id to 1980000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

-- drop the test cluster
\c postgres - - :worker_1_port
DROP DATABASE stat_counters_test_db;
\c postgres - - :worker_2_port
DROP DATABASE stat_counters_test_db;
\c postgres - - :master_port
DROP DATABASE stat_counters_test_db;

-- clean up for the current database
\c regression - - -
SET client_min_messages TO WARNING;
DROP SCHEMA stat_counters CASCADE;
