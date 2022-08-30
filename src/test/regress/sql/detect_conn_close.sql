--
-- PG15+ test as WL_SOCKET_CLOSED exposed for PG15+
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
\q
\endif

CREATE SCHEMA socket_close;
SET search_path TO socket_close;

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE TABLE socket_test_table(id bigserial, value text);
SELECT create_distributed_table('socket_test_table', 'id');
INSERT INTO socket_test_table (value) SELECT i::text FROM generate_series(0,100)i;

-- first, simulate that we only have one cached connection per node
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SELECT count(*) FROM socket_test_table;

-- kill all the cached backends on the workers
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

-- show that none remains
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pid FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

-- even though the cached connections closed, the execution recovers and establishes new connections
SELECT count(*) FROM socket_test_table;


-- now, use 16 connections per worker, we can still recover all connections
SET citus.max_adaptive_executor_pool_size TO 16;
SET citus.max_cached_conns_per_worker TO 16;
SET citus.force_max_query_parallelization  TO ON;
SELECT count(*) FROM socket_test_table;
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);
-- show that none remains
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pid FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

SELECT count(*) FROM socket_test_table;

-- now, get back to sane defaults
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SET citus.force_max_query_parallelization  TO OFF;

-- we can recover for modification queries as well

-- single row INSERT
INSERT INTO socket_test_table VALUES (1);
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$) ORDER BY result;
-- show that none remains
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pid FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

INSERT INTO socket_test_table VALUES (1);


-- single row UPDATE
UPDATE socket_test_table SET value = 15 WHERE id = 1;
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$) ORDER BY result;
-- show that none remains
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pid FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

UPDATE socket_test_table SET value = 15 WHERE id = 1;


-- we cannot recover in a transaction block
BEGIN;
  SELECT count(*) FROM socket_test_table;

  -- kill all the cached backends on the workers
  SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

  SELECT count(*) FROM socket_test_table;
ROLLBACK;


SET client_min_messages TO ERROR;
DROP SCHEMA socket_close CASCADE;
