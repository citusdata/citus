--
-- PG15+ test as WL_SOCKET_CLOSED exposed for PG15+
--

CREATE SCHEMA socket_close;
SET search_path TO socket_close;

CREATE OR REPLACE FUNCTION kill_all_cached_internal_conns(gpid bigint)
RETURNS bool LANGUAGE plpgsql
AS $function$
DECLARE
  killed_backend_ct int;
BEGIN
  -- kill all the cached backends on the workers
   WITH command AS (SELECT 'SELECT count(*) FROM (SELECT pg_terminate_backend(pid, 1000) FROM pg_stat_activity WHERE application_name ILIKE ''%citus_internal gpid=' || gpid::text  ||''' AND pid !=pg_backend_pid()) as foo')
  SELECT sum(result::int) INTO killed_backend_ct FROM command JOIN LATERAL run_command_on_workers((SELECT * FROM command)) on (true);

  RETURN killed_backend_ct > 0;
END;
$function$;


SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 1;

CREATE TABLE socket_test_table(id bigserial, value text);
SELECT create_distributed_table('socket_test_table', 'id');
INSERT INTO socket_test_table (value) SELECT i::text FROM generate_series(0,100)i;

-- first, simulate that we only have one cached connection per node
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SELECT count(*) FROM socket_test_table;

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

-- show that none remains
SELECT result FROM run_command_on_workers($$SELECT count(*) FROM (SELECT pid FROM pg_stat_activity WHERE query ilike '%socket_test_table%' AND pid !=pg_backend_pid()) as foo$$);

-- even though the cached connections closed, the execution recovers and establishes new connections
SELECT count(*) FROM socket_test_table;

-- now, use 16 connections per worker, we can still recover all connections
SET citus.max_adaptive_executor_pool_size TO 16;
SET citus.max_cached_conns_per_worker TO 16;
SET citus.force_max_query_parallelization  TO ON;
SELECT count(*) FROM socket_test_table;

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

SELECT count(*) FROM socket_test_table;

-- now, get back to sane defaults
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SET citus.force_max_query_parallelization  TO OFF;

-- we can recover for modification queries as well

-- single row INSERT
INSERT INTO socket_test_table VALUES (1);

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

INSERT INTO socket_test_table VALUES (1);

-- single row UPDATE
UPDATE socket_test_table SET value = 15 WHERE id = 1;
-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());
UPDATE socket_test_table SET value = 15 WHERE id = 1;

-- we cannot recover in a transaction block
BEGIN;
  SELECT count(*) FROM socket_test_table;

  -- kill all the cached backends on the workers initiated by the current gpid
  select kill_all_cached_internal_conns(citus_backend_gpid());

  SELECT count(*) FROM socket_test_table;
ROLLBACK;


-- repartition joins also can recover
SET citus.enable_repartition_joins TO on;
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SELECT count(*) FROM socket_test_table t1 JOIN socket_test_table t2 USING(value);

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

-- even though the cached connections closed, the execution recovers and establishes new connections
SELECT count(*) FROM socket_test_table t1 JOIN socket_test_table t2 USING(value);

-- also, recover insert .. select repartitioning
INSERT INTO socket_test_table SELECT value::bigint, value FROM socket_test_table;

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

-- even though the cached connections closed, the execution recovers and establishes new connections
INSERT INTO socket_test_table SELECT value::bigint, value FROM socket_test_table;

-- also, recover with intermediate results
WITH cte_1 AS (SELECT * FROM socket_test_table LIMIT 1) SELECT count(*) FROM cte_1;

-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());

-- even though the cached connections closed, the execution recovers and establishes new connections
WITH cte_1 AS (SELECT * FROM socket_test_table LIMIT 1) SELECT count(*) FROM cte_1;

-- although should have no difference, we can recover from the failures on the workers as well
\c - - - :worker_1_port
SET search_path TO socket_close;

SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.max_cached_conns_per_worker TO 1;
SET citus.force_max_query_parallelization  TO ON;
SELECT count(*) FROM socket_test_table;
-- kill all the cached backends on the workers initiated by the current gpid
select kill_all_cached_internal_conns(citus_backend_gpid());
SELECT count(*) FROM socket_test_table;

\c - - - :master_port

SET client_min_messages TO ERROR;
DROP SCHEMA socket_close CASCADE;
