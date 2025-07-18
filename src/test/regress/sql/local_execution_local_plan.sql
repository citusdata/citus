-- Test local execution with local plan in a sharded environment.
-- This is an enhancement to local execution where instead of deparsing
-- and compiling the shard query, the planner replaces the OID of the
-- distributed table with the OID of the local shard in the parse tree
-- and plans that.
--
-- https://github.com/citusdata/citus/pull/8035

CREATE SCHEMA local_shard_execution_local_plan;
SET search_path TO local_shard_execution_local_plan;

SET citus.next_shard_id TO 86000000;

-- Test row-based sharding

SET citus.shard_replication_factor TO 1;
CREATE TABLE test_tbl (a int, b int, data_f double precision);
SELECT create_distributed_table('test_tbl', 'a');
SELECT setseed(0.42); -- make the random data inserted deterministic
INSERT INTO test_tbl
SELECT (random()*20)::int AS a,
       (random()*20)::int AS b,
       random()*10000.0 AS data_f
FROM generate_series(1, 10000);

-- Put the shard on worker 1 to ensure consistent test output across different schedules
SET client_min_messages to ERROR; -- suppress warning if shard is already on worker 1
SELECT citus_move_shard_placement(86000000, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes');
SELECT public.wait_for_resource_cleanup(); -- otherwise fails flakiness tests
RESET client_min_messages;

\c - - - :worker_1_port
SET search_path TO local_shard_execution_local_plan;

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
SET citus.log_local_commands TO ON;

-- This query resolves to a single shard (aka fast path)
-- which is located on worker_1; with client_min_messages
-- at DEBUG2 we see a message that the planner is avoiding
-- query deparse and plan
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

BEGIN;
  -- force accessing local placements via remote connections first
  SET citus.enable_local_execution TO false;
  RESET client_min_messages;
  RESET citus.log_remote_commands;
  RESET citus.log_local_commands;
  SELECT count(*), b FROM test_tbl GROUP BY b ORDER BY b;
  -- Now, even if we enable local execution back before the query that
  -- could normally benefit from fast path local query optimizations,
  -- this time it won't be the case because local execution was implicitly
  -- disabled by Citus as we accessed local shard placements via remote
  -- connections.
  SET citus.enable_local_execution TO true;
  SET client_min_messages TO DEBUG2;
  SET citus.log_remote_commands TO ON;
  SET citus.log_local_commands TO ON;
  SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
  FROM test_tbl
  WHERE a = 8 AND b IN (1,3,5,8,13,21)
  GROUP BY b
  ORDER BY b;
SET client_min_messages TO ERROR; -- keep COMMIT output quiet
COMMIT;
SET client_min_messages TO DEBUG2;

SET citus.enable_local_fast_path_query_optimization TO OFF;

-- With local execution local plan disabled, the same query
-- does query deparse and planning of the shard query and
-- provides the same results
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

\c - - - :worker_2_port
SET search_path TO local_shard_execution_local_plan;

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
SET citus.log_local_commands TO ON;

-- Run the same query on the other worker - the local
-- execution path is not taken because the shard is not
-- local to this worker
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

\c - - - :master_port

SET search_path TO local_shard_execution_local_plan;
SET citus.next_shard_id TO 86001000;

-- Test citus local and reference tables
CREATE TABLE ref_tbl (a int, b int, data_f double precision);
SELECT create_reference_table('ref_tbl');
SELECT setseed(0.42); -- make the random data inserted deterministic
INSERT INTO ref_tbl
SELECT (random()*20)::int AS a,
       (random()*20)::int AS b,
       random()*10000.0 AS data_f
FROM generate_series(1, 10000);

SET citus.next_shard_id TO 86002000;
CREATE TABLE citus_tbl (a int, b int, data_f double precision);
SELECT citus_set_coordinator_host('localhost', :master_port);
SELECT citus_add_local_table_to_metadata('citus_tbl');
INSERT INTO citus_tbl SELECT a, b, data_f FROM ref_tbl;

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
SET citus.log_local_commands TO ON;

-- citus local table: can use the fast path optimization
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM citus_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

-- reference table: does not use the fast path optimization.
-- It may be enabled in a future enhancement.
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM ref_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

\c - - - :master_port

-- Now test local execution with local plan for a schema sharded table.

SET citus.enable_schema_based_sharding to on;
CREATE SCHEMA schema_sharding_test;
SET search_path TO schema_sharding_test;

SET citus.next_shard_id TO 87000000;

SET citus.shard_replication_factor TO 1;
CREATE TABLE test_tbl (a int, b int, data_f double precision);
SELECT setseed(0.42); -- make the random data inserted deterministic
INSERT INTO test_tbl
SELECT (random()*20)::int AS a,
       (random()*20)::int AS b,
       random()*10000.0 AS data_f
FROM generate_series(1, 10000);

-- Put the shard on worker 2 to ensure consistent test output across different schedules
SET client_min_messages to ERROR; -- suppress warning if shard is already on worker 2
SELECT citus_move_shard_placement(87000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
SELECT public.wait_for_resource_cleanup(); -- otherwise fails flakiness tests
RESET client_min_messages;

\c - - - :worker_1_port

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
SET citus.log_local_commands TO ON;

-- Run the test query on worker_1; with schema based sharding
-- the data is not local to this worker so local execution
-- path is not taken.
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM schema_sharding_test.test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

\c - - - :worker_2_port

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;
SET citus.log_local_commands TO ON;

-- Run the test query on worker_2; with schema based sharding
-- the data is local to this worker so local execution
-- path is taken, and the planner avoids query deparse and
-- planning of the shard query.

SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM schema_sharding_test.test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

SET citus.enable_local_fast_path_query_optimization TO OFF;

-- Run the test query on worker_2 but with local execution
-- local plan disabled; now the planner does query deparse
-- and planning of the shard query.
SELECT b, AVG(data_f), MIN(data_f), MAX(data_f), COUNT(1)
FROM schema_sharding_test.test_tbl
WHERE a = 8 AND b IN (1,3,5,8,13,21)
GROUP BY b
ORDER BY b;

\c - - - :master_port

SET client_min_messages to ERROR;
DROP SCHEMA local_shard_execution_local_plan CASCADE;
DROP SCHEMA schema_sharding_test CASCADE;
RESET ALL;
