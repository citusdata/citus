CREATE SCHEMA "statistics'test";

SET search_path TO "statistics'test";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- test create statistics propagation
CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

CREATE STATISTICS s1 (dependencies) ON a, b FROM test_stats;

-- test for distributing an already existing statistics
CREATE TABLE test_stats2 (
    a   int,
    b   int
);

CREATE STATISTICS s2 (dependencies) ON a, b FROM test_stats2;

SELECT create_distributed_table('test_stats2', 'a');

-- test when stats is on a different schema
CREATE SCHEMA sc1;
CREATE TABLE tbl (a int, b int);
SELECT create_distributed_table ('tbl', 'a');

CREATE STATISTICS sc1.st1 ON a, b FROM tbl;

\c - - - :worker_1_port
SELECT stxname FROM pg_statistic_ext ORDER BY stxname ASC;
SELECT count(DISTINCT stxnamespace) FROM pg_statistic_ext;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA "statistics'test" CASCADE;
DROP SCHEMA sc1 CASCADE;
