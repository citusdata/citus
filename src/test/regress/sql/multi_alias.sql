--
-- MULTI_ALIAS
--
-- Here we test using various types of aliases for a distributed table
-- Test file is created when fixing #4269 column alias bug
--
CREATE SCHEMA alias;
SET search_path TO alias;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 90630800;

CREATE TABLE alias_test(a int, b int);
SELECT create_distributed_table('alias_test', 'a');
INSERT INTO alias_test VALUES (0, 0), (1, 1);

SET citus.log_remote_commands TO on;

SELECT * FROM alias_test;
SELECT * FROM alias_test AS alias_test;
SELECT * FROM alias_test AS another_name;
SELECT * FROM alias_test AS alias_test(col1, col2);
SELECT * FROM alias_test AS another_name(col1, col2);

RESET citus.log_remote_commands;

-- test everything on https://github.com/citusdata/citus/issues/4269
CREATE TABLE test (x int, y int, z int);
INSERT INTO test VALUES (0, 1, 2), (3, 4, 5), (6, 7, 8);

-- on PG works fine
SELECT * FROM test AS t1 (a, b, c) ORDER BY 1;
SELECT * FROM test AS t1 (a, b, c) WHERE a  = 6 ORDER BY 2;

CREATE TABLE test_2 (x int, y int);
INSERT INTO test_2 VALUES (0, 10), (3, 30);

-- on PG works fine
SELECT *
  FROM test t1 (a, b, c) JOIN test_2 t2 (a, d) USING (a)
  ORDER BY a, d;

-- same queries on Citus now also work!
SELECT create_distributed_table('test', 'x');
SELECT create_distributed_table('test_2', 'x');
SET citus.log_remote_commands TO on;

SELECT * FROM test AS t1 (a, b, c) ORDER BY 1;

SELECT *
  FROM test t1 (a, b, c) JOIN test_2 t2 (a, d) USING (a)
  ORDER BY a, d;

SELECT * FROM test AS t1 (a, b, c) WHERE a  = 6 ORDER BY 2;

SET citus.enable_fast_path_router_planner TO off;
SELECT * FROM test AS t1 (a, b, c) WHERE a  = 6 ORDER BY 2;
RESET citus.enable_fast_path_router_planner;

-- outer JOINs go through pushdown planner
SELECT *
  FROM test t1 (a, b, c) LEFT  JOIN test_2 t2 (a, d) USING (a)
  ORDER BY a, d;

RESET citus.log_remote_commands;
DROP SCHEMA alias CASCADE;
