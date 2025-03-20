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

-- test everything on https://github.com/citusdata/citus/issues/7684
CREATE TABLE test_name_prefix (
attribute1 varchar(255),
attribute2 varchar(255),
attribute3 varchar(255)
);

INSERT INTO test_name_prefix (attribute1, attribute2, attribute3)
VALUES ('Phone', 'John', 'A'),
('Phone', 'Eric', 'A'),
('Tablet','Eric', 'B');

-- vanilla Postgres result
-- with DISTINCT ON (T.attribute1, T.attribute2)
-- we have 3 distinct groups of 1 row each
-- (Phone, John) (Phone, Eric) and (Tablet, Eric)
SELECT DISTINCT ON (T.attribute1, T.attribute2)
T.attribute1 AS attribute1,
T.attribute3 AS attribute2,
T.attribute2 AS attribute3
FROM test_name_prefix T ORDER BY T.attribute1, T.attribute2;

-- vanilla Postgres result
-- changes when we remove the table-name prefix to attribute2
-- in this case it uses the output column name,
-- which is actually T.attribute3 (AS attribute2)
-- so, with DISTINCT ON (T.attribute1, T.attribute3)
-- we have only 2 distinct groups
-- (Phone, A) and (Tablet, B)
SELECT DISTINCT ON (T.attribute1, attribute2)
T.attribute1 AS attribute1,
T.attribute3 AS attribute2, -- name match in output column name
T.attribute2 AS attribute3
FROM test_name_prefix T ORDER BY T.attribute1, attribute2;

-- now, let's verify the distributed query scenario
SELECT create_distributed_table('test_name_prefix', 'attribute1');

SET citus.log_remote_commands TO on;

-- make sure we preserve the table-name prefix to attribute2
-- when building the shard query
-- (before this patch we wouldn't preserve T.attribute2)
-- note that we only need to preserve T.attribute2, not T.attribute1
-- because there is no confusion there
SELECT DISTINCT ON (T.attribute1, T.attribute2)
T.attribute1 AS attribute1,
T.attribute3 AS attribute2,
T.attribute2 AS attribute3
FROM test_name_prefix T ORDER BY T.attribute1, T.attribute2;

-- here Citus will replace attribute2 with T.attribute3
SELECT DISTINCT ON (T.attribute1, attribute2)
T.attribute1 AS attribute1,
T.attribute3 AS attribute2,
T.attribute2 AS attribute3
FROM test_name_prefix T ORDER BY T.attribute1, attribute2;

RESET citus.log_remote_commands;

DROP SCHEMA alias CASCADE;
