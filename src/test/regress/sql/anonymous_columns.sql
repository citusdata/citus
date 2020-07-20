SET citus.next_shard_id TO 20080000;

CREATE SCHEMA anonymous_columns;
SET search_path TO anonymous_columns;

CREATE TABLE t0 (a int PRIMARY KEY, b int, "?column?" text);
SELECT create_distributed_table('t0', 'a');
INSERT INTO t0 VALUES (1, 2, 'hello'), (2, 4, 'world');

SELECT "?column?" FROM t0 ORDER BY 1;

WITH a AS (SELECT * FROM t0) SELECT "?column?" FROM a ORDER BY 1;
WITH a AS (SELECT '' FROM t0) SELECT * FROM a;

-- test CTE's that could be rewritten as subquery
WITH a AS (SELECT '' FROM t0 GROUP BY a) SELECT * FROM a;
WITH a AS (SELECT '' FROM t0 GROUP BY b) SELECT * FROM a;
WITH a AS (SELECT '','' FROM t0 GROUP BY a) SELECT * FROM a;
WITH a AS (SELECT '','' FROM t0 GROUP BY b) SELECT * FROM a;
WITH a AS (SELECT 1, * FROM t0 WHERE a = 1) SELECT * FROM a;

-- test CTE's that are referenced multiple times and hence need to stay CTE's
WITH a AS (SELECT '' FROM t0 WHERE a = 1) SELECT * FROM a, a b;
WITH a AS (SELECT '','' FROM t0 WHERE a = 42) SELECT * FROM a, a b;

-- test with explicit subqueries
SELECT * FROM (SELECT a, '' FROM t0 GROUP BY a) as foo ORDER BY 1;
SELECT * FROM (SELECT a, '', '' FROM t0 GROUP BY a ) as foo ORDER BY 1;
SELECT * FROM (SELECT b, '' FROM t0 GROUP BY b ) as foo ORDER BY 1;
SELECT * FROM (SELECT b, '', '' FROM t0 GROUP BY b ) as foo ORDER BY 1;

-- some tests that follow very similar codeoaths
SELECT a + 1 FROM t0 ORDER BY 1;
SELECT a + 1, a - 1 FROM t0 ORDER BY 1;
WITH cte1 AS (SELECT row_to_json(row(a))->'f1' FROM t0) SELECT * FROM cte1 ORDER BY "?column?"::text;

-- clean up after test
SET client_min_messages TO WARNING;
DROP SCHEMA anonymous_columns CASCADE;
