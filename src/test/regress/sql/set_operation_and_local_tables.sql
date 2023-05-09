CREATE SCHEMA recursive_set_local;
SET search_path TO recursive_set_local, public;

SET citus.enable_repartition_joins to ON;

CREATE TABLE recursive_set_local.test (x int, y int);
SELECT create_distributed_table('test', 'x');

CREATE TABLE recursive_set_local.ref (a int, b int);
SELECT create_reference_table('ref');

CREATE TABLE recursive_set_local.local_test (x int, y int);

INSERT INTO test VALUES (1,1), (2,2);
INSERT INTO ref VALUES (2,2), (3,3);
INSERT INTO local_test VALUES (3,3), (4,4);

SET client_min_messages TO DEBUG;

-- we should be able to run set operations with local tables
(SELECT x FROM test) INTERSECT (SELECT x FROM local_test) ORDER BY 1 DESC;

-- we should be able to run set operations with generate series
(SELECT x FROM test) INTERSECT (SELECT i FROM generate_series(0, 100) i) ORDER BY 1 DESC;

-- we'd first recursively plan the query with "test", thus don't need to recursively
-- plan other query
(SELECT x FROM test LIMIT 5) INTERSECT (SELECT i FROM generate_series(0, 100) i) ORDER BY 1 DESC;

-- this doesn't require any recursive planning
(SELECT a FROM ref) INTERSECT (SELECT i FROM generate_series(0, 100) i) ORDER BY 1 DESC;

-- same query with a failure on the worker (i.e., division by zero)
(SELECT x FROM test) INTERSECT (SELECT i/0 FROM generate_series(0, 100) i) ORDER BY 1 DESC;

-- we should be able to run set operations with generate series and local tables as well
((SELECT x FROM local_test) UNION ALL (SELECT x FROM test)) INTERSECT (SELECT i FROM generate_series(0, 100) i) ORDER BY 1 DESC;

-- two local tables are on different leaf queries, so safe to plan & execute
((SELECT x FROM local_test) UNION ALL (SELECT x FROM test)) INTERSECT (SELECT x FROM local_test) ORDER BY 1 DESC;

-- use ctes inside unions along with local tables on the top level
WITH
cte_1 AS (SELECT user_id FROM users_table),
cte_2 AS (SELECT user_id FROM events_table)
((SELECT * FROM cte_1) UNION (SELECT * FROM cte_2) UNION (SELECT x FROM local_test)) INTERSECT (SELECT i FROM generate_series(0, 100) i)
ORDER BY 1 DESC;

-- CTEs inside subqueries unioned with local table
-- final query is real-time
SELECT
	count(*)
FROM
	(
		((WITH cte_1 AS (SELECT x FROM test) SELECT * FROM cte_1) UNION
		(WITH cte_1 AS (SELECT a FROM ref) SELECT * FROM cte_1)) INTERSECT
		(SELECT x FROM local_test)
	) as foo,
	test
	WHERE test.y = foo.x;

-- CTEs inside subqueries unioned with local table
-- final query is router
SELECT
	count(*)
FROM
	(
		((WITH cte_1 AS (SELECT x FROM test) SELECT * FROM cte_1) UNION
		(WITH cte_1 AS (SELECT a FROM ref) SELECT * FROM cte_1)) INTERSECT
		(SELECT x FROM local_test)
	) as foo,
	ref
	WHERE ref.a = foo.x;

-- subquery union in WHERE clause without parition column equality is recursively planned including the local tables
SELECT * FROM test a WHERE x IN (SELECT x FROM test b UNION SELECT y FROM test c UNION SELECT y FROM local_test d) ORDER BY 1,2;

-- same query with subquery in where is wrapped in CTE
SELECT * FROM test a WHERE x IN (WITH cte AS (SELECT x FROM test b UNION SELECT y FROM test c UNION SELECT y FROM local_test d) SELECT * FROM cte) ORDER BY 1,2;

-- supported since final step only has local table and intermediate result
SELECT * FROM ((SELECT * FROM test) EXCEPT (SELECT * FROM test ORDER BY x LIMIT 1)) u JOIN local_test USING (x) ORDER BY 1,2;

-- though we replace some queries including the local query, the intermediate result is on the outer part of an outer join
SELECT * FROM ((SELECT * FROM local_test) INTERSECT (SELECT * FROM test ORDER BY x LIMIT 1)) u LEFT JOIN test USING (x) ORDER BY 1,2;

-- we replace some queries including the local query, the intermediate result is on the inner part of an outer join
SELECT * FROM ((SELECT * FROM local_test) INTERSECT (SELECT * FROM test ORDER BY x LIMIT 1)) u RIGHT JOIN test USING (x) ORDER BY 1,2;

-- recurively plan left part of the join, and run a final real-time query
SELECT * FROM ((SELECT * FROM local_test) INTERSECT (SELECT * FROM test ORDER BY x LIMIT 1)) u INNER JOIN test USING (x) ORDER BY 1,2;

-- set operations and the sublink can be recursively planned
SELECT * FROM ((SELECT x FROM test) UNION (SELECT x FROM (SELECT x FROM local_test) as foo WHERE x IN (SELECT x FROM test))) u ORDER BY 1;


--  repartition is recursively planned before the set operation
(SELECT x FROM test) INTERSECT (SELECT t1.x FROM test as t1, test as t2 WHERE t1.x = t2.y LIMIT 2) INTERSECT (((SELECT x FROM local_test) UNION ALL (SELECT x FROM test)) INTERSECT (SELECT i FROM generate_series(0, 100) i)) ORDER BY 1 DESC;

SET client_min_messages TO WARNING;
DROP SCHEMA recursive_set_local CASCADE;
