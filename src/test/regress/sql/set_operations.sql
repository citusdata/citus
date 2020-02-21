CREATE SCHEMA recursive_union;
SET search_path TO recursive_union, public;
SET citus.coordinator_aggregation_strategy TO 'disabled';

CREATE TABLE recursive_union.test (x int, y int);
SELECT create_distributed_table('test', 'x');

CREATE TABLE recursive_union.ref (a int, b int);
SELECT create_reference_table('ref');

CREATE TABLE test_not_colocated (LIKE test);
SELECT create_distributed_table('test_not_colocated', 'x', colocate_with := 'none');

INSERT INTO test VALUES (1,1), (2,2);
INSERT INTO ref VALUES (2,2), (3,3);

-- top-level set operations are supported through recursive planning
SET client_min_messages TO DEBUG;

(SELECT * FROM test) UNION (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) UNION (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) UNION (SELECT * FROM ref) ORDER BY 1,2;

(SELECT * FROM test) UNION ALL (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) UNION ALL (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) UNION ALL (SELECT * FROM ref) ORDER BY 1,2;

(SELECT * FROM test) INTERSECT (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) INTERSECT (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) INTERSECT (SELECT * FROM ref) ORDER BY 1,2;

(SELECT * FROM test) INTERSECT ALL (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) INTERSECT ALL (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) INTERSECT ALL (SELECT * FROM ref) ORDER BY 1,2;

(SELECT * FROM test) EXCEPT (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) EXCEPT (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) EXCEPT (SELECT * FROM ref) ORDER BY 1,2;

(SELECT * FROM test) EXCEPT ALL (SELECT * FROM test) ORDER BY 1,2;
(SELECT * FROM test) EXCEPT ALL (SELECT * FROM ref) ORDER BY 1,2;
(SELECT * FROM ref) EXCEPT ALL (SELECT * FROM ref) ORDER BY 1,2;

-- more complex set operation trees are supported
(SELECT * FROM test)
INTERSECT
(SELECT * FROM ref)
UNION ALL
(SELECT s, s FROM generate_series(1,10) s)
EXCEPT
(SELECT 1,1)
UNION
(SELECT test.x, ref.a FROM test LEFT JOIN ref ON (x = a))
ORDER BY 1,2;

-- within a subquery, some unions can be pushed down
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT x, y FROM test) UNION (SELECT y, x FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM ref)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM ref) UNION (SELECT * FROM ref)) u ORDER BY 1,2;

SELECT * FROM ((SELECT * FROM test) UNION ALL (SELECT * FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT x, y FROM test) UNION ALL (SELECT y, x FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) UNION ALL (SELECT * FROM ref)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM ref) UNION ALL (SELECT * FROM ref)) u ORDER BY 1,2;

SELECT * FROM ((SELECT * FROM test) INTERSECT (SELECT * FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT x, y FROM test) INTERSECT (SELECT y, x FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) INTERSECT (SELECT * FROM ref)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM ref) INTERSECT (SELECT * FROM ref)) u ORDER BY 1,2;

SELECT * FROM ((SELECT * FROM test) EXCEPT (SELECT * FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT x, y FROM test) EXCEPT (SELECT y, x FROM test)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) EXCEPT (SELECT * FROM ref)) u ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM ref) EXCEPT (SELECT * FROM ref)) u ORDER BY 1,2;

-- unions can even be pushed down within a join
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test)) u JOIN test USING (x) ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) UNION ALL (SELECT * FROM test)) u LEFT JOIN test USING (x) ORDER BY 1,2;

-- unions cannot be pushed down if one leaf recurs
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test ORDER BY x LIMIT 1)) u JOIN test USING (x) ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) UNION ALL (SELECT * FROM test ORDER BY x LIMIT 1)) u LEFT JOIN test USING (x) ORDER BY 1,2;

-- unions in a join without partition column equality (column names from first query are used for join)
SELECT * FROM ((SELECT x, y FROM test) UNION (SELECT y, x FROM test)) u JOIN test USING (x) ORDER BY 1,2;
SELECT * FROM ((SELECT x, y FROM test) UNION (SELECT 1, 1 FROM test)) u JOIN test USING (x) ORDER BY 1,2;

-- a join between a set operation and a generate_series which is pushdownable
 SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test ORDER BY x)) u JOIN generate_series(1,10) x USING (x) ORDER BY 1,2;

-- a join between a set operation and a generate_series which is not pushdownable due to EXCEPT
 SELECT * FROM ((SELECT * FROM test) EXCEPT (SELECT * FROM test ORDER BY x)) u JOIN generate_series(1,10) x USING (x) ORDER BY 1,2;

-- subqueries in WHERE clause with set operations fails due to the current limitaions of recursive planning IN WHERE clause
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test)) foo WHERE x IN (SELECT y FROM test);

-- subqueries in WHERE clause forced to be recursively planned
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test)) foo WHERE x IN (SELECT y FROM test ORDER BY 1 LIMIT 4) ORDER BY 1;

-- now both the set operations and the sublink is recursively planned
SELECT * FROM ((SELECT x,y FROM test) UNION (SELECT y,x FROM test)) foo WHERE x IN (SELECT y FROM test ORDER BY 1 LIMIT 4) ORDER BY 1;

-- set operations and the sublink can be recursively planned
SELECT * FROM ((SELECT x,y FROM test) UNION (SELECT y,x FROM test)) foo WHERE x IN (SELECT y FROM test) ORDER BY 1;

-- set operations work fine with pushdownable window functions
SELECT x, y, rnk FROM (SELECT *, rank() OVER my_win as rnk FROM test WINDOW my_win AS (PARTITION BY x ORDER BY y DESC)) as foo
UNION
SELECT x, y, rnk FROM (SELECT *, rank() OVER my_win as rnk FROM test WINDOW my_win AS (PARTITION BY x ORDER BY y DESC)) as bar
ORDER BY 1 DESC, 2 DESC, 3 DESC;

-- set operations work fine with non-pushdownable window functions
SELECT x, y, rnk FROM (SELECT *, rank() OVER my_win as rnk FROM test WINDOW my_win AS (PARTITION BY y ORDER BY x DESC)) as foo
UNION
SELECT x, y, rnk FROM (SELECT *, rank() OVER my_win as rnk FROM test WINDOW my_win AS (PARTITION BY y ORDER BY x DESC)) as bar
ORDER BY 1 DESC, 2 DESC, 3 DESC;

-- other set operations in joins also cannot be pushed down
SELECT * FROM ((SELECT * FROM test) EXCEPT (SELECT * FROM test ORDER BY x LIMIT 1)) u JOIN test USING (x) ORDER BY 1,2;
SELECT * FROM ((SELECT * FROM test) INTERSECT (SELECT * FROM test ORDER BY x LIMIT 1)) u LEFT JOIN test USING (x) ORDER BY 1,2;

-- distributed table in WHERE clause is recursively planned
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM ref WHERE a IN (SELECT x FROM test))) u ORDER BY 1,2;

-- subquery union in WHERE clause with partition column equality and implicit join is pushed down
SELECT * FROM test a WHERE x IN (SELECT x FROM test b WHERE y = 1 UNION SELECT x FROM test c WHERE y = 2) ORDER BY 1,2;

-- subquery union in WHERE clause with partition column equality, without implicit join on partition column is recursively planned
SELECT * FROM test a WHERE x NOT IN (SELECT x FROM test b WHERE y = 1 UNION SELECT x FROM test c WHERE y = 2) ORDER BY 1,2;

-- subquery union in WHERE clause without parition column equality is recursively planned
SELECT * FROM test a WHERE x IN (SELECT x FROM test b UNION SELECT y FROM test c) ORDER BY 1,2;

-- correlated subquery with union in WHERE clause
SELECT * FROM test a WHERE x IN (SELECT x FROM test b UNION SELECT y FROM test c WHERE a.x = c.x) ORDER BY 1,2;

-- force unions to be planned while subqueries are being planned
SELECT * FROM ((SELECT * FROM test) UNION (SELECT * FROM test) ORDER BY 1,2 LIMIT 5) as foo ORDER BY 1 DESC LIMIT 3;

-- distinct and count distinct should work without any problems
select count(DISTINCT t.x) FROM ((SELECT DISTINCT x FROM test) UNION (SELECT DISTINCT y FROM test)) as t(x) ORDER BY 1;
select count(DISTINCT t.x) FROM ((SELECT count(DISTINCT x) FROM test) UNION (SELECT count(DISTINCT y) FROM test)) as t(x) ORDER BY 1;

-- other agg. distincts are also supported when group by includes partition key
select avg(DISTINCT t.x) FROM ((SELECT avg(DISTINCT y) FROM test GROUP BY x) UNION (SELECT avg(DISTINCT y) FROM test GROUP BY x)) as t(x) ORDER BY 1;

-- other agg. distincts are not supported when group by doesn't include partition key
select count(DISTINCT t.x) FROM ((SELECT avg(DISTINCT y) FROM test GROUP BY y) UNION (SELECT avg(DISTINCT y) FROM test GROUP BY y)) as t(x) ORDER BY 1;

-- one of the leaves is a repartition join
SET citus.enable_repartition_joins TO ON;

--  repartition is recursively planned before the set operation
(SELECT x FROM test) INTERSECT (SELECT t1.x FROM test as t1, test as t2 WHERE t1.x = t2.y LIMIT 0) ORDER BY 1 DESC;

--  repartition is recursively planned with the set operation
(SELECT x FROM test) INTERSECT (SELECT t1.x FROM test as t1, test as t2 WHERE t1.x = t2.y) ORDER BY 1 DESC;

SET citus.enable_repartition_joins TO OFF;

-- this should be recursively planned
CREATE VIEW set_view_recursive AS (SELECT y FROM test) UNION (SELECT y FROM test);
SELECT * FROM set_view_recursive ORDER BY 1 DESC;

-- this should be pushed down
CREATE VIEW set_view_pushdown AS (SELECT x FROM test) UNION (SELECT x FROM test);
SELECT * FROM set_view_pushdown ORDER BY 1 DESC;

-- this should be recursively planned
CREATE VIEW set_view_recursive_second AS SELECT u.x, test.y FROM ((SELECT x, y FROM test) UNION (SELECT 1, 1 FROM test)) u JOIN test USING (x) ORDER BY 1,2;
SELECT * FROM set_view_recursive_second ORDER BY 1,2;

-- this should create lots of recursive calls since both views and set operations lead to recursive plans :)
((SELECT x FROM set_view_recursive_second) INTERSECT (SELECT * FROM set_view_recursive)) EXCEPT (SELECT * FROM set_view_pushdown);

-- queries on non-colocated tables that would push down if they were not colocated are recursivelu planned
SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test_not_colocated) u ORDER BY 1,2;
SELECT * FROM (SELECT * FROM test UNION ALL SELECT * FROM test_not_colocated) u ORDER BY 1,2;

RESET client_min_messages;
DROP SCHEMA recursive_union CASCADE;
