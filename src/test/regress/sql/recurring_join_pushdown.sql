CREATE SCHEMA recurring_join_pushdown;
SET search_path TO recurring_join_pushdown;

SET citus.next_shard_id TO 1520000;
SET citus.shard_count TO 4;

CREATE TABLE r1(a int, b int);
SELECT create_reference_table('r1');
INSERT INTO r1 VALUES (1,10), (1,11), (1,20), (2,10), (2,12), (2, 20), (3, 20), (10, 1), (10, 2);

--- For testing, remove before merge
CREATE TABLE r1_local(like r1);
INSERT INTO r1_local select * from r1;

CREATE TABLE d1(a int, b int);
SELECT create_distributed_table('d1', 'a');
INSERT INTO d1 VALUES (1,10), (1,11), (1,20), (2,10), (2,12), (2, 20), (4, 10);

CREATE TABLE d2(a int, c text);
SELECT create_distributed_table('d2', 'a');
INSERT INTO d2(a, c) VALUES (1,'a'), (1,'b'), (1,'c'), (2,'d'), (2,'e'), (2,'f'), (4,'g');

--- For testing, remove before merge
CREATE TABLE d1_local(like d1);
INSERT INTO d1_local select * from d1;

CREATE TABLE d2_local(like d2);
INSERT INTO d2_local select * from d2;

SET citus.shard_count TO 2;
CREATE TABLE d3_not_colocated(like d1);
SELECT create_distributed_table('d3_not_colocated', 'a');


SET client_min_messages TO DEBUG3;

-- Basic test cases
-- Test that the join is pushed down to the worker nodes, using "using" syntax
SELECT count(*) FROM r1 LEFT JOIN d1 using (a);
SELECT count(*) FROM r1_local LEFT JOIN d1_local using (a);

SELECT * FROM r1 LEFT JOIN d1 using (a, b) ORDER BY 1, 2;
SELECT * FROM r1_local LEFT JOIN d1_local using (a, b) ORDER BY 1, 2;

SET client_min_messages TO DEBUG1;
-- Test that the join is not pushed down when joined on a non-distributed column
SELECT count(*) FROM r1 LEFT JOIN d1 USING (b);
SELECT count(*) FROM r1_local LEFT JOIN d1_local USING (b);

-- Test that the join is not pushed down when we have non-colocated tables in the RHS
SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d3_not_colocated.b FROM d3_not_colocated FULL JOIN d1 ON d3_not_colocated.a = d1.a) USING (a);
-- The same error with its RIGHT JOIN variant
SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d3_not_colocated.b FROM d3_not_colocated JOIN d1 ON d3_not_colocated.a = d1.a) USING (a);

-- Basic test cases with ON syntax
-- Test that the join is pushed down to the worker nodes, using "on" syntax
SET client_min_messages TO DEBUG3;
SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.a = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a;
SELECT * FROM r1 LEFT JOIN d1 ON r1.a = d1.a AND r1.b = d1.b ORDER BY 1, 2;
SELECT * FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a AND r1_local.b = d1_local.b ORDER BY 1, 2;

-- Verfiy that the join is pushed via the execution plan.
EXPLAIN (COSTS OFF) SELECT * FROM r1 LEFT JOIN d1 ON r1.a = d1.a AND r1.b = d1.b ORDER BY 1, 2;

SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.b = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.b = d1_local.a;  
-- Test that the join is not pushed down when joined on a non-distributed column
SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.b = d1.b;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.b = d1_local.b;
SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.a = d1.b;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.b;  

SET client_min_messages TO DEBUG1;
-- Test that the join is not pushed down when joined on a distributed column with disjunctive conditions
SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.a = d1.a OR r1.b = d1.b;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a OR r1_local.b = d1_local.b; 

-- Test join pushdown behavior when the inner part of the join is a subquery
-- Using 'using' syntax
SET client_min_messages TO DEBUG3;
SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1) USING (a);
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM d1_local) USING (a);

SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1 WHERE a > 1) USING (a);
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM d1_local WHERE a > 1) USING (a);

SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM (SELECT * FROM d1) WHERE a > 1) USING (a);
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM (SELECT * FROM d1_local) WHERE a > 1) USING (a);

SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1 JOIN d1 as d1_1 USING (a))  USING (a);
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM d1_local JOIN d1_local as d1_1 USING (a))  USING (a);

SELECT count(*) FROM r1 LEFT JOIN (d1 LEFT JOIN d1 as d1_1 USING (a))  USING (a);
SELECT count(*) FROM r1_local LEFT JOIN (d1_local LEFT JOIN d1_local as d1_1 USING (a))  USING (a);

EXPLAIN (COSTS OFF) SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1) USING (a);


-- Using 'on' syntax
SET client_min_messages TO DEBUG3;
SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1) AS d1 ON r1.a = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM d1_local) AS d1_local ON r1_local.a = d1_local.a;

SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM d1 WHERE a > 1) AS d1 ON r1.a = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM d1_local WHERE a > 1) AS d1_local ON r1_local.a = d1_local.a;

SELECT count(*) FROM r1 LEFT JOIN (SELECT * FROM (SELECT * FROM d1) AS d1 WHERE a > 1) AS d1 ON r1.a = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT * FROM (SELECT * FROM d1_local) AS d1_local WHERE a > 1) AS d1_local ON r1_local.a = d1_local.a;

SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a as a, d1.b, d1_1.a AS a_1 FROM d1 LEFT JOIN d1 as d1_1 ON d1.a = d1_1.a) AS d1_2 ON r1.a = d1_2.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT d1_local.a as a, d1_local.b, d1_1.a AS a_1 FROM d1_local LEFT JOIN d1_local as d1_1 ON d1_local.a = d1_1.a) AS d1_2 ON r1_local.a = d1_2.a;


-- Nested joins
-- It is safe to push the inner join to compute t1. However, as the var of the inner table for the top level join (t1.a) resolves to r1.a, the outer join cannot be pushed down.
SELECT count(*) FROM r1 LEFT JOIN (SELECT r1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON r1.a = t1.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT r1_local.a, d1_local.b FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a) AS t1 ON r1_local.a = t1.a;
EXPLAIN (COSTS OFF) SELECT count(*) FROM r1 LEFT JOIN (SELECT r1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON r1.a = t1.a;

-- In the following case, it is safe to push down both joins as t1.a resolves to d1.a.
SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON r1.a = t1.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT d1_local.a, d1_local.b FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a) AS t1 ON r1_local.a = t1.a;
EXPLAIN (COSTS OFF) SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON r1.a = t1.a;

-- In the following case, the lower level joins will be pushed down, but as the top level join is chained, subquery pushdown will not be applied at the top level.
SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON t1.a = r1.a LEFT JOIN (SELECT d2.a, d2.c FROM r1 LEFT JOIN d2 ON r1.a = d2.a) AS t2 ON t1.a = t2.a;
SELECT count(*) FROM r1_local LEFT JOIN (SELECT d1_local.a, d1_local.b FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a) AS t1 ON t1.a = r1_local.a LEFT JOIN (SELECT d2_local.a, d2_local.c FROM r1_local LEFT JOIN d2_local ON r1_local.a = d2_local.a) AS t2 ON t1.a = t2.a;
EXPLAIN (COSTS OFF) SELECT count(*) FROM r1 LEFT JOIN (SELECT d1.a, d1.b FROM r1 LEFT JOIN d1 ON r1.a = d1.a) AS t1 ON t1.a = r1.a LEFT JOIN (SELECT d2.a, d2.c FROM r1 LEFT JOIN d2 ON r1.a = d2.a) AS t2 ON t1.a = t2.a;

--- As both subqueries are pushed and the top level join is over their results on distribution colums, the query is pushed down as a whole.
SELECT count(*) FROM (SELECT d1_1.a, r1.b FROM r1 LEFT JOIN d1 as d1_1 ON r1.a = d1_1.a) AS t1 LEFT JOIN
 (SELECT d2.a, d2.c, r1.b FROM r1 LEFT JOIN d2 ON r1.a = d2.a) AS t2 ON t1.a = t2.a;
SELECT count(*) FROM (SELECT d1_1.a, r1_local.b FROM r1_local LEFT JOIN d1_local as d1_1 ON r1_local.a = d1_1.a) AS t1_local LEFT JOIN
 (SELECT d2_local.a, d2_local.c, r1_local.b FROM r1_local LEFT JOIN d2_local ON r1_local.a = d2_local.a) AS t2_local ON t1_local.a = t2_local.a;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT d1_1.a, r1.b FROM r1 LEFT JOIN d1 as d1_1 ON r1.a = d1_1.a) AS t1 LEFT JOIN
 (SELECT d2.a, d2.c, r1.b FROM r1 LEFT JOIN d2 ON r1.a = d2.a) AS t2 ON t1.a = t2.a;
