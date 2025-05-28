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
INSERT INTO d1 VALUES (1,10), (1,11), (1,20), (2,10), (2,12), (2, 20);

--- For testing, remove before merge
CREATE TABLE d1_local(like d1);
INSERT INTO d1_local select * from d1;


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

-- Basic test cases with ON syntax
-- Test that the join is pushed down to the worker nodes, using "on" syntax
SET client_min_messages TO DEBUG3;
SELECT count(*) FROM r1 LEFT JOIN d1 ON r1.a = d1.a;
SELECT count(*) FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a;
SELECT * FROM r1 LEFT JOIN d1 ON r1.a = d1.a AND r1.b = d1.b ORDER BY 1, 2;
SELECT * FROM r1_local LEFT JOIN d1_local ON r1_local.a = d1_local.a AND r1_local.b = d1_local.b ORDER BY 1, 2;


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