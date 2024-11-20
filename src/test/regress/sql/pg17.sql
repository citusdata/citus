--
-- PG17
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 17 AS server_version_ge_17
\gset

-- PG17 has the capabilty to pull up a correlated ANY subquery to a join if
-- the subquery only refers to its immediate parent query. Previously, the
-- subquery needed to be implemented as a SubPlan node, typically as a
-- filter on a scan or join node. This PG17 capability enables Citus to
-- run queries with correlated subqueries in certain cases, as shown here.
-- Relevant PG commit:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=9f1337639

-- This feature is tested for all PG versions, not just PG17; each test query with
-- a correlated subquery should fail with PG version < 17.0, but the test query
-- rewritten to reflect how PG17 optimizes it should succeed with PG < 17.0

CREATE SCHEMA pg17_corr_subq_folding;
SET search_path TO pg17_corr_subq_folding;
SET citus.next_shard_id TO 20240017;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

CREATE TABLE test (x int, y int);
SELECT create_distributed_table('test', 'x');
INSERT INTO test VALUES (1,1), (2,2);

-- Query 1: WHERE clause has a correlated subquery with a UNION. PG17 can plan
-- this as a nested loop join with the subquery as the inner. The correlation
-- is on the distribution column so the join can be pushed down by Citus.
explain (costs off)
SELECT *
FROM test a
WHERE x IN (SELECT x FROM test b UNION SELECT y FROM test c WHERE a.x = c.x)
ORDER BY 1,2;

SET client_min_messages TO DEBUG2;
SELECT *
FROM test a
WHERE x IN (SELECT x FROM test b UNION SELECT y FROM test c WHERE a.x = c.x)
ORDER BY 1,2;
RESET client_min_messages;

-- Query 1 rewritten with subquery pulled up to a join, as done by PG17 planner;
-- this query can be run without issues by Citus with older (pre PG17) PGs.
explain (costs off)
SELECT a.*
FROM test a JOIN LATERAL (SELECT x FROM test b UNION SELECT y FROM test c WHERE a.x = c.x) dt1 ON a.x = dt1.x
ORDER BY 1,2;

SET client_min_messages TO DEBUG2;
SELECT a.*
FROM test a JOIN LATERAL (SELECT x FROM test b UNION SELECT y FROM test c WHERE a.x = c.x) dt1 ON a.x = dt1.x
ORDER BY 1,2;
RESET client_min_messages;

CREATE TABLE users (user_id int, time int, dept int, info bigint);
CREATE TABLE events (user_id int, time int, event_type int, payload text);
select create_distributed_table('users', 'user_id');
select create_distributed_table('events', 'user_id');

insert into users
select i, 2021 + (i % 3), i % 5, 99999 * i from generate_series(1, 10) i;

insert into events
select i % 10 + 1, 2021 + (i % 3), i %11, md5((i*i)::text) from generate_series(1, 100) i;

-- Query 2. In Citus correlated subqueries can not be used in the WHERE
-- clause but if the subquery can be pulled up to a join it becomes possible
-- for Citus to run the query, per this example. Pre PG17 the suqbuery
-- was implemented as a SubPlan filter on the events table scan.
EXPLAIN (costs off)
WITH event_id
     AS(SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type
         FROM   events)
SELECT Count(*)
FROM   event_id
WHERE  (events_user_id) IN (SELECT user_id
                          FROM   users
                          WHERE  users.time = events_time);

SET client_min_messages TO DEBUG2;
WITH event_id
     AS(SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type
         FROM   events)
SELECT Count(*)
FROM   event_id
WHERE  (events_user_id) IN (SELECT user_id
                          FROM   users
                          WHERE  users.time = events_time);
RESET client_min_messages;

-- Query 2 rewritten with subquery pulled up to a join, as done by pg17 planner. Citus
-- Citus is able to run this query with previous pg versions. Note that the CTE can be
-- disregarded because it is inlined, being only referenced once.
EXPLAIN (COSTS OFF)
SELECT Count(*)
FROM (SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type FROM   events) dt1
INNER JOIN (SELECT distinct user_id, time FROM   users) dt
    ON events_user_id = dt.user_id and events_time = dt.time;

SET client_min_messages TO DEBUG2;
SELECT Count(*)
FROM (SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type FROM   events) dt1
INNER JOIN (SELECT distinct user_id, time FROM   users) dt
    ON events_user_id = dt.user_id and events_time = dt.time;
RESET client_min_messages;

-- Query 3: another example where recursive planning was prevented due to
-- correlated subqueries, but with PG17 folding the subquery to a join it is
-- possible for Citus to plan and run the query.
EXPLAIN (costs off)
SELECT dept, sum(user_id) FROM
(SELECT users.dept, users.user_id
FROM users, events as d1
WHERE d1.user_id = users.user_id
	AND users.dept IN (3,4)
	AND users.user_id IN
	(SELECT s2.user_id FROM users as s2
		GROUP BY d1.user_id, s2.user_id)) dt
GROUP BY dept;

SET client_min_messages TO DEBUG2;
SELECT dept, sum(user_id) FROM
(SELECT users.dept, users.user_id
FROM users, events as d1
WHERE d1.user_id = users.user_id
	AND users.dept IN (3,4)
	AND users.user_id IN
	(SELECT s2.user_id FROM users as s2
		GROUP BY d1.user_id, s2.user_id)) dt
GROUP BY dept;
RESET client_min_messages;

-- Query 3 rewritten in a similar way to how the PG17 pulls up the subquery;
-- the join is on the distribution key so Citus can push down.
EXPLAIN (costs off)
SELECT dept, sum(user_id) FROM
(SELECT users.dept, users.user_id
FROM users, events as d1
     JOIN LATERAL (SELECT s2.user_id FROM users as s2
		   GROUP BY s2.user_id HAVING d1.user_id IS NOT NULL) as d2 ON 1=1
WHERE d1.user_id = users.user_id
      AND users.dept IN (3,4)
	AND users.user_id = d2.user_id) dt
GROUP BY dept;

SET client_min_messages TO DEBUG2;
SELECT dept, sum(user_id) FROM
(SELECT users.dept, users.user_id
FROM users, events as d1
     JOIN LATERAL (SELECT s2.user_id FROM users as s2
		   GROUP BY s2.user_id HAVING d1.user_id IS NOT NULL) as d2 ON 1=1
WHERE d1.user_id = users.user_id
      AND users.dept IN (3,4)
	AND users.user_id = d2.user_id) dt
GROUP BY dept;
RESET client_min_messages;

RESET search_path;
RESET citus.next_shard_id;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
DROP SCHEMA pg17_corr_subq_folding CASCADE;

\if :server_version_ge_17
\else
\q
\endif

-- PG17-specific tests go here.
--
