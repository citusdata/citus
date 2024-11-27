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
DROP SCHEMA pg17_corr_subq_folding CASCADE;

\if :server_version_ge_17
\else
\q
\endif

-- PG17-specific tests go here.
--
CREATE SCHEMA pg17;
SET search_path to pg17;

-- Test specifying access method on partitioned tables. PG17 feature, added by:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=374c7a229
-- The following tests were failing tests in tableam but will pass on PG >= 17.
-- There is some set-up duplication of tableam, and this test can be returned
-- to tableam when 17 is the minimum supported PG version.

SELECT public.run_command_on_coordinator_and_workers($Q$
        SET citus.enable_ddl_propagation TO off;
        CREATE FUNCTION fake_am_handler(internal)
        RETURNS table_am_handler
        AS 'citus'
        LANGUAGE C;
        CREATE ACCESS METHOD fake_am TYPE TABLE HANDLER fake_am_handler;
$Q$);

-- Since Citus assumes access methods are part of the extension, make fake_am
-- owned manually to be able to pass checks on Citus while distributing tables.
ALTER EXTENSION citus ADD ACCESS METHOD fake_am;

CREATE TABLE test_partitioned(id int, p int, val int)
PARTITION BY RANGE (p) USING fake_am;

-- Test that children inherit access method from parent
CREATE TABLE test_partitioned_p1 PARTITION OF test_partitioned
        FOR VALUES FROM (1) TO (10);
CREATE TABLE test_partitioned_p2 PARTITION OF test_partitioned
        FOR VALUES FROM (11) TO (20);

INSERT INTO test_partitioned VALUES (1, 5, -1), (2, 15, -2);
INSERT INTO test_partitioned VALUES (3, 6, -6), (4, 16, -4);

SELECT count(1) FROM test_partitioned_p1;
SELECT count(1) FROM test_partitioned_p2;

-- Both child table partitions inherit fake_am
SELECT c.relname, am.amname FROM pg_class c, pg_am am
WHERE c.relam = am.oid AND c.oid IN ('test_partitioned_p1'::regclass, 'test_partitioned_p2'::regclass)
ORDER BY c.relname;

-- Clean up
DROP TABLE test_partitioned;
ALTER EXTENSION citus DROP ACCESS METHOD fake_am;
SELECT public.run_command_on_coordinator_and_workers($Q$
        RESET citus.enable_ddl_propagation;
$Q$);

-- End of testing specifying access method on partitioned tables.

-- MAINTAIN privilege tests

CREATE ROLE regress_maintain;
CREATE ROLE regress_no_maintain;

ALTER ROLE regress_maintain WITH login;
GRANT USAGE ON SCHEMA pg17 TO regress_maintain;
ALTER ROLE regress_no_maintain WITH login;
GRANT USAGE ON SCHEMA pg17 TO regress_no_maintain;

SET citus.shard_count TO 1; -- For consistent remote command logging
CREATE TABLE dist_test(a int, b int);
SELECT create_distributed_table('dist_test', 'a');
INSERT INTO dist_test SELECT i % 10, i FROM generate_series(1, 100) t(i);

SET citus.log_remote_commands TO on;

SET citus.grep_remote_commands = '%maintain%';
GRANT MAINTAIN ON dist_test TO regress_maintain;
RESET citus.grep_remote_commands;

SET ROLE regress_no_maintain;
-- Current role does not have MAINTAIN privileges on dist_test
ANALYZE dist_test;
VACUUM dist_test;

SET ROLE regress_maintain;
-- Current role has MAINTAIN privileges on dist_test
ANALYZE dist_test;
VACUUM dist_test;

-- Take away regress_maintain's MAINTAIN privileges on dist_test
RESET ROLE;
SET citus.grep_remote_commands = '%maintain%';
REVOKE MAINTAIN ON dist_test FROM regress_maintain;
RESET citus.grep_remote_commands;

SET ROLE regress_maintain;
-- Current role does not have MAINTAIN privileges on dist_test
ANALYZE dist_test;
VACUUM dist_test;

RESET ROLE;

-- End of MAINTAIN privilege tests

RESET citus.log_remote_commands;
RESET citus.next_shard_id;
RESET citus.shard_count;
RESET citus.shard_replication_factor;

DROP SCHEMA pg17 CASCADE;
DROP ROLE regress_maintain;
DROP ROLE regress_no_maintain;
