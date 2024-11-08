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

-- Partitions inherit identity column

RESET citus.log_remote_commands;

-- PG17 added support for identity columns in partioned tables:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=699586315
-- In particular, partitions with their own identity columns are not allowed.
-- Citus does not need to propagate identity columns in partitions; the identity
-- is inherited by PG17 behavior, as shown in this test.

CREATE TABLE partitioned_table (
    a bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10),
    c int
)
PARTITION BY RANGE (c);
CREATE TABLE pt_1 PARTITION OF partitioned_table FOR VALUES FROM (1) TO (50);

SELECT create_distributed_table('partitioned_table', 'a');

CREATE TABLE pt_2 PARTITION OF partitioned_table FOR VALUES FROM (50) TO (1000);

-- (1) The partitioned table has pt_1 and pt_2 as its partitions
\d+ partitioned_table;

-- (2) The partitions have the same identity column as the parent table;
-- This is PG17 behavior for support for identity in partitioned tables.
\d pt_1;
\d pt_2;

-- Attaching a partition inherits the identity column from the parent table
CREATE TABLE pt_3 (a bigint not null, c int);
ALTER TABLE partitioned_table ATTACH PARTITION pt_3 FOR VALUES FROM (1000) TO (2000);

\d+ partitioned_table;
\d pt_3;

-- Partition pt_4 has its own identity column, which is not allowed in PG17
-- and will produce an error on attempting to attach it to the partitioned table
CREATE TABLE pt_4 (a bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10), c int);
ALTER TABLE partitioned_table ATTACH PARTITION pt_4 FOR VALUES FROM (2000) TO (3000);

\c - - - :worker_1_port

SET search_path TO pg17;
-- Show that DDL for partitioned_table has correctly propagated to the worker node;
-- (1) The partitioned table has pt_1, pt_2 and pt_3 as its partitions
\d+ partitioned_table;

-- (2) The partititions have the same identity column as the parent table
\d pt_1;
\d pt_2;
\d pt_3;

\c - - - :master_port
SET search_path TO pg17;

-- Test detaching a partition with an identity column
ALTER TABLE partitioned_table DETACH PARTITION pt_3;

-- partitioned_table has pt_1, pt_2 as its partitions
-- and pt_3 does not have an identity column
\d+ partitioned_table;
\d pt_3;

-- Verify that the detach has propagated to the worker node
\c - - - :worker_1_port
SET search_path TO pg17;

\d+ partitioned_table;
\d pt_3;

\c - - - :master_port
SET search_path TO pg17;

CREATE TABLE alt_test (a int, b date, c int) PARTITION BY RANGE(c);
SELECT create_distributed_table('alt_test', 'a');

CREATE TABLE alt_test_pt_1 PARTITION OF alt_test FOR VALUES FROM (1) TO (50);
CREATE TABLE alt_test_pt_2 PARTITION OF alt_test FOR VALUES FROM (50) TO (100);

-- Citus does not support adding an identity column for a distributed table (#6738)
-- Attempting to add a column with identity produces an error
ALTER TABLE alt_test ADD COLUMN d bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10);

-- alter table set identity is currently not supported, so adding identity to
-- an existing column generates an error
ALTER TABLE alt_test ALTER COLUMN a SET GENERATED BY DEFAULT SET INCREMENT BY 2 SET START WITH 75 RESTART;

-- Verify that the identity column was not added, on coordinator and worker nodes
\d+ alt_test;
\d alt_test_pt_1;
\d alt_test_pt_2;

\c - - - :worker_1_port
SET search_path TO pg17;

\d+ alt_test;
\d alt_test_pt_1;
\d alt_test_pt_2;

\c - - - :master_port
SET search_path TO pg17;

DROP TABLE alt_test;
CREATE TABLE alt_test (a bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10),
                     b int,
                     c int)
PARTITION BY RANGE(c);

SELECT create_distributed_table('alt_test', 'b');
CREATE TABLE alt_test_pt_1 PARTITION OF alt_test FOR VALUES FROM (1) TO (50);
CREATE TABLE alt_test_pt_2 PARTITION OF alt_test FOR VALUES FROM (50) TO (100);

-- Dropping of the identity property from a column is currently not supported;
-- Attempting to drop identity produces an error
ALTER TABLE alt_test ALTER COLUMN a DROP IDENTITY;

-- Verify that alt_test still has identity on column a
\d+ alt_test;
\d alt_test_pt_1;
\d alt_test_pt_2;

\c - - - :worker_1_port
SET search_path TO pg17;

\d+ alt_test;
\d alt_test_pt_1;
\d alt_test_pt_2

\c - - - :master_port
SET search_path TO pg17;

-- Repeat testing of partitions with identity column on a citus local table

CREATE TABLE local_partitioned_table (
    a bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10),
    c int
)
PARTITION BY RANGE (c);
CREATE TABLE lpt_1 PARTITION OF local_partitioned_table FOR VALUES FROM (1) TO (50);

SELECT citus_add_local_table_to_metadata('local_partitioned_table');

-- Can create tables as partitions and attach tables as partitions to a citus local table:
CREATE TABLE lpt_2 PARTITION OF local_partitioned_table FOR VALUES FROM (50) TO (1000);

CREATE TABLE lpt_3 (a bigint not null, c int);
ALTER TABLE local_partitioned_table ATTACH PARTITION lpt_3 FOR VALUES FROM (1000) TO (2000);

-- The partitions have the same identity column as the parent table, on coordinator and worker nodes
\d+ local_partitioned_table;
\d lpt_1;
\d lpt_2;
\d lpt_3;

\c - - - :worker_1_port
SET search_path TO pg17;

\d+ local_partitioned_table;
\d lpt_1;
\d lpt_2;
\d lpt_3;

\c - - - :master_port
SET search_path TO pg17;

-- Test detaching a partition with an identity column from a citus local table
ALTER TABLE local_partitioned_table DETACH PARTITION lpt_3;

\d+ local_partitioned_table;
\d lpt_3;

\c - - - :worker_1_port
SET search_path TO pg17;

\d+ local_partitioned_table;
\d lpt_3;

\c - - - :master_port
SET search_path TO pg17;

DROP TABLE partitioned_table;
DROP TABLE local_partitioned_table;
DROP TABLE lpt_3;
DROP TABLE pt_3;
DROP TABLE pt_4;
DROP TABLE alt_test;

-- End of partition with identity columns testing

-- Test for exclusion constraints on partitioned and distributed partitioned tables in Citus environment
-- Step 1: Create a distributed partitioned table
\c - - :master_host :master_port
CREATE TABLE distributed_partitioned_table (
    id serial NOT NULL,
    partition_col int NOT NULL,
    PRIMARY KEY (id, partition_col)
) PARTITION BY RANGE (partition_col);
-- Add partitions to the distributed partitioned table
CREATE TABLE distributed_partitioned_table_p1 PARTITION OF distributed_partitioned_table
FOR VALUES FROM (1) TO (100);
CREATE TABLE distributed_partitioned_table_p2 PARTITION OF distributed_partitioned_table
FOR VALUES FROM (100) TO (200);
-- Distribute the table
SELECT create_distributed_table('distributed_partitioned_table', 'id');

-- Additional test for long names and sequential execution mode
-- Create schema if it doesn't already exist
CREATE SCHEMA IF NOT EXISTS AT_AddConstNoName;

CREATE TABLE AT_AddConstNoName.dist_partitioned_table (
    dist_col int,
    another_col int,
    partition_col timestamp
) PARTITION BY RANGE (partition_col);
CREATE TABLE AT_AddConstNoName.p1 PARTITION OF AT_AddConstNoName.dist_partitioned_table
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE AT_AddConstNoName.longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc PARTITION OF AT_AddConstNoName.dist_partitioned_table
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SELECT create_distributed_table('AT_AddConstNoName.dist_partitioned_table', 'partition_col');

-- Step 1.1: Insert and query data from long name partition
INSERT INTO AT_AddConstNoName.dist_partitioned_table (dist_col, another_col, partition_col)
VALUES (1, 10, '2020-06-01'), (2, 20, '2020-09-01');
SELECT * FROM AT_AddConstNoName.longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc;

-- Step 1.2: Verify sequential execution mode
EXPLAIN SELECT * FROM AT_AddConstNoName.dist_partitioned_table WHERE partition_col = '2020-06-01';

-- Step 1.3: Add exclusion constraints on parent table
ALTER TABLE AT_AddConstNoName.dist_partitioned_table
ADD CONSTRAINT long_name_exclude EXCLUDE USING btree (dist_col WITH =, partition_col WITH =);

-- Verify the constraint was added
SELECT conname FROM pg_constraint WHERE conrelid = 'AT_AddConstNoName.dist_partitioned_table'::regclass AND conname = 'long_name_exclude';

-- Step 2: Create a partitioned Citus local table
CREATE TABLE local_partitioned_table (
    id serial NOT NULL,
    partition_col int NOT NULL,
    PRIMARY KEY (id, partition_col)
) PARTITION BY RANGE (partition_col);
-- Add partitions to the local partitioned table
CREATE TABLE local_partitioned_table_p1 PARTITION OF local_partitioned_table
FOR VALUES FROM (1) TO (100);
CREATE TABLE local_partitioned_table_p2 PARTITION OF local_partitioned_table
FOR VALUES FROM (100) TO (200);
SELECT citus_add_local_table_to_metadata('local_partitioned_table');

-- Verify the Citus tables
SELECT table_name, citus_table_type FROM citus_tables
WHERE table_name::regclass::text like '%_partitioned_table' ORDER BY 1;

-- Step 3: Add an exclusion constraint with a name to the distributed partitioned table
ALTER TABLE distributed_partitioned_table ADD CONSTRAINT dist_exclude_named EXCLUDE USING btree (id WITH =, partition_col WITH =);

-- Step 4: Verify propagation of exclusion constraint to worker nodes
\c - - :public_worker_1_host :worker_1_port
SELECT conname FROM pg_constraint WHERE conrelid = 'distributed_partitioned_table'::regclass AND conname = 'dist_exclude_named';

-- Step 5: Add an exclusion constraint with a name to the Citus local partitioned table
\c - - :master_host :master_port
ALTER TABLE local_partitioned_table ADD CONSTRAINT local_exclude_named EXCLUDE USING btree (partition_col WITH =);

-- Step 6: Verify the exclusion constraint on the local partitioned table
SELECT conname, contype FROM pg_constraint WHERE conname = 'local_exclude_named' AND contype = 'x';

-- Step 7: Add exclusion constraints without names to both tables
ALTER TABLE distributed_partitioned_table ADD EXCLUDE USING btree (id WITH =, partition_col WITH =);
ALTER TABLE local_partitioned_table ADD EXCLUDE USING btree (partition_col WITH =);

-- Step 8: Verify the unnamed exclusion constraints were added
SELECT conname, contype FROM pg_constraint WHERE conrelid = 'local_partitioned_table'::regclass AND contype = 'x';
\c - - :public_worker_1_host :worker_1_port
SELECT conname, contype FROM pg_constraint WHERE conrelid = 'distributed_partitioned_table'::regclass AND contype = 'x';

-- Step 9: Drop the exclusion constraints from both tables
\c - - :master_host :master_port
ALTER TABLE distributed_partitioned_table DROP CONSTRAINT dist_exclude_named;
ALTER TABLE local_partitioned_table DROP CONSTRAINT local_exclude_named;

-- Step 10: Verify the constraints were dropped
SELECT * FROM pg_constraint WHERE conname = 'dist_exclude_named' AND contype = 'x';
SELECT * FROM pg_constraint WHERE conname = 'local_exclude_named' AND contype = 'x';

-- Step 11: Clean up - Drop the tables
DROP TABLE distributed_partitioned_table, local_partitioned_table;
DROP TABLE AT_AddConstNoName.p1,
          AT_AddConstNoName.longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc,
          AT_AddConstNoName.dist_partitioned_table;
DROP SCHEMA AT_AddConstNoName CASCADE;
-- End of Test for exclusion constraints on partitioned and distributed partitioned tables in Citus environment

-- Correlated sublinks are now supported as of PostgreSQL 17, resolving issue #4470.
-- Enable DEBUG-level logging to capture detailed execution plans

-- Create the tables
CREATE TABLE postgres_table (key int, value text, value_2 jsonb);
CREATE TABLE reference_table (key int, value text, value_2 jsonb);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table', 'key');

-- Insert test data
INSERT INTO postgres_table SELECT i, i::varchar(256), '{}'::jsonb FROM generate_series(1, 10) i;
INSERT INTO reference_table SELECT i, i::varchar(256), '{}'::jsonb FROM generate_series(1, 10) i;
INSERT INTO distributed_table SELECT i, i::varchar(256), '{}'::jsonb FROM generate_series(1, 10) i;

-- Set local table join policy to auto before running the tests
SET citus.local_table_join_policy TO 'auto';
SET client_min_messages TO DEBUG1;

-- Correlated sublinks are supported in PostgreSQL 17
SELECT COUNT(*) FROM distributed_table d1 JOIN postgres_table USING (key)
WHERE d1.key IN (SELECT key FROM distributed_table WHERE d1.key = key AND key = 5);

SELECT COUNT(*) FROM distributed_table d1 JOIN postgres_table USING (key)
WHERE d1.key IN (SELECT key FROM distributed_table WHERE d1.key = key AND key = 5);

SET citus.local_table_join_policy TO 'prefer-distributed';
SELECT COUNT(*) FROM distributed_table d1 JOIN postgres_table USING (key)
WHERE d1.key IN (SELECT key FROM distributed_table WHERE d1.key = key AND key = 5);

RESET citus.local_table_join_policy;
RESET client_min_messages;
DROP TABLE reference_table;
-- End for Correlated sublinks are now supported as of PostgreSQL 17, resolving issue #4470.

DROP SCHEMA pg17 CASCADE;
DROP ROLE regress_maintain;
DROP ROLE regress_no_maintain;
