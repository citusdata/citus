-- This tests file includes tests for local execution of utility commands.
-- For now, this file includes tests only for local execution of
-- `TRUNCATE/DROP/DDL` commands for all kinds of distributed tables from
-- the coordinator node having regular distributed tables' shards
-- (shouldHaveShards = on) and having reference table placements in it.

\set VERBOSITY terse

SET citus.next_shard_id TO 1500000;
SET citus.next_placement_id TO 8300000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.shard_COUNT TO 32;
SET citus.log_local_commands TO ON;

CREATE SCHEMA local_commands_test_schema;
SET search_path TO local_commands_test_schema;

-- let coordinator have distributed table shards/placements
set client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

SELECT master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

-----------------------------------------
------ local execution of TRUNCATE ------
-----------------------------------------

CREATE TABLE ref_table (a int primary key);
SELECT create_reference_table('ref_table');

CREATE TABLE dist_table(a int);
SELECT create_distributed_table('dist_table', 'a', colocate_with:='none');

ALTER TABLE dist_table ADD CONSTRAINT fkey FOREIGN KEY(a) REFERENCES ref_table(a);

-- insert some data
INSERT INTO ref_table VALUES(1);
INSERT INTO dist_table VALUES(1);

-- Currently, we support local execution of TRUNCATE commands for all kinds
-- Hence, cascading to distributed tables wouldn't be a problem even in the
-- case that coordinator have some local distributed table shards.
TRUNCATE ref_table CASCADE;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM ref_table, dist_table;

-- insert some data
INSERT INTO ref_table VALUES(1);
INSERT INTO dist_table VALUES(1);

-- As SELECT accesses local placements of reference table, TRUNCATE would also
-- be forced to local execution even if they operate on different tables.
BEGIN;
  SELECT COUNT(*) FROM ref_table;
  TRUNCATE dist_table;
COMMIT;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM dist_table;

-- insert some data
INSERT INTO ref_table VALUES(2);
INSERT INTO dist_table VALUES(2);

-- SELECT would access local placements via local execution as that is
-- in a transaction block even though it contains multi local shards.
BEGIN;
  SELECT COUNT(*) FROM dist_table;
  TRUNCATE dist_table;
COMMIT;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM dist_table;

-- insert some data
INSERT INTO ref_table VALUES(3);
INSERT INTO dist_table VALUES(3);

-- TRUNCATE on dist_table (note that: again no cascade here) would
-- just be handled via remote executions even on its local shards
TRUNCATE dist_table;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM dist_table;

-- insert some data
INSERT INTO ref_table VALUES(4);

-- Creating a dist. table is handled by local execution inside a transaction block.
-- Hence, the commands following it (INSERT & TRUNCATE) would also be
-- handled via local execution.
BEGIN;
  CREATE TABLE ref_table_1(a int);
  SELECT create_reference_table('ref_table_1');

  -- insert some data
  INSERT INTO ref_table_1 VALUES(5);

  TRUNCATE ref_table_1;
COMMIT;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM ref_table_1;

-- However, as SELECT would access local placements via local execution
-- for regular distributed tables, below TRUNCATE would error
-- out
BEGIN;
  SELECT COUNT(*) FROM dist_table;
  TRUNCATE ref_table CASCADE;
COMMIT;

-- as we do not support local ANALYZE execution yet, below block would error out
BEGIN;
  TRUNCATE ref_table CASCADE;
  ANALYZE ref_table;
COMMIT;

-- insert some data
INSERT INTO ref_table VALUES(7);
INSERT INTO dist_table VALUES(7);

-- we can TRUNCATE those two tables within the same command
TRUNCATE ref_table, dist_table;

-- show that TRUNCATE is successfull
SELECT COUNT(*) FROM ref_table, dist_table;

-------------------------------------
------ local execution of DROP ------
-------------------------------------

-- droping just the referenced table would error out as dist_table references it
DROP TABLE ref_table;

-- drop those two tables via remote execution
DROP TABLE ref_table, dist_table;

-- drop the other standalone table locally
DROP TABLE ref_table_1;

-- show that DROP commands are successfull
SELECT tablename FROM pg_tables where schemaname='local_commands_test_schema' ORDER BY tablename;

CREATE TABLE ref_table (a int primary key);
SELECT create_reference_table('ref_table');

-- We execute SELECT command within the below block locally.
-- Hence we should execute the DROP command locally as well.
BEGIN;
  SELECT COUNT(*) FROM ref_table;
  DROP TABLE ref_table;
COMMIT;

CREATE TABLE ref_table (a int primary key);
SELECT create_reference_table('ref_table');

CREATE TABLE dist_table(a int);
SELECT create_distributed_table('dist_table', 'a', colocate_with:='none');

ALTER TABLE dist_table ADD CONSTRAINT fkey FOREIGN KEY(a) REFERENCES ref_table(a);

-- show that DROP command is rollback'd successfully (should print 1)
SELECT 1 FROM pg_tables where tablename='dist_table';

-- As SELECT will be executed remotely, the DROP command should also be executed
-- remotely to prevent possible self-deadlocks & inconsistencies.
-- FIXME: we have a known bug for below case described in
-- https://github.com/citusdata/citus/issues/3526. Hence, commented out as it could
-- randomly fall into distributed deadlocks
--BEGIN;
--  SELECT COUNT(*) FROM dist_table;
--  DROP TABLE dist_table;
--END;

-- As SELECT will be executed remotely, the DROP command below should also be
-- executed remotely.
CREATE TABLE another_dist_table(a int);
SELECT create_distributed_table('another_dist_table', 'a', colocate_with:='dist_table');

BEGIN;
  SELECT COUNT(*) FROM another_dist_table;
  DROP TABLE another_dist_table;
COMMIT;

-- show that DROP command is committed successfully
SELECT 1 FROM pg_tables where tablename='another_dist_table';

-- below DROP will be executed remotely.
DROP TABLE dist_table;

-- show that DROP command is successfull
SELECT 1 FROM pg_tables where tablename='dist_table';

CREATE TABLE dist_table(a int);
SELECT create_distributed_table('dist_table', 'a', colocate_with:='none');

ALTER TABLE dist_table ADD CONSTRAINT fkey FOREIGN KEY(a) REFERENCES ref_table(a);

-- as SELECT on ref_table will be executed locally, the SELECT and DROP following
-- it would also be executed locally
BEGIN;
  SELECT COUNT(*) FROM ref_table;
  DROP TABLE dist_table CASCADE;
ROLLBACK;

-- show that DROP command is rollback'd successfully (should print 1)
SELECT 1 FROM pg_tables where tablename='dist_table';

---------------------------------------------
------ local execution of DDL commands ------
---------------------------------------------

-- try some complicated CASCADE cases along with DDL commands

CREATE TABLE ref_table_1(a int primary key);
SELECT create_reference_table('ref_table_1');

-- below block should execute successfully
BEGIN;
  SELECT COUNT(*) FROM ref_table;

  -- as SELECT above runs locally and as now we support local execution of DDL commands,
  -- below DDL should be able to define foreign key constraint successfully
  ALTER TABLE ref_table ADD CONSTRAINT fkey FOREIGN KEY(a) REFERENCES ref_table_1(a);

  -- insert some data
  INSERT INTO ref_table_1 VALUES (1);
  INSERT INTO ref_table_1 VALUES (2);
  INSERT INTO ref_table VALUES (1);

  -- chain foreign key constraints
  -- local execution should be observed here as well
  ALTER TABLE dist_table ADD CONSTRAINT fkey1 FOREIGN KEY(a) REFERENCES ref_table(a);

  INSERT INTO dist_table VALUES (1);

  DELETE FROM ref_table_1 WHERE a=2;

  -- add another column to dist_table
  -- note that we execute below DDL locally as well
  ALTER TABLE ref_table ADD b int;

  -- define self reference
  ALTER TABLE ref_table ADD CONSTRAINT fkey2 FOREIGN KEY(b) REFERENCES ref_table(a);

  SELECT COUNT(*) FROM ref_table_1, ref_table, dist_table;

  -- observe DROP on a self-referencing table also works
  DROP TABLE ref_table_1, ref_table, dist_table;

  -- show that DROP command is successfull
  SELECT tablename FROM pg_tables where schemaname='local_commands_test_schema' ORDER BY tablename;
ROLLBACK;

-- add another column to dist_table (should be executed remotely)
ALTER TABLE dist_table ADD b int;

CREATE SCHEMA foo_schema;

-- As SELECT will be executed remotely, ALTER TABLE SET SCHEMA command should alse be executed remotely
BEGIN;
  SELECT COUNT(*) FROM dist_table;

  ALTER TABLE dist_table SET SCHEMA foo_schema;

  -- show that ALTER TABLE SET SCHEMA is successfull
  SELECT tablename FROM pg_tables where schemaname='foo_schema' ORDER BY tablename;
ROLLBACK;

-- However, below ALTER TABLE SET SCHEMA command will be executed locally
BEGIN;
  ALTER TABLE ref_table SET SCHEMA foo_schema;

  -- show that ALTER TABLE SET SCHEMA is successfull
  SELECT tablename FROM pg_tables where schemaname='foo_schema' ORDER BY tablename;
ROLLBACK;

BEGIN;
  -- here this SELECT will enforce the whole block for local execution
  SELECT COUNT(*) FROM ref_table;

  -- execute bunch of DDL & DROP commands succesfully
  ALTER TABLE dist_table ADD column c int;
  ALTER TABLE dist_table ALTER COLUMN c SET NOT NULL;

  CREATE TABLE another_dist_table(a int);
  SELECT create_distributed_table('another_dist_table', 'a', colocate_with:='dist_table');
COMMIT;

-- add a foreign key for next test
ALTER TABLE dist_table ADD CONSTRAINT fkey_dist_to_ref FOREIGN KEY (b) REFERENCES ref_table(a);

BEGIN;
  SELECT count(*) FROM ref_table;

  -- should show parallel
  SHOW citus.multi_shard_modify_mode ;

  -- wants to do parallel execution but will switch to sequential mode
  ALTER TABLE dist_table DROP COLUMN c;

  -- should show sequential
  SHOW citus.multi_shard_modify_mode;
ROLLBACK;

---------------------------------------------
------------ partitioned tables -------------
---------------------------------------------

-- test combination of TRUNCATE & DROP & DDL commands with partitioned tables as well
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);

CREATE TABLE partitioning_test_2012 PARTITION OF partitioning_test FOR VALUES FROM ('2012-06-06') TO ('2012-08-08');
CREATE TABLE partitioning_test_2013 PARTITION OF partitioning_test FOR VALUES FROM ('2013-06-06') TO ('2013-07-07');

-- load some data
INSERT INTO partitioning_test VALUES (5, '2012-06-06');
INSERT INTO partitioning_test VALUES (6, '2012-07-07');
INSERT INTO partitioning_test VALUES (5, '2013-06-06');

SELECT create_distributed_table('partitioning_test', 'id', colocate_with:='dist_table');

-- all commands below should be executed via local execution due to SELECT on ref_table
BEGIN;
  SELECT * from ref_table;
  INSERT INTO partitioning_test VALUES (7, '2012-07-07');
  SELECT COUNT(*) FROM partitioning_test;

  -- execute bunch of DDL & DROP commands succesfully
  ALTER TABLE partitioning_test ADD column c int;
  TRUNCATE partitioning_test;
  DROP TABLE partitioning_test;
ROLLBACK;

-- below should be executed via remote connections
TRUNCATE partitioning_test;
DROP TABLE partitioning_test;

-- cleanup at exit
DROP SCHEMA local_commands_test_schema CASCADE;
DROP SCHEMA foo_schema;
SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', false);
