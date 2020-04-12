-- This tests file includes tests for citus local tables.

\set VERBOSITY terse

SET citus.next_shard_id TO 1504000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_tables_test_schema;
SET search_path TO citus_local_tables_test_schema;

------------------------------------------
------- citus local table creation -------
------------------------------------------

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE citus_local_table_1 (a int);

-- this should work as coordinator is added to pg_dist_node
SELECT create_citus_local_table('citus_local_table_1');

-- try to remove coordinator and observe failure as there exist a citus local table
SELECT 1 FROM master_remove_node('localhost', :master_port);

DROP TABLE citus_local_table_1;

-- this should work
SELECT 1 FROM master_remove_node('localhost', :master_port);

CREATE TABLE citus_local_table_1 (a int primary key);

-- this should fail as coordinator is removed from pg_dist_node
SELECT create_citus_local_table('citus_local_table_1');

-- let coordinator have citus local tables again for next tests
set client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- creating citus local table having no data initially would work
SELECT create_citus_local_table('citus_local_table_1');

-- creating citus local table having data in it would also work
CREATE TABLE citus_local_table_2(a int primary key);
INSERT INTO citus_local_table_2 VALUES(1);

SELECT create_citus_local_table('citus_local_table_2');

-- also create indexes on them
CREATE INDEX citus_local_table_1_idx ON citus_local_table_1(a);
CREATE INDEX citus_local_table_2_idx ON citus_local_table_2(a);

-- drop them for next tests
DROP TABLE citus_local_table_1, citus_local_table_2;

-- create indexes before creating the citus local tables

-- .. for an initially empty table
CREATE TABLE citus_local_table_1(a int);
CREATE INDEX citus_local_table_1_idx ON citus_local_table_1(a);
SELECT create_citus_local_table('citus_local_table_1');

-- .. and for another table having data in it before creating citus local table
CREATE TABLE citus_local_table_2(a int);
INSERT INTO citus_local_table_2 VALUES(1);
CREATE INDEX citus_local_table_2_idx ON citus_local_table_2(a);
SELECT create_citus_local_table('citus_local_table_2');

-- TODO: also show that we do not allow creating citus local tables from mx nodes

-- cannot create citus local table from an existing citus table
CREATE TABLE distributed_table (a int);
SELECT create_distributed_table('distributed_table', 'a');

-- this will error out
SELECT create_citus_local_table('distributed_table');

-- partitiond table tests --

CREATE TABLE partitioned_table(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE partitioned_table_1 PARTITION OF partitioned_table FOR VALUES FROM (0) TO (10);
CREATE TABLE partitioned_table_2 PARTITION OF partitioned_table FOR VALUES FROM (10) TO (20);

-- cannot create partitioned citus local tables
SELECT create_citus_local_table('partitioned_table');

-- cannot create citus local table as a partition of a local table
BEGIN;
  CREATE TABLE citus_local_table PARTITION OF partitioned_table FOR VALUES FROM (20) TO (30);

  -- this should fail
  SELECT create_citus_local_table('citus_local_table');
ROLLBACK;

-- cannot create citus local table as a partition of a local table
-- via ALTER TABLE commands as well
BEGIN;
  CREATE TABLE citus_local_table (a int, b int);

  SELECT create_citus_local_table('citus_local_table');

  -- this should fail
  ALTER TABLE partitioned_table ATTACH PARTITION citus_local_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

-- cannot attach citus local table to a partitioned distributed table
BEGIN;
  SELECT create_distributed_table('partitioned_table', 'a');

  CREATE TABLE citus_local_table (a int, b int);
  SELECT create_citus_local_table('citus_local_table');

  -- this should fail
  ALTER TABLE partitioned_table ATTACH PARTITION citus_local_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

-- drop them for next tests
DROP TABLE citus_local_table_1, citus_local_table_2, distributed_table;

-- create test tables
CREATE TABLE citus_local_table_1 (a int primary key);
SELECT create_citus_local_table('citus_local_table_1');

CREATE TABLE citus_local_table_2 (a int primary key);
SELECT create_citus_local_table('citus_local_table_2');

CREATE TABLE local_table (a int primary key);

CREATE TABLE distributed_table (a int primary key);
SELECT create_distributed_table('distributed_table', 'a');

CREATE TABLE reference_table (a int primary key);
SELECT create_reference_table('reference_table');

-- show that colociation of citus local tables are not supported for now

-- between citus local tables
SELECT mark_tables_colocated('citus_local_table_1', ARRAY['citus_local_table_2']);

-- between citus local tables and reference tables
SELECT mark_tables_colocated('citus_local_table_1', ARRAY['reference_table']);
SELECT mark_tables_colocated('reference_table', ARRAY['citus_local_table_1']);

-- between citus local tables and distributed tables
SELECT mark_tables_colocated('citus_local_table_1', ARRAY['distributed_table']);
SELECT mark_tables_colocated('distributed_table', ARRAY['citus_local_table_1']);

-----------------------------------
---- utility command execution ----
-----------------------------------

-- at this point, we want to see which planner is preferred by citus
SET client_min_messages TO DEBUG2;

-- any foreign key between citus local tables and other tables cannot be set for now
-- each should error out (for now meaningless error messages)

-- between citus local tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_c FOREIGN KEY(a) references citus_local_table_2(a);

-- between citus local tables and reference tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_ref FOREIGN KEY(a) references reference_table(a);
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_c FOREIGN KEY(a) references citus_local_table_1(a);

-- between citus local tables and distributed tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_dist FOREIGN KEY(a) references distributed_table(a);
ALTER TABLE distributed_table ADD CONSTRAINT fkey_dist_to_c FOREIGN KEY(a) references citus_local_table_1(a);

-- between citus local tables and local tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_local FOREIGN KEY(a) references local_table(a);
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_c FOREIGN KEY(a) references citus_local_table_1(a);

-- prevent verbose logs from DROP command
RESET client_min_messages;

-- drop them for next tests
DROP TABLE citus_local_table_1, citus_local_table_2, distributed_table, local_table, reference_table;

SET client_min_messages TO DEBUG2;

-------------------------------------------------
------- SELECT / INSERT / UPDATE / DELETE -------
-------------------------------------------------

CREATE TABLE citus_local_table(a int, b int);
SELECT create_citus_local_table('citus_local_table');

CREATE TABLE citus_local_table_2(a int, b int);
SELECT create_citus_local_table('citus_local_table_2');

CREATE TABLE reference_table(a int, b int);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table(a int, b int);
SELECT create_distributed_table('distributed_table', 'a');

CREATE TABLE local_table(a int, b int);

----------------
---- SELECT ----
----------------

-- join between citus local tables and reference tables would success
SELECT count(*) FROM citus_local_table, reference_table WHERE citus_local_table.a = reference_table.a;

-- join between citus local tables and distributed tables would fail
SELECT citus_local_table.a FROM citus_local_table, distributed_table;

-- join between citus local tables and local tables are okey
SELECT citus_local_table.b, local_table.a
FROM citus_local_table, local_table
WHERE citus_local_table.a = local_table.b;

---------------------------
----- INSERT / SELECT -----
---------------------------

-- those would fail as they are inserting into citus local table
-- from distributed table or reference table

INSERT INTO citus_local_table
SELECT * FROM reference_table;

INSERT INTO citus_local_table
SELECT * from distributed_table;

-- below are ok (simple cases)

INSERT INTO citus_local_table VALUES (1, 2);

INSERT INTO citus_local_table
SELECT * from local_table;

INSERT INTO local_table
SELECT * from citus_local_table;

INSERT INTO citus_local_table
SELECT * FROM citus_local_table_2;

-- INSERT SELECT into distributed or reference table from
-- citus local table is okay, see below two

INSERT INTO distributed_table
SELECT * from citus_local_table;

INSERT INTO reference_table
SELECT * from citus_local_table;

INSERT INTO reference_table
SELECT citus_local_table.a, citus_local_table.b
FROM citus_local_table, local_table
WHERE citus_local_table.a > local_table.b;

---------------------------
----- DELETE / UPDATE -----
---------------------------

-- DELETE/UPDATE commands on local tables and citus local tables are all ok

DELETE FROM citus_local_table
USING local_table
WHERE citus_local_table.b = local_table.b;

UPDATE citus_local_table
SET b = 5
FROM local_table
WHERE citus_local_table.a = 3 AND citus_local_table.b = local_table.b;

-- all should fail (either updating citus local table or reference table or distributed table)
-- as they include citus local table along with other citus tables

UPDATE distributed_table
SET b = 6
FROM citus_local_table
WHERE citus_local_table.a = distributed_table.a;

UPDATE reference_table
SET b = 6
FROM citus_local_table
WHERE citus_local_table.a = reference_table.a;

UPDATE citus_local_table
SET b = 6
FROM distributed_table
WHERE citus_local_table.a = distributed_table.a;

UPDATE citus_local_table
SET b = 6
FROM reference_table
WHERE citus_local_table.a = reference_table.a;

DELETE FROM distributed_table
USING citus_local_table
WHERE citus_local_table.a = distributed_table.a;

DELETE FROM citus_local_table
USING distributed_table
WHERE citus_local_table.a = distributed_table.a;

DELETE FROM reference_table
USING citus_local_table
WHERE citus_local_table.a = reference_table.a;

DELETE FROM citus_local_table
USING reference_table
WHERE citus_local_table.a = reference_table.a;

-- even if we use subquery or cte, they would still fail. see below
DELETE FROM citus_local_table
WHERE citus_local_table.a IN (SELECT a FROM distributed_table);

DELETE FROM citus_local_table
WHERE citus_local_table.a IN (SELECT a FROM reference_table);

WITH distributed_table_cte AS (SELECT * FROM distributed_table)
UPDATE citus_local_table
SET b = 6
FROM distributed_table_cte
WHERE citus_local_table.a = distributed_table_cte.a;

WITH reference_table_cte AS (SELECT * FROM reference_table)
UPDATE citus_local_table
SET b = 6
FROM reference_table_cte
WHERE citus_local_table.a = reference_table_cte.a;

-- prevent verbose logs from DROP command
RESET client_min_messages;

-- cleanup at exit
DROP SCHEMA citus_local_tables_test_schema CASCADE;
