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

-- this should work now as the citus local table is dropped
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

CREATE TABLE distributed_table (a int);
-- cannot create citus local table from an existing citus table
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

------------------------------------------------------------------
----- tests for object names that should be escaped properly -----
------------------------------------------------------------------

CREATE SCHEMA "CiTUS!LocalTables";

-- create table with weird names
CREATE TABLE "CiTUS!LocalTables"."LocalTabLE.1!?!"(id int, "TeNANt_Id" int);

-- should work
SELECT create_citus_local_table('"CiTUS!LocalTables"."LocalTabLE.1!?!"');

-- drop the table before creating it when the search path is set
SET search_path to "CiTUS!LocalTables" ;
DROP TABLE "LocalTabLE.1!?!";

-- have a custom type in the local table
CREATE TYPE local_type AS (key int, value jsonb);

-- create btree_gist for GiST index
CREATE EXTENSION btree_gist;

CREATE TABLE "LocalTabLE.1!?!"(
  id int PRIMARY KEY,
  "TeNANt_Id" int,
  "local_Type" local_type,
  "jsondata" jsonb NOT NULL,
  name text,
  price numeric CHECK (price > 0),
  serial_data bigserial, UNIQUE (id, price),
  EXCLUDE USING GIST (name WITH =));

-- create some objects before create_citus_local_table
CREATE INDEX "my!Index1" ON "LocalTabLE.1!?!"(id) WITH ( fillfactor = 80 ) WHERE  id > 10;
CREATE UNIQUE INDEX uniqueIndex ON "LocalTabLE.1!?!" (id);

-- ingest some data before create_citus_local_table
INSERT INTO "LocalTabLE.1!?!" VALUES (1, 1, (1, row_to_json(row(1,1)))::local_type, row_to_json(row(1,1), true)),
                                     (2, 1, (2, row_to_json(row(2,2)))::local_type, row_to_json(row(2,2), 'false'));

-- create a replica identity before create_citus_local_table
ALTER TABLE "LocalTabLE.1!?!" REPLICA IDENTITY USING INDEX uniqueIndex;

-- this shouldn't give any syntax errors
SELECT create_citus_local_table('"LocalTabLE.1!?!"');

-- create some objects after create_citus_local_table
CREATE INDEX "my!Index2" ON "LocalTabLE.1!?!"(id) WITH ( fillfactor = 90 ) WHERE id < 20;
CREATE UNIQUE INDEX uniqueIndex2 ON "LocalTabLE.1!?!"(id);

-----------------------------------
---- utility command execution ----
-----------------------------------

SET search_path TO citus_local_tables_test_schema;

-- any foreign key between citus local tables and other tables cannot be set for now
-- most should error out (for now with meaningless error messages)

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

-- cleanup at exit
DROP SCHEMA citus_local_tables_test_schema, "CiTUS!LocalTables" CASCADE;
