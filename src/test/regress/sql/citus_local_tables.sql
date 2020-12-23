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
SELECT create_distributed_table('distributed_table', 'a');

-- cannot create citus local table from an existing citus table
SELECT create_citus_local_table('distributed_table');

-- partitioned table tests --

CREATE TABLE partitioned_table(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE partitioned_table_1 PARTITION OF partitioned_table FOR VALUES FROM (0) TO (10);
CREATE TABLE partitioned_table_2 PARTITION OF partitioned_table FOR VALUES FROM (10) TO (20);

-- cannot create partitioned citus local tables
SELECT create_citus_local_table('partitioned_table');

BEGIN;
  CREATE TABLE citus_local_table PARTITION OF partitioned_table FOR VALUES FROM (20) TO (30);

  -- cannot create citus local table as a partition of a local table
  SELECT create_citus_local_table('citus_local_table');
ROLLBACK;

BEGIN;
  CREATE TABLE citus_local_table (a int, b int);

  SELECT create_citus_local_table('citus_local_table');

  -- cannot create citus local table as a partition of a local table
  -- via ALTER TABLE commands as well
  ALTER TABLE partitioned_table ATTACH PARTITION citus_local_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

BEGIN;
  SELECT create_distributed_table('partitioned_table', 'a');

  CREATE TABLE citus_local_table (a int, b int);
  SELECT create_citus_local_table('citus_local_table');

  -- cannot attach citus local table to a partitioned distributed table
  ALTER TABLE partitioned_table ATTACH PARTITION citus_local_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

-- show that we do not support inheritance relationships --

CREATE TABLE parent_table (a int, b text);
CREATE TABLE child_table () INHERITS (parent_table);

-- both of below should error out
SELECT create_citus_local_table('parent_table');
SELECT create_citus_local_table('child_table');

-- show that we support UNLOGGED tables --

CREATE UNLOGGED TABLE unlogged_table (a int primary key);
SELECT create_citus_local_table('unlogged_table');

-- show that we allow triggers --

BEGIN;
  CREATE TABLE citus_local_table_3 (value int);

  -- create a simple function to be invoked by trigger
  CREATE FUNCTION update_value() RETURNS trigger AS $update_value$
  BEGIN
      UPDATE citus_local_table_3 SET value=value+1;
      RETURN NEW;
  END;
  $update_value$ LANGUAGE plpgsql;

  CREATE TRIGGER insert_trigger
  AFTER INSERT ON citus_local_table_3
  FOR EACH STATEMENT EXECUTE FUNCTION update_value();

  SELECT create_citus_local_table('citus_local_table_3');

  INSERT INTO citus_local_table_3 VALUES (1);

  -- show that trigger is executed only once, we should see "2" (not "3")
  SELECT * FROM citus_local_table_3;
ROLLBACK;

-- show that we do not support policies in citus community --

BEGIN;
  CREATE TABLE citus_local_table_3 (table_user text);

  ALTER TABLE citus_local_table_3 ENABLE ROW LEVEL SECURITY;

  CREATE ROLE table_users;
  CREATE POLICY table_policy ON citus_local_table_3 TO table_users
      USING (table_user = current_user);

  -- this should error out
  SELECT create_citus_local_table('citus_local_table_3');
ROLLBACK;

-- show that we properly handle sequences on citus local tables --

BEGIN;
  CREATE SEQUENCE col3_seq;
  CREATE TABLE citus_local_table_3 (col1 serial, col2 int, col3 int DEFAULT nextval('col3_seq'));

  SELECT create_citus_local_table('citus_local_table_3');

  -- print column default expressions
  -- we should only see shell relation below
  SELECT table_name, column_name, column_default
  FROM information_schema.COLUMNS
  WHERE table_name like 'citus_local_table_3%' and column_default != '' ORDER BY 1,2;

  -- print sequence ownerships
  -- show that the only internal sequence is on col1 and it is owned by shell relation
  SELECT s.relname as sequence_name, t.relname, a.attname
  FROM pg_class s
    JOIN pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass
    JOIN pg_class t on t.oid=d.refobjid
    JOIN pg_attribute a on a.attrelid=t.oid and a.attnum=d.refobjsubid
  WHERE s.relkind='S' and s.relname like 'citus_local_table_3%' ORDER BY 1,2;
ROLLBACK;

-- test foreign tables using fake FDW --

CREATE FOREIGN TABLE foreign_table (
  id bigint not null,
  full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true');

-- observe that we do not create fdw server for shell table, both shard relation
-- & shell relation points to the same same server object
SELECT create_citus_local_table('foreign_table');

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

-- master_create_empty_shard is not supported
SELECT master_create_empty_shard('citus_local_table_1');
-- get_shard_id_for_distribution_column is supported
SELECT get_shard_id_for_distribution_column('citus_local_table_1', 'not_checking_this_arg_for_non_dist_tables');
SELECT get_shard_id_for_distribution_column('citus_local_table_1');
-- master_copy_shard_placement is not supported
SELECT master_copy_shard_placement(shardid, 'localhost', :master_port, 'localhost', :worker_1_port, true)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table_1'::regclass) as shardid;
-- undistribute_table is supported
BEGIN;
  SELECT undistribute_table('citus_local_table_1');
ROLLBACK;

-- tests with citus local tables initially having foreign key relationships

CREATE TABLE local_table_1 (a int primary key);
CREATE TABLE local_table_2 (a int primary key references local_table_1(a));
CREATE TABLE local_table_3 (a int primary key, b int references local_table_3(a));

-- below two should fail as we do not allow foreign keys between
-- postgres local tables and citus local tables
SELECT create_citus_local_table('local_table_1');
SELECT create_citus_local_table('local_table_2');

-- below should work as we allow initial self references in citus local tables
SELECT create_citus_local_table('local_table_3');

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

-- any foreign key between citus local tables and other tables except reference tables cannot be set
-- more tests at ref_citus_local_fkeys.sql

-- between citus local tables and distributed tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_dist FOREIGN KEY(a) references distributed_table(a);
ALTER TABLE distributed_table ADD CONSTRAINT fkey_dist_to_c FOREIGN KEY(a) references citus_local_table_1(a);

-- between citus local tables and local tables
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_local FOREIGN KEY(a) references local_table(a);
ALTER TABLE local_table
  ADD CONSTRAINT fkey_local_to_c FOREIGN KEY(a) references citus_local_table_1(a),
  ADD CONSTRAINT fkey_self FOREIGN KEY(a) references local_table(a);
ALTER TABLE local_table
  ADD COLUMN b int references citus_local_table_1(a),
  ADD COLUMN c int references local_table(a);
CREATE TABLE local_table_4 (
  a int unique references citus_local_table_1(a),
  b int references local_table_4(a));

ALTER TABLE citus_local_table_1 ADD COLUMN b int NOT NULL;
-- show that we added column with NOT NULL
SELECT table_name, column_name, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name LIKE 'citus_local_table_1%' AND column_name = 'b'
ORDER BY 1;

ALTER TABLE citus_local_table_1 ADD CONSTRAINT unique_a_b UNIQUE (a, b);
-- show that we defined unique constraints
SELECT conrelid::regclass, conname, conkey
FROM pg_constraint
WHERE conrelid::regclass::text LIKE 'citus_local_table_1%' AND contype = 'u'
ORDER BY 1;

CREATE UNIQUE INDEX citus_local_table_1_idx ON citus_local_table_1(b);
-- show that we successfully defined the unique index
SELECT indexrelid::regclass, indrelid::regclass, indkey
FROM pg_index
WHERE indrelid::regclass::text LIKE 'citus_local_table_1%' AND indexrelid::regclass::text LIKE 'unique_a_b%'
ORDER BY 1;

-- execute truncate & drop commands for multiple relations to see that we don't break local execution
TRUNCATE citus_local_table_1, citus_local_table_2, distributed_table, local_table, reference_table;

-- test vacuum
VACUUM citus_local_table_1;
VACUUM citus_local_table_1, distributed_table, local_table, reference_table;

-- test drop
DROP TABLE citus_local_table_1, citus_local_table_2, distributed_table, local_table, reference_table;

-- test some other udf's with citus local tables

CREATE TABLE citus_local_table_4(a int);
SELECT create_citus_local_table('citus_local_table_4');

-- should work --

-- insert some data & create an index for table size udf's
INSERT INTO citus_local_table_4 VALUES (1), (2), (3);
CREATE INDEX citus_local_table_4_idx ON citus_local_table_4(a);

SELECT citus_table_size('citus_local_table_4');
SELECT citus_total_relation_size('citus_local_table_4');
SELECT citus_relation_size('citus_local_table_4');

BEGIN;
  SELECT lock_relation_if_exists('citus_local_table_4', 'ACCESS SHARE');
  SELECT count(*) FROM pg_locks where relation='citus_local_table_4'::regclass;
COMMIT;

-- hide first column (relationId) as it might change
SELECT part_storage_type, part_method, part_key, part_replica_count, part_max_size, part_placement_policy FROM master_get_table_metadata('citus_local_table_4');
SELECT master_get_table_ddl_events('citus_local_table_4');

SELECT column_to_column_name(logicalrelid, partkey)
FROM pg_dist_partition WHERE logicalrelid = 'citus_local_table_4'::regclass;

SELECT column_name_to_column('citus_local_table_4', 'a');

SELECT master_update_shard_statistics(shardid)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table_4'::regclass) as shardid;

-- will always be no-op as we create the shell table from scratch
-- while creating a citus local table, but let's see it works
SELECT truncate_local_data_after_distributing_table('citus_local_table_4');

BEGIN;
  SELECT worker_drop_distributed_table('citus_local_table_4');

  -- should only see shard relation
  SELECT tableName FROM pg_catalog.pg_tables WHERE tablename LIKE 'citus_local_table_4%';
ROLLBACK;

-- should return a single element array that only includes its own shard id
SELECT shardid, get_colocated_shard_array(shardid)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table_4'::regclass) as shardid;

BEGIN;
  SELECT master_remove_partition_metadata('citus_local_table_4'::regclass::oid, 'citus_local_tables_test_schema', 'citus_local_table_4');

  -- should print 0
  select count(*) from pg_dist_partition where logicalrelid='citus_local_table_4'::regclass;
ROLLBACK;

-- should fail --

SELECT update_distributed_table_colocation('citus_local_table_4', colocate_with => 'none');

SELECT master_create_worker_shards('citus_local_table_4', 10, 1);
SELECT master_create_empty_shard('citus_local_table_4');
SELECT master_apply_delete_command('DELETE FROM citus_local_table_4');

CREATE TABLE postgres_local_table (a int);
SELECT master_append_table_to_shard(shardId, 'postgres_local_table', 'localhost', :master_port)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table_4'::regclass) as shardid;

-- return true
SELECT citus_table_is_visible('citus_local_table_4'::regclass::oid);

-- return false
SELECT relation_is_a_known_shard('citus_local_table_4');

-- return | false | true |
SELECT citus_table_is_visible(tableName::regclass::oid), relation_is_a_known_shard(tableName::regclass)
FROM (SELECT tableName FROM pg_catalog.pg_tables WHERE tablename LIKE 'citus_local_table_4_%') as tableName;

-- cannot create a citus local table from a catalog table
SELECT create_citus_local_table('pg_class');

CREATE TABLE referencing_table(a int);
SELECT create_citus_local_table('referencing_table');

CREATE TABLE referenced_table(a int UNIQUE);
SELECT create_citus_local_table('referenced_table');

ALTER TABLE referencing_table ADD CONSTRAINT fkey_cl_to_cl FOREIGN KEY (a) REFERENCES referenced_table(a);

-- observe the debug messages telling that we switch to sequential
-- execution when truncating a citus local table that is referenced
-- by another table
\set VERBOSITY default
SET client_min_messages TO DEBUG1;

TRUNCATE referenced_table CASCADE;

RESET client_min_messages;
\set VERBOSITY terse

-- cleanup at exit
DROP SCHEMA citus_local_tables_test_schema, "CiTUS!LocalTables" CASCADE;
