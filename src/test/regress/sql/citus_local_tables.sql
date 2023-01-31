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
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

-- try to remove coordinator and observe failure as there exist a citus local table
SELECT 1 FROM master_remove_node('localhost', :master_port);

DROP TABLE citus_local_table_1;

-- this should work now as the citus local table is dropped
SELECT 1 FROM master_remove_node('localhost', :master_port);

CREATE TABLE citus_local_table_1 (a int primary key);

-- this should fail as coordinator is removed from pg_dist_node
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

-- This should also fail as coordinator is removed from pg_dist_node.
--
-- This is not a great place to test this but is one of those places that we
-- have workers in metadata but not the coordinator.
SELECT create_distributed_table_concurrently('citus_local_table_1', 'a');

-- let coordinator have citus local tables again for next tests
set client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

BEGIN;
  CREATE TEMPORARY TABLE temp_table (a int);
  -- errors out as we don't support creating citus local table from a temp table
  SELECT citus_add_local_table_to_metadata('temp_table');
ROLLBACK;

-- creating citus local table having no data initially would work
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

-- creating citus local table having data in it would also work
CREATE TABLE citus_local_table_2(a int primary key);
INSERT INTO citus_local_table_2 VALUES(1);

SELECT citus_add_local_table_to_metadata('citus_local_table_2');

-- also create indexes on them
CREATE INDEX citus_local_table_1_idx ON citus_local_table_1(a);
CREATE INDEX citus_local_table_2_idx ON citus_local_table_2(a);

-- drop them for next tests
DROP TABLE citus_local_table_1, citus_local_table_2;

-- create indexes before creating the citus local tables

-- .. for an initially empty table
CREATE TABLE citus_local_table_1(a int);
CREATE INDEX citus_local_table_1_idx ON citus_local_table_1(a);
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

-- .. and for another table having data in it before creating citus local table
CREATE TABLE citus_local_table_2(a int);
INSERT INTO citus_local_table_2 VALUES(1);
CREATE INDEX citus_local_table_2_idx ON citus_local_table_2(a);
SELECT citus_add_local_table_to_metadata('citus_local_table_2');

CREATE TABLE distributed_table (a int);
SELECT create_distributed_table('distributed_table', 'a');

-- cannot create citus local table from an existing citus table
SELECT citus_add_local_table_to_metadata('distributed_table');

-- show that we do not support inheritance relationships --

CREATE TABLE parent_table (a int, b text);
CREATE TABLE child_table () INHERITS (parent_table);

-- both of below should error out
SELECT citus_add_local_table_to_metadata('parent_table');
SELECT citus_add_local_table_to_metadata('child_table');

-- show that we support UNLOGGED tables --

CREATE UNLOGGED TABLE unlogged_table (a int primary key);
SELECT citus_add_local_table_to_metadata('unlogged_table');


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

  SELECT citus_add_local_table_to_metadata('citus_local_table_3');

  INSERT INTO citus_local_table_3 VALUES (1);

  -- show that trigger is executed only once, we should see "2" (not "3")
  SELECT * FROM citus_local_table_3;
ROLLBACK;

-- show that we support policies in citus enterprise --

BEGIN;
  CREATE TABLE citus_local_table_3 (table_user text);

  ALTER TABLE citus_local_table_3 ENABLE ROW LEVEL SECURITY;

  CREATE ROLE table_users;
  CREATE POLICY table_policy ON citus_local_table_3 TO table_users
      USING (table_user = current_user);

  SELECT citus_add_local_table_to_metadata('citus_local_table_3');
ROLLBACK;

-- show that we properly handle sequences on citus local tables --

BEGIN;
  CREATE SEQUENCE col3_seq;
  CREATE TABLE citus_local_table_3 (col1 serial, col2 int, col3 int DEFAULT nextval('col3_seq'));

  SELECT citus_add_local_table_to_metadata('citus_local_table_3');

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
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true', table_name 'foreign_table');

-- observe that we do not create fdw server for shell table, both shard relation
-- & shell relation points to the same same server object
-- Disable metadata sync since citus doesn't support distributing
-- foreign data wrappers for now.
SET citus.enable_metadata_sync TO OFF;
SELECT citus_add_local_table_to_metadata('foreign_table');
RESET citus.enable_metadata_sync;

DROP FOREIGN TABLE foreign_table;

-- drop them for next tests
DROP TABLE citus_local_table_1, citus_local_table_2, distributed_table;

-- create test tables
CREATE TABLE citus_local_table_1 (a int primary key);
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

CREATE TABLE citus_local_table_2 (a int primary key);
SELECT citus_add_local_table_to_metadata('citus_local_table_2');

CREATE TABLE local_table (a int primary key);

CREATE TABLE distributed_table (a int primary key);
SELECT create_distributed_table('distributed_table', 'a');

CREATE TABLE reference_table (a int primary key);
SELECT create_reference_table('reference_table');

-- show that colociation of citus local tables are not supported for now
-- between citus local tables
SELECT update_distributed_table_colocation('citus_local_table_1', colocate_with => 'citus_local_table_2');

-- between citus local tables and reference tables
SELECT update_distributed_table_colocation('citus_local_table_1', colocate_with => 'reference_table');
SELECT update_distributed_table_colocation('reference_table', colocate_with => 'citus_local_table_1');

-- between citus local tables and distributed tables
SELECT update_distributed_table_colocation('citus_local_table_1', colocate_with => 'distributed_table');
SELECT update_distributed_table_colocation('distributed_table', colocate_with => 'citus_local_table_1');

-- master_create_empty_shard is not supported
SELECT master_create_empty_shard('citus_local_table_1');
-- get_shard_id_for_distribution_column is supported
SELECT get_shard_id_for_distribution_column('citus_local_table_1', 'not_checking_this_arg_for_non_dist_tables');
SELECT get_shard_id_for_distribution_column('citus_local_table_1');
-- citus_copy_shard_placement is not supported
SELECT citus_copy_shard_placement(shardid, 'localhost', :master_port, 'localhost', :worker_1_port)
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
SELECT citus_add_local_table_to_metadata('local_table_1');
SELECT citus_add_local_table_to_metadata('local_table_2');

-- below should work as we allow initial self references in citus local tables
SELECT citus_add_local_table_to_metadata('local_table_3');

------------------------------------------------------------------
----- tests for object names that should be escaped properly -----
------------------------------------------------------------------

CREATE SCHEMA "CiTUS!LocalTables";

-- create table with weird names
CREATE TABLE "CiTUS!LocalTables"."LocalTabLE.1!?!"(id int, "TeNANt_Id" int);

-- should work
SELECT citus_add_local_table_to_metadata('"CiTUS!LocalTables"."LocalTabLE.1!?!"');

-- drop the table before creating it when the search path is set
SET search_path to "CiTUS!LocalTables" ;
DROP TABLE "LocalTabLE.1!?!";

-- have a custom type in the local table
CREATE TYPE local_type AS (key int, value jsonb);

-- create btree_gist for GiST index
CREATE EXTENSION btree_gist;

CREATE TABLE "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"(
  id int PRIMARY KEY,
  "TeNANt_Id" int,
  "local_Type" local_type,
  "jsondata" jsonb NOT NULL,
  name text,
  price numeric CHECK (price > 0),
  serial_data bigserial, UNIQUE (id, price),
  EXCLUDE USING GIST (name WITH =));

-- create some objects before citus_add_local_table_to_metadata
CREATE INDEX "my!Index1" ON "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) WITH ( fillfactor = 80 ) WHERE  id > 10;
CREATE UNIQUE INDEX uniqueIndex ON "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789" (id);

-- ingest some data before citus_add_local_table_to_metadata
set client_min_messages to ERROR;
INSERT INTO "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (1, 1, (1, row_to_json(row(1,1)))::local_type, row_to_json(row(1,1), true)),
                                     (2, 1, (2, row_to_json(row(2,2)))::local_type, row_to_json(row(2,2), 'false'));
reset client_min_messages;
-- create a replica identity before citus_add_local_table_to_metadata
ALTER TABLE "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789" REPLICA IDENTITY USING INDEX uniqueIndex;

CREATE FUNCTION update_id() RETURNS trigger AS $update_id$
BEGIN
    UPDATE "CiTUS!LocalTables"."LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789" SET id=id+1;
    RETURN NEW;
END;
$update_id$ LANGUAGE plpgsql;

CREATE TRIGGER insert_trigger
AFTER INSERT ON "CiTUS!LocalTables"."LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"
FOR EACH STATEMENT EXECUTE FUNCTION update_id();

-- this shouldn't give any syntax errors
SELECT citus_add_local_table_to_metadata('"LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"');

-- create some objects after citus_add_local_table_to_metadata
CREATE INDEX "my!Index2" ON "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) WITH ( fillfactor = 90 ) WHERE id < 20;
CREATE UNIQUE INDEX uniqueIndex2 ON "LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

-----------------------------------
---- utility command execution ----
-----------------------------------

SET search_path TO citus_local_tables_test_schema;

CREATE TABLE dummy_reference_table (a INT PRIMARY KEY);
SELECT create_reference_table('dummy_reference_table');

BEGIN;
  SET client_min_messages TO ERROR;
  SELECT remove_local_tables_from_metadata();

  -- should see only citus local tables that are not converted automatically
  SELECT logicalrelid::regclass::text FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='citus_local_tables_test_schema' AND
        partmethod = 'n' AND repmodel = 's'
  ORDER BY 1;
ROLLBACK;

-- define foreign keys between dummy_reference_table and citus local tables
-- not to undistribute them automatically
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
ALTER TABLE citus_local_table_2 ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
ALTER TABLE unlogged_table ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
ALTER TABLE local_table_3 ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
ALTER TABLE dummy_reference_table ADD CONSTRAINT fkey_from_dummy_ref FOREIGN KEY (a) REFERENCES "CiTUS!LocalTables"."LocalTabLE.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

BEGIN;
  SET client_min_messages TO ERROR;
  SELECT remove_local_tables_from_metadata();

  -- now we defined foreign keys with above citus local tables, we should still see them
  SELECT logicalrelid::regclass::text FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='citus_local_tables_test_schema' AND
        partmethod = 'n' AND repmodel = 's'
  ORDER BY 1;
ROLLBACK;

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

-- test creating citus local table with an index from non-default schema
CREATE SCHEMA "test_\'index_schema";
CREATE TABLE "test_\'index_schema".testindex (a int, b int);
CREATE INDEX ind ON "test_\'index_schema".testindex (a);
ALTER TABLE "test_\'index_schema".testindex ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
SELECT COUNT(*)=2 FROM pg_indexes WHERE tablename LIKE 'testindex%' AND indexname LIKE 'ind%';

-- execute truncate & drop commands for multiple relations to see that we don't break local execution
TRUNCATE citus_local_table_1, citus_local_table_2, distributed_table, local_table, reference_table, local_table_4;

-- test vacuum
VACUUM citus_local_table_1;
VACUUM citus_local_table_1, distributed_table, local_table, reference_table;

-- test drop
DROP TABLE citus_local_table_1, citus_local_table_2, distributed_table, local_table, reference_table, local_table_4;

-- test some other udf's with citus local tables

CREATE TABLE citus_local_table_4(a int);
SELECT citus_add_local_table_to_metadata('citus_local_table_4');

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

SELECT partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid = 'citus_local_table_4'::regclass;
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
SELECT shardid=unnest(get_colocated_shard_array(shardid))
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table_4'::regclass) as shardid;

BEGIN;
  SELECT master_remove_partition_metadata('citus_local_table_4'::regclass::oid, 'citus_local_tables_test_schema', 'citus_local_table_4');

  -- should print 0
  select count(*) from pg_dist_partition where logicalrelid='citus_local_table_4'::regclass;
ROLLBACK;

-- should fail --

SELECT update_distributed_table_colocation('citus_local_table_4', colocate_with => 'none');

SELECT master_create_empty_shard('citus_local_table_4');

-- return true
SELECT citus_table_is_visible('citus_local_table_4'::regclass::oid);

-- return false
SELECT relation_is_a_known_shard('citus_local_table_4');

-- return | false | true |
SELECT citus_table_is_visible(tableName::regclass::oid), relation_is_a_known_shard(tableName::regclass)
FROM (SELECT tableName FROM pg_catalog.pg_tables WHERE tablename LIKE 'citus_local_table_4_%') as tableName;

-- cannot create a citus local table from a catalog table
SELECT citus_add_local_table_to_metadata('pg_class');

-- testing foreign key connection between citus local tables,
-- using the GUC use_citus_managed_tables to add tables to metadata
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE referencing_table(a int);
CREATE TABLE referenced_table(a int UNIQUE);
RESET citus.use_citus_managed_tables;

ALTER TABLE referencing_table ADD CONSTRAINT fkey_cl_to_cl FOREIGN KEY (a) REFERENCES referenced_table(a);

-- verify creating citus local table with extended statistics
CREATE TABLE test_citus_local_table_with_stats(a int, b int);
CREATE STATISTICS stx1 ON a, b FROM test_citus_local_table_with_stats;
ALTER TABLE test_citus_local_table_with_stats ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES dummy_reference_table(a);
CREATE STATISTICS "CiTUS!LocalTables"."Bad\'StatName" ON a, b FROM test_citus_local_table_with_stats;
SELECT COUNT(*)=4 FROM pg_statistic_ext WHERE stxname LIKE 'stx1%' or stxname LIKE 'Bad\\''StatName%' ;

-- observe the debug messages telling that we switch to sequential
-- execution when truncating a citus local table that is referenced
-- by another table
\set VERBOSITY default
SET client_min_messages TO DEBUG1;

TRUNCATE referenced_table CASCADE;

RESET client_min_messages;
\set VERBOSITY terse

-- test for partitioned tables
SET client_min_messages TO ERROR;
-- verify we can convert partitioned tables into Citus Local Tables
CREATE TABLE partitioned (user_id int, time timestamp with time zone, data jsonb, PRIMARY KEY (user_id, time )) PARTITION BY RANGE ("time");
CREATE TABLE partition1 PARTITION OF partitioned FOR VALUES FROM ('2018-04-13 00:00:00+00') TO ('2018-04-14 00:00:00+00');
CREATE TABLE partition2 PARTITION OF partitioned FOR VALUES FROM ('2018-04-14 00:00:00+00') TO ('2018-04-15 00:00:00+00');
SELECT citus_add_local_table_to_metadata('partitioned');
-- partitions added after the conversion get converted into CLT as well
CREATE TABLE partition3 PARTITION OF partitioned FOR VALUES FROM ('2018-04-15 00:00:00+00') TO ('2018-04-16 00:00:00+00');
--verify partitioning hierarchy is preserved after conversion
select inhrelid::regclass from pg_inherits where inhparent='partitioned'::regclass order by 1;
SELECT partition, from_value, to_value, access_method
  FROM time_partitions
  WHERE partition::text LIKE '%partition%'
  ORDER BY partition::text;
-- undistribute succesfully
SELECT undistribute_table('partitioned');
-- verify the partitioning hierarchy is preserved after undistributing
select inhrelid::regclass from pg_inherits where inhparent='partitioned'::regclass order by 1;

-- verify table is undistributed
SELECT relname FROM pg_class WHERE relname LIKE 'partition3%' AND relnamespace IN
    (SELECT oid FROM pg_namespace WHERE nspname = 'citus_local_tables_test_schema')
    ORDER BY relname;

-- drop successfully
DROP TABLE partitioned;

-- test creating distributed tables from partitioned citus local tables
CREATE TABLE partitioned_distributed (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE partitioned_distributed_1 PARTITION OF partitioned_distributed FOR VALUES FROM (1) TO (4);
CREATE TABLE partitioned_distributed_2 PARTITION OF partitioned_distributed FOR VALUES FROM (5) TO (8);
SELECT citus_add_local_table_to_metadata('partitioned_distributed');
SELECT create_distributed_table('partitioned_distributed','a');

\c - - - :worker_1_port
SELECT relname FROM pg_class
  WHERE relname LIKE 'partitioned_distributed%'
    AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'citus_local_tables_test_schema')
  ORDER BY relname;
\c - - - :master_port
SET citus.next_shard_id TO 1904000;
SET search_path TO citus_local_tables_test_schema;

-- error out if converting multi-level partitioned table
CREATE TABLE multi_par (id text, country text) PARTITION BY RANGE (id);
ALTER TABLE multi_par ADD CONSTRAINT unique_constraint UNIQUE (id, country);
CREATE TABLE multi_par_a_to_i PARTITION OF multi_par FOR VALUES FROM ('a') TO ('j');
-- multi-level partitioning
CREATE TABLE multi_par_j_to_r PARTITION OF multi_par FOR VALUES FROM ('j') TO ('s') PARTITION BY LIST  (country);
CREATE TABLE multi_par_j_to_r_japan PARTITION OF multi_par_j_to_r FOR VALUES IN ('japan');
-- these two should error out
SELECT citus_add_local_table_to_metadata('multi_par');
SELECT citus_add_local_table_to_metadata('multi_par',cascade_via_foreign_keys=>true);

-- should error out when cascading via fkeys as well
CREATE TABLE cas_1 (a text, b text);
ALTER TABLE cas_1 ADD CONSTRAINT unique_constraint_2 UNIQUE (a, b);
ALTER TABLE cas_1 ADD CONSTRAINT fkey_to_multi_parti FOREIGN KEY (a,b) REFERENCES multi_par(id, country);
SELECT citus_add_local_table_to_metadata('cas_1');
SELECT citus_add_local_table_to_metadata('cas_1', cascade_via_foreign_keys=>true);
SELECT create_reference_table('cas_1');
ALTER TABLE cas_1 DROP CONSTRAINT fkey_to_multi_parti;
SELECT create_reference_table('cas_1');
ALTER TABLE cas_1 ADD CONSTRAINT fkey_to_multi_parti FOREIGN KEY (a,b) REFERENCES multi_par(id, country);

-- undistribute tables to avoid unnecessary log messages later
select undistribute_table('citus_local_table_4', cascade_via_foreign_keys=>true);
select undistribute_table('referencing_table', cascade_via_foreign_keys=>true);

-- test dropping fkey
CREATE TABLE parent_2_child_1 (a int);
CREATE TABLE parent_1_child_1 (a int);
CREATE TABLE parent_2 (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE parent_1 (a INT UNIQUE) PARTITION BY RANGE(a);
alter table parent_1 attach partition parent_1_child_1 default ;
alter table parent_2 attach partition parent_2_child_1 default ;
CREATE TABLE ref_table(a int unique);
alter table parent_1 add constraint fkey_test_drop foreign key(a) references ref_table(a);
alter table parent_2 add constraint fkey1 foreign key(a) references ref_table(a);
alter table parent_1 add constraint fkey2 foreign key(a) references parent_2(a);
SELECT create_reference_table('ref_table');
alter table parent_1 drop constraint fkey_test_drop;
select count(*) from pg_constraint where conname = 'fkey_test_drop';
-- verify we still preserve the child-parent hierarchy after all conversions
-- check the shard partition
select inhrelid::regclass from pg_inherits where (select inhparent::regclass::text) ~ '^parent_1_\d{7}$' order by 1;
-- check the shell partition
select inhrelid::regclass from pg_inherits where inhparent='parent_1'::regclass order by 1;

-- cleanup at exit
SET client_min_messages TO ERROR;
DROP SCHEMA citus_local_tables_test_schema, "CiTUS!LocalTables", "test_\'index_schema" CASCADE;
