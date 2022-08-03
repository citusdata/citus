\set VERBOSITY terse

SET citus.next_shard_id TO 1517000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;

CREATE SCHEMA undistribute_table_cascade_mx;
SET search_path TO undistribute_table_cascade_mx;

SET client_min_messages to ERROR;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

-- ensure that we sync metadata to worker 1 & 2
SELECT 1 FROM start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT 1 FROM start_metadata_sync_to_node('localhost', :worker_2_port);

CREATE TABLE reference_table_1 (col_1 INT UNIQUE, col_2 INT UNIQUE, UNIQUE (col_2, col_1));
SELECT create_reference_table('reference_table_1');

CREATE TABLE distributed_table_1 (col_1 INT UNIQUE);
SELECT create_distributed_table('distributed_table_1', 'col_1');

CREATE TABLE citus_local_table_1 (col_1 INT UNIQUE);
SELECT citus_add_local_table_to_metadata('citus_local_table_1');

CREATE TABLE citus_local_table_2 (col_1 INT UNIQUE);
SELECT citus_add_local_table_to_metadata('citus_local_table_2');

CREATE TABLE partitioned_table_1 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_1_100_200 PARTITION OF partitioned_table_1 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_1_200_300 PARTITION OF partitioned_table_1 FOR VALUES FROM (200) TO (300);
SELECT create_distributed_table('partitioned_table_1', 'col_1');

ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_2) REFERENCES reference_table_1(col_1);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);
ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_6 FOREIGN KEY (col_1) REFERENCES citus_local_table_2(col_1);

SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);

-- both workers should print 0 as we undistributed all relations in this schema
SELECT run_command_on_workers(
$$
SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname='undistribute_table_cascade_mx'
$$);

-- drop parititoned table as citus_add_local_table_to_metadata doesn't support partitioned tables
DROP TABLE partitioned_table_1;
SELECT citus_add_local_table_to_metadata('citus_local_table_1', cascade_via_foreign_keys=>true);

-- both workers should print 4 as we converted all tables except
-- partitioned table in this schema to a citus local table
SELECT run_command_on_workers(
$$
SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname='undistribute_table_cascade_mx'
$$);

-- create a reference table with an implicit sequence
CREATE TABLE reference_table_2 (id bigserial);
SELECT create_reference_table('reference_table_2');

-- this should work fine since we won't try to change sequence dependencies on mx workers
SELECT undistribute_table('reference_table_2');

create table countries(
  id bigserial primary key
  , name text
  , code varchar(2) collate "C" unique
);
insert into countries(name, code) select 'country-'||i, i::text from generate_series(10,99) i;
select create_reference_table('countries');

CREATE TABLE users (
    id bigserial
  , org_id bigint
  , name text
  , created_at timestamptz default now()
  , country_id int references countries(id)
  , primary key (org_id, id)
);

-- "users" table was implicitly added to citus metadata when defining foreign key,
-- so create_distributed_table would first undistribute it.
-- Show that it works well when changing sequence dependencies on mx workers.
select create_distributed_table('users', 'org_id');

-- count sequences that depend on "users" table's "id" column
SELECT COUNT(*)
FROM pg_class s
  JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
  JOIN pg_class t ON t.oid=d.refobjid
  JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
WHERE s.relkind='S' AND t.relname = 'users' AND a.attname = 'id';

SELECT run_command_on_workers(
$$
SELECT COUNT(*)
FROM pg_class s
  JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
  JOIN pg_class t ON t.oid=d.refobjid
  JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
WHERE s.relkind='S' AND t.relname = 'users' AND a.attname = 'id';
$$);

SELECT alter_distributed_table ('users', shard_count=>10);

-- first drop the column that has a foreign key since
-- alter_table_set_access_method doesn't support foreign keys
ALTER TABLE users DROP country_id;

SELECT alter_table_set_access_method('users', 'columnar');

SELECT COUNT(*)
FROM pg_class s
  JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
  JOIN pg_class t ON t.oid=d.refobjid
  JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
WHERE s.relkind='S' AND t.relname = 'users' AND a.attname = 'id';

SELECT run_command_on_workers(
$$
SELECT COUNT(*)
FROM pg_class s
  JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
  JOIN pg_class t ON t.oid=d.refobjid
  JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
WHERE s.relkind='S' AND t.relname = 'users' AND a.attname = 'id';
$$);


-- cleanup at exit
DROP SCHEMA undistribute_table_cascade_mx CASCADE;
