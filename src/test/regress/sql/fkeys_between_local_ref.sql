\set VERBOSITY terse

SET citus.next_shard_id TO 1518000;
SET citus.next_placement_id TO 4090000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA fkeys_between_local_ref;
SET search_path TO fkeys_between_local_ref;

SET client_min_messages to ERROR;

-- create a view for testing
CREATE VIEW citus_local_tables_in_schema AS
SELECT logicalrelid FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='fkeys_between_local_ref' AND
      partmethod = 'n' AND repmodel = 's';


-- remove coordinator if it is added to pg_dist_node and test
-- behavior when coordinator is not added to metadata
SELECT COUNT(master_remove_node(nodename, nodeport)) < 2
FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:master_port;

create table ref (a int primary key);
select create_reference_table('ref');
-- creating local table that references to reference table is supported
create table other (x int primary key, y int);

-- creating reference table from a local table that references
-- to reference table is supported
alter table other add constraint fk foreign key (y) references ref (a) on delete cascade;
select create_reference_table('other');

drop table if exists ref, ref2 cascade;

create table ref (a int primary key);
create table ref2 (x int);
alter table ref2 add constraint fk foreign key (x) references ref (a);
select create_reference_table('ref');
-- we can also define more foreign keys after creating reference
-- table from referenced table
alter table ref2 add constraint fk2 foreign key (x) references ref (a);
-- then we can create reference table from referencing table
select create_reference_table('ref2');

drop table if exists ref, ref2, other cascade;


-- add coordinator to pg_dist_node for rest of the tests
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

CREATE TABLE local_table_1 (col_1 INT UNIQUE);
CREATE TABLE local_table_2 (col_1 INT UNIQUE);
CREATE TABLE local_table_3 (col_1 INT UNIQUE);
CREATE TABLE local_table_4 (col_1 INT UNIQUE);
INSERT INTO local_table_1 SELECT i FROM generate_series(195, 205) i;
INSERT INTO local_table_2 SELECT i FROM generate_series(195, 205) i;
INSERT INTO local_table_3 SELECT i FROM generate_series(195, 205) i;
INSERT INTO local_table_4 SELECT i FROM generate_series(195, 205) i;

--                                        _
--                                       | |
--                                       | v
-- local_table_2 -> local_table_1 -> local_table_4
--                      ^ |               |
--                      | v               |
--                  local_table_3 <--------
ALTER TABLE local_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);
ALTER TABLE local_table_3 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES local_table_3(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES local_table_4(col_1);
ALTER TABLE local_table_4 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1) REFERENCES local_table_3(col_1);
ALTER TABLE local_table_4 ADD CONSTRAINT fkey_6 FOREIGN KEY (col_1) REFERENCES local_table_4(col_1);

CREATE TABLE reference_table_1(col_1 INT UNIQUE, col_2 INT);
INSERT INTO reference_table_1 SELECT i FROM generate_series(195, 205) i;
SELECT create_reference_table('reference_table_1');

CREATE TABLE partitioned_table_1 (col_1 INT, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_1_100_200 PARTITION OF partitioned_table_1 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_1_200_300 PARTITION OF partitioned_table_1 FOR VALUES FROM (200) TO (300);
INSERT INTO partitioned_table_1 SELECT i FROM generate_series(195, 205) i;

ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_8 FOREIGN KEY (col_1) REFERENCES local_table_4(col_1);
BEGIN;
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_9 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);
ROLLBACK;
ALTER TABLE partitioned_table_1 DROP CONSTRAINT fkey_8;

BEGIN;
  -- now that we detached partitioned table from graph, succeeds
  ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_10 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);

  -- show that we converted all 4 local tables in this schema to citus local tables
  SELECT COUNT(*)=4 FROM citus_local_tables_in_schema;

  -- dropping that column would undistribute those 4 citus local tables
  ALTER TABLE local_table_1 DROP COLUMN col_1 CASCADE;
  SELECT COUNT(*)=0 FROM citus_local_tables_in_schema;
ROLLBACK;

BEGIN;
  ALTER TABLE local_table_1 ADD COLUMN col_3 INT REFERENCES reference_table_1(col_1);

  -- show that we converted all 4 local tables in this schema to citus local tables
  SELECT COUNT(*)=4 FROM citus_local_tables_in_schema;
ROLLBACK;

BEGIN;
  -- define a foreign key so that all 4 local tables become citus local tables
  ALTER TABLE local_table_1 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);

  CREATE TABLE local_table_5 (col_1 INT UNIQUE);
  -- now define foreign key from local to citus local table
  ALTER TABLE local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES local_table_2(col_1);

  -- now we have 5 citus local tables in this schema
  SELECT COUNT(*)=5 FROM citus_local_tables_in_schema;

  -- dropping foreign key from local_table_2 would only undistribute local_table_2 & local_table_5
  ALTER TABLE local_table_2 DROP CONSTRAINT fkey_1;
  SELECT logicalrelid::regclass::text FROM citus_local_tables_in_schema ORDER BY logicalrelid;

    -- dropping local_table_1 would undistribute last two citus local tables as local_table_1
    -- was the bridge to reference table
  DROP TABLE local_table_1 CASCADE;
  SELECT COUNT(*)=0 FROM citus_local_tables_in_schema;
ROLLBACK;

-- they fail as local_table_99 does not exist
ALTER TABLE local_table_99 ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES local_table_99(col_1);

-- they fail as col_99 does not exist
ALTER TABLE local_table_1 ADD CONSTRAINT fkey FOREIGN KEY (col_99) REFERENCES reference_table_1(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES reference_table_1(col_99);

-- fails as col_2 does not have a unique/primary key constraint
ALTER TABLE local_table_1 ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);

CREATE TABLE reference_table_2 (col_1 INT UNIQUE, col_2 INT);
INSERT INTO reference_table_2 SELECT i FROM generate_series(195, 205) i;

BEGIN;
  -- define foreign key when both ends are local tables
  ALTER TABLE local_table_1 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_2(col_1);

  -- we still don't convert local tables to citus local tables when
  -- creating reference tables
  SELECT create_reference_table('reference_table_2');

  -- now defining another foreign key would convert local tables to
  -- citus local tables as reference_table_2 is now a reference table
  ALTER TABLE local_table_1 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES reference_table_2(col_1);

  -- now print metadata to show that everyting is fine
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

-- we don't support foreign keys from citus local to reference tables
-- with ON DELETE/UPDATE CASCADE behavior, so below two errors out

BEGIN;
  SELECT create_reference_table('reference_table_2');
  ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES local_table_2(col_1) ON DELETE CASCADE;
ROLLBACK;

BEGIN;
  SELECT create_reference_table('reference_table_2');
  ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY (col_1) REFERENCES local_table_2(col_1) ON UPDATE CASCADE;
ROLLBACK;

-- but we support such foreign key behaviors when foreign key is from
-- citus local to reference table

BEGIN;
  SELECT create_reference_table('reference_table_2');
  ALTER TABLE local_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_2(col_1) ON DELETE CASCADE;

  DELETE FROM reference_table_2 WHERE col_1=200;
  -- we deleted one row as DELETE cascades, so we should have 10 rows
  SELECT COUNT(*) FROM local_table_2;
ROLLBACK;

BEGIN;
  SELECT create_reference_table('reference_table_2');
  ALTER TABLE local_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_2(col_1) ON UPDATE CASCADE;
ROLLBACK;

--
-- create table tests
--

BEGIN;
  CREATE TABLE local_table_6 (col_1 INT PRIMARY KEY);

  -- create a table that references to
  --  * local table graph
  --  * reference table
  --  * another local table
  --  * itself
  CREATE TABLE local_table_5 (
    col_1 INT UNIQUE REFERENCES local_table_1(col_1),
    col_2 INT,
    FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1),
    -- not specify column for this foreign key
    FOREIGN KEY (col_2) REFERENCES local_table_6,
    -- also have a self reference
    FOREIGN KEY (col_2) REFERENCES local_table_5(col_1));

  -- now print metadata to show that all local tables are converted
  -- to citus local tables
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

CREATE TABLE distributed_table (col_1 INT PRIMARY KEY);
SELECT create_distributed_table('distributed_table', 'col_1');

-- Creating a table that both references to a reference table and a
-- distributed table fails.
-- This is because, we convert local table to a citus local table
-- due to its foreign key to reference table.
-- But citus local tables can't have foreign keys to distributed tables.
CREATE TABLE local_table_5 (
  col_1 INT UNIQUE REFERENCES distributed_table(col_1),
  FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1));

BEGIN;
  ALTER TABLE distributed_table ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);

  CREATE TABLE local_table_5 (
    col_1 INT UNIQUE REFERENCES reference_table_1(col_1),
    FOREIGN KEY (col_1) REFERENCES local_table_1(col_1));

  INSERT INTO local_table_5 SELECT i FROM generate_series(195, 205) i;

  -- Now show that when converting local table to a citus local table,
  -- distributed table (that is referenced by reference table) stays as is.
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;

  -- show that we validate foreign key constraints, errors out
  INSERT INTO local_table_5 VALUES (300);
ROLLBACK;

BEGIN;
  SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
  CREATE SCHEMA another_schema_fkeys_between_local_ref;
  CREATE TABLE another_schema_fkeys_between_local_ref.local_table_6 (col_1 INT PRIMARY KEY);

  -- first convert local tables to citus local tables in graph
  ALTER TABLE local_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1) ON DELETE CASCADE;

  CREATE TABLE local_table_5 (
    col_1 INT UNIQUE REFERENCES another_schema_fkeys_between_local_ref.local_table_6(col_1) CHECK (col_1 > 0),
    col_2 INT REFERENCES local_table_3(col_1),
    FOREIGN KEY (col_1) REFERENCES local_table_5(col_1));

  -- Now show that we converted local_table_5 & 6 to citus local tables
  -- as local_table_5 has foreign key to a citus local table too
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref' UNION
                               SELECT 'another_schema_fkeys_between_local_ref.local_table_6')
  ORDER BY tablename;

  DROP TABLE local_table_3 CASCADE;
  DROP SCHEMA another_schema_fkeys_between_local_ref CASCADE;

  -- now we shouldn't see local_table_5 since now it is not connected to any reference tables/citus local tables
  -- and it's converted automatically
  SELECT logicalrelid::text AS tablename, partmethod, repmodel, autoconverted FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
  CREATE TABLE local_table_6 (col_1 INT PRIMARY KEY);
  -- first convert local tables to citus local tables in graph
  ALTER TABLE local_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1) ON DELETE CASCADE;

  -- create a table that references to
  --  * citus local table graph (via local_table_1)
  --  * another local table (local_table_6)
  --  * itself
  CREATE TABLE local_table_5 (
    col_1 INT UNIQUE REFERENCES local_table_1(col_1),
    col_2 INT CHECK (col_2 > 0),
    -- not specify column for this foreign key
    FOREIGN KEY (col_2) REFERENCES local_table_6,
    FOREIGN KEY (col_2) REFERENCES local_table_5(col_1));

  -- now print metadata to show that all local tables are converted
  -- to citus local tables
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;

  -- now make some of them reference tables
  SELECT create_reference_table('local_table_2');
  SELECT create_reference_table('local_table_6');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;

  CREATE SCHEMA another_schema_fkeys_between_local_ref;
  CREATE TABLE another_schema_fkeys_between_local_ref.reference_table_3 (col_1 INT UNIQUE);
  SELECT create_reference_table('another_schema_fkeys_between_local_ref.reference_table_3');
  TRUNCATE local_table_4 CASCADE;
  ALTER TABLE local_table_4 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES another_schema_fkeys_between_local_ref.reference_table_3(col_1);

  DROP TABLE local_table_5 CASCADE;
  ALTER TABLE local_table_2 DROP CONSTRAINT fkey_1;
  DROP SCHEMA another_schema_fkeys_between_local_ref CASCADE;

  -- now we shouldn't see any citus local tables
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  -- disable foreign keys to reference tables
  SET LOCAL citus.enable_local_reference_table_foreign_keys TO false;
  CREATE TABLE local_table_6 (col_1 INT PRIMARY KEY);

  ALTER TABLE local_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1) ON DELETE CASCADE;

  CREATE TABLE local_table_5 (
    col_1 INT UNIQUE REFERENCES local_table_6(col_1),
    col_2 INT REFERENCES local_table_3(col_1),
    FOREIGN KEY (col_1) REFERENCES local_table_5(col_1),
    FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1));

  -- Now show none of local_table_5 & 6 to should be converted to citus local tables
  -- as it is disabled
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

-- converting any local table to a citus local table in graph converts
-- other tables to citus local tables, test this in below xact blocks

BEGIN;
  SELECT create_reference_table('local_table_1');

  SELECT create_distributed_table('local_table_2', 'col_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  SELECT create_reference_table('local_table_4');

  SELECT create_reference_table('local_table_3');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  CREATE TABLE local_table_5 (col_1 INT REFERENCES local_table_1(col_1));

  SELECT create_reference_table('local_table_1');

  SELECT create_distributed_table('local_table_2', 'col_1');
  SELECT create_distributed_table('local_table_5', 'col_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  ALTER TABLE local_table_1 ADD CONSTRAINT fkey_13 FOREIGN KEY (col_1) REFERENCES local_table_2(col_1) ON DELETE CASCADE;

  -- errors out as foreign keys from reference tables to citus local tables
  -- cannot have CASCADE behavior
  SELECT create_reference_table('local_table_1');
ROLLBACK;

SET citus.enable_local_execution TO OFF;
-- show that this errors out as it tries to convert connected relations to citus
-- local tables and creating citus local table requires local execution but local
-- execution is disabled
SELECT create_reference_table('local_table_1');
SET citus.enable_local_execution TO ON;

-- test behavior when outside of the xact block

CREATE TABLE local_table_6 (col_1 INT REFERENCES local_table_1(col_1));

SELECT create_reference_table('local_table_1');

SELECT create_distributed_table('local_table_2', 'col_1');
SELECT create_distributed_table('local_table_6', 'col_1');

SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
ORDER BY tablename;

BEGIN;
CREATE TABLE part_local_table (col_1 INT REFERENCES reference_table_1(col_1)) PARTITION BY RANGE (col_1);
ROLLBACK;

-- they fail as col_99 does not exist
CREATE TABLE local_table_5 (col_1 INT, FOREIGN KEY (col_99) REFERENCES reference_table_1(col_1));
CREATE TABLE local_table_5 (col_1 INT, FOREIGN KEY (col_1) REFERENCES reference_table_1(col_99));

-- fails as referenced table does not exist
CREATE TABLE local_table_5 (col_1 INT, FOREIGN KEY (col_1) REFERENCES table_does_not_exist(dummy));

-- drop & recreate schema to prevent noise in next test outputs
DROP SCHEMA fkeys_between_local_ref CASCADE;
CREATE SCHEMA fkeys_between_local_ref;
SET search_path TO fkeys_between_local_ref;

-- now have some tests to test behavior before/after enabling foreign keys
-- between local tables & reference tables

BEGIN;
  SET citus.enable_local_reference_table_foreign_keys TO OFF;

  CREATE TABLE ref_1(a int PRIMARY KEY);
  CREATE TABLE pg_local_1(a int PRIMARY KEY REFERENCES ref_1(a));
  SELECT create_reference_table('ref_1');

  SET citus.enable_local_reference_table_foreign_keys TO ON;

  CREATE TABLE ref_2(a int PRIMARY KEY);
  SELECT create_reference_table('ref_2');
  ALTER TABLE pg_local_1 ADD CONSTRAINT c1 FOREIGN KEY(a) REFERENCES ref_2(a);

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  SET citus.enable_local_reference_table_foreign_keys TO OFF;

  CREATE TABLE ref_1(a int PRIMARY KEY);
  CREATE TABLE pg_local_1(a int PRIMARY KEY REFERENCES ref_1(a));
  SELECT create_reference_table('ref_1');

  SET citus.enable_local_reference_table_foreign_keys TO ON;

  CREATE TABLE ref_2(a int PRIMARY KEY REFERENCES pg_local_1(a));
  SELECT create_reference_table('ref_2');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  SET citus.enable_local_reference_table_foreign_keys TO OFF;

  CREATE TABLE ref_1(a int PRIMARY KEY);
  CREATE TABLE pg_local_1(a int PRIMARY KEY REFERENCES ref_1(a));
  SELECT create_reference_table('ref_1');

  SET citus.enable_local_reference_table_foreign_keys TO ON;

  CREATE TABLE ref_2(a int PRIMARY KEY REFERENCES pg_local_1(a));
  SELECT create_reference_table('ref_2');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  SET citus.enable_local_reference_table_foreign_keys TO OFF;

  CREATE TABLE ref_1(a int PRIMARY KEY);
  CREATE TABLE pg_local_1(a int PRIMARY KEY REFERENCES ref_1(a));
  SELECT create_reference_table('ref_1');

  SET citus.enable_local_reference_table_foreign_keys TO ON;

  CREATE TABLE pg_local_2(a int PRIMARY KEY REFERENCES pg_local_1(a));

  -- we still didn't convert local tables to citus local tables
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;

  CREATE TABLE pg_local_3(a int PRIMARY KEY REFERENCES ref_1(a));

  -- pg_local_3 is not connected to other local tables, so we will just
  -- convert pg_local_3 to a citus local table
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;

  CREATE TABLE pg_local_4(a int PRIMARY KEY REFERENCES ref_1(a), FOREIGN KEY (a) REFERENCES pg_local_2(a));

  -- pg_local_4 is connected to ref_1, pg_local_1 and pg_local_2,
  -- so we will convert those two local tables to citus local tables too
  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='fkeys_between_local_ref')
  ORDER BY tablename;
ROLLBACK;

-- cleanup at exit
DROP SCHEMA fkeys_between_local_ref CASCADE;
