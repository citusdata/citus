\set VERBOSITY terse

SET citus.next_shard_id TO 1518000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA fkeys_between_local_ref;
SET search_path TO fkeys_between_local_ref;

SET client_min_messages to ERROR;

-- create a view for testing
CREATE VIEW citus_local_tables_in_schema AS
SELECT logicalrelid FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='fkeys_between_local_ref' AND
      partmethod = 'n' AND repmodel = 'c';


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

-- now that we attached partitioned table to graph below errors out
-- since we cannot create citus local table from partitioned tables
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_9 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);

ALTER TABLE partitioned_table_1 DROP CONSTRAINT fkey_8;

BEGIN;
  -- now that we detached partitioned table from graph, succeeds
  ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_10 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);

  -- show that we converted all 4 local tables in this schema to citus local tables
  SELECT COUNT(*)=4 FROM citus_local_tables_in_schema;
ROLLBACK;

-- this actually attempts to convert local tables to citus local tables but errors out
-- as citus doesn't support defining foreign keys via add column commands
ALTER TABLE local_table_1 ADD COLUMN col_3 INT REFERENCES reference_table_1(col_1);

BEGIN;
  -- define a foreign key so that all 4 local tables become citus local tables
  ALTER TABLE local_table_1 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);

  CREATE TABLE local_table_5 (col_1 INT UNIQUE);
  -- now define foreign key from local to citus local table
  ALTER TABLE local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES local_table_2(col_1);

  -- now we have 5 citus local tables in this schema
  SELECT COUNT(*)=5 FROM citus_local_tables_in_schema;
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

-- cleanup at exit
DROP SCHEMA fkeys_between_local_ref CASCADE;
