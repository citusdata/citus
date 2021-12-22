\set VERBOSITY terse

SET citus.next_shard_id TO 1800000;
SET citus.next_placement_id TO 8500000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA create_ref_dist_from_citus_local;
SET search_path TO create_ref_dist_from_citus_local;

SET client_min_messages to ERROR;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

CREATE TABLE citus_local_table_1 (col_1 INT UNIQUE);
CREATE TABLE citus_local_table_2 (col_1 INT UNIQUE);
CREATE TABLE citus_local_table_3 (col_1 INT UNIQUE);
CREATE TABLE citus_local_table_4 (col_1 INT UNIQUE);
ALTER TABLE citus_local_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES citus_local_table_1(col_1);
ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_1) REFERENCES citus_local_table_1(col_1);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES citus_local_table_3(col_1);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES citus_local_table_4(col_1);
ALTER TABLE citus_local_table_4 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1) REFERENCES citus_local_table_3(col_1);
ALTER TABLE citus_local_table_4 ADD CONSTRAINT fkey_6 FOREIGN KEY (col_1) REFERENCES citus_local_table_4(col_1);

SELECT citus_add_local_table_to_metadata('citus_local_table_1', cascade_via_foreign_keys=>true);

CREATE TABLE reference_table_1(col_1 INT UNIQUE, col_2 INT UNIQUE);
CREATE TABLE reference_table_2(col_1 INT UNIQUE, col_2 INT UNIQUE);

SELECT create_reference_table('reference_table_1');
SELECT create_reference_table('reference_table_2');

ALTER TABLE citus_local_table_4 ADD CONSTRAINT fkey_7 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_8 FOREIGN KEY (col_1) REFERENCES citus_local_table_2(col_1);

CREATE TABLE distributed_table_1(col_1 INT UNIQUE, col_2 INT);
CREATE TABLE partitioned_dist_table_1 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);

SELECT create_distributed_table('distributed_table_1', 'col_1');
SELECT create_distributed_table('partitioned_dist_table_1', 'col_1');

ALTER TABLE partitioned_dist_table_1 ADD CONSTRAINT fkey_9 FOREIGN KEY (col_1) REFERENCES distributed_table_1(col_1);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_10 FOREIGN KEY (col_1) REFERENCES reference_table_2(col_2);
ALTER TABLE partitioned_dist_table_1 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);

-- As we will heavily rely on this feature after implementing automatic
-- convertion of postgres tables to citus local tables, let's have a
-- complex foreign key graph to see everything is fine.
--
--  distributed_table_1  <----------------    partitioned_dist_table_1
--           |                                           |
--           v                                           v
--  reference_table_2                 _          reference_table_1
--           |                       | |                 ^
--           v                       | v                 |
-- citus_local_table_2 -> citus_local_table_1 -> citus_local_table_4
--                                   ^ |                 |
--                                   | v                 |
--                           citus_local_table_3 <--------

-- Now print metadata after each of create_reference/distributed_table
-- operations to show that everything is fine. Also show that we
-- preserve foreign keys.

BEGIN;
  SELECT create_reference_table('citus_local_table_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='create_ref_dist_from_citus_local')
  ORDER BY tablename;

  SELECT COUNT(*)=11 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='create_ref_dist_from_citus_local') AND
        conname ~ '^fkey\_\d+$';
ROLLBACK;

BEGIN;
  SELECT create_reference_table('citus_local_table_2');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='create_ref_dist_from_citus_local')
  ORDER BY tablename;

  SELECT COUNT(*)=11 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='create_ref_dist_from_citus_local') AND
        conname ~ '^fkey\_\d+$';
ROLLBACK;

-- those two errors out as they reference to citus local tables but
-- distributed tables cannot reference to postgres or citus local tables
SELECT create_distributed_table('citus_local_table_1', 'col_1');
SELECT create_distributed_table('citus_local_table_4', 'col_1');

BEGIN;
  SELECT create_reference_table('citus_local_table_2');
  -- this would error out
  SELECT create_reference_table('citus_local_table_2');
ROLLBACK;

-- test with a standalone table
CREATE TABLE citus_local_table_5 (col_1 INT UNIQUE);
SELECT citus_add_local_table_to_metadata('citus_local_table_5');

BEGIN;
  SELECT create_distributed_table('citus_local_table_5', 'col_1');
  -- this would error out
  SELECT create_reference_table('citus_local_table_5');
ROLLBACK;

BEGIN;
  SELECT create_reference_table('citus_local_table_5');
ROLLBACK;

BEGIN;
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_5(col_1);
  SELECT create_reference_table('citus_local_table_5');
ROLLBACK;

BEGIN;
  SELECT create_distributed_table('citus_local_table_5', 'col_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='create_ref_dist_from_citus_local')
  ORDER BY tablename;

  SELECT COUNT(*)=11 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='create_ref_dist_from_citus_local') AND
        conname ~ '^fkey\_\d+$';
ROLLBACK;

BEGIN;
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_5(col_1);
  SELECT create_distributed_table('citus_local_table_5', 'col_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='create_ref_dist_from_citus_local')
  ORDER BY tablename;

  SELECT COUNT(*)=12 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='create_ref_dist_from_citus_local') AND
        conname ~ '^fkey\_\d+$';
ROLLBACK;

BEGIN;
  -- define a self reference and a foreign key to reference table
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_5(col_1);
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_13 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);

  SELECT create_distributed_table('citus_local_table_5', 'col_1');

  SELECT logicalrelid::text AS tablename, partmethod, repmodel FROM pg_dist_partition
  WHERE logicalrelid::text IN (SELECT tablename FROM pg_tables WHERE schemaname='create_ref_dist_from_citus_local')
  ORDER BY tablename;

  SELECT COUNT(*)=13 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='create_ref_dist_from_citus_local') AND
        conname ~ '^fkey\_\d+$';
ROLLBACK;

CREATE TABLE citus_local_table_6 (col_1 INT UNIQUE);
SELECT citus_add_local_table_to_metadata('citus_local_table_6');

BEGIN;
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_6(col_1);
  -- errors out as foreign keys from distributed tables to citus
  -- local tables are not supported
  SELECT create_distributed_table('citus_local_table_5', 'col_1');
ROLLBACK;

BEGIN;
  -- errors out as foreign keys from citus local tables to distributed
  -- tables are not supported
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_6(col_1);
  SELECT create_distributed_table('citus_local_table_6', 'col_1');
ROLLBACK;

-- have some more tests with foreign keys between citus local
-- and reference tables

BEGIN;
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_6(col_1);
  SELECT create_reference_table('citus_local_table_5');
ROLLBACK;

BEGIN;
  ALTER TABLE citus_local_table_5 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_1) REFERENCES citus_local_table_6(col_1);
  SELECT create_reference_table('citus_local_table_6');
ROLLBACK;

BEGIN;
  CREATE FUNCTION update_value() RETURNS trigger AS $update_value$
  BEGIN
      NEW.value := value+1 ;
      RETURN NEW;
  END;
  $update_value$ LANGUAGE plpgsql;

  CREATE TRIGGER update_value_dist
  AFTER INSERT ON citus_local_table_6
  FOR EACH ROW EXECUTE PROCEDURE update_value();

  -- show that we error out as we don't supprt triggers on distributed tables
  SELECT create_distributed_table('citus_local_table_6', 'col_1');
ROLLBACK;

-- make sure that creating append / range distributed tables is also ok
BEGIN;
  SELECT create_distributed_table('citus_local_table_5', 'col_1', 'range');
ROLLBACK;

BEGIN;
  ALTER TABLE citus_local_table_5 DROP CONSTRAINT citus_local_table_5_col_1_key;
  SELECT create_distributed_table('citus_local_table_5', 'col_1', 'append');
ROLLBACK;

-- cleanup at exit
DROP SCHEMA create_ref_dist_from_citus_local CASCADE;
