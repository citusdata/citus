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

\set VERBOSITY DEFAULT

-- Test the UDFs that we use to convert Citus local tables to single-shard tables and
-- reference tables.

SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(1, 't', 1, true);
SELECT pg_catalog.citus_internal_delete_placement_metadata(1);

CREATE ROLE test_user_create_ref_dist WITH LOGIN;
GRANT ALL ON SCHEMA create_ref_dist_from_citus_local TO test_user_create_ref_dist;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'test_user_create_ref_dist';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET ROLE test_user_create_ref_dist;

SET citus.next_shard_id TO 1850000;
SET citus.next_placement_id TO 8510000;
SET citus.shard_replication_factor TO 1;
SET search_path TO create_ref_dist_from_citus_local;

SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(null, 't', 1, true);
SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(1, null, 1, true);
SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(1, 't', null, true);
SELECT pg_catalog.citus_internal_update_none_dist_table_metadata(1, 't', 1, null);

SELECT pg_catalog.citus_internal_delete_placement_metadata(null);

CREATE TABLE udf_test (col_1 int);
SELECT citus_add_local_table_to_metadata('udf_test');

BEGIN;
    SELECT pg_catalog.citus_internal_update_none_dist_table_metadata('create_ref_dist_from_citus_local.udf_test'::regclass, 'k', 99999, true);

    SELECT COUNT(*)=1 FROM pg_dist_partition
    WHERE logicalrelid = 'create_ref_dist_from_citus_local.udf_test'::regclass AND repmodel = 'k' AND colocationid = 99999 AND autoconverted = true;

    SELECT placementid AS udf_test_placementid FROM pg_dist_shard_placement
    WHERE shardid = get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.udf_test') \gset

    SELECT pg_catalog.citus_internal_delete_placement_metadata(:udf_test_placementid);

    SELECT COUNT(*)=0 FROM pg_dist_placement WHERE placementid = :udf_test_placementid;
ROLLBACK;

RESET ROLE;
DROP TABLE udf_test;
REVOKE ALL ON SCHEMA create_ref_dist_from_citus_local FROM test_user_create_ref_dist;
DROP USER test_user_create_ref_dist;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- Test lazy conversion from Citus local to single-shard tables and reference tables.

SET citus.next_shard_id TO 1860000;
SET citus.next_placement_id TO 8520000;
SET citus.shard_replication_factor TO 1;
SET search_path TO create_ref_dist_from_citus_local;
SET client_min_messages to ERROR;

INSERT INTO reference_table_1 VALUES (1, 1), (2, 2), (201, 201), (202, 202);

CREATE TABLE citus_local_table_7 (col_1 int UNIQUE);
INSERT INTO citus_local_table_7 VALUES (1), (2), (201), (202);
SELECT citus_add_local_table_to_metadata('citus_local_table_7');

CREATE TABLE fkey_test (
    int_col_1 int PRIMARY KEY,
    text_col_1 text UNIQUE,
    int_col_2 int
);
INSERT INTO fkey_test VALUES (1, '1', 1), (2, '2', 2), (201, '201', 201), (202, '202', 202);
SELECT citus_add_local_table_to_metadata('fkey_test');

-- check unsupported foreign key constraints
ALTER TABLE reference_table_1 ADD CONSTRAINT ref_1_col_1_fkey_test_int_col_1 FOREIGN KEY (col_1) REFERENCES fkey_test(int_col_1);
SELECT create_distributed_table('fkey_test', null, colocate_with=>'none');
ALTER TABLE reference_table_1 DROP CONSTRAINT ref_1_col_1_fkey_test_int_col_1;

ALTER TABLE citus_local_table_7 ADD CONSTRAINT citus_local_1_col_1_fkey_test_int_col_1 FOREIGN KEY (col_1) REFERENCES fkey_test(int_col_1);
SELECT create_distributed_table('fkey_test', null, colocate_with=>'none');
ALTER TABLE citus_local_table_7 DROP CONSTRAINT citus_local_1_col_1_fkey_test_int_col_1;

ALTER TABLE fkey_test ADD CONSTRAINT fkey_test_int_col_1_citus_local_1_col_1 FOREIGN KEY (int_col_1) REFERENCES citus_local_table_7(col_1);
SELECT create_distributed_table('fkey_test', null, colocate_with=>'none');
ALTER TABLE fkey_test DROP CONSTRAINT fkey_test_int_col_1_citus_local_1_col_1;

CREATE TABLE tbl_1 (
    int_col_1 int PRIMARY KEY,
    text_col_1 text UNIQUE,
    int_col_2 int
);
CREATE INDEX tbl_1_int_col_2_idx ON tbl_1 (int_col_2);

INSERT INTO tbl_1 VALUES (1, '1', 1), (2, '2', 2), (201, '201', 201), (202, '202', 202);

ALTER TABLE tbl_1 ADD CONSTRAINT tbl_1_int_col_1_ref_1_col_1 FOREIGN KEY (int_col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE tbl_1 ADD CONSTRAINT tbl_1_int_col_2_ref_1_col_1 FOREIGN KEY (int_col_2) REFERENCES reference_table_1(col_1);
ALTER TABLE tbl_1 ADD CONSTRAINT tbl_1_int_col_2_tbl_1_int_col_1 FOREIGN KEY (int_col_2) REFERENCES tbl_1(int_col_1);
SELECT citus_add_local_table_to_metadata('tbl_1');

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_1') AS tbl_1_old_shard_id \gset

SELECT create_distributed_table('tbl_1', null, colocate_with=>'none');

-- check data
SELECT * FROM tbl_1 ORDER BY int_col_1;

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_1');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_1', :tbl_1_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_1', 3);
SELECT cardinality(fkey_names) = 3 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_1');

-- test partitioning
CREATE TABLE tbl_2 (
    int_col_1 int PRIMARY KEY,
    text_col_1 text,
    int_col_2 int
) PARTITION BY RANGE (int_col_1);
CREATE TABLE tbl_2_child_1 PARTITION OF tbl_2 FOR VALUES FROM (0) TO (100);
CREATE TABLE tbl_2_child_2 PARTITION OF tbl_2 FOR VALUES FROM (200) TO (300);

INSERT INTO tbl_2 VALUES (1, '1', 1), (2, '2', 2), (201, '201', 201), (202, '202', 202);

SELECT citus_add_local_table_to_metadata('tbl_2');

ALTER TABLE tbl_2 ADD CONSTRAINT tbl_2_int_col_1_ref_1_col_1 FOREIGN KEY (int_col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE tbl_2 ADD CONSTRAINT tbl_2_int_col_2_ref_1_col_1 FOREIGN KEY (int_col_2) REFERENCES reference_table_1(col_1);

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_2') AS tbl_2_old_shard_id \gset
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_2_child_1') AS tbl_2_child_1_old_shard_id \gset
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_2_child_2') AS tbl_2_child_2_old_shard_id \gset

SELECT create_distributed_table('tbl_2', null, colocate_with=>'tbl_1');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2', :tbl_2_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_2', 1);
SELECT cardinality(fkey_names) = 2 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_2');
SELECT public.verify_partition_count_on_placements('create_ref_dist_from_citus_local.tbl_2', 2);

-- verify the same for children
SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2_child_1');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2_child_1', :tbl_2_child_1_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_2_child_1', 1);
SELECT cardinality(fkey_names) = 2 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_2_child_1');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2_child_2');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_2_child_2', :tbl_2_child_2_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_2_child_2', 1);
SELECT cardinality(fkey_names) = 2 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_2_child_2');

-- verify that placements of all 4 tables are on the same node
SELECT COUNT(DISTINCT(groupid)) = 1 FROM pg_dist_placement WHERE shardid IN (
    :tbl_1_old_shard_id, :tbl_2_old_shard_id, :tbl_2_child_1_old_shard_id, :tbl_2_child_2_old_shard_id
);

-- verify the same by executing a router query that targets both tables
SET client_min_messages to DEBUG2;
SELECT COUNT(*) FROM tbl_1, tbl_2;
SET client_min_messages to ERROR;

CREATE TABLE reference_table_3(col_1 INT UNIQUE, col_2 INT UNIQUE);
INSERT INTO reference_table_3 VALUES (1, 1), (2, 2), (201, 201), (202, 202);

CREATE TABLE tbl_3 (
    int_col_1 int PRIMARY KEY,
    text_col_1 text,
    int_col_2 int
) PARTITION BY RANGE (int_col_1);
CREATE TABLE tbl_3_child_1 PARTITION OF tbl_3 FOR VALUES FROM (0) TO (100);

ALTER TABLE tbl_3 ADD CONSTRAINT tbl_3_int_col_1_ref_1_col_1 FOREIGN KEY (int_col_1) REFERENCES reference_table_3(col_1);

SELECT create_reference_table('reference_table_3');

INSERT INTO tbl_3 VALUES (1, '1', 1), (2, '2', 2);

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_3') AS tbl_3_old_shard_id \gset
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.tbl_3_child_1') AS tbl_3_child_1_old_shard_id \gset

SELECT create_distributed_table('tbl_3', null, colocate_with=>'none');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_3');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_3', :tbl_3_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_3', 1);
SELECT cardinality(fkey_names) = 1 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_3');
SELECT public.verify_partition_count_on_placements('create_ref_dist_from_citus_local.tbl_3', 1);

-- verify the same for children
SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.tbl_3_child_1');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.tbl_3_child_1', :tbl_3_child_1_old_shard_id, false);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.tbl_3_child_1', 1);
SELECT cardinality(fkey_names) = 1 AS verify_fkey_count_on_shards FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.tbl_3_child_1');

-- verify that placements of all 2 tables are on the same node
SELECT COUNT(DISTINCT(groupid)) = 1 FROM pg_dist_placement WHERE shardid IN (
    :tbl_3_old_shard_id, :tbl_3_child_1_old_shard_id
);

-- verify the same by executing a router query that targets the table
SET client_min_messages to DEBUG2;
SELECT COUNT(*) FROM tbl_3;
SET client_min_messages to ERROR;

CREATE TABLE single_shard_conversion_colocated_1 (
    int_col_1 int PRIMARY KEY,
    text_col_1 text UNIQUE,
    int_col_2 int
);
SELECT citus_add_local_table_to_metadata('single_shard_conversion_colocated_1');

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.single_shard_conversion_colocated_1') AS single_shard_conversion_colocated_1_old_shard_id \gset

SELECT create_distributed_table('single_shard_conversion_colocated_1', null, colocate_with=>'none');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_colocated_1');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_colocated_1', :single_shard_conversion_colocated_1_old_shard_id, false);

CREATE TABLE single_shard_conversion_colocated_2 (
    int_col_1 int
);
SELECT citus_add_local_table_to_metadata('single_shard_conversion_colocated_2');

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.single_shard_conversion_colocated_2') AS single_shard_conversion_colocated_2_old_shard_id \gset

SELECT create_distributed_table('single_shard_conversion_colocated_2', null, colocate_with=>'single_shard_conversion_colocated_1');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_colocated_2');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_colocated_2', :single_shard_conversion_colocated_2_old_shard_id, false);

-- make sure that they're created on the same colocation group
SELECT
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_ref_dist_from_citus_local.single_shard_conversion_colocated_1'::regclass
)
=
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_ref_dist_from_citus_local.single_shard_conversion_colocated_2'::regclass
);

-- verify that placements of 2 tables are on the same node
SELECT COUNT(DISTINCT(groupid)) = 1 FROM pg_dist_placement WHERE shardid IN (
    :single_shard_conversion_colocated_1_old_shard_id, :single_shard_conversion_colocated_2_old_shard_id
);

CREATE TABLE single_shard_conversion_noncolocated_1 (
    int_col_1 int
);
SELECT citus_add_local_table_to_metadata('single_shard_conversion_noncolocated_1');

-- save old shardid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.single_shard_conversion_noncolocated_1') AS single_shard_conversion_noncolocated_1_old_shard_id \gset

SELECT create_distributed_table('single_shard_conversion_noncolocated_1', null, colocate_with=>'none');

SELECT public.verify_pg_dist_partition_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_noncolocated_1');
SELECT public.verify_shard_placement_for_single_shard_table('create_ref_dist_from_citus_local.single_shard_conversion_noncolocated_1', :single_shard_conversion_noncolocated_1_old_shard_id, false);

-- make sure that they're created on different colocation groups
SELECT
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_ref_dist_from_citus_local.single_shard_conversion_colocated_1'::regclass
)
!=
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_ref_dist_from_citus_local.single_shard_conversion_noncolocated_1'::regclass
);

-- Test creating a reference table from a Citus local table
-- (ref_table_conversion_test) that has foreign keys from/to Citus
-- local tables and reference tables:
--
--       citus_local_referencing ----------     ----------> citus_local_referenced
--                                        |     ^
--                                        v     |
--                                ref_table_conversion_test
--                                        ^     |
--                                        |     v
--   reference_table_referencing ----------     ----------> reference_table_referenced
--

CREATE TABLE citus_local_referenced(a int PRIMARY KEY);
SELECT citus_add_local_table_to_metadata('citus_local_referenced');
INSERT INTO citus_local_referenced VALUES (1), (2), (3), (4);

CREATE TABLE reference_table_referenced(a int PRIMARY KEY);
SELECT create_reference_table('reference_table_referenced');
INSERT INTO reference_table_referenced VALUES (1), (2), (3), (4);

CREATE TABLE ref_table_conversion_test (
    a int PRIMARY KEY
);
SELECT citus_add_local_table_to_metadata('ref_table_conversion_test');
ALTER TABLE ref_table_conversion_test ADD CONSTRAINT ref_table_a_citus_local_referenced_a FOREIGN KEY (a) REFERENCES citus_local_referenced(a);
ALTER TABLE ref_table_conversion_test ADD CONSTRAINT ref_table_a_reference_table_referenced_a FOREIGN KEY (a) REFERENCES reference_table_referenced(a);
INSERT INTO ref_table_conversion_test VALUES (1), (2), (3), (4);

CREATE INDEX ref_table_conversion_test_a_idx1 ON ref_table_conversion_test (a);
CREATE INDEX ref_table_conversion_test_a_idx2 ON ref_table_conversion_test (a);

CREATE TABLE citus_local_referencing(a int);
ALTER TABLE citus_local_referencing ADD CONSTRAINT citus_local_referencing_a_ref_table_a FOREIGN KEY (a) REFERENCES ref_table_conversion_test(a);
SELECT citus_add_local_table_to_metadata('citus_local_referencing');
INSERT INTO citus_local_referencing VALUES (1), (2), (3), (4);

CREATE TABLE reference_table_referencing(a int);
ALTER TABLE reference_table_referencing ADD CONSTRAINT reference_table_referencing_a_ref_table_a FOREIGN KEY (a) REFERENCES ref_table_conversion_test(a);
SELECT create_reference_table('reference_table_referencing');
INSERT INTO reference_table_referencing VALUES (1), (2), (3), (4);

-- save old shardid and placementid
SELECT get_shard_id_for_distribution_column('create_ref_dist_from_citus_local.ref_table_conversion_test') AS ref_table_conversion_test_old_shard_id \gset
SELECT placementid AS ref_table_conversion_test_old_coord_placement_id FROM pg_dist_placement WHERE shardid = :ref_table_conversion_test_old_shard_id \gset

SELECT create_reference_table('ref_table_conversion_test');

-- check data on all placements
SELECT result FROM run_command_on_all_nodes(
    $$SELECT COUNT(*)=4 FROM create_ref_dist_from_citus_local.ref_table_conversion_test$$
);

SELECT public.verify_pg_dist_partition_for_reference_table('create_ref_dist_from_citus_local.ref_table_conversion_test');
SELECT public.verify_shard_placements_for_reference_table('create_ref_dist_from_citus_local.ref_table_conversion_test',
                                                          :ref_table_conversion_test_old_shard_id,
                                                          :ref_table_conversion_test_old_coord_placement_id);
SELECT public.verify_index_count_on_shard_placements('create_ref_dist_from_citus_local.ref_table_conversion_test', 3);
SELECT on_node, fkey_names FROM public.get_fkey_names_on_placements('create_ref_dist_from_citus_local.ref_table_conversion_test') ORDER BY 1,2;

CREATE TABLE dropped_column_test(a int, b int, c text not null, d text not null);
INSERT INTO dropped_column_test VALUES(1, null, 'text_1', 'text_2');
ALTER TABLE dropped_column_test DROP column b;

SELECT citus_add_local_table_to_metadata('dropped_column_test');
SELECT create_reference_table('dropped_column_test');

-- check data on all placements
SELECT result FROM run_command_on_all_nodes(
    $$
    SELECT jsonb_agg(q.*) FROM (
        SELECT * FROM create_ref_dist_from_citus_local.dropped_column_test
    ) q
    $$
);

SET citus.shard_replication_factor TO 2;

CREATE TABLE replication_factor_test(a int);
SELECT citus_add_local_table_to_metadata('replication_factor_test');

SELECT create_distributed_table('replication_factor_test', null);

SET citus.shard_replication_factor TO 1;

-- cleanup at exit
DROP SCHEMA create_ref_dist_from_citus_local CASCADE;
