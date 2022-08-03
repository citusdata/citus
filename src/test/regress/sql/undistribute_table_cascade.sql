\set VERBOSITY terse

SET citus.next_shard_id TO 1515000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA undistribute_table_cascade;
SET search_path TO undistribute_table_cascade;

SET client_min_messages to ERROR;

-- remove coordinator if it is added to pg_dist_node
SELECT COUNT(master_remove_node(nodename, nodeport)) < 2
FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:master_port;

BEGIN;
  CREATE TABLE reference_table(col_1 INT UNIQUE);
  CREATE TABLE distributed_table(col_1 INT UNIQUE);
  CREATE TABLE local_table (col_1 INT REFERENCES reference_table(col_1), FOREIGN KEY (col_1) REFERENCES distributed_table(col_1));

  SELECT create_reference_table('reference_table');
  SELECT create_distributed_table('distributed_table', 'col_1');

  -- show that we skip postgres tables when undistributing citus tables
  SELECT undistribute_table('reference_table', cascade_via_foreign_keys=>true);
ROLLBACK;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

CREATE TABLE reference_table_1 (col_1 INT UNIQUE, col_2 INT UNIQUE, UNIQUE (col_2, col_1));
CREATE TABLE reference_table_2 (col_1 INT UNIQUE, col_2 INT UNIQUE);
SELECT create_reference_table('reference_table_1');
SELECT create_reference_table('reference_table_2');

CREATE TABLE distributed_table_1 (col_1 INT UNIQUE);
CREATE TABLE distributed_table_2 (col_1 INT UNIQUE);
CREATE TABLE distributed_table_3 (col_1 INT UNIQUE);
SELECT create_distributed_table('distributed_table_1', 'col_1');
SELECT create_distributed_table('distributed_table_2', 'col_1');
SELECT create_distributed_table('distributed_table_3', 'col_1');

CREATE TABLE citus_local_table_1 (col_1 INT UNIQUE);
CREATE TABLE citus_local_table_2 (col_1 INT UNIQUE);
SELECT citus_add_local_table_to_metadata('citus_local_table_1');
SELECT citus_add_local_table_to_metadata('citus_local_table_2');

--        ---                                            ---
--        | |                                            | |
--        | v                                            | v
-- distributed_table_2 -> distributed_table_1 -> reference_table_1 <- reference_table_2
--         ^                        |                     ^                    |
--         v                        |                     |                    v
-- distributed_table_3  <------------            citus_local_table_1  citus_local_table_2
--
ALTER TABLE distributed_table_3 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES distributed_table_2(col_1);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_1) REFERENCES distributed_table_3(col_1);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES distributed_table_1(col_1);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1, col_2) REFERENCES reference_table_1(col_2, col_1);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_6 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_7 FOREIGN KEY (col_1) REFERENCES citus_local_table_2(col_1);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_8 FOREIGN KEY (col_1) REFERENCES distributed_table_3(col_1);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_11 FOREIGN KEY (col_1) REFERENCES distributed_table_2(col_1);
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_12 FOREIGN KEY (col_2) REFERENCES reference_table_1(col_1);

-- show that all of below fails as we didn't provide cascade=true
SELECT undistribute_table('distributed_table_1');
SELECT undistribute_table('citus_local_table_1', cascade_via_foreign_keys=>false);
SELECT undistribute_table('reference_table_2');

-- In each of below transation blocks, show that we preserve foreign keys.
-- Also show that we don't have any citus tables in current schema after
-- undistribute_table(cascade).
-- So in each transaction, both selects should return true.

BEGIN;
  SELECT undistribute_table('distributed_table_2', cascade_via_foreign_keys=>true);

  -- show that we switch to sequential execution as there are
  -- reference tables in our subgraph
  show citus.multi_shard_modify_mode;

  SELECT COUNT(*)=10 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='undistribute_table_cascade') AND
        conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT undistribute_table('reference_table_1', cascade_via_foreign_keys=>true);

  SELECT COUNT(*)=10 FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='undistribute_table_cascade') AND
        conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT undistribute_table('citus_local_table_1', cascade_via_foreign_keys=>true);

  -- print foreign keys only in one of xact blocks not to make tests too verbose
  SELECT conname, conrelid::regclass, confrelid::regclass
  FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='undistribute_table_cascade') AND
        conname ~ '^fkey\_\d+$'
  ORDER BY conname;

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT COUNT(*) FROM distributed_table_1;
  -- show that we error out as select is executed in parallel mode
  -- and there are reference tables in our subgraph
  SELECT undistribute_table('reference_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  SELECT COUNT(*) FROM distributed_table_1;
  -- even if there are reference tables in our subgraph, show that
  -- we don't error out as we already switched to sequential execution
  SELECT undistribute_table('reference_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

ALTER TABLE distributed_table_1 DROP CONSTRAINT fkey_4;

BEGIN;
  SELECT undistribute_table('distributed_table_2', cascade_via_foreign_keys=>true);

  -- as we splitted distributed_table_1,2 & 3 into a seperate subgraph
  -- by dropping reference_table_1, we should not switch to sequential
  -- execution
  show citus.multi_shard_modify_mode;
ROLLBACK;

-- split distributed_table_2 & distributed_table_3 into a seperate foreign
-- key subgraph then undistribute them
ALTER TABLE distributed_table_2 DROP CONSTRAINT fkey_3;
ALTER TABLE distributed_table_1 DROP CONSTRAINT fkey_8;
SELECT undistribute_table('distributed_table_2', cascade_via_foreign_keys=>true);

-- should return true as we undistributed those two tables
SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='undistribute_table_cascade' AND
      (tablename='distributed_table_2' OR tablename='distributed_table_3');

-- other tables should stay as is since we splited those two tables
SELECT COUNT(*)=5 FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='undistribute_table_cascade';

-- test partitioned tables
CREATE TABLE partitioned_table_1 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_1_100_200 PARTITION OF partitioned_table_1 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_1_200_300 PARTITION OF partitioned_table_1 FOR VALUES FROM (200) TO (300);
SELECT create_distributed_table('partitioned_table_1', 'col_1');

CREATE TABLE partitioned_table_2 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_2_100_200 PARTITION OF partitioned_table_2 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_2_200_300 PARTITION OF partitioned_table_2 FOR VALUES FROM (200) TO (300);
SELECT create_distributed_table('partitioned_table_2', 'col_1');

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT fkey FOREIGN KEY(col_1) REFERENCES partitioned_table_1_100_200(col_1);
  SELECT undistribute_table('partitioned_table_1', true);
ROLLBACK;

CREATE TABLE distributed_table_4 (col_1 INT UNIQUE, col_2 INT);
SELECT create_distributed_table('distributed_table_4', 'col_1');

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE distributed_table_4 ADD CONSTRAINT fkey FOREIGN KEY(col_1) REFERENCES partitioned_table_1_100_200(col_1);
  SELECT undistribute_table('partitioned_table_1', true);
ROLLBACK;

CREATE TABLE reference_table_3 (col_1 INT UNIQUE, col_2 INT UNIQUE);
SELECT create_reference_table('reference_table_3');

ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_9 FOREIGN KEY (col_1) REFERENCES reference_table_3(col_2);
ALTER TABLE partitioned_table_2 ADD CONSTRAINT fkey_10 FOREIGN KEY (col_1) REFERENCES reference_table_3(col_2);

-- show that we properly handle cases where undistribute_table is not supported
-- error out when table is a partition table
SELECT undistribute_table('partitioned_table_2_100_200', cascade_via_foreign_keys=>true);
-- error if table does not exist
SELECT undistribute_table('non_existent_table', cascade_via_foreign_keys=>true);
-- error if table is a postgres table
CREATE TABLE local_table(a int);
SELECT undistribute_table('local_table', cascade_via_foreign_keys=>true);

ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_15 FOREIGN KEY (col_1) REFERENCES partitioned_table_1(col_1);

BEGIN;
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);

  -- show that we preserve foreign keys on partitions too
  SELECT conname, conrelid::regclass, confrelid::regclass
  FROM pg_constraint
  WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname='undistribute_table_cascade') AND
        conname = 'fkey_9' OR conname = 'fkey_10'
  ORDER BY 1,2,3;
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY(col_1) REFERENCES reference_table_3(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY(col_1) REFERENCES reference_table_3(col_1);
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY(col_1) REFERENCES reference_table_3(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY(col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY(col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE distributed_table_4 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('distributed_table_4', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE distributed_table_4 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_1_100_200(col_1);
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE distributed_table_4 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_1_100_200(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT fkey FOREIGN KEY(col_1) REFERENCES distributed_table_4(col_1);
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT fkey FOREIGN KEY(col_1) REFERENCES distributed_table_4(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_1_100_200 ADD CONSTRAINT fkey FOREIGN KEY(col_1) REFERENCES distributed_table_4(col_1);
  SELECT undistribute_table('distributed_table_4', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_2 ADD CONSTRAINT fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_1_100_200(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_2_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('reference_table_3', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_2_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

BEGIN;
  set citus.multi_shard_modify_mode to 'sequential';
  ALTER TABLE partitioned_table_2_100_200 ADD CONSTRAINT non_inherited_fkey FOREIGN KEY (col_1) REFERENCES partitioned_table_2_100_200(col_1);
  SELECT undistribute_table('partitioned_table_1', cascade_via_foreign_keys=>true);
ROLLBACK;

ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_13 FOREIGN KEY (col_1) REFERENCES partitioned_table_2(col_1);

BEGIN;
  -- For pg versions 11, 12 & 13, partitioned_table_1 references to reference_table_3
  -- and partitioned_table_2 references to reference_table_3.
  -- For pg versions > 11, partitioned_table_1 references to partitioned_table_2 as well.
  -- Anyway show that undistribute_table with cascade is fine.
  SELECT undistribute_table('partitioned_table_2', cascade_via_foreign_keys=>true);
ROLLBACK;

-- now merge partitioned_table_1, 2 and reference_table_3 into right
-- hand-side of the graph
ALTER TABLE reference_table_3 ADD CONSTRAINT fkey_14 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);

BEGIN;
  SELECT undistribute_table('citus_local_table_1', cascade_via_foreign_keys=>true);

  -- undistributing citus_local_table_1 cascades to partitioned tables too
  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade' AND
        tablename LIKE 'partitioned_table_%';
ROLLBACK;

BEGIN;
  ALTER TABLE reference_table_2 DROP CONSTRAINT fkey_5;
  ALTER TABLE reference_table_2 DROP CONSTRAINT fkey_7;

  -- since now reference_table_2 has no foreign keys, show that
  -- cascade_via_foreign_keys option still works fine
  SELECT undistribute_table('reference_table_2', cascade_via_foreign_keys=>true);

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade' AND
        tablename='reference_table_2';
ROLLBACK;

CREATE SCHEMA "bad!schemaName";

CREATE TABLE "bad!schemaName"."LocalTabLE.1!?!"(col_1 INT UNIQUE);
CREATE TABLE "bad!schemaName"."LocalTabLE.2!?!"(col_1 INT UNIQUE);

SELECT citus_add_local_table_to_metadata('"bad!schemaName"."LocalTabLE.1!?!"');
SELECT citus_add_local_table_to_metadata('"bad!schemaName"."LocalTabLE.2!?!"');

ALTER TABLE "bad!schemaName"."LocalTabLE.1!?!" ADD CONSTRAINT "bad!constraintName" FOREIGN KEY (col_1) REFERENCES "bad!schemaName"."LocalTabLE.2!?!"(col_1);

-- test with weird schema, table & constraint names
SELECT undistribute_table('"bad!schemaName"."LocalTabLE.1!?!"', cascade_via_foreign_keys=>true);

-- cleanup at exit
DROP SCHEMA undistribute_table_cascade, "bad!schemaName" CASCADE;
