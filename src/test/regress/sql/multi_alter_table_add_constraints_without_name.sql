--
-- MULTI_ALTER_TABLE_ADD_CONSTRAINTS_WITHOUT_NAME
--
-- Test checks whether constraints of distributed tables can be adjusted using
-- the ALTER TABLE ... ADD without specifying a name.

SET citus.shard_count TO 32;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 5410000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 5410000;

CREATE SCHEMA sc1;

-- Check "ADD PRIMARY KEY"
CREATE TABLE sc1.products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('sc1.products', 'product_no');

ALTER TABLE sc1.products ADD PRIMARY KEY(product_no);
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
	      WHERE rel.relname = 'products';

-- Check that the primary key name created on the coordinator is sent to workers and
-- the constraints created for the shard tables conform to the <conname>_shardid scheme.
\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
		WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE sc1.products DROP CONSTRAINT products_pkey;

ALTER TABLE sc1.products ADD PRIMARY KEY(product_no);

DROP TABLE sc1.products;

-- Check "ADD PRIMARY KEY" with reference table
CREATE TABLE sc1.products_ref (
    product_no integer,
    name text,
    price numeric
);

CREATE TABLE sc1.products_ref_2 (
	    product_no integer,
	    name text,
	    price numeric
);

CREATE TABLE sc1.products_ref_3 (
            product_no integer,
            name text,
            price numeric
);

SELECT create_reference_table('sc1.products_ref');
SELECT create_reference_table('sc1.products_ref_3');

-- Check for name collisions
ALTER TABLE sc1.products_ref_3 ADD CONSTRAINT products_ref_pkey PRIMARY KEY(name);
ALTER TABLE sc1.products_ref_2 ADD CONSTRAINT products_ref_pkey1 PRIMARY KEY(name);
ALTER TABLE sc1.products_ref ADD PRIMARY KEY(name);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE sc1.products_ref DROP CONSTRAINT products_ref_pkey2;

DROP TABLE sc1.products_ref;

-- Check with max table name (63 chars)
CREATE TABLE sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglonger (
	            product_no integer,
	            name text,
                    price numeric
		);

SELECT create_distributed_table('sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon', 'product_no');

ALTER TABLE sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD PRIMARY KEY(product_no);

-- Constraint should be created on the coordinator with a shortened name
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'very%';

-- Constraints for the main table and the shards should be created on the worker with a shortened name
\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
		WHERE rel.relname LIKE 'very%' ORDER BY con.conname ASC;

-- Constraint can be deleted via the coordinator
\c - - :master_host :master_port
ALTER TABLE sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglonglonglonglo_pkey;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

-- Test the scenario where a partitioned distributed table has a child with max allowed name
-- Verify that we switch to sequential execution mode to avoid deadlock in this scenario
\c - - :master_host :master_port
CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
CREATE TABLE  p1 PARTITION OF dist_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE  longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc PARTITION OF dist_partitioned_table FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SELECT create_distributed_table('dist_partitioned_table', 'partition_col');

SET client_min_messages TO DEBUG1;
ALTER TABLE dist_partitioned_table ADD PRIMARY KEY(partition_col);
RESET client_min_messages;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'dist_partitioned_table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE dist_partitioned_table DROP CONSTRAINT dist_partitioned_table_pkey;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

-- Test we error out when creating a primary key on a partition table with a long name if we cannot
-- switch to sequential execution
\c - - :master_host :master_port
BEGIN;
	SELECT count(*) FROM dist_partitioned_table;
	ALTER TABLE dist_partitioned_table ADD PRIMARY KEY(partition_col);
ROLLBACK;
-- try inside a sequential block
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM dist_partitioned_table;
	ALTER TABLE dist_partitioned_table ADD PRIMARY KEY(partition_col);
	ROLLBACK;

-- Test primary key name is generated by postgres for citus local table.
\c - - :master_host :master_port
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE citus_local_table(id int, other_column int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

ALTER TABLE citus_local_table ADD PRIMARY KEY(id);

-- Check the primary key is created for the local table and its shard.
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

SELECT create_distributed_table('citus_local_table','id');

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
DROP TABLE citus_local_table;
SELECT 1 FROM master_remove_node('localhost', :master_port);

-- Test with unusual table and column names
CREATE TABLE "2nd table" ( "2nd id" INTEGER, "3rd id" INTEGER);
SELECT create_distributed_table('"2nd table"','2nd id');

ALTER TABLE  "2nd table" ADD PRIMARY KEY ("2nd id", "3rd id");
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = '2nd table';

-- Check if a primary key constraint is created for the shard tables on the workers
\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE '2nd table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE  "2nd table" DROP CONSTRAINT "2nd table_pkey";

DROP SCHEMA sc1 CASCADE;
