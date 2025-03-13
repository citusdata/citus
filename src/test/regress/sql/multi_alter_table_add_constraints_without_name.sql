--
-- MULTI_ALTER_TABLE_ADD_CONSTRAINTS_WITHOUT_NAME
--
-- Test checks whether constraints of distributed tables can be adjusted using
-- the ALTER TABLE ... ADD without specifying a name.

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 5410000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 5410000;

CREATE SCHEMA AT_AddConstNoName;

-- Check "ADD PRIMARY KEY"
CREATE TABLE AT_AddConstNoName.products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('AT_AddConstNoName.products', 'product_no');

ALTER TABLE AT_AddConstNoName.products ADD PRIMARY KEY(product_no);
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
	      WHERE rel.relname = 'products';

-- Check that the primary key name created on the coordinator is sent to workers and
-- the constraints created for the shard tables conform to the <conname>_shardid naming scheme.
\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
		WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_pkey;
-- Check "ADD PRIMARY KEY USING INDEX ..."

CREATE TABLE  AT_AddConstNoName.tbl(col1 int, col2 int);
SELECT create_distributed_table('AT_AddConstNoName.tbl', 'col1');
CREATE UNIQUE INDEX my_index ON AT_AddConstNoName.tbl(col1);
ALTER TABLE AT_AddConstNoName.tbl ADD PRIMARY KEY USING INDEX my_index;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'tbl';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
                WHERE rel.relname LIKE 'tbl%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.tbl DROP CONSTRAINT my_index;

-- Check "ADD UNIQUE USING INDEX ..."
CREATE UNIQUE INDEX my_index ON AT_AddConstNoName.tbl(col1);
ALTER TABLE AT_AddConstNoName.tbl ADD UNIQUE USING INDEX my_index;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'tbl';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
                WHERE rel.relname LIKE 'tbl%'ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.tbl DROP CONSTRAINT my_index;

-- Check "ADD PRIMARY KEY DEFERRABLE"
ALTER TABLE AT_AddConstNoName.products ADD PRIMARY KEY(product_no) DEFERRABLE;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
	FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_pkey;

ALTER TABLE AT_AddConstNoName.products ADD PRIMARY KEY(product_no) DEFERRABLE INITIALLY DEFERRED;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
        FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	        WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_pkey;

-- Check "ADD UNIQUE"
ALTER TABLE AT_AddConstNoName.products ADD UNIQUE(product_no);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

-- Check that UNIQUE constraint name created on the coordinator is sent to workers and
-- the constraints created for the shard tables conform to the <conname>_shardid scheme.
\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
                WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_key;

-- Check "ADD UNIQUE" with column name list
ALTER TABLE AT_AddConstNoName.products ADD UNIQUE(product_no,name);
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
                WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_name_key;

-- Check "ADD UNIQUE ... INCLUDE"
ALTER TABLE AT_AddConstNoName.products ADD UNIQUE(product_no) INCLUDE(price);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_key;


-- Check "ADD UNIQUE NULLS NOT DISTICT"
ALTER TABLE AT_AddConstNoName.products ADD UNIQUE NULLS NOT DISTINCT (product_no, price);
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_price_key;

-- Check "ADD UNIQUE ... DEFERRABLE"
ALTER TABLE AT_AddConstNoName.products ADD UNIQUE(product_no) INCLUDE(price) DEFERRABLE;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
        FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	        WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_key;

ALTER TABLE AT_AddConstNoName.products ADD UNIQUE(product_no) INCLUDE(price) DEFERRABLE INITIALLY DEFERRED;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
        FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	        WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port

ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_product_no_key;

-- Check "ADD EXCLUDE"
CREATE EXTENSION btree_gist;
ALTER TABLE AT_AddConstNoName.products ADD EXCLUDE USING gist (name WITH <> , product_no WITH =);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_name_product_no_excl;

-- Check "ADD EXCLUDE ... DEFERRABLE"
ALTER TABLE AT_AddConstNoName.products ADD EXCLUDE USING gist (name WITH <> , product_no WITH =) DEFERRABLE;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
        FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	                WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_name_product_no_excl;

ALTER TABLE AT_AddConstNoName.products ADD EXCLUDE USING gist (name WITH <> , product_no WITH =) DEFERRABLE INITIALLY DEFERRED;
\c - - :public_worker_1_host :worker_1_port
SELECT conname, tgfoid::regproc, tgtype, tgdeferrable, tginitdeferred
        FROM pg_trigger JOIN pg_constraint con ON con.oid = tgconstraint
	                WHERE tgrelid = 'AT_AddConstNoName.products_5410000'::regclass;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_name_product_no_excl;

-- Check "ADD CHECK"
ALTER TABLE AT_AddConstNoName.products ADD CHECK (product_no > 0 AND price > 0);
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_check;

-- Check "ADD CHECK ... NOINHERIT"
ALTER TABLE AT_AddConstNoName.products ADD CHECK (product_no > 0 AND price > 0) NO INHERIT;

SELECT con.conname, con.connoinherit
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.connoinherit
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_check;

-- Check "ADD CHECK ... NOT VALID"
ALTER TABLE AT_AddConstNoName.products ADD CHECK (product_no > 0 AND price > 0) NOT VALID;

SELECT con.conname, con.convalidated
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.convalidated
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_5410000';

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.products DROP CONSTRAINT products_check;

DROP TABLE AT_AddConstNoName.products;

-- Check "ADD PRIMARY KEY" with reference table
CREATE TABLE AT_AddConstNoName.products_ref (
    product_no integer,
    name text,
    price numeric
);

CREATE TABLE AT_AddConstNoName.products_ref_2 (
	    product_no integer,
	    name text,
	    price numeric
);

CREATE TABLE AT_AddConstNoName.products_ref_3 (
            product_no integer,
            name text,
            price numeric
);

SELECT create_reference_table('AT_AddConstNoName.products_ref');
SELECT create_reference_table('AT_AddConstNoName.products_ref_3');

-- Check that name collisions are handled for PRIMARY KEY.
ALTER TABLE AT_AddConstNoName.products_ref_3 ADD CONSTRAINT products_ref_pkey PRIMARY KEY(name);
ALTER TABLE AT_AddConstNoName.products_ref_2 ADD CONSTRAINT products_ref_pkey1 PRIMARY KEY(name);
ALTER TABLE AT_AddConstNoName.products_ref ADD PRIMARY KEY(name);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE AT_AddConstNoName.products_ref DROP CONSTRAINT products_ref_pkey2;

-- Check that name collisions are handled for UNIQUE.
ALTER TABLE AT_AddConstNoName.products_ref_3 ADD CONSTRAINT products_ref_name_key UNIQUE(name);
ALTER TABLE AT_AddConstNoName.products_ref_2 ADD CONSTRAINT products_ref_name_key1 UNIQUE(name);
ALTER TABLE AT_AddConstNoName.products_ref ADD UNIQUE(name);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE AT_AddConstNoName.products_ref DROP CONSTRAINT products_ref_name_key2;

-- Check that name collisions are handled for EXCLUDE
ALTER TABLE AT_AddConstNoName.products_ref_3 ADD CONSTRAINT products_ref_product_no_excl  EXCLUDE (product_no WITH =);
ALTER TABLE AT_AddConstNoName.products_ref_2 ADD CONSTRAINT products_ref_product_no_excl1 EXCLUDE (product_no WITH =);
ALTER TABLE AT_AddConstNoName.products_ref ADD EXCLUDE(product_no WITH =);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE AT_AddConstNoName.products_ref DROP CONSTRAINT products_ref_product_no_excl2;

-- Check that name collisions are handled for CHECK
ALTER TABLE AT_AddConstNoName.products_ref_3 ADD CONSTRAINT products_ref_check  CHECK (product_no > 0);
ALTER TABLE AT_AddConstNoName.products_ref_2 ADD CONSTRAINT products_ref_check1  CHECK (product_no > 0);
ALTER TABLE AT_AddConstNoName.products_ref ADD CHECK (product_no > 0);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE AT_AddConstNoName.products_ref DROP CONSTRAINT products_ref_check2;

DROP TABLE AT_AddConstNoName.products_ref;

-- Check "ADD PRIMARY KEY" with max table name (63 chars)
CREATE TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglonger (
	            product_no integer,
	            name text,
                    price numeric
		);

SELECT create_distributed_table('AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon', 'product_no');

ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD PRIMARY KEY(product_no);

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
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglonglonglonglo_pkey;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

-- Check "ADD UNIQUE" with max table name (63 chars)
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD UNIQUE(product_no);

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

-- UNIQUE constraint can be deleted via the coordinator
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglong_product_no_key;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

-- Check "ADD EXCLUDE" with max table name (63 chars)
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD EXCLUDE (product_no WITH =);

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

-- EXCLUDE constraint can be deleted via the coordinator
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglon_product_no_excl;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

-- Check "ADD CHECK" with max table name (63 chars)
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD CHECK (product_no > 0);
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

-- CHECK constraint can be deleted via the coordinator
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglonglonglongl_check;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

-- Test the scenario where a partitioned distributed table has a child with max allowed name
-- Verify that we switch to sequential execution mode to avoid deadlock in this scenario
\c - - :master_host :master_port
CREATE TABLE AT_AddConstNoName.dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
CREATE TABLE AT_AddConstNoName.p1 PARTITION OF AT_AddConstNoName.dist_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE AT_AddConstNoName.longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc PARTITION OF AT_AddConstNoName.dist_partitioned_table FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SELECT create_distributed_table('AT_AddConstNoName.dist_partitioned_table', 'partition_col');

-- Check "ADD PRIMARY KEY"
SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD PRIMARY KEY(partition_col);
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
ALTER TABLE AT_AddConstNoName.dist_partitioned_table DROP CONSTRAINT dist_partitioned_table_pkey;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

-- Check "ADD UNIQUE"
\c - - :master_host :master_port
SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD UNIQUE(partition_col);
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
ALTER TABLE AT_AddConstNoName.dist_partitioned_table DROP CONSTRAINT  dist_partitioned_table_partition_col_key;

-- Check "ADD CHECK"
SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD CHECK(dist_col >= another_col);
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
ALTER TABLE AT_AddConstNoName.dist_partitioned_table DROP CONSTRAINT  dist_partitioned_table_check;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
           WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

-- Test we error out when creating a constraint on a partition table with a long name if we cannot
-- switch to sequential execution

-- Check "ADD PRIMARY KEY"
\c - - :master_host :master_port
BEGIN;
	SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
	ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD PRIMARY KEY(partition_col);
ROLLBACK;
-- try inside a sequential block
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
	ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD PRIMARY KEY(partition_col);
	ROLLBACK;

-- Check "ADD UNIQUE"
\c - - :master_host :master_port
BEGIN;
	SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
	ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD UNIQUE(partition_col);
ROLLBACK;
-- try inside a sequential block
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
	ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD UNIQUE(partition_col);
	ROLLBACK;

-- Check "ADD CHECK"
\c - - :master_host :master_port
BEGIN;
	SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
	ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD CHECK(dist_col > another_col);
	ROLLBACK;
-- try inside a sequential block
BEGIN;
        SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
        SELECT count(*) FROM AT_AddConstNoName.dist_partitioned_table;
        ALTER TABLE AT_AddConstNoName.dist_partitioned_table ADD CHECK(dist_col > another_col);
        ROLLBACK;

DROP TABLE AT_AddConstNoName.dist_partitioned_table;

-- Test with Citus Local Tables

-- Test "ADD PRIMARY KEY"
\c - - :master_host :master_port
CREATE TABLE AT_AddConstNoName.citus_local_table(id int, other_column int);
SELECT citus_add_local_table_to_metadata('AT_AddConstNoName.citus_local_table');

ALTER TABLE AT_AddConstNoName.citus_local_table ADD PRIMARY KEY(id);

-- Check the primary key is created for the local table and its shard
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

SELECT create_distributed_table('AT_AddConstNoName.citus_local_table','id');

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
ALTER TABLE AT_AddConstNoName.citus_local_table DROP CONSTRAINT citus_local_table_pkey;

-- Check "ADD UNIQUE"
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_table ADD UNIQUE(id);

-- Check the UNIQUE constraint is created for the local table and its shard
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
ALTER TABLE AT_AddConstNoName.citus_local_table DROP CONSTRAINT citus_local_table_id_key;

-- Check "ADD EXCLUDE"
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_table ADD EXCLUDE(id WITH =);

-- Check the EXCLUDE constraint is created for the local table and its shard
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

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
ALTER TABLE AT_AddConstNoName.citus_local_table DROP CONSTRAINT citus_local_table_id_excl;

-- Check "ADD CHECK"
\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_table ADD CHECK(id > 100);

-- Check the CHECK constraint is created for the local table and its shard
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'citus_local_table%' ORDER BY con.conname ASC;

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
ALTER TABLE AT_AddConstNoName.citus_local_table DROP CONSTRAINT citus_local_table_check;

DROP TABLE AT_AddConstNoName.citus_local_table;

-- Test with partitioned citus local table
CREATE TABLE AT_AddConstNoName.citus_local_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
CREATE TABLE AT_AddConstNoName.p1 PARTITION OF AT_AddConstNoName.citus_local_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE AT_AddConstNoName.longlonglonglonglonglonglonglonglonglonglonglonglonglonglongabc PARTITION OF AT_AddConstNoName.citus_local_partitioned_table FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SELECT citus_add_local_table_to_metadata('AT_AddConstNoName.citus_local_partitioned_table');

-- Check "ADD PRIMARY KEY"
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table ADD PRIMARY KEY(partition_col);

SELECT create_distributed_table('AT_AddConstNoName.citus_local_partitioned_table', 'partition_col');

ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table DROP CONSTRAINT citus_local_partitioned_table_pkey;

SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table ADD PRIMARY KEY(partition_col);
RESET client_min_messages;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'citus_local_partitioned_table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table DROP CONSTRAINT citus_local_partitioned_table_pkey;

-- Check "ADD UNIQUE"
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table ADD UNIQUE(partition_col);

ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table DROP CONSTRAINT citus_local_partitioned_table_partition_col_key;

SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table ADD UNIQUE(partition_col);
RESET client_min_messages;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'citus_local_partitioned_table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table DROP CONSTRAINT citus_local_partitioned_table_partition_col_key;

-- Check "ADD CHECK"
SET client_min_messages TO DEBUG1;
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table ADD CHECK (dist_col > 0);
RESET client_min_messages;

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'citus_local_partitioned_table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE AT_AddConstNoName.citus_local_partitioned_table DROP CONSTRAINT citus_local_partitioned_table_check;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'longlonglonglonglonglonglonglonglong%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
-- Test with unusual table and column names
CREATE TABLE AT_AddConstNoName."2nd table" ( "2nd id" INTEGER, "3rd id" INTEGER);
SELECT create_distributed_table('AT_AddConstNoName."2nd table"','2nd id');

-- Check "ADD PRIMARY KEY"
ALTER TABLE  AT_AddConstNoName."2nd table" ADD PRIMARY KEY ("2nd id", "3rd id");
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
ALTER TABLE  AT_AddConstNoName."2nd table" DROP CONSTRAINT "2nd table_pkey";

-- Check "ADD UNIQUE"
\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" ADD UNIQUE ("2nd id", "3rd id");

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = '2nd table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE '2nd table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" DROP CONSTRAINT "2nd table_2nd id_3rd id_key";

-- Check "ADD EXCLUDE"
\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" ADD EXCLUDE ("2nd id" WITH =);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = '2nd table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE '2nd table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" DROP CONSTRAINT "2nd table_2nd id_excl";

-- Check "ADD CHECK"
\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" ADD CHECK ("2nd id" > 0 );

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = '2nd table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE '2nd table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
ALTER TABLE  AT_AddConstNoName."2nd table" DROP CONSTRAINT "2nd table_check";

DROP EXTENSION btree_gist;
DROP SCHEMA AT_AddConstNoName CASCADE;
