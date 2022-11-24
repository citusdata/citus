--
-- MULTI_ALTER_TABLE_ADD_CONSTRAINTS_WITHOUT_NAME
--
-- Test checks whether constraints of distributed tables can be adjusted using
-- the ALTER TABLE ... ADD without specifying a name.

SET citus.shard_count TO 32;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1450000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 1450000;

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
		WHERE rel.relname = 'products_1450000';

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
CREATE TABLE sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon (
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
SELECT Count(con.conname)
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
		WHERE rel.relname LIKE 'very%';

-- Constraint can be deleted via the coordinator
\c - - :master_host :master_port
ALTER TABLE sc1.verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglonglonglonglo_pkey;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname LIKE 'very%';

\c - - :master_host :master_port
DROP SCHEMA sc1 CASCADE;
