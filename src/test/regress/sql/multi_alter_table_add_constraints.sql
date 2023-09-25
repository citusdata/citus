--
-- MULTI_ALTER_TABLE_ADD_CONSTRAINTS
--
-- Test checks whether constraints of distributed tables can be adjusted using
-- the ALTER TABLE ... ADD CONSTRAINT ... command.

SET citus.shard_count TO 32;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1450000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 1450000;

-- Check "PRIMARY KEY CONSTRAINT"
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products', 'product_no');

-- Can only add primary key constraint on distribution column (or group of columns
-- including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(name);
ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(product_no);

INSERT INTO products VALUES(1, 'product_1', 1);

-- Should error out, since we are trying to add a new row having a value on p_key column
-- conflicting with the existing row.
INSERT INTO products VALUES(1, 'product_1', 1);

ALTER TABLE products DROP CONSTRAINT p_key;
INSERT INTO products VALUES(1, 'product_1', 1);

-- Can not create constraint since it conflicts with the existing data
ALTER TABLE products ADD CONSTRAINT p_key PRIMARY KEY(product_no);

DROP TABLE products;

-- Check "PRIMARY KEY CONSTRAINT" with reference table
CREATE TABLE products_ref (
    product_no integer,
    name text,
    price numeric
);

SELECT create_reference_table('products_ref');

-- Can add PRIMARY KEY to any column
ALTER TABLE products_ref ADD CONSTRAINT p_key PRIMARY KEY(name);
ALTER TABLE products_ref DROP CONSTRAINT p_key;
ALTER TABLE products_ref ADD CONSTRAINT p_key PRIMARY KEY(product_no);

INSERT INTO products_ref VALUES(1, 'product_1', 1);

-- Should error out, since we are trying to add new row having a value on p_key column
-- conflicting with the existing row.
INSERT INTO products_ref VALUES(1, 'product_1', 1);

DROP TABLE products_ref;

-- Check "PRIMARY KEY CONSTRAINT" on append table
CREATE TABLE products_append (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products_append', 'product_no', 'append');
SELECT master_create_empty_shard('products_append') AS shardid \gset

-- Can only add primary key constraint on distribution column (or group
-- of columns including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE products_append ADD CONSTRAINT p_key_name PRIMARY KEY(name);
ALTER TABLE products_append ADD CONSTRAINT p_key PRIMARY KEY(product_no);

--- Error out since first and third rows have the same product_no
COPY products_append FROM STDIN WITH (DELIMITER ',', append_to_shard :shardid);
1, Product_1, 10
2, Product_2, 15
1, Product_3, 8
\.

DROP TABLE products_append;


-- Check "UNIQUE CONSTRAINT"
CREATE TABLE unique_test_table(id int, name varchar(20));
SELECT create_distributed_table('unique_test_table', 'id');

-- Can only add unique constraint on distribution column (or group
-- of columns including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE unique_test_table ADD CONSTRAINT unn_name UNIQUE(name);
ALTER TABLE unique_test_table ADD CONSTRAINT unn_id UNIQUE(id);

-- Error out, since table can not have two rows with same id.
INSERT INTO unique_test_table VALUES(1, 'Ahmet');
INSERT INTO unique_test_table VALUES(1, 'Mehmet');

ALTER TABLE unique_test_table DROP CONSTRAINT unn_id;

-- Insert row which will conflict with the next unique constraint command
INSERT INTO unique_test_table VALUES(1, 'Mehmet');

-- Can not create constraint since it conflicts with the existing data
ALTER TABLE unique_test_table ADD CONSTRAINT unn_id UNIQUE(id);

-- Can create unique constraint over multiple columns which must include
-- distribution column
ALTER TABLE unique_test_table ADD CONSTRAINT unn_id_name UNIQUE(id, name);

-- Error out, since tables can not have two rows with same id and name.
INSERT INTO unique_test_table VALUES(1, 'Mehmet');
DROP TABLE unique_test_table;

-- Check "UNIQUE CONSTRAINT" with reference table
CREATE TABLE unique_test_table_ref(id int, name varchar(20));
SELECT create_reference_table('unique_test_table_ref');

-- We can add unique constraint on any column with reference tables
ALTER TABLE unique_test_table_ref ADD CONSTRAINT unn_name UNIQUE(name);
ALTER TABLE unique_test_table_ref ADD CONSTRAINT unn_id UNIQUE(id);

-- Error out. Since the table can not have two rows with the same id.
INSERT INTO unique_test_table_ref VALUES(1, 'Ahmet');
INSERT INTO unique_test_table_ref VALUES(1, 'Mehmet');

-- We can add unique constraint with multiple columns
ALTER TABLE unique_test_table_ref DROP CONSTRAINT unn_id;
ALTER TABLE unique_test_table_ref ADD CONSTRAINT unn_id_name UNIQUE(id,name);

-- Error out, since two rows can not have the same id or name.
INSERT INTO unique_test_table_ref VALUES(1, 'Mehmet');

DROP TABLE unique_test_table_ref;

-- Check "UNIQUE CONSTRAINT" with append table
CREATE TABLE unique_test_table_append(id int, name varchar(20));
SELECT create_distributed_table('unique_test_table_append', 'id', 'append');
SELECT master_create_empty_shard('unique_test_table_append') AS shardid \gset

-- Can only add unique constraint on distribution column (or group
-- of columns including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE unique_test_table_append ADD CONSTRAINT unn_name UNIQUE(name);
ALTER TABLE unique_test_table_append ADD CONSTRAINT unn_id UNIQUE(id);

-- Error out. Table can not have two rows with the same id.
COPY unique_test_table_append FROM STDIN WITH (DELIMITER ',', append_to_shard :shardid);
1, Product_1
2, Product_2
1, Product_3
\.

DROP TABLE unique_test_table_append;

-- Check "CHECK CONSTRAINT"
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric,
    discounted_price numeric
);

SELECT create_distributed_table('products', 'product_no');

-- Can add column and table check constraints
ALTER TABLE products ADD CONSTRAINT p_check CHECK(price > 0);
ALTER TABLE products ADD CONSTRAINT p_multi_check CHECK(price > discounted_price);

-- First and third queries will error out, because of conflicts with p_check and
-- p_multi_check, respectively.
INSERT INTO products VALUES(1, 'product_1', -1, -2);
INSERT INTO products VALUES(1, 'product_1', 5, 3);
INSERT INTO products VALUES(1, 'product_1', 2, 3);

DROP TABLE products;

-- Check "CHECK CONSTRAINT" with reference table
CREATE TABLE products_ref (
    product_no integer,
    name text,
    price numeric,
    discounted_price numeric
);

SELECT create_reference_table('products_ref');

-- Can add column and table check constraints
ALTER TABLE products_ref ADD CONSTRAINT p_check CHECK(price > 0);
ALTER TABLE products_ref ADD CONSTRAINT p_multi_check CHECK(price > discounted_price);

-- First and third queries will error out, because of conflicts with p_check and
-- p_multi_check, respectively.
INSERT INTO products_ref VALUES(1, 'product_1', -1, -2);
INSERT INTO products_ref VALUES(1, 'product_1', 5, 3);
INSERT INTO products_ref VALUES(1, 'product_1', 2, 3);

DROP TABLE products_ref;

-- Check "CHECK CONSTRAINT" with append table
CREATE TABLE products_append (
    product_no int,
    name varchar(20),
    price int,
    discounted_price int
);

SELECT create_distributed_table('products_append', 'product_no', 'append');
SELECT master_create_empty_shard('products_append') AS shardid \gset

-- Can add column and table check constraints
ALTER TABLE products_append ADD CONSTRAINT p_check CHECK(price > 0);
ALTER TABLE products_append ADD CONSTRAINT p_multi_check CHECK(price > discounted_price);

-- Error out,since the third row conflicting with the p_multi_check
COPY products_append FROM STDIN WITH (DELIMITER ',', append_to_shard :shardid);
1, Product_1, 10, 5
2, Product_2, 15, 8
1, Product_3, 8, 10
\.

DROP TABLE products_append;


-- Check "EXCLUSION CONSTRAINT"
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products', 'product_no');

-- Can only add exclusion constraint on distribution column (or group of columns
-- including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE products ADD CONSTRAINT exc_name EXCLUDE USING btree (name with =);

-- check that we can disable the constraint check for EXCLUDE
BEGIN;
SET LOCAL citus.allow_unsafe_constraints TO on;
ALTER TABLE products ADD CONSTRAINT exc_name EXCLUDE USING btree (name with =);
-- not enforced across shards
INSERT INTO products  VALUES (1,'boat',10.0);
INSERT INTO products  VALUES (2,'boat',11.0);
-- enforced within the shard
INSERT INTO products  VALUES (1,'boat',12.0);
ROLLBACK;

-- We can add composite exclusion
ALTER TABLE products ADD CONSTRAINT exc_pno_name EXCLUDE USING btree (product_no with =, name with =);

-- 4th command will error out since it conflicts with exc_pno_name constraint
INSERT INTO products VALUES(1,'product_1', 5);
INSERT INTO products VALUES(1,'product_2', 10);
INSERT INTO products VALUES(2,'product_2', 5);
INSERT INTO products VALUES(2,'product_2', 5);

DROP TABLE products;

-- Check "EXCLUSION CONSTRAINT" with reference table
CREATE TABLE products_ref (
    product_no integer,
    name text,
    price numeric
);

SELECT create_reference_table('products_ref');

-- We can add exclusion constraint on any column
ALTER TABLE products_ref ADD CONSTRAINT exc_name EXCLUDE USING btree (name with =);

-- We can add composite exclusion because none of pair of rows are conflicting
ALTER TABLE products_ref ADD CONSTRAINT exc_pno_name EXCLUDE USING btree (product_no with =, name with =);

-- Third insertion will error out, since it has the same name with second insertion
INSERT INTO products_ref VALUES(1,'product_1', 5);
INSERT INTO products_ref VALUES(1,'product_2', 10);
INSERT INTO products_ref VALUES(2,'product_2', 5);

DROP TABLE products_ref;

-- Check "EXCLUSION CONSTRAINT" with append table
CREATE TABLE products_append (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products_append', 'product_no','append');
SELECT master_create_empty_shard('products_append') AS shardid \gset

-- Can only add exclusion constraint on distribution column (or group of column
-- including distribution column)
-- Command below should error out since 'name' is not a distribution column
ALTER TABLE products_append ADD CONSTRAINT exc_name EXCLUDE USING btree (name with =);
ALTER TABLE products_append ADD CONSTRAINT exc_pno_name EXCLUDE USING btree (product_no with =, name with =);

-- Error out since first and third can not pass the exclusion check.
COPY products_append FROM STDIN WITH (DELIMITER ',', append_to_shard :shardid);
1, Product_1, 10
1, Product_2, 15
1, Product_1, 8
\.

DROP TABLE products_append;

-- Check "NOT NULL"
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products', 'product_no');

ALTER TABLE products ALTER COLUMN name SET NOT NULL;

-- Insertions will error out since both product_no and name can not have NULL value
INSERT INTO products VALUES(1,NULL,5);
INSERT INTO products VALUES(NULL,'product_1', 5);

DROP TABLE products;

-- Check "NOT NULL" with reference table
CREATE TABLE products_ref (
    product_no integer,
    name text,
    price numeric
);

SELECT create_reference_table('products_ref');

ALTER TABLE products_ref ALTER COLUMN name SET NOT NULL;

-- Insertions will error out since both product_no and name can not have NULL value
INSERT INTO products_ref VALUES(1,NULL,5);
INSERT INTO products_ref VALUES(NULL,'product_1', 5);

DROP TABLE products_ref;

-- Check "NOT NULL" with append table
CREATE TABLE products_append (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products_append', 'product_no', 'append');
SELECT master_create_empty_shard('products_append') AS shardid \gset

ALTER TABLE products_append ALTER COLUMN name SET NOT NULL;

-- Error out since name and product_no columns can not handle NULL value.
COPY products_append FROM STDIN WITH (DELIMITER ',', append_to_shard :shardid);
1, \N, 10
\N, Product_2, 15
1, Product_1, 8
\.

DROP TABLE products_append;


-- Tests for ADD CONSTRAINT is not only subcommand
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
);

SELECT create_distributed_table('products', 'product_no');

-- Should error out since add constraint is not the single subcommand
ALTER TABLE products ADD CONSTRAINT unn_1 UNIQUE(product_no, price), ADD CONSTRAINT unn_2 UNIQUE(product_no, name);

-- ... with names, we can add/drop the constraints just fine
ALTER TABLE products ADD CONSTRAINT nonzero_product_no CHECK(product_no <> 0);
ALTER TABLE products ADD CONSTRAINT uniq_product_no EXCLUDE USING btree (product_no with =);

ALTER TABLE products DROP CONSTRAINT nonzero_product_no;
ALTER TABLE products DROP CONSTRAINT uniq_product_no;

DROP TABLE products;


-- Tests with transactions
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric,
    discounted_price numeric
);

SELECT create_distributed_table('products', 'product_no');

BEGIN;
INSERT INTO products VALUES(1,'product_1', 5);

-- DDL should pick the right connections after a single INSERT
ALTER TABLE products ADD CONSTRAINT unn_pno UNIQUE(product_no);
ROLLBACK;

-- check that we can disable the constraint check for CREATE UNIQUE INDEX
BEGIN;
SET LOCAL citus.allow_unsafe_constraints TO on;
CREATE UNIQUE INDEX ON products (name, price);
-- not enforced across shards
INSERT INTO products  VALUES (1,'boat',10.0);
INSERT INTO products  VALUES (2,'boat',11.0);
-- enforced within the shard
INSERT INTO products  VALUES (1,'boat',10.0);
ROLLBACK;

-- check that we can disable the constraint check for CREATE UNIQUE INDEX CONCURRENTLY
SET citus.allow_unsafe_constraints TO on;
CREATE UNIQUE INDEX CONCURRENTLY product_idx ON products (name, price);
-- not enforced across shards
INSERT INTO products  VALUES (1,'boat',10.0);
INSERT INTO products  VALUES (2,'boat',11.0);
-- enforced within the shard
INSERT INTO products  VALUES (1,'boat',10.0);
DROP INDEX product_idx;
TRUNCATE products;
RESET citus.allow_unsafe_constraints;

-- check that we can disable the constraint check for ADD CONSTRAINT .. PRIMARY KEY
BEGIN;
SET LOCAL citus.allow_unsafe_constraints TO on;
ALTER TABLE products ADD CONSTRAINT products_pk PRIMARY KEY (name, price);
-- not enforced across shards
INSERT INTO products  VALUES (1,'boat',10.0);
INSERT INTO products  VALUES (2,'boat',11.0);
-- enforced within the shard
INSERT INTO products  VALUES (1,'boat',10.0);
ROLLBACK;

BEGIN;
-- Add constraints
ALTER TABLE products ADD CONSTRAINT unn_pno UNIQUE(product_no);
ALTER TABLE products ADD CONSTRAINT check_price CHECK(price > discounted_price);
ALTER TABLE products ALTER COLUMN product_no SET NOT NULL;
ALTER TABLE products ADD CONSTRAINT p_key_product PRIMARY KEY(product_no);

INSERT INTO products VALUES(1,'product_1', 10, 8);
ROLLBACK;

-- There should be no constraint on master and worker(s)
SELECT "Constraint", "Definition" FROM table_checks WHERE relid='products'::regclass;
\c - - :public_worker_1_host :worker_1_port

SELECT "Constraint", "Definition" FROM table_checks WHERE relid='public.products_1450202'::regclass;

\c - - :master_host :master_port

-- Tests to check the effect of rollback
BEGIN;
-- Add constraints (which will be rollbacked)
ALTER TABLE products ADD CONSTRAINT unn_pno UNIQUE(product_no);
ALTER TABLE products ADD CONSTRAINT check_price CHECK(price > discounted_price);
ALTER TABLE products ADD CONSTRAINT p_key_product PRIMARY KEY(product_no);
ROLLBACK;

-- There should be no constraint on master and worker(s)
SELECT "Constraint", "Definition" FROM table_checks WHERE relid='products'::regclass;

\c - - :public_worker_1_host :worker_1_port

SELECT "Constraint", "Definition" FROM table_checks WHERE relid='public.products_1450202'::regclass;

\c - - :master_host :master_port

DROP TABLE products;

SET citus.shard_count to 2;
-- Test if the ALTER TABLE %s ADD %s PRIMARY KEY %s works
CREATE SCHEMA sc1;
CREATE TABLE sc1.alter_add_prim_key(x int, y int);
CREATE UNIQUE INDEX CONCURRENTLY alter_pk_idx ON sc1.alter_add_prim_key(x);
ALTER TABLE sc1.alter_add_prim_key ADD CONSTRAINT alter_pk_idx PRIMARY KEY USING INDEX alter_pk_idx;
SELECT create_distributed_table('sc1.alter_add_prim_key', 'x');
SELECT (run_command_on_workers($$
    SELECT
        kc.constraint_name
    FROM
        information_schema.table_constraints tc join information_schema.key_column_usage kc on (kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name)
    WHERE
        kc.table_schema = 'sc1' and tc.constraint_type = 'PRIMARY KEY' and kc.table_name LIKE 'alter_add_prim_key_%'
    ORDER BY
    1
    LIMIT
        1;
    $$)).*
ORDER BY
    1,2,3,4;

CREATE SCHEMA sc2;
CREATE TABLE sc2.alter_add_prim_key(x int, y int);
SET search_path TO 'sc2';
SELECT create_distributed_table('alter_add_prim_key', 'x');
CREATE UNIQUE INDEX CONCURRENTLY alter_pk_idx ON alter_add_prim_key(x);
ALTER TABLE alter_add_prim_key ADD CONSTRAINT alter_pk_idx PRIMARY KEY USING INDEX alter_pk_idx;
SELECT (run_command_on_workers($$
    SELECT
        kc.constraint_name
    FROM
        information_schema.table_constraints tc join information_schema.key_column_usage kc on (kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name)
    WHERE
        kc.table_schema = 'sc2' and tc.constraint_type = 'PRIMARY KEY' and kc.table_name LIKE 'alter_add_prim_key_%'
    ORDER BY
    1
    LIMIT
        1;
    $$)).*
ORDER BY
    1,2,3,4;

-- We are running almost the same test with a slight change on the constraint name because if the constraint has a different name than the index, Postgres renames the index.
CREATE SCHEMA sc3;
CREATE TABLE sc3.alter_add_prim_key(x int);
INSERT INTO sc3.alter_add_prim_key(x) SELECT generate_series(1,100);
SET search_path TO 'sc3';
SELECT create_distributed_table('alter_add_prim_key', 'x');
CREATE UNIQUE INDEX CONCURRENTLY alter_pk_idx ON alter_add_prim_key(x);
ALTER TABLE alter_add_prim_key ADD CONSTRAINT a_constraint PRIMARY KEY USING INDEX alter_pk_idx;
SELECT (run_command_on_workers($$
    SELECT
        kc.constraint_name
    FROM
        information_schema.table_constraints tc join information_schema.key_column_usage kc on (kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name)
    WHERE
        kc.table_schema = 'sc3' and tc.constraint_type = 'PRIMARY KEY' and kc.table_name LIKE 'alter_add_prim_key_%'
    ORDER BY
    1
    LIMIT
        1;
    $$)).*
ORDER BY
    1,2,3,4;

ALTER TABLE alter_add_prim_key DROP CONSTRAINT a_constraint;
SELECT (run_command_on_workers($$
    SELECT
        kc.constraint_name
    FROM
        information_schema.table_constraints tc join information_schema.key_column_usage kc on (kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name)
    WHERE
        kc.table_schema = 'sc3' and tc.constraint_type = 'PRIMARY KEY' and kc.table_name LIKE 'alter_add_prim_key_%'
    ORDER BY
    1
    LIMIT
        1;
    $$)).*
ORDER BY
    1,2,3,4;

CREATE TABLE alter_add_unique(x int, y int);
CREATE UNIQUE INDEX CONCURRENTLY alter_unique_idx ON alter_add_unique(x);
SELECT create_distributed_table('alter_add_unique', 'x');

ALTER TABLE alter_add_unique ADD CONSTRAINT unique_constraint_test UNIQUE USING INDEX alter_unique_idx;
ALTER TABLE alter_add_unique DROP CONSTRAINT unique_constraint_test;

CREATE TABLE unique_test_table_single_shard(id int, name varchar(20));
SELECT create_distributed_table('unique_test_table_single_shard', 'id', shard_count=>1);

ALTER TABLE unique_test_table_single_shard ADD UNIQUE(id, name) WITH (fillfactor=20);

SELECT (groupid = 0) AS is_coordinator, result FROM run_command_on_all_nodes(
  $$SELECT get_index_defs FROM get_index_defs('sc3', 'unique_test_table_single_shard')$$
)
JOIN pg_dist_node USING (nodeid)
ORDER BY is_coordinator DESC, result;

DROP TABLE unique_test_table_single_shard;

SET search_path TO 'public';

DROP SCHEMA sc1 CASCADE;
DROP SCHEMA sc2 CASCADE;
DROP SCHEMA sc3 CASCADE;

CREATE SCHEMA test_auto_explain;
SET search_path TO 'test_auto_explain';

-- Test ALTER TABLE ... ADD CONSTRAINT ... does not cause a crash when auto_explain module is loaded
CREATE TABLE target_table(col_1 int primary key, col_2 int);
SELECT create_distributed_table('target_table','col_1');
INSERT INTO target_table VALUES(1,2),(2,3),(3,4),(4,5),(5,6);

CREATE TABLE test_ref_table (key int PRIMARY KEY);
SELECT create_reference_table('test_ref_table');
INSERT INTO test_ref_table VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_level = LOG;
SET client_min_messages to LOG;
SET auto_explain.log_timing TO off;
SET auto_explain.log_format = JSON;
BEGIN;
-- simulate being a worker session/backend
SET LOCAL application_name to 'citus_internal gpid=10000000001';
SET citus.enable_ddl_propagation TO OFF;
-- alter table triggers SELECT, and auto_explain catches that
ALTER TABLE target_table ADD CONSTRAINT fkey_167 FOREIGN KEY (col_1) REFERENCES test_ref_table(key) ON DELETE CASCADE;
END;

RESET citus.enable_ddl_propagation;
SET client_min_messages to ERROR;
SET search_path TO 'public';

DROP SCHEMA test_auto_explain CASCADE;
