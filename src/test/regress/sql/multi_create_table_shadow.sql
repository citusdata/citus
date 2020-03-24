--
-- MULTI_CREATE_TABLE
--

-- Some of the tests fail but without them some other tests fail. So failing parts
-- are removed to create shadow tests. This is one of them.

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 360000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 100000;

-- Create new table definitions for use in testing in distributed planning and
-- execution functionality. Also create indexes to boost performance. Since we
-- need to cover both reference join and partitioned join, we have created
-- reference and append distributed version of orders, customer and part tables.

CREATE TABLE lineitem (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null,
	PRIMARY KEY(l_orderkey, l_linenumber) );
SELECT create_distributed_table('lineitem', 'l_orderkey', 'append');

CREATE INDEX lineitem_time_index ON lineitem (l_shipdate);

CREATE TABLE orders (
	o_orderkey bigint not null,
	o_custkey integer not null,
	o_orderstatus char(1) not null,
	o_totalprice decimal(15,2) not null,
	o_orderdate date not null,
	o_orderpriority char(15) not null,
	o_clerk char(15) not null,
	o_shippriority integer not null,
	o_comment varchar(79) not null,
	PRIMARY KEY(o_orderkey) );
SELECT create_distributed_table('orders', 'o_orderkey', 'append');

CREATE TABLE orders_reference (
	o_orderkey bigint not null,
	o_custkey integer not null,
	o_orderstatus char(1) not null,
	o_totalprice decimal(15,2) not null,
	o_orderdate date not null,
	o_orderpriority char(15) not null,
	o_clerk char(15) not null,
	o_shippriority integer not null,
	o_comment varchar(79) not null,
	PRIMARY KEY(o_orderkey) );
SELECT create_reference_table('orders_reference');


CREATE TABLE customer (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null);
SELECT create_reference_table('customer');

CREATE TABLE customer_append (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null);
SELECT create_distributed_table('customer_append', 'c_custkey', 'append');

CREATE TABLE nation (
	n_nationkey integer not null,
	n_name char(25) not null,
	n_regionkey integer not null,
	n_comment varchar(152));

SELECT create_reference_table('nation');

CREATE TABLE part (
	p_partkey integer not null,
	p_name varchar(55) not null,
	p_mfgr char(25) not null,
	p_brand char(10) not null,
	p_type varchar(25) not null,
	p_size integer not null,
	p_container char(10) not null,
	p_retailprice decimal(15,2) not null,
	p_comment varchar(23) not null);
SELECT create_reference_table('part');

CREATE TABLE part_append (
	p_partkey integer not null,
	p_name varchar(55) not null,
	p_mfgr char(25) not null,
	p_brand char(10) not null,
	p_type varchar(25) not null,
	p_size integer not null,
	p_container char(10) not null,
	p_retailprice decimal(15,2) not null,
	p_comment varchar(23) not null);
SELECT create_distributed_table('part_append', 'p_partkey', 'append');

CREATE TABLE supplier
(
	s_suppkey integer not null,
	s_name char(25) not null,
	s_address varchar(40) not null,
	s_nationkey integer,
	s_phone char(15) not null,
	s_acctbal decimal(15,2) not null,
	s_comment varchar(101) not null
);
SELECT create_reference_table('supplier');

-- create a single shard supplier table which is not
-- a reference table
CREATE TABLE supplier_single_shard
(
	s_suppkey integer not null,
 	s_name char(25) not null,
 	s_address varchar(40) not null,
 	s_nationkey integer,
 	s_phone char(15) not null,
  	s_acctbal decimal(15,2) not null,
  	s_comment varchar(101) not null
);
SELECT create_distributed_table('supplier_single_shard', 's_suppkey', 'append');

CREATE TABLE mx_table_test (col1 int, col2 text);

-- Since we're superuser, we can set the replication model to 'streaming' to
-- create a one-off MX table... but if we forget to set the replication factor to one,
-- we should see an error reminding us to fix that
SET citus.replication_model TO 'streaming';
SELECT create_distributed_table('mx_table_test', 'col1');

-- ok, so now actually create the one-off MX table
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('mx_table_test', 'col1');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='mx_table_test'::regclass;
DROP TABLE mx_table_test;

-- Show that master_create_distributed_table ignores citus.replication_model GUC
CREATE TABLE s_table(a int);
SELECT master_create_distributed_table('s_table', 'a', 'hash');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='s_table'::regclass;

-- Show that master_create_worker_shards complains when RF>1 and replication model is streaming
UPDATE pg_dist_partition SET repmodel = 's' WHERE logicalrelid='s_table'::regclass;
SELECT master_create_worker_shards('s_table', 4, 2);

DROP TABLE s_table;

RESET citus.replication_model;

-- Show that create_distributed_table with append and range distributions ignore
-- citus.replication_model GUC
SET citus.shard_replication_factor TO 2;
SET citus.replication_model TO streaming;

CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

-- Show that master_create_distributed_table created statement replicated tables no matter
-- what citus.replication_model set to

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'hash');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

-- Check that the replication_model overwrite behavior is the same with RF=1
SET citus.shard_replication_factor TO 1;

CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'hash');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

RESET citus.replication_model;

-- Test initial data loading
CREATE TABLE data_load_test (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test VALUES (132, 'hello');
INSERT INTO data_load_test VALUES (243, 'world');

-- table must be empty when using append- or range-partitioning
SELECT create_distributed_table('data_load_test', 'col1', 'append');
SELECT create_distributed_table('data_load_test', 'col1', 'range');

-- table must be empty when using master_create_distributed_table (no shards created)
SELECT master_create_distributed_table('data_load_test', 'col1', 'hash');

-- create_distributed_table creates shards and copies data into the distributed table
SELECT create_distributed_table('data_load_test', 'col1');
SELECT * FROM data_load_test ORDER BY col1;
DROP TABLE data_load_test;

-- test queries on distributed tables with no shards
CREATE TABLE no_shard_test (col1 int, col2 text);
SELECT create_distributed_table('no_shard_test', 'col1', 'append');
SELECT * FROM no_shard_test WHERE col1 > 1;
DROP TABLE no_shard_test;

CREATE TABLE no_shard_test (col1 int, col2 text);
SELECT create_distributed_table('no_shard_test', 'col1', 'range');
SELECT * FROM no_shard_test WHERE col1 > 1;
DROP TABLE no_shard_test;

CREATE TABLE no_shard_test (col1 int, col2 text);
SELECT master_create_distributed_table('no_shard_test', 'col1', 'hash');
SELECT * FROM no_shard_test WHERE col1 > 1;
DROP TABLE no_shard_test;

-- ensure writes in the same transaction as create_distributed_table are visible
BEGIN;
CREATE TABLE data_load_test (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test VALUES (132, 'hello');
SELECT create_distributed_table('data_load_test', 'col1');
INSERT INTO data_load_test VALUES (243, 'world');
END;
SELECT * FROM data_load_test ORDER BY col1;
DROP TABLE data_load_test;

-- creating co-located distributed tables in the same transaction works
BEGIN;
CREATE TABLE data_load_test1 (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test1 VALUES (132, 'hello');
SELECT create_distributed_table('data_load_test1', 'col1');

CREATE TABLE data_load_test2 (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test2 VALUES (132, 'world');
SELECT create_distributed_table('data_load_test2', 'col1');

SELECT a.col2 ||' '|| b.col2
FROM data_load_test1 a JOIN data_load_test2 b USING (col1)
WHERE col1 = 132;

DROP TABLE data_load_test1, data_load_test2;
END;
