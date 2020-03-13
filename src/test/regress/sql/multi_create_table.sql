--
-- MULTI_CREATE_TABLE
--

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
