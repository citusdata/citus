--
-- MULTI_CREATE_TABLE
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 360000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 360000;

-- Create new table definitions for use in testing in distributed planning and
-- execution functionality. Also create indexes to boost performance.

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
SELECT master_create_distributed_table('lineitem', 'l_orderkey', 'append');

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
SELECT master_create_distributed_table('orders', 'o_orderkey', 'append');

CREATE TABLE customer (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null);
SELECT master_create_distributed_table('customer', 'c_custkey', 'append');

CREATE TABLE nation (
	n_nationkey integer not null,
	n_name char(25) not null,
	n_regionkey integer not null,
	n_comment varchar(152));

\COPY nation FROM STDIN WITH CSV
1,'name',1,'comment_1'
2,'name',2,'comment_2'
3,'name',3,'comment_3'
4,'name',4,'comment_4'
5,'name',5,'comment_5'
\.

SELECT master_create_distributed_table('nation', 'n_nationkey', 'append');

TRUNCATE nation;

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
SELECT master_create_distributed_table('part', 'p_partkey', 'append');

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
SELECT master_create_distributed_table('supplier_single_shard', 's_suppkey', 'append');

-- Show that when a hash distributed table with replication factor=1 is created, it 
-- automatically marked as streaming replicated
SET citus.shard_replication_factor TO 1;

CREATE TABLE mx_table_test (col1 int, col2 text);
SELECT create_distributed_table('mx_table_test', 'col1');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='mx_table_test'::regclass;
DROP TABLE mx_table_test; 

-- Show that it is not possible to create an mx table with the old 
-- master_create_distributed_table function
CREATE TABLE mx_table_test (col1 int, col2 text);
SELECT master_create_distributed_table('mx_table_test', 'col1', 'hash');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='mx_table_test'::regclass;
DROP TABLE mx_table_test;

-- Show that when replication factor > 1 the table is created as coordinator-replicated
SET citus.shard_replication_factor TO 2;
CREATE TABLE mx_table_test (col1 int, col2 text);
SELECT create_distributed_table('mx_table_test', 'col1');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='mx_table_test'::regclass;
DROP TABLE mx_table_test; 

SET citus.shard_replication_factor TO default;

SET citus.shard_count to 4;

CREATE TABLE lineitem_hash_part (like lineitem);
SELECT create_distributed_table('lineitem_hash_part', 'l_orderkey');

CREATE TABLE orders_hash_part (like orders);
SELECT create_distributed_table('orders_hash_part', 'o_orderkey');
