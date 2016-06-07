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
SELECT master_create_distributed_table('nation', 'n_nationkey', 'append');

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
SELECT master_create_distributed_table('supplier', 's_suppkey', 'append');


-- now test that Citus cannot distribute unique constraints that do not include
-- the partition column
CREATE TABLE primary_key_on_non_part_col
(
	partition_col integer,
	other_col integer PRIMARY KEY
);
SELECT master_create_distributed_table('primary_key_on_non_part_col', 'partition_col', 'hash');

CREATE TABLE unique_const_on_non_part_col
(
	partition_col integer,
	other_col integer UNIQUE
);
SELECT master_create_distributed_table('primary_key_on_non_part_col', 'partition_col', 'hash');

-- now show that Citus can distribute unique constrints that include
-- the partition column
CREATE TABLE primary_key_on_part_col
(
	partition_col integer PRIMARY KEY,
	other_col integer
);
SELECT master_create_distributed_table('primary_key_on_part_col', 'partition_col', 'hash');

CREATE TABLE unique_const_on_part_col
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('unique_const_on_part_col', 'partition_col', 'hash');

CREATE TABLE unique_const_on_two_columns
(
	partition_col integer,
	other_col integer,
	UNIQUE (partition_col, other_col)
);
SELECT master_create_distributed_table('unique_const_on_two_columns', 'partition_col', 'hash');

CREATE TABLE unique_const_append_partitioned_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('unique_const_append_partitioned_tables', 'partition_col', 'append');

CREATE TABLE unique_const_range_partitioned_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('unique_const_range_partitioned_tables', 'partition_col', 'range');

-- drop unnecessary tables
DROP TABLE primary_key_on_non_part_col, unique_const_on_non_part_col CASCADE;
DROP TABLE primary_key_on_part_col, unique_const_on_part_col, unique_const_on_two_columns CASCADE;
DROP TABLE unique_const_range_partitioned_tables CASCADE;


