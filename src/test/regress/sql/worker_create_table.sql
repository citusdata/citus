--
-- WORKER_CREATE_TABLE
--

-- Create new table definitions for lineitem and supplier tables to test worker
-- node execution logic. For now,the tests include range and hash partitioning
-- of existing tables.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1110000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1110000;


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

CREATE TABLE lineitem_complex (
	l_partkey integer not null,
	l_discount decimal(15, 2) not null,
	l_shipdate date not null,
	l_comment varchar(44) not null );

-- Range partitioned lineitem data are inserted into these four tables

CREATE TABLE lineitem_range_part_00 ( LIKE lineitem );
CREATE TABLE lineitem_range_part_01 ( LIKE lineitem );
CREATE TABLE lineitem_range_part_02 ( LIKE lineitem );
CREATE TABLE lineitem_range_part_03 ( LIKE lineitem );

-- Complex range partitioned lineitem data are inserted into these four tables

CREATE TABLE lineitem_range_complex_part_00 ( LIKE lineitem_complex );
CREATE TABLE lineitem_range_complex_part_01 ( LIKE lineitem_complex );
CREATE TABLE lineitem_range_complex_part_02 ( LIKE lineitem_complex );
CREATE TABLE lineitem_range_complex_part_03 ( LIKE lineitem_complex );

-- Hash partitioned lineitem data are inserted into these four tables

CREATE TABLE lineitem_hash_part_00 ( LIKE lineitem );
CREATE TABLE lineitem_hash_part_01 ( LIKE lineitem );
CREATE TABLE lineitem_hash_part_02 ( LIKE lineitem );
CREATE TABLE lineitem_hash_part_03 ( LIKE lineitem );

-- Complex hash partitioned lineitem data are inserted into these four tables

CREATE TABLE lineitem_hash_complex_part_00 ( LIKE lineitem_complex );
CREATE TABLE lineitem_hash_complex_part_01 ( LIKE lineitem_complex );
CREATE TABLE lineitem_hash_complex_part_02 ( LIKE lineitem_complex );
CREATE TABLE lineitem_hash_complex_part_03 ( LIKE lineitem_complex );


-- Now create a supplier table to test repartitioning the data on the nation key
-- column, where the column's values can be null or zero.

CREATE TABLE SUPPLIER
(
	s_suppkey integer not null,
	s_name char(25) not null,
	s_address varchar(40) not null,
	s_nationkey integer,
	s_phone char(15) not null,
	s_acctbal decimal(15,2) not null,
	s_comment varchar(101) not null
);

-- Range partitioned supplier data are inserted into three tables

CREATE TABLE supplier_range_part_00 ( LIKE supplier );
CREATE TABLE supplier_range_part_01 ( LIKE supplier );
CREATE TABLE supplier_range_part_02 ( LIKE supplier );

-- Hash partitioned supplier data are inserted into three tables

CREATE TABLE supplier_hash_part_00 ( LIKE supplier );
CREATE TABLE supplier_hash_part_01 ( LIKE supplier );
CREATE TABLE supplier_hash_part_02 ( LIKE supplier );
