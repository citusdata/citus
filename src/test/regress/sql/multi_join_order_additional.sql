--
-- MULTI_JOIN_ORDER_ADDITIONAL
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 650000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 650000;


-- Set configuration to print table join order and pruned shards

SET citus.explain_distributed_queries TO off;
SET citus.log_multi_join_order TO TRUE;
SET client_min_messages TO DEBUG2;

-- Create new table definitions for use in testing in distributed planning and
-- execution functionality. Also create indexes to boost performance.

CREATE TABLE lineitem_hash (
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
SELECT master_create_distributed_table('lineitem_hash', 'l_orderkey', 'hash');
SELECT master_create_worker_shards('lineitem_hash', 2, 1);

CREATE INDEX lineitem_hash_time_index ON lineitem_hash (l_shipdate);

CREATE TABLE orders_hash (
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
SELECT master_create_distributed_table('orders_hash', 'o_orderkey', 'hash');
SELECT master_create_worker_shards('orders_hash', 2, 1);

CREATE TABLE customer_hash (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null);
SELECT master_create_distributed_table('customer_hash', 'c_custkey', 'hash');
SELECT master_create_worker_shards('customer_hash', 1, 1);

-- The following query checks that we can correctly handle self-joins

EXPLAIN SELECT l1.l_quantity FROM lineitem l1, lineitem l2
	WHERE l1.l_orderkey = l2.l_orderkey AND l1.l_quantity > 5;

-- Update configuration to treat lineitem and orders tables as large

SET citus.large_table_shard_count TO 2;
SET client_min_messages TO LOG;

-- The following queries check that we correctly handle joins and OR clauses. In
-- particular, these queries check that we factorize out OR clauses if possible,
-- and that we default to a cartesian product otherwise.

EXPLAIN SELECT count(*) FROM lineitem, orders
	WHERE (l_orderkey = o_orderkey AND l_quantity > 5)
	OR (l_orderkey = o_orderkey AND l_quantity < 10);

EXPLAIN SELECT l_quantity FROM lineitem, orders
	WHERE (l_orderkey = o_orderkey OR l_quantity > 5);

-- The below queries modify the partition method in pg_dist_partition. We thus
-- begin a transaction here so the changes don't impact any other parallel
-- running tests.
BEGIN;

-- Validate that we take into account the partition method when building the
-- join-order plan.

EXPLAIN SELECT count(*) FROM orders, lineitem_hash
	WHERE o_orderkey = l_orderkey;

-- Verify we handle local joins between two hash-partitioned tables.
EXPLAIN SELECT count(*) FROM orders_hash, lineitem_hash
	WHERE o_orderkey = l_orderkey;

-- Validate that we can handle broadcast joins with hash-partitioned tables.
EXPLAIN SELECT count(*) FROM customer_hash, nation
	WHERE c_nationkey = n_nationkey;

-- Update the large table shard count for all the following tests.
SET citus.large_table_shard_count TO 1;

-- Validate that we don't use a single-partition join method for a hash
-- re-partitioned table, thus preventing a partition of just the customer table.
EXPLAIN SELECT count(*) FROM orders, lineitem, customer
	WHERE o_custkey = l_partkey AND o_custkey = c_nationkey;

-- Validate that we don't chose a single-partition join method with a
-- hash-partitioned base table
EXPLAIN SELECT count(*) FROM orders, customer_hash
	WHERE c_custkey = o_custkey;

-- Validate that we can re-partition a hash partitioned table to join with a
-- range partitioned one.
EXPLAIN SELECT count(*) FROM orders_hash, customer
	WHERE c_custkey = o_custkey;

COMMIT;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;

DROP TABLE lineitem_hash;
DROP TABLE orders_hash;
DROP TABLE customer_hash;
