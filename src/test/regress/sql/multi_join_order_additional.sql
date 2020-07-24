--
-- MULTI_JOIN_ORDER_ADDITIONAL
--


SET citus.next_shard_id TO 650000;

-- Set configuration to print table join order and pruned shards

SET citus.explain_distributed_queries TO off;
SET citus.log_multi_join_order TO TRUE;
SET citus.enable_repartition_joins to ON;
SET citus.shard_count to 2;
SET citus.shard_replication_factor to 1;
RESET client_min_messages;

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
SELECT create_distributed_table('lineitem_hash', 'l_orderkey');

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
SELECT create_distributed_table('orders_hash', 'o_orderkey');

CREATE TABLE customer_hash (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null);
SELECT create_distributed_table('customer_hash', 'c_custkey');

SET client_min_messages TO DEBUG2;
-- The following query checks that we can correctly handle self-joins

EXPLAIN (COSTS OFF)
SELECT l1.l_quantity FROM lineitem l1, lineitem l2
	WHERE l1.l_orderkey = l2.l_orderkey AND l1.l_quantity > 5;

SET client_min_messages TO LOG;

-- The following queries check that we correctly handle joins and OR clauses. In
-- particular, these queries check that we factorize out OR clauses if possible,
-- and that we default to a cartesian product otherwise.

EXPLAIN (COSTS OFF)
SELECT count(*) FROM lineitem, orders
	WHERE (l_orderkey = o_orderkey AND l_quantity > 5)
	OR (l_orderkey = o_orderkey AND l_quantity < 10);

EXPLAIN (COSTS OFF)
SELECT l_quantity FROM lineitem, orders
	WHERE (l_orderkey = o_orderkey OR l_quantity > 5);

EXPLAIN (COSTS OFF)
SELECT count(*) FROM orders, lineitem_hash
	WHERE o_orderkey = l_orderkey;

-- Verify we handle local joins between two hash-partitioned tables.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM orders_hash, lineitem_hash
	WHERE o_orderkey = l_orderkey;

-- Validate that we can handle broadcast joins with hash-partitioned tables.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM customer_hash, nation
	WHERE c_nationkey = n_nationkey;

-- Validate that we don't use a single-partition join method for a hash
-- re-partitioned table, thus preventing a partition of just the customer table.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM orders, lineitem, customer_append
	WHERE o_custkey = l_partkey AND o_custkey = c_nationkey;

-- Validate that we don't chose a single-partition join method with a
-- hash-partitioned base table
EXPLAIN (COSTS OFF)
SELECT count(*) FROM orders, customer_hash
	WHERE c_custkey = o_custkey;

-- Validate that we can re-partition a hash partitioned table to join with a
-- range partitioned one.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM orders_hash, customer_append
	WHERE c_custkey = o_custkey;

-- Validate a 4 way join that could be done locally is planned as such by the logical
-- planner. It used to be planned as a repartition join due to no 1 table being directly
-- joined to all other tables, but instead follows a chain.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM (
    SELECT users_table.user_id
      FROM users_table
      JOIN events_table USING (user_id)
     WHERE event_type = 5
) AS bar
JOIN (
    SELECT users_table.user_id
      FROM users_table
      JOIN events_table USING (user_id)
     WHERE event_type = 5
) AS some_users ON (some_users.user_id = bar.user_id);

-- Reset client logging level to its previous value
SET client_min_messages TO NOTICE;

DROP TABLE lineitem_hash;
DROP TABLE orders_hash;
DROP TABLE customer_hash;
