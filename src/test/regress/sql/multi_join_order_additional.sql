--
-- MULTI_JOIN_ORDER_ADDITIONAL
--

-- Set configuration to print table join order and pruned shards

SET citus.log_multi_join_order TO TRUE;
SET client_min_messages TO DEBUG2;

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

UPDATE pg_dist_partition SET partmethod = 'h' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'lineitem');

EXPLAIN SELECT count(*) FROM orders, lineitem
	WHERE o_orderkey = l_orderkey;

-- Verify we handle local joins between two hash-partitioned tables.
UPDATE pg_dist_partition SET partmethod = 'h' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'orders');

EXPLAIN SELECT count(*) FROM orders, lineitem
	WHERE o_orderkey = l_orderkey;

UPDATE pg_dist_partition SET partmethod = 'a' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'lineitem');
UPDATE pg_dist_partition SET partmethod = 'a' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'orders');


-- Validate that we can handle broadcast joins with hash-partitioned tables.
UPDATE pg_dist_partition SET partmethod = 'h' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'customer');

EXPLAIN SELECT count(*) FROM customer, nation
	WHERE c_nationkey = n_nationkey;

UPDATE pg_dist_partition SET partmethod = 'a' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'customer');

-- Update the large table shard count for all the following tests.
SET citus.large_table_shard_count TO 1;

-- Validate that we don't use a single-partition join method for a hash
-- re-partitioned table, thus preventing a partition of just the customer table.
EXPLAIN SELECT count(*) FROM orders, lineitem, customer
	WHERE o_custkey = l_partkey AND o_custkey = c_nationkey;

-- Validate that we don't chose a single-partition join method with a
-- hash-partitioned base table
UPDATE pg_dist_partition SET partmethod = 'h' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'customer');

EXPLAIN SELECT count(*) FROM orders, customer
	WHERE c_custkey = o_custkey;

UPDATE pg_dist_partition SET partmethod = 'a' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'customer');

-- Validate that we can re-partition a hash partitioned table to join with a
-- range partitioned one.
UPDATE pg_dist_partition SET partmethod = 'h' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'orders');

EXPLAIN SELECT count(*) FROM orders, customer
	WHERE c_custkey = o_custkey;

UPDATE pg_dist_partition SET partmethod = 'a' WHERE
       logicalrelid = (SELECT relfilenode FROM pg_class WHERE relname = 'orders');

COMMIT;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
