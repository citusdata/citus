--
-- MULTI_JOIN_PRUNING
--


ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 680000;


-- Check that join-pruning works for joins between two large relations. For now
-- we only check for join-pruning between locally partitioned relations. In the
-- future we want to check for pruning between re-partitioned relations as well.

SET citus.explain_distributed_queries TO off;
SET client_min_messages TO DEBUG2;

-- Change configuration to treat all tables as large

SET citus.large_table_shard_count TO 2;

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 9030;

-- Shards for the lineitem table have been pruned away. Check that join pruning
-- works as expected in this case.

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 20000;

-- Partition pruning left three shards for the lineitem and one shard for the
-- orders table. These shard sets don't overlap, so join pruning should prune
-- out all the shards, and leave us with an empty task list.

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 6000 AND o_orderkey < 6000;

-- These tests check that we can do join pruning for tables partitioned over
-- different type of columns including varchar, array types, composite types
-- etc. This is in response to a bug we had where we were not able to resolve
-- correct operator types for some kind of column types.

EXPLAIN SELECT count(*)
	FROM array_partitioned_table table1, array_partitioned_table table2
	WHERE table1.array_column = table2.array_column;

EXPLAIN SELECT count(*)
	FROM composite_partitioned_table table1, composite_partitioned_table table2
	WHERE table1.composite_column = table2.composite_column;

-- Test that large table joins on partition varchar columns work

EXPLAIN SELECT count(*)
	FROM varchar_partitioned_table table1, varchar_partitioned_table table2
	WHERE table1.varchar_column = table2.varchar_column;

SET client_min_messages TO NOTICE;
