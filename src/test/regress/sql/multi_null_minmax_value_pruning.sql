--
-- MULTI_NULL_MINMAX_VALUE_PRUNING
--

-- This test checks that we can handle null min/max values in shard statistics
-- and that we don't partition or join prune shards that have null values.


SET citus.next_shard_id TO 760000;

-- print whether we're using version > 9 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 9 AS version_above_nine;

SET client_min_messages TO DEBUG2;
SET citus.explain_all_tasks TO on;
-- to avoid differing explain output - executor doesn't matter,
-- because were testing pruning here.
SET citus.task_executor_type TO 'real-time';

-- Change configuration to treat lineitem and orders tables as large

SET citus.large_table_shard_count TO 2;

SELECT shardminvalue, shardmaxvalue from pg_dist_shard WHERE shardid = 290000;
SELECT shardminvalue, shardmaxvalue from pg_dist_shard WHERE shardid = 290001;

-- Check that partition and join pruning works when min/max values exist
-- Adding l_orderkey = 1 to make the query not router executable
EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030 or l_orderkey = 1;

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

-- Now set the minimum value for a shard to null. Then check that we don't apply
-- partition or join pruning for the shard with null min value.

UPDATE pg_dist_shard SET shardminvalue = NULL WHERE shardid = 290000;

EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

-- Next, set the maximum value for another shard to null. Then check that we
-- don't apply partition or join pruning for this other shard either.

UPDATE pg_dist_shard SET shardmaxvalue = NULL WHERE shardid = 290001;

EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

-- Last, set the minimum value to 0 and check that we don't treat it as null. We
-- should apply partition and join pruning for this shard now.

UPDATE pg_dist_shard SET shardminvalue = '0' WHERE shardid = 290000;

EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

-- Set minimum and maximum values for two shards back to their original values

UPDATE pg_dist_shard SET shardminvalue = '1' WHERE shardid = 290000;
UPDATE pg_dist_shard SET shardmaxvalue = '14947' WHERE shardid = 290001;

SET client_min_messages TO NOTICE;
