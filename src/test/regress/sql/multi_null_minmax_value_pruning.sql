--
-- MULTI_NULL_MINMAX_VALUE_PRUNING
--

-- This test checks that we can handle null min/max values in shard statistics
-- and that we don't partition or join prune shards that have null values.
CREATE SCHEMA multi_null_minmax_value_pruning;
SET search_path TO multi_null_minmax_value_pruning;

SET citus.explain_all_tasks TO on;

SET citus.log_multi_join_order to true;
SET citus.enable_repartition_joins to ON;

SET citus.next_shard_id = 290000;

CREATE TABLE lineitem (LIKE public.lineitem);
SELECT create_distributed_table('lineitem', 'l_orderkey', 'range');
SELECT master_create_empty_shard('lineitem') as lineitem_shardid1 \gset
SELECT master_create_empty_shard('lineitem') as lineitem_shardid2 \gset

CREATE TABLE orders (LIKE public.orders);
SELECT create_distributed_table('orders', 'o_orderkey', 'range');
SELECT master_create_empty_shard('orders') as orders_shardid1 \gset
SELECT master_create_empty_shard('orders') as orders_shardid2 \gset

SET client_min_messages TO DEBUG2;

UPDATE pg_dist_shard SET shardminvalue = '1', shardmaxvalue = '6000' WHERE shardid = :lineitem_shardid1 OR shardid = :orders_shardid1;
UPDATE pg_dist_shard SET shardminvalue = '6001', shardmaxvalue = '20000' WHERE shardid = :lineitem_shardid2 OR shardid = :orders_shardid2;
UPDATE pg_dist_partition SET colocationid = 87091 WHERE logicalrelid = 'orders'::regclass OR logicalrelid = 'lineitem'::regclass;

-- Check that partition and join pruning works when min/max values exist
-- Adding l_orderkey = 1 to make the query not router executable
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030 or l_orderkey = 1;
$Q$);

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey;

-- Now set the minimum value for a shard to null. Then check that we don't apply
-- partition or join pruning for the shard with null min value. Since it is not
-- supported with single-repartition join, dual-repartition has been used.

UPDATE pg_dist_shard SET shardminvalue = NULL WHERE shardid = :lineitem_shardid1;

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;
$Q$);

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_partkey = o_custkey;

-- Next, set the maximum value for another shard to null. Then check that we
-- don't apply partition or join pruning for this other shard either. Since it
-- is not supported with single-repartition join, dual-repartition has been used.

UPDATE pg_dist_shard SET shardmaxvalue = NULL WHERE shardid = :lineitem_shardid2;

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;
$Q$);

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_partkey = o_custkey;

-- Last, set the minimum value to 0 and check that we don't treat it as null. We
-- should apply partition and join pruning for this shard now. Since it is not
-- supported with single-repartition join, dual-repartition has been used.

UPDATE pg_dist_shard SET shardminvalue = '0' WHERE shardid = :lineitem_shardid1;

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS FALSE)
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030;
$Q$);

EXPLAIN (COSTS FALSE)
SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_partkey = o_custkey;

RESET client_min_messages;
DROP SCHEMA multi_null_minmax_value_pruning CASCADE;
