--
-- MULTI_DYNAMIC_EXECUTOR
--
-- Run different types of queries using the dynamic executor.

SET citusdb.large_table_shard_count TO 2;
SET citusdb.task_executor_type TO 'dynamic';

-- Should be able to run regular queries with dynamic executor
SELECT
	count(*)
FROM
	orders;

-- Should be able to run re-partition queries with dynamic executor
SELECT
	o_orderkey, count(*)
FROM
	orders, customer
WHERE
	c_custkey = o_custkey
GROUP BY
	o_orderkey
ORDER BY
	o_orderkey
LIMIT 1;

-- Should be able to run router queries with dynamic executor
SELECT
	o_orderkey
FROM
	orders
WHERE
	o_orderkey = 1;
