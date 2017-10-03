--
-- MULTI_LARGE_TABLE_TASK_ASSIGNMENT
--
-- Tests which cover task assignment for MapMerge jobs for single range repartition
-- and dual hash repartition joins. The tests also cover task assignment propagation
-- from a sql task to its depended tasks. Note that we set the executor type to task
-- tracker executor here, as we cannot run repartition jobs with real time executor.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 710000;

-- print whether we're using version > 9 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 9 AS version_above_nine;


BEGIN;
SET client_min_messages TO DEBUG3;
SET citus.large_table_shard_count TO 2;
SET citus.task_executor_type TO 'task-tracker';

-- Single range repartition join to test anchor-shard based task assignment and
-- assignment propagation to merge and data-fetch tasks.

SELECT
	count(*)
FROM
	orders, customer
WHERE
	o_custkey = c_custkey;

-- Single range repartition join, along with a join with a small table containing
-- more than one shard. This situation results in multiple sql tasks depending on
-- the same merge task, and tests our constraint group creation and assignment
-- propagation. Here 'orders' is considered the small table.

SET citus.large_table_shard_count TO 3;

SELECT
	count(*)
FROM
	orders, customer, lineitem
WHERE
	o_custkey = c_custkey AND
	o_orderkey = l_orderkey;

SET citus.large_table_shard_count TO 2;

-- Dual hash repartition join which tests the separate hash repartition join
-- task assignment algorithm.

SELECT
	count(*)
FROM
	lineitem, customer
WHERE
	l_partkey = c_nationkey;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
COMMIT;
