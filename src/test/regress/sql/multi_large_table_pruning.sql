--
-- MULTI_LARGE_TABLE_PRUNING
--
-- Tests covering partition and join-pruning for large table joins. Note that we
-- set executor type to task tracker executor here, as we cannot run repartition
-- jobs with real time executor.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 700000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 700000;


SET citus.large_table_shard_count TO 2;
SET client_min_messages TO DEBUG2;
SET citus.task_executor_type TO 'task-tracker';

-- Single range-repartition join to test join-pruning behaviour.

SELECT
	count(*)
FROM
	orders, customer
WHERE
	o_custkey = c_custkey;

-- Single range-repartition join with a selection clause on the partitioned
-- table to test the case when all map tasks are pruned away.

SELECT
	count(*)
FROM
	orders, customer
WHERE
	o_custkey = c_custkey AND
	o_orderkey < 0;

-- Single range-repartition join with a selection clause on the base table to
-- test the case when all sql tasks are pruned away.

SELECT
	count(*)
FROM
	orders, customer
WHERE
	o_custkey = c_custkey AND
	c_custkey < 0;

-- Dual hash-repartition join test case. Note that this query doesn't produce
-- meaningful results and is only to test hash-partitioning of two large tables
-- on non-partition columns.

SELECT
	count(*)
FROM
	lineitem, customer
WHERE
	l_partkey = c_nationkey;

-- Dual hash-repartition join with a selection clause on one of the tables to
-- test the case when all map tasks are pruned away.

SELECT
	count(*)
FROM
	lineitem, customer
WHERE
	l_partkey = c_nationkey AND
	l_orderkey < 0;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
