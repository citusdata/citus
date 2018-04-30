--
-- MULTI_REPARTITION_JOIN_PRUNING
--
-- Tests covering partition and join-pruning for repartition joins. Note that we
-- set executor type to task tracker executor here, as we cannot run repartition
-- jobs with real time executor.


SET citus.next_shard_id TO 700000;


SET client_min_messages TO DEBUG2;
SET citus.task_executor_type TO 'task-tracker';

-- Single range-repartition join to test join-pruning behaviour.
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey;
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey;

-- Single range-repartition join with a selection clause on the partitioned
-- table to test the case when all map tasks are pruned away.
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey AND
	o_orderkey < 0;
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey AND
	o_orderkey < 0;

-- Single range-repartition join with a selection clause on the base table to
-- test the case when all sql tasks are pruned away.
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey AND
	c_custkey < 0;
SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey AND
	c_custkey < 0;

-- Dual hash-repartition join test case. Note that this query doesn't produce
-- meaningful results and is only to test hash-partitioning of two large tables
-- on non-partition columns.
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	lineitem, customer_append
WHERE
	l_partkey = c_nationkey;
SELECT
	count(*)
FROM
	lineitem, customer_append
WHERE
	l_partkey = c_nationkey;

-- Dual hash-repartition join with a selection clause on one of the tables to
-- test the case when all map tasks are pruned away.
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	lineitem, customer_append
WHERE
	l_partkey = c_nationkey AND
	l_orderkey < 0;
SELECT
	count(*)
FROM
	lineitem, customer_append
WHERE
	l_partkey = c_nationkey AND
	l_orderkey < 0;

-- Test cases with false in the WHERE clause
EXPLAIN (COSTS OFF)
SELECT
	o_orderkey
FROM
	orders INNER JOIN customer_append ON (o_custkey = c_custkey)
WHERE
	false;
-- execute once, to verify that's handled
SELECT
	o_orderkey
FROM
	orders INNER JOIN customer_append ON (o_custkey = c_custkey)
WHERE
	false;

EXPLAIN (COSTS OFF)
SELECT
	o_orderkey
FROM
	orders INNER JOIN customer_append ON (o_custkey = c_custkey)
WHERE
	1=0 AND c_custkey < 0;

EXPLAIN (COSTS OFF)
SELECT
	o_orderkey
FROM
	orders INNER JOIN customer_append ON (o_custkey = c_custkey AND false);

EXPLAIN (COSTS OFF)
SELECT
	o_orderkey
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey AND false;
