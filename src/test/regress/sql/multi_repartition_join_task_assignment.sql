--
-- MULTI_REPARTITION_JOIN_TASK_ASSIGNMENT
--
-- Tests which cover task assignment for MapMerge jobs for single range repartition
-- and dual hash repartition joins. The tests also cover task assignment propagation
-- from a sql task to its dependent tasks. Note that we set the executor type to task
-- tracker executor here, as we cannot run repartition jobs with real time executor.


SET citus.next_shard_id TO 710000;

BEGIN;
SET client_min_messages TO DEBUG3;
SET citus.enable_repartition_joins to ON;

-- Single range repartition join to test anchor-shard based task assignment and
-- assignment propagation to merge and data-fetch tasks.

SELECT
	count(*)
FROM
	orders, customer_append
WHERE
	o_custkey = c_custkey;

-- Single range repartition join, along with a join with a small table containing
-- more than one shard. This situation results in multiple sql tasks depending on
-- the same merge task, and tests our constraint group creation and assignment
-- propagation.

SELECT
	count(*)
FROM
	orders_reference, customer_append, lineitem
WHERE
	o_custkey = c_custkey AND
	o_orderkey = l_orderkey;

-- Dual hash repartition join which tests the separate hash repartition join
-- task assignment algorithm.

SELECT
	count(*)
FROM
	lineitem, customer_append
WHERE
	l_partkey = c_nationkey;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
COMMIT;
