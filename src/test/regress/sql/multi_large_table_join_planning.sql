--
-- MULTI_LARGE_TABLE_PLANNING
--
-- Tests that cover large table join planning. Note that we explicitly start a
-- transaction block here so that we don't emit debug messages with changing
-- transaction ids in them. Also, we set the executor type to task tracker
-- executor here, as we cannot run repartition jobs with real time executor.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 690000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 690000;


BEGIN;
SET client_min_messages TO DEBUG4;
SET citus.large_table_shard_count TO 2;
SET citus.task_executor_type TO 'task-tracker';

-- Debug4 log messages display jobIds within them. We explicitly set the jobId
-- sequence here so that the regression output becomes independent of the number
-- of jobs executed prior to running this test.

ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1250;

-- Multi-level repartition join to verify our projection columns are correctly
-- referenced and propagated across multiple repartition jobs. The test also
-- validates that only the minimal necessary projection columns are transferred
-- between jobs.

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part, orders, customer
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10.0 AND
        c_acctbal < 5000.0 AND l_partkey < 1000
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, orders
WHERE
	l_suppkey = o_shippriority AND
        l_quantity < 5.0 AND o_totalprice <> 4.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey;

-- Reset client logging level to its previous value

SET client_min_messages TO NOTICE;
COMMIT;
