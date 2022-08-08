--
-- MULTI_MULTIUSER_BASIC_QUERIES
--

SET ROLE full_access;

-- Execute simple sum, average, and count queries on data recently uploaded to
-- our partitioned table.

SELECT count(*) FROM lineitem;

SELECT sum(l_extendedprice) FROM lineitem;

SELECT avg(l_extendedprice) FROM lineitem;

RESET ROLE;

-- and again, to check a read-only user can query
SET ROLE read_access;
SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM lineitem;
RESET citus.task_executor_type;
SELECT count(*) FROM lineitem;

-- and yet again, to prove we're failing when a user doesn't have permissions
SET ROLE no_access;
SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM lineitem;
RESET citus.task_executor_type;
SELECT count(*) FROM lineitem;
RESET ROLE;

-- verify that broadcast joins work

SET ROLE read_access;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part, orders, customer
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10 AND
        c_acctbal < 5000.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey
LIMIT 30;
RESET ROLE;

SET ROLE no_access;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part, orders, customer
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10 AND
        c_acctbal < 5000.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey
LIMIT 30;
RESET ROLE;

-- verify that re-partition queries work
SET citus.task_executor_type TO 'task-tracker';

SET ROLE read_access;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part, orders, customer
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10 AND
        c_acctbal < 5000.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey
LIMIT 30;
RESET ROLE;

SET ROLE no_access;

SELECT
	l_partkey, o_orderkey, count(*)
FROM
	lineitem, part, orders, customer
WHERE
	l_orderkey = o_orderkey AND
	l_partkey = p_partkey AND
	c_custkey = o_custkey AND
        (l_quantity > 5.0 OR l_extendedprice > 1200.0) AND
        p_size > 8 AND o_totalprice > 10 AND
        c_acctbal < 5000.0
GROUP BY
	l_partkey, o_orderkey
ORDER BY
	l_partkey, o_orderkey
LIMIT 30;
RESET ROLE;
