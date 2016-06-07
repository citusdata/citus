--
-- MULTI_PREPARE_SQL
--

-- Tests covering PREPARE statements. Many of the queries are
-- taken from other regression test files and converted into
-- prepared statements.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 790000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 790000;


PREPARE prepared_test_1 AS
SELECT
	count(*)
FROM
	orders;

PREPARE prepared_test_2 AS
SELECT
	count(*)
FROM
	orders, lineitem
WHERE
	o_orderkey = l_orderkey;

PREPARE prepared_test_3 AS
SELECT
	count(*)
FROM
	orders, customer
WHERE
	o_custkey = c_custkey;

PREPARE prepared_test_4 AS
SELECT
	count(*)
FROM
	orders, customer, lineitem
WHERE
	o_custkey = c_custkey AND
	o_orderkey = l_orderkey;

PREPARE prepared_test_5 AS
SELECT
	count(*)
FROM
	lineitem, customer
WHERE
	l_partkey = c_nationkey;

PREPARE prepared_test_6(int) AS
SELECT
	count(*)
FROM
	orders, lineitem
WHERE
	o_orderkey = l_orderkey AND
	l_suppkey > $1;

PREPARE prepared_test_7(text, text) AS
SELECT
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) AS revenue
FROM
	(
	SELECT
		supp_nation,
		cust_nation,
		extract(year FROM l_shipdate) AS l_year,
		l_extendedprice * (1 - l_discount) AS volume
	FROM
		supplier,
		lineitem,
		orders,
		customer,
		(
		SELECT
			n1.n_nationkey AS supp_nation_key,
			n2.n_nationkey AS cust_nation_key,
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation
		FROM
			nation n1,
			nation n2
		WHERE
			(
			(n1.n_name = $1 AND n2.n_name = $2)
			OR (n1.n_name = $2 AND n2.n_name = $1)
			)
		) AS temp
        WHERE
		s_suppkey = l_suppkey
		AND o_orderkey = l_orderkey
		AND c_custkey = o_custkey
		AND s_nationkey = supp_nation_key
		AND c_nationkey = cust_nation_key
		AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
	) AS shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;

SET citus.task_executor_type TO 'task-tracker';
SET client_min_messages TO INFO;

-- execute prepared statements
EXECUTE prepared_test_1;
EXECUTE prepared_test_2;
EXECUTE prepared_test_3;
EXECUTE prepared_test_4;
EXECUTE prepared_test_5;

-- execute prepared statements with different parameters
EXECUTE prepared_test_6(155);
EXECUTE prepared_test_6(1555);
EXECUTE prepared_test_7('UNITED KINGDOM', 'CHINA');
EXECUTE prepared_test_7('FRANCE', 'GERMANY');

-- now, execute prepared statements with random order
EXECUTE prepared_test_6(155);
EXECUTE prepared_test_3;
EXECUTE prepared_test_7('FRANCE', 'GERMANY');
EXECUTE prepared_test_5;
EXECUTE prepared_test_1;
EXECUTE prepared_test_6(1555);
EXECUTE prepared_test_4;
EXECUTE prepared_test_7('UNITED KINGDOM', 'CHINA');
EXECUTE prepared_test_2;

-- CREATE TABLE ... AS EXECUTE prepared_statement tests
CREATE TEMP TABLE prepared_sql_test_7 AS EXECUTE prepared_test_7('UNITED KINGDOM', 'CHINA');
SELECT * from prepared_sql_test_7;

-- now, run some of the tests with real-time executor
SET citus.task_executor_type TO 'real-time';

-- execute prepared statements
EXECUTE prepared_test_1;
EXECUTE prepared_test_2;

-- execute prepared statements with different parameters
EXECUTE prepared_test_6(155);
-- FIXME: temporarily disabled
-- EXECUTE prepared_test_6(1555);

-- clean-up prepared statements
DEALLOCATE ALL;
