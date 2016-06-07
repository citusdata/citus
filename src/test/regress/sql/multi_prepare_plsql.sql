--
-- MULTI_PREPARE_PLSQL
--

-- Many of the queries are taken from other regression test files
-- and converted into both plain SQL and PL/pgsql functions, which
-- use prepared statements internally.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 780000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 780000;


CREATE FUNCTION sql_test_no_1() RETURNS bigint AS '
	SELECT
		count(*)
	FROM
		orders;
' LANGUAGE SQL;

CREATE FUNCTION sql_test_no_2() RETURNS bigint AS '
	SELECT
		count(*)
	FROM
		orders, lineitem
	WHERE
		o_orderkey = l_orderkey;
' LANGUAGE SQL;

CREATE FUNCTION sql_test_no_3() RETURNS bigint AS '
	SELECT
		count(*)
	FROM
		orders, customer
	WHERE
		o_custkey = c_custkey;
' LANGUAGE SQL;

CREATE FUNCTION sql_test_no_4() RETURNS bigint AS '
	SELECT
		count(*)
	FROM
		orders, customer, lineitem
	WHERE
		o_custkey = c_custkey AND
		o_orderkey = l_orderkey;
' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sql_test_no_6(integer) RETURNS bigint AS  $$
	SELECT
		count(*)
	FROM
		orders, lineitem
	WHERE
		o_orderkey = l_orderkey AND
		l_suppkey > $1;
$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION plpgsql_test_1() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
         SELECT
			count(*)
		FROM
			orders;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_2() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
		SELECT
			count(*)
		FROM
			orders, lineitem
		WHERE
			o_orderkey = l_orderkey;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_3() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
		SELECT
			count(*)
		FROM
			orders, customer
		WHERE
			o_custkey = c_custkey;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_4() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
		SELECT
			count(*)
		FROM
			orders, customer, lineitem
		WHERE
			o_custkey = c_custkey AND
			o_orderkey = l_orderkey;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_5() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
		SELECT
			count(*)
		FROM
			lineitem, customer
		WHERE
			l_partkey = c_nationkey;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_6(int) RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
		SELECT
			count(*)
		FROM
			orders, lineitem
		WHERE
			o_orderkey = l_orderkey AND
			l_suppkey > $1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsql_test_7(text, text) RETURNS TABLE(supp_natadsion text, cusasdt_nation text, l_yeasdar int, sasdaum double precision) AS $$
DECLARE
BEGIN
	RETURN  QUERY
		SELECT
			supp_nation::text,
			cust_nation::text,
			l_year::int,
			sum(volume)::double precision AS revenue
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
END;
$$ LANGUAGE plpgsql;

SET citus.task_executor_type TO 'task-tracker';
SET client_min_messages TO INFO;

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();
SELECT sql_test_no_3();
SELECT sql_test_no_4();

-- now, run PL/pgsql functions
SELECT plpgsql_test_1();
SELECT plpgsql_test_2();
SELECT plpgsql_test_3();
SELECT plpgsql_test_4();
SELECT plpgsql_test_5();

-- run PL/pgsql functions with different parameters
SELECT plpgsql_test_6(155);
SELECT plpgsql_test_6(1555);
SELECT plpgsql_test_7('UNITED KINGDOM', 'CHINA');
SELECT plpgsql_test_7('FRANCE', 'GERMANY');

-- now, PL/pgsql functions with random order
SELECT plpgsql_test_6(155);
SELECT plpgsql_test_3();
SELECT plpgsql_test_7('FRANCE', 'GERMANY');
SELECT plpgsql_test_5();
SELECT plpgsql_test_1();
SELECT plpgsql_test_6(1555);
SELECT plpgsql_test_4();
SELECT plpgsql_test_7('UNITED KINGDOM', 'CHINA');
SELECT plpgsql_test_2();

-- run the tests which do not require re-partition
-- with real-time executor
SET citus.task_executor_type TO 'real-time';

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();

-- plain SQL functions with parameters cannot be executed
-- FIXME: temporarily disabled, bad error message - waiting for proper parametrized query
-- FIXME: support
-- SELECT sql_test_no_6(155);

-- now, run PL/pgsql functions
SELECT plpgsql_test_1();
SELECT plpgsql_test_2();

-- run PL/pgsql functions with different parameters
-- FIXME: temporarily disabled, waiting for proper parametrized query support
-- SELECT plpgsql_test_6(155);
-- SELECT plpgsql_test_6(1555);

-- clean-up functions
DROP FUNCTION sql_test_no_1();
DROP FUNCTION sql_test_no_2();
DROP FUNCTION sql_test_no_3();
DROP FUNCTION sql_test_no_4();
DROP FUNCTION sql_test_no_6(int);
DROP FUNCTION plpgsql_test_1();
DROP FUNCTION plpgsql_test_2();
DROP FUNCTION plpgsql_test_3();
DROP FUNCTION plpgsql_test_4();
DROP FUNCTION plpgsql_test_5();
DROP FUNCTION plpgsql_test_6(int);
DROP FUNCTION plpgsql_test_7(text, text);
