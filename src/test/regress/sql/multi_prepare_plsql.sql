--
-- MULTI_PREPARE_PLSQL
--

-- Many of the queries are taken from other regression test files
-- and converted into both plain SQL and PL/pgsql functions, which
-- use prepared statements internally.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 780000;


CREATE FUNCTION plpgsql_test_1() RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
         SELECT
			count(*)
		FROM
			orders;

END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION plpgsql_test_2() RETURNS TABLE(count bigint) AS $$
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

CREATE FUNCTION plpgsql_test_3() RETURNS TABLE(count bigint) AS $$
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

CREATE FUNCTION plpgsql_test_4() RETURNS TABLE(count bigint) AS $$
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

CREATE FUNCTION plpgsql_test_5() RETURNS TABLE(count bigint) AS $$
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

CREATE FUNCTION plpgsql_test_6(int) RETURNS TABLE(count bigint) AS $$
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

CREATE FUNCTION plpgsql_test_7(text, text) RETURNS TABLE(supp_natadsion text, cusasdt_nation text, l_yeasdar int, sasdaum double precision) AS $$
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

-- now, run PL/pgsql functions
SELECT plpgsql_test_1();
SELECT plpgsql_test_2();

-- run PL/pgsql functions with different parameters
SELECT plpgsql_test_6(155);
SELECT plpgsql_test_6(1555);

-- test router executor parameterized PL/pgsql functions
CREATE TABLE plpgsql_table (
	key int,
	value int
);
SELECT master_create_distributed_table('plpgsql_table','key','hash');
SELECT master_create_worker_shards('plpgsql_table',4,1);

CREATE FUNCTION no_parameter_insert() RETURNS void as $$
BEGIN
	INSERT INTO plpgsql_table (key) VALUES (0);
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT no_parameter_insert();
SELECT no_parameter_insert();
SELECT no_parameter_insert();
SELECT no_parameter_insert();
SELECT no_parameter_insert();
SELECT no_parameter_insert();

CREATE FUNCTION single_parameter_insert(key_arg int)
	RETURNS void as $$
BEGIN
	INSERT INTO plpgsql_table (key) VALUES (key_arg);
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT single_parameter_insert(1);
SELECT single_parameter_insert(2);
SELECT single_parameter_insert(3);
SELECT single_parameter_insert(4);
SELECT single_parameter_insert(5);
SELECT single_parameter_insert(6);

CREATE FUNCTION double_parameter_insert(key_arg int, value_arg int)
	RETURNS void as $$
BEGIN
	INSERT INTO plpgsql_table (key, value) VALUES (key_arg, value_arg);
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT double_parameter_insert(1, 10);
SELECT double_parameter_insert(2, 20);
SELECT double_parameter_insert(3, 30);
SELECT double_parameter_insert(4, 40);
SELECT double_parameter_insert(5, 50);
SELECT double_parameter_insert(6, 60);

CREATE FUNCTION non_partition_parameter_insert(value_arg int)
	RETURNS void as $$
BEGIN
	INSERT INTO plpgsql_table (key, value) VALUES (0, value_arg);
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT non_partition_parameter_insert(10);
SELECT non_partition_parameter_insert(20);
SELECT non_partition_parameter_insert(30);
SELECT non_partition_parameter_insert(40);
SELECT non_partition_parameter_insert(50);
SELECT non_partition_parameter_insert(60);

-- check inserted values
SELECT * FROM plpgsql_table ORDER BY key, value;

-- check router executor select
CREATE FUNCTION router_partition_column_select(key_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.key = key_arg
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT router_partition_column_select(1);
SELECT router_partition_column_select(2);
SELECT router_partition_column_select(3);
SELECT router_partition_column_select(4);
SELECT router_partition_column_select(5);
SELECT router_partition_column_select(6);

CREATE FUNCTION router_non_partition_column_select(value_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.key = 0 AND
		plpgsql_table.value = value_arg
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT router_non_partition_column_select(10);
SELECT router_non_partition_column_select(20);
SELECT router_non_partition_column_select(30);
SELECT router_non_partition_column_select(40);
SELECT router_non_partition_column_select(50);
SELECT router_non_partition_column_select(60);

-- check real-time executor
CREATE FUNCTION real_time_non_partition_column_select(value_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.value = value_arg
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT real_time_non_partition_column_select(10);
SELECT real_time_non_partition_column_select(20);
SELECT real_time_non_partition_column_select(30);
SELECT real_time_non_partition_column_select(40);
SELECT real_time_non_partition_column_select(50);
SELECT real_time_non_partition_column_select(60);

CREATE FUNCTION real_time_partition_column_select(key_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.key = key_arg OR
		plpgsql_table.value = 10
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT real_time_partition_column_select(1);
SELECT real_time_partition_column_select(2);
SELECT real_time_partition_column_select(3);
SELECT real_time_partition_column_select(4);
SELECT real_time_partition_column_select(5);
SELECT real_time_partition_column_select(6);

-- check task-tracker executor
SET citus.task_executor_type TO 'task-tracker';

CREATE FUNCTION task_tracker_non_partition_column_select(value_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.value = value_arg
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT task_tracker_non_partition_column_select(10);
SELECT task_tracker_non_partition_column_select(20);
SELECT task_tracker_non_partition_column_select(30);
SELECT task_tracker_non_partition_column_select(40);
SELECT task_tracker_non_partition_column_select(50);
SELECT real_time_non_partition_column_select(60);

CREATE FUNCTION task_tracker_partition_column_select(key_arg int)
	RETURNS TABLE(key int, value int) AS $$
DECLARE
BEGIN
    RETURN QUERY
	SELECT
		plpgsql_table.key,
		plpgsql_table.value
	FROM
		plpgsql_table
	WHERE
		plpgsql_table.key = key_arg OR
		plpgsql_table.value = 10
	ORDER BY
		key,
		value;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT task_tracker_partition_column_select(1);
SELECT task_tracker_partition_column_select(2);
SELECT task_tracker_partition_column_select(3);
SELECT task_tracker_partition_column_select(4);
SELECT task_tracker_partition_column_select(5);
SELECT task_tracker_partition_column_select(6);

SET citus.task_executor_type TO 'real-time';

-- check updates
CREATE FUNCTION partition_parameter_update(int, int) RETURNS void as $$
BEGIN
	UPDATE plpgsql_table SET value = $2 WHERE key = $1;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT partition_parameter_update(1, 11);
SELECT partition_parameter_update(2, 21);
SELECT partition_parameter_update(3, 31);
SELECT partition_parameter_update(4, 41);
SELECT partition_parameter_update(5, 51);
SELECT partition_parameter_update(6, 61);

CREATE FUNCTION non_partition_parameter_update(int, int) RETURNS void as $$
BEGIN
	UPDATE plpgsql_table SET value = $2 WHERE key = 0 AND value = $1;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT non_partition_parameter_update(10, 12);
SELECT non_partition_parameter_update(20, 22);
SELECT non_partition_parameter_update(30, 32);
SELECT non_partition_parameter_update(40, 42);
SELECT non_partition_parameter_update(50, 52);
SELECT non_partition_parameter_update(60, 62);

-- check table after updates
SELECT * FROM plpgsql_table ORDER BY key, value;

-- check deletes
CREATE FUNCTION partition_parameter_delete(int, int) RETURNS void as $$
BEGIN
	DELETE FROM plpgsql_table WHERE key = $1 AND value = $2;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT partition_parameter_delete(1, 11);
SELECT partition_parameter_delete(2, 21);
SELECT partition_parameter_delete(3, 31);
SELECT partition_parameter_delete(4, 41);
SELECT partition_parameter_delete(5, 51);
SELECT partition_parameter_delete(6, 61);

CREATE FUNCTION  non_partition_parameter_delete(int) RETURNS void as $$
BEGIN
	DELETE FROM plpgsql_table WHERE key = 0 AND value = $1;
END;
$$ LANGUAGE plpgsql;

-- execute 6 times to trigger prepared statement usage
SELECT non_partition_parameter_delete(12);
SELECT non_partition_parameter_delete(22);
SELECT non_partition_parameter_delete(32);
SELECT non_partition_parameter_delete(42);
SELECT non_partition_parameter_delete(52);
SELECT non_partition_parameter_delete(62);

-- check table after deletes
SELECT * FROM plpgsql_table ORDER BY key, value;

-- check whether we can handle execute parameters
CREATE TABLE execute_parameter_test (key int, val date);
SELECT create_distributed_table('execute_parameter_test', 'key');
DO $$
BEGIN
 EXECUTE 'INSERT INTO execute_parameter_test VALUES (3, $1)' USING date '2000-01-01';
 EXECUTE 'INSERT INTO execute_parameter_test VALUES (3, $1)' USING NULL::date;
END;
$$;
DROP TABLE execute_parameter_test;

-- check whether we can handle parameters + default
CREATE TABLE func_parameter_test (
    key text NOT NULL,
    seq int4 NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (key, seq)

);
SELECT create_distributed_table('func_parameter_test', 'key');

CREATE OR REPLACE FUNCTION insert_with_max(pkey text) RETURNS VOID AS
$BODY$
    DECLARE
        max_seq int4;
    BEGIN
        SELECT MAX(seq) INTO max_seq
        FROM func_parameter_test
        WHERE func_parameter_test.key = pkey;

        IF max_seq IS NULL THEN
            max_seq := 0;
        END IF;

        INSERT INTO func_parameter_test(key, seq) VALUES (pkey, max_seq + 1);
    END;
$BODY$
LANGUAGE plpgsql;

SELECT insert_with_max('key');
SELECT insert_with_max('key');
SELECT insert_with_max('key');
SELECT insert_with_max('key');
SELECT insert_with_max('key');
SELECT insert_with_max('key');

SELECT key, seq FROM func_parameter_test ORDER BY seq;

DROP FUNCTION insert_with_max(text);
DROP TABLE func_parameter_test;

-- test prepared DDL, mainly to verify we don't mess up the query tree
SET citus.multi_shard_commit_protocol TO '2pc';
CREATE TABLE prepare_ddl (x int, y int);
SELECT create_distributed_table('prepare_ddl', 'x');

CREATE OR REPLACE FUNCTION ddl_in_plpgsql()
RETURNS VOID  AS
$BODY$
BEGIN
    CREATE INDEX prepared_index ON public.prepare_ddl(x);
    DROP INDEX prepared_index;
END;
$BODY$ LANGUAGE plpgsql;

SELECT ddl_in_plpgsql();
SELECT ddl_in_plpgsql();

-- test prepared COPY
CREATE OR REPLACE FUNCTION copy_in_plpgsql()
RETURNS VOID  AS
$BODY$
BEGIN
    COPY prepare_ddl (x) FROM PROGRAM 'echo 1' WITH CSV;
END;
$BODY$ LANGUAGE plpgsql;

SELECT copy_in_plpgsql();
SELECT copy_in_plpgsql();

DROP FUNCTION ddl_in_plpgsql();
DROP FUNCTION copy_in_plpgsql();
DROP TABLE prepare_ddl;

-- clean-up functions
DROP FUNCTION plpgsql_test_1();
DROP FUNCTION plpgsql_test_2();
DROP FUNCTION plpgsql_test_3();
DROP FUNCTION plpgsql_test_4();
DROP FUNCTION plpgsql_test_5();
DROP FUNCTION plpgsql_test_6(int);
DROP FUNCTION plpgsql_test_7(text, text);
DROP FUNCTION no_parameter_insert();
DROP FUNCTION single_parameter_insert(int);
DROP FUNCTION double_parameter_insert(int, int);
DROP FUNCTION non_partition_parameter_insert(int);
DROP FUNCTION router_partition_column_select(int);
DROP FUNCTION router_non_partition_column_select(int);
DROP FUNCTION real_time_non_partition_column_select(int);
DROP FUNCTION real_time_partition_column_select(int);
DROP FUNCTION task_tracker_non_partition_column_select(int);
DROP FUNCTION task_tracker_partition_column_select(int);
DROP FUNCTION partition_parameter_update(int, int);
DROP FUNCTION non_partition_parameter_update(int, int);
DROP FUNCTION partition_parameter_delete(int, int);
DROP FUNCTION non_partition_parameter_delete(int);
