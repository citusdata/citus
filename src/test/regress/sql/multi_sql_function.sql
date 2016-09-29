--
-- MULTI_SQL_FUNCTION
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1230000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1230000;


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

CREATE FUNCTION sql_test_no_6(integer) RETURNS bigint AS  $$
	SELECT
		count(*)
	FROM
		orders, lineitem
	WHERE
		o_orderkey = l_orderkey AND
		l_suppkey > $1;
$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT;

SET citus.task_executor_type TO 'task-tracker';
SET client_min_messages TO INFO;

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();
SELECT sql_test_no_3();
SELECT sql_test_no_4();

-- run the tests which do not require re-partition
-- with real-time executor
SET citus.task_executor_type TO 'real-time';

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();

-- plain SQL functions with parameters cannot be executed
-- FIXME: temporarily disabled, bad error message - waiting for proper
-- parametrized query support
-- SELECT sql_test_no_6(155);

-- test router executor parameterized sql functions
CREATE TABLE temp_table (
	key int,
	value int
);
SELECT master_create_distributed_table('temp_table','key','hash');
SELECT master_create_worker_shards('temp_table',4,1);

CREATE FUNCTION no_parameter_insert_sql() RETURNS void AS $$
	INSERT INTO temp_table (key) VALUES (0);
$$ LANGUAGE SQL;

-- execute 6 times
SELECT no_parameter_insert_sql();
SELECT no_parameter_insert_sql();
SELECT no_parameter_insert_sql();
SELECT no_parameter_insert_sql();
SELECT no_parameter_insert_sql();
SELECT no_parameter_insert_sql();

CREATE FUNCTION non_partition_parameter_insert_sql(int) RETURNS void AS $$
	INSERT INTO temp_table (key, value) VALUES (0, $1);
$$ LANGUAGE SQL;

-- execute 6 times
SELECT non_partition_parameter_insert_sql(10);
SELECT non_partition_parameter_insert_sql(20);
SELECT non_partition_parameter_insert_sql(30);
SELECT non_partition_parameter_insert_sql(40);
SELECT non_partition_parameter_insert_sql(50);
SELECT non_partition_parameter_insert_sql(60);

-- check inserted values
SELECT * FROM temp_table ORDER BY key, value;

-- check updates
CREATE FUNCTION non_partition_parameter_update_sql(int, int) RETURNS void AS $$
	UPDATE temp_table SET value = $2 WHERE key = 0 AND value = $1;
$$ LANGUAGE SQL;

-- execute 6 times
SELECT non_partition_parameter_update_sql(10, 12);
SELECT non_partition_parameter_update_sql(20, 22);
SELECT non_partition_parameter_update_sql(30, 32);
SELECT non_partition_parameter_update_sql(40, 42);
SELECT non_partition_parameter_update_sql(50, 52);
SELECT non_partition_parameter_update_sql(60, 62);

-- check after updates
SELECT * FROM temp_table ORDER BY key, value;

-- check deletes
CREATE FUNCTION non_partition_parameter_delete_sql(int) RETURNS void AS $$
	DELETE FROM prepare_table WHERE key = 0 AND value = $1;
$$ LANGUAGE SQL;

-- execute 6 times to trigger prepared statement usage
SELECT non_partition_parameter_delete_sql(12);
SELECT non_partition_parameter_delete_sql(22);
SELECT non_partition_parameter_delete_sql(32);
SELECT non_partition_parameter_delete_sql(42);
SELECT non_partition_parameter_delete_sql(52);
SELECT non_partition_parameter_delete_sql(62);

-- check after deletes
SELECT * FROM prepare_table ORDER BY key, value;

DROP TABLE temp_table;

-- clean-up functions
DROP FUNCTION sql_test_no_1();
DROP FUNCTION sql_test_no_2();
DROP FUNCTION sql_test_no_3();
DROP FUNCTION sql_test_no_4();
DROP FUNCTION sql_test_no_6(int);
DROP FUNCTION no_parameter_insert_sql();
DROP FUNCTION non_partition_parameter_insert_sql(int);
DROP FUNCTION non_partition_parameter_update_sql(int, int);
DROP FUNCTION non_partition_parameter_delete_sql(int);
