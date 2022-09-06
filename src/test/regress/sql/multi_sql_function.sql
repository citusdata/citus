--
-- MULTI_SQL_FUNCTION
--

SET citus.next_shard_id TO 1230000;


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

SET client_min_messages TO INFO;

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();
SELECT sql_test_no_3();
SELECT sql_test_no_4();

-- run the tests which do not require re-partition
-- with real-time executor

-- now, run plain SQL functions
SELECT sql_test_no_1();
SELECT sql_test_no_2();

-- test router executor parameterized sql functions
CREATE TABLE temp_table (
	key int,
	value int
);
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('temp_table','key','hash');

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
	DELETE FROM temp_table WHERE key = 0 AND value = $1;
$$ LANGUAGE SQL;

-- execute 6 times to trigger prepared statement usage
SELECT non_partition_parameter_delete_sql(12);
SELECT non_partition_parameter_delete_sql(22);
SELECT non_partition_parameter_delete_sql(32);
SELECT non_partition_parameter_delete_sql(42);
SELECT non_partition_parameter_delete_sql(52);
SELECT non_partition_parameter_delete_sql(62);

-- check after deletes
SELECT * FROM temp_table ORDER BY key, value;

-- test running parameterized SQL function
CREATE TABLE test_parameterized_sql(id integer, org_id integer);
select create_distributed_table('test_parameterized_sql','org_id');

CREATE OR REPLACE FUNCTION test_parameterized_sql_function(org_id_val integer)
RETURNS TABLE (a bigint)
AS $$
    SELECT count(*) AS count_val from test_parameterized_sql where org_id = org_id_val;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION test_parameterized_sql_function_in_subquery_where(org_id_val integer)
RETURNS TABLE (a bigint)
AS $$
    SELECT count(*) AS count_val from test_parameterized_sql as t1 where
    org_id IN (SELECT org_id FROM test_parameterized_sql as t2 WHERE t2.org_id = t1.org_id AND org_id = org_id_val);
$$ LANGUAGE SQL STABLE;


INSERT INTO test_parameterized_sql VALUES(1, 1);

-- all of them should fail
SELECT * FROM test_parameterized_sql_function(1);

SELECT (SELECT 1 FROM test_parameterized_sql limit 1) FROM test_parameterized_sql_function(1);

SELECT test_parameterized_sql_function_in_subquery_where(1);

-- postgres behaves slightly differently for the following
-- query where the target list is empty
SELECT test_parameterized_sql_function(1);

-- test that sql function calls are treated as multi-statement transactions
-- and are rolled back properly. Single-row inserts for not-replicated tables
-- don't go over 2PC if they are not part of a bigger transaction.
CREATE TABLE table_with_unique_constraint (a int UNIQUE);
SELECT create_distributed_table('table_with_unique_constraint', 'a');

INSERT INTO table_with_unique_constraint VALUES (1), (2), (3);

CREATE OR REPLACE FUNCTION insert_twice() RETURNS VOID
AS $$
  INSERT INTO table_with_unique_constraint VALUES (4);
  INSERT INTO table_with_unique_constraint VALUES (4);
$$ LANGUAGE SQL;

SELECT insert_twice();

SELECT * FROM table_with_unique_constraint ORDER BY a;

DROP TABLE temp_table, test_parameterized_sql, table_with_unique_constraint;

-- clean-up functions
DROP FUNCTION sql_test_no_1();
DROP FUNCTION sql_test_no_2();
DROP FUNCTION sql_test_no_3();
DROP FUNCTION sql_test_no_4();
DROP FUNCTION no_parameter_insert_sql();
DROP FUNCTION non_partition_parameter_insert_sql(int);
DROP FUNCTION non_partition_parameter_update_sql(int, int);
DROP FUNCTION non_partition_parameter_delete_sql(int);
DROP FUNCTION test_parameterized_sql_function(int);
DROP FUNCTION test_parameterized_sql_function_in_subquery_where(int);
