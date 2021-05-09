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

create schema args_test_function;
set search_path to public, args_test_function;

CREATE OR REPLACE FUNCTION args_test_function(
IN in1 integer,  IN in2 integer, IN in3 integer, IN in4 integer, IN in5 integer, IN in6 integer, IN in7 integer, IN in8 integer, IN in9 integer, IN in10 integer,
IN in11 integer,  IN in12 integer, IN in13 integer, IN in14 integer, IN in15 integer, IN in16 integer, IN in17 integer, IN in18 integer, IN in19 integer, IN in20 integer,
IN in21 integer,  IN in22 integer, IN in23 integer, IN in24 integer, IN in25 integer, IN in26 integer, IN in27 integer, IN in28 integer, IN in29 integer, IN in30 integer,
IN in31 integer,  IN in32 integer, IN in33 integer, IN in34 integer, IN in35 integer, IN in36 integer, IN in37 integer, IN in38 integer, IN in39 integer, IN in40 integer,
IN in41 integer,  IN in42 integer, IN in43 integer, IN in44 integer, IN in45 integer, IN in46 integer, IN in47 integer, IN in48 integer, IN in49 integer, IN in50 integer,
IN in51 integer,  IN in52 integer, IN in53 integer, IN in54 integer, IN in55 integer, IN in56 integer, IN in57 integer, IN in58 integer, IN in59 integer, IN in60 integer,
IN in61 integer,  IN in62 integer, IN in63 integer, IN in64 integer, IN in65 integer, IN in66 integer, IN in67 integer, IN in68 integer, IN in69 integer, IN in70 integer,
IN in71 integer,  IN in72 integer, IN in73 integer, IN in74 integer, IN in75 integer, IN in76 integer, IN in77 integer, IN in78 integer, IN in79 integer, IN in80 integer,
IN in81 integer,  IN in82 integer, IN in83 integer, IN in84 integer, IN in85 integer, IN in86 integer, IN in87 integer, IN in88 integer, IN in89 integer, IN in90 integer,
IN in91 integer,  IN in92 integer, IN in93 integer, IN in94 integer, IN in95 integer, IN in96 integer, IN in97 integer, IN in98 integer, IN in99 integer, IN in100 integer,
OUT out1 integer)
    LANGUAGE 'plpgsql'
AS $BODY$
begin
   out1 = 1;
end $BODY$;

drop schema args_test_function cascade;

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

