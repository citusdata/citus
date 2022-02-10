SET citus.log_remote_commands TO OFF;
DROP SCHEMA IF EXISTS forcepushdown_schema CASCADE;
CREATE SCHEMA forcepushdown_schema;
SET search_path TO 'forcepushdown_schema';
SET citus.shard_replication_factor = 1;
SET citus.shard_count = 32;
SET citus.next_shard_id TO 900000;

CREATE TABLE test_forcepushdown(intcol int PRIMARY KEY, data char(50) default 'default');
SELECT create_distributed_table('test_forcepushdown', 'intcol', colocate_with := 'none');

--
--Table in a different colocation group
--
CREATE TABLE test_forcepushdown_noncolocate(intcol int PRIMARY KEY);
SELECT create_distributed_table('test_forcepushdown_noncolocate', 'intcol', colocate_with := 'none');

CREATE FUNCTION insert_data(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (a);
END;
$fn$;

CREATE FUNCTION insert_data_non_distarg(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (a+1);
END;
$fn$;

CREATE FUNCTION update_data_nonlocal(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	UPDATE forcepushdown_schema.test_forcepushdown SET data = 'non-default';
END;
$fn$;

CREATE FUNCTION insert_data_noncolocation(a int)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	-- Insert into a different table than the function is colocated with
	INSERT INTO forcepushdown_schema.test_forcepushdown_noncolocate VALUES (a);
END;
$fn$;

SELECT create_distributed_function(
  'insert_data(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

SELECT create_distributed_function(
  'insert_data_non_distarg(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

SELECT create_distributed_function(
  'update_data_nonlocal(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

SELECT create_distributed_function(
  'insert_data_noncolocation(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

SET client_min_messages TO DEBUG1;
--SET citus.log_remote_commands TO on;

SELECT public.wait_until_metadata_sync(30000);

SELECT 'Transaction with no errors' Testing;
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (1);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(2);
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (3);
COMMIT;

SELECT 'Transaction with duplicate error in the remote function' Testing;
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (4);
-- This call will fail with duplicate error on the remote worker
SELECT insert_data(3);
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (5);
COMMIT;

SELECT 'Transaction with duplicate error in the local statement' Testing;
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (6);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(7);
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (8);
-- This will fail
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (8);
COMMIT;

SELECT 'Transaction with function using non-distribution argument' Testing;
BEGIN;
-- This should fail
SELECT insert_data_non_distarg(9);
COMMIT;

SELECT 'Transaction with function doing remote connection' Testing;
BEGIN;
-- This statement will pass
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (11);
-- This call will try to update rows locally and on remote node(s)
SELECT update_data_nonlocal(12);
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (13);
COMMIT;

SELECT 'Transaction with no errors but with a rollback' Testing;
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (14);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(15);
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (16);
ROLLBACK;

--
-- Add function with pushdown=true in the targetList of a query
--
BEGIN;
-- Query gets delegated to the node of the shard xx_900001 for the key=1,
-- and the function inserts value (1+17) locally on the shard xx_900031
SELECT insert_data(intcol+17) from test_forcepushdown where intcol = 1;

-- This will fail with duplicate error as the function already inserted
-- the value(1+17)
SELECT insert_data(18);
COMMIT;

--
-- Access a table with the same shard key as distribution argument but in a
-- different colocation group.
--
BEGIN;
SELECT insert_data_noncolocation(19);
COMMIT;

SELECT insert_data_noncolocation(19);

-- This should have only the first 3 rows as all other transactions were rolled back.
SELECT * FROM forcepushdown_schema.test_forcepushdown ORDER BY 1;

--
-- Nested call, function with pushdown=false calling function with pushdown=true
--
CREATE TABLE test_nested (id int, name text);
SELECT create_distributed_table('test_nested','id');
INSERT INTO test_nested VALUES (100,'hundred');
INSERT INTO test_nested VALUES (200,'twohundred');
INSERT INTO test_nested VALUES (300,'threehundred');
INSERT INTO test_nested VALUES (400,'fourhundred');
INSERT INTO test_nested VALUES (512,'fivetwelve');

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION inner_force_delegation_function(int)
RETURNS NUMERIC AS $$
DECLARE ret_val NUMERIC;
BEGIN
        SELECT max(id)::numeric+1 INTO ret_val  FROM forcepushdown_schema.test_nested WHERE id = $1;
	RAISE NOTICE 'inner_force_delegation_function():%', ret_val;
        RETURN ret_val;
END;
$$  LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

CREATE OR REPLACE FUNCTION func_calls_forcepush_func()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
BEGIN
	-- Constant distribution argument
	SELECT inner_force_delegation_function INTO incremented_val FROM inner_force_delegation_function(100);
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;

SELECT create_distributed_function('func_calls_forcepush_func()');
SELECT create_distributed_function('inner_force_delegation_function(int)', '$1', colocate_with := 'test_nested', force_delegation := true);
SELECT public.wait_until_metadata_sync(30000);

BEGIN;
SELECT func_calls_forcepush_func();
COMMIT;

SELECT func_calls_forcepush_func();

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION get_val()
RETURNS INT AS $$
BEGIN
        RETURN 100::INT;
END;
$$  LANGUAGE plpgsql;

--
-- UDF calling another UDF in a FROM clause
-- fn()
-- {
--   select res into var from fn();
-- }
--
CREATE OR REPLACE FUNCTION func_calls_forcepush_func_infrom()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
DECLARE add_val INT;
BEGIN
	add_val := get_val();
	SELECT inner_force_delegation_function INTO incremented_val FROM inner_force_delegation_function(add_val + 100);
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

SELECT func_calls_forcepush_func_infrom();

BEGIN;
SELECT func_calls_forcepush_func_infrom();
COMMIT;

--
-- UDF calling another UDF in the SELECT targetList
-- fn()
-- {
--   select fn() into var;
-- }
--
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION func_calls_forcepush_func_intarget()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
DECLARE add_val INT;
BEGIN
	add_val := get_val();
	SELECT inner_force_delegation_function(100 + 100) INTO incremented_val OFFSET 0;
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

SELECT func_calls_forcepush_func_intarget();

BEGIN;
SELECT func_calls_forcepush_func_intarget();
COMMIT;

--
-- Recursive function call with pushdown=true
--
CREATE OR REPLACE FUNCTION test_recursive(inp integer)
RETURNS INT AS $$
DECLARE var INT;
BEGIN
	RAISE NOTICE 'input:%', inp;
	if (inp > 1) then
		inp := inp - 1;
		var := forcepushdown_schema.test_recursive(inp);
		RETURN var;
	else
		RETURN inp;
	END if;
END;
$$  LANGUAGE plpgsql;

SELECT create_distributed_function('test_recursive(int)', '$1', colocate_with := 'test_nested', force_delegation := true);

BEGIN;
SELECT test_recursive(5);
END;

--
-- Distributed function gets delegated indirectly (as part of a query)
--
BEGIN;

-- Query lands on the shard with key = 300(shard __900089) and the function inserts locally
SELECT inner_force_delegation_function(id) FROM test_nested WHERE id = 300;

-- Query lands on the shard with key = 300(shard __900089) and the function inserts remotely
SELECT insert_data_non_distarg(id) FROM test_nested WHERE id = 300;

END;

--
-- Non constant distribution arguments
--

-- Param(PARAM_EXEC) node e.g. SELECT fn((SELECT col from test_nested where col=val))
BEGIN;
SELECT inner_force_delegation_function((SELECT id+112 FROM test_nested WHERE id=400));
END;

CREATE OR REPLACE FUNCTION test_non_constant(x int, y bigint)
RETURNS int
AS $$
DECLARE
BEGIN
        RAISE NOTICE 'test_non_constant: % %', x, y;
        RETURN x + y;
END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function(
'test_non_constant(int,bigint)',
'$1',
colocate_with := 'test_forcepushdown',
force_delegation := true);

SELECT count(*) FROM test_nested;

-- Result should print 99, count(*) from test_nested
WITH c AS (SELECT count(*) FROM test_nested),
		     b as (SELECT test_non_constant(99::int, (SELECT COUNT FROM c)))
			SELECT COUNT(*) FROM b;

CREATE TABLE emp (
    empname           text NOT NULL,
    salary            integer
);

CREATE TABLE emp_audit(
    operation         char(1)   NOT NULL,
    stamp             timestamp NOT NULL,
    userid            text      NOT NULL,
    empname           text      NOT NULL,
    salary integer
);

SELECT create_distributed_table('emp','empname');
SELECT create_distributed_table('emp_audit','empname');

CREATE OR REPLACE FUNCTION inner_emp(empname text)
RETURNS void
AS $$
DECLARE
BEGIN
    INSERT INTO emp VALUES (empname, 33);
END;
$$ LANGUAGE plpgsql;
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION outer_emp()
RETURNS void
AS $$
DECLARE
BEGIN
    PERFORM inner_emp('hello');
END;
$$ LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

SELECT create_distributed_function('inner_emp(text)','empname', force_delegation := true);
SELECT outer_emp();
SELECT * from emp;

--
-- INSERT..SELECT
--
CREATE FUNCTION insert_select_data(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown SELECT(a+1);
END;
$fn$;

SELECT create_distributed_function(
  'insert_select_data(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

-- Function lands on worker1 and issues COPY ... INSERT on the worker2 into the shard_900021
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (30);
-- This will fail
SELECT insert_select_data(20);
COMMIT;

-- Function lands on worker2 and issues COPY ... INSERT on the same node into the shard_900029
BEGIN;
-- This will pass
SELECT insert_select_data(21);
END;

-- Function lands on worker2 and issues COPY ... INSERT on the worker1 into the shard_900028
BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (30);
-- This will fail
SELECT insert_select_data(22);
END;

-- Functions lands on worker1 and issues COPY ... INSERT on the worker2 into the shard_900021
-- This will pass as there is no surrounding transaction
SELECT insert_select_data(20);

-- (21+1) and (20+1) should appear
SELECT * FROM forcepushdown_schema.test_forcepushdown ORDER BY 1;

CREATE FUNCTION insert_select_data_nonlocal(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown(intcol)
		SELECT intcol FROM forcepushdown_schema.test_forcepushdown_noncolocate;
END;
$fn$;

SELECT create_distributed_function(
  'insert_select_data_nonlocal(int)', 'a',
  colocate_with := 'test_forcepushdown',
  force_delegation := true
);

INSERT INTO forcepushdown_schema.test_forcepushdown_noncolocate VALUES (30);
INSERT INTO forcepushdown_schema.test_forcepushdown_noncolocate VALUES (31);
INSERT INTO forcepushdown_schema.test_forcepushdown_noncolocate VALUES (32);

BEGIN;
INSERT INTO forcepushdown_schema.test_forcepushdown VALUES (40);
-- This will fail
SELECT insert_select_data_nonlocal(41);
COMMIT;

-- Above 3 rows (30, 31, 32) should appear now
SELECT insert_select_data_nonlocal(40);

SELECT * FROM forcepushdown_schema.test_forcepushdown ORDER BY 1;

CREATE TABLE test_forcepushdown_char(data char(50) PRIMARY KEY);
SELECT create_distributed_table('test_forcepushdown_char', 'data', colocate_with := 'none');
CREATE TABLE test_forcepushdown_varchar(data varchar PRIMARY KEY);
SELECT create_distributed_table('test_forcepushdown_varchar', 'data', colocate_with := 'none');
CREATE TABLE test_forcepushdown_text(data text PRIMARY KEY);
SELECT create_distributed_table('test_forcepushdown_text', 'data', colocate_with := 'none');

CREATE FUNCTION insert_data_char(a char(50))
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown_char VALUES (a);
END;
$fn$;
SELECT create_distributed_function(
  'insert_data_char(char)', 'a',
  colocate_with := 'test_forcepushdown_char',
  force_delegation := true
);

CREATE FUNCTION insert_data_varchar(a varchar)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown_varchar VALUES (a);
END;
$fn$;
SELECT create_distributed_function(
  'insert_data_varchar(varchar)', 'a',
  colocate_with := 'test_forcepushdown_varchar',
  force_delegation := true
);

CREATE FUNCTION insert_data_text(a text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO forcepushdown_schema.test_forcepushdown_text VALUES (a);
END;
$fn$;
SELECT create_distributed_function(
  'insert_data_text(text)', 'a',
  colocate_with := 'test_forcepushdown_text',
  force_delegation := true
);

SELECT insert_data_varchar('VARCHAR');
BEGIN;
SELECT insert_data_varchar('VARCHAR2');
COMMIT;

SELECT insert_data_text('TEXT');
BEGIN;
SELECT insert_data_text('TEXT2');
COMMIT;

-- Char is failing as the datatype is represented differently in the
-- PL/PgSQL and the exec engine.
SELECT insert_data_char('CHAR');
BEGIN;
SELECT insert_data_char('CHAR');
COMMIT;

SELECT * FROM test_forcepushdown_char ORDER BY 1;
SELECT * FROM test_forcepushdown_varchar ORDER BY 1;
SELECT * FROM test_forcepushdown_text ORDER BY 1;

-- Test sub query
CREATE TABLE test_subquery(data int, result int);
SELECT create_distributed_table('test_subquery', 'data', colocate_with := 'none');

CREATE TABLE test_non_colocated(id int);
SELECT create_distributed_table('test_non_colocated', 'id', colocate_with := 'none');

CREATE FUNCTION select_data(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE var INT;
BEGIN
        SELECT result INTO var FROM forcepushdown_schema.test_subquery WHERE data =
		(SELECT data FROM forcepushdown_schema.test_subquery WHERE data = a);
	RAISE NOTICE 'Result: %', var;
END;
$fn$;
SELECT create_distributed_function(
  'select_data(int)', 'a',
  colocate_with := 'test_subquery',
  force_delegation := true
);

CREATE FUNCTION select_data_noncolocate(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE var INT;
BEGIN
	-- Key is the same but colocation ID is different
        SELECT data INTO var FROM forcepushdown_schema.test_subquery WHERE data =
		(SELECT id FROM forcepushdown_schema.test_non_colocated WHERE id = a);
	RAISE NOTICE 'Result: %', var;
END;
$fn$;
SELECT create_distributed_function(
  'select_data_noncolocate(int)', 'a',
  colocate_with := 'test_subquery',
  force_delegation := true
);

CREATE FUNCTION insert_select_data_cte1(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE var INT;
BEGIN
	WITH ins AS (INSERT INTO forcepushdown_schema.test_subquery VALUES (a) RETURNING data)
			SELECT ins.data INTO var FROM ins;
	RAISE NOTICE 'Result: %', var;
END;
$fn$;
SELECT create_distributed_function(
  'insert_select_data_cte1(int)', 'a',
  colocate_with := 'test_subquery',
  force_delegation := true
);

CREATE FUNCTION insert_select_data_cte2(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE var INT;
BEGIN
	WITH ins AS (INSERT INTO forcepushdown_schema.test_subquery VALUES (a) RETURNING data)
		SELECT ins.data INTO var FROM forcepushdown_schema.test_subquery, ins WHERE forcepushdown_schema.test_subquery.data = a;
	RAISE NOTICE 'Result: %', var;
END;
$fn$;
SELECT create_distributed_function(
  'insert_select_data_cte2(int)', 'a',
  colocate_with := 'test_subquery',
  force_delegation := true
);

CREATE FUNCTION insert_data_cte_nondist(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE var INT;
BEGIN
	-- Inserting a non-distribution argument (a+1)
	WITH ins AS (INSERT INTO forcepushdown_schema.test_subquery VALUES (a+1) RETURNING data)
		SELECT ins.data INTO var FROM forcepushdown_schema.test_subquery, ins WHERE forcepushdown_schema.test_subquery.data = a;
	RAISE NOTICE 'Result: %', var;
END;
$fn$;
SELECT create_distributed_function(
  'insert_data_cte_nondist(int)', 'a',
  colocate_with := 'test_subquery',
  force_delegation := true
);

INSERT INTO forcepushdown_schema.test_subquery VALUES(100, -1);

-- This should pass
SELECT select_data(100);
BEGIN;
SELECT select_data(100);
END;

-- This should fail
SELECT select_data_noncolocate(100);
BEGIN;
SELECT select_data_noncolocate(100);
END;

-- This should pass
SELECT insert_select_data_cte1(200);
BEGIN;
SELECT insert_select_data_cte1(200);
COMMIT;

-- This should pass
SELECT insert_select_data_cte2(300);
BEGIN;
SELECT insert_select_data_cte2(300);
COMMIT;

-- This should fail
SELECT insert_data_cte_nondist(400);
BEGIN;
SELECT insert_data_cte_nondist(400);
COMMIT;

-- Rows 100, 200, 300 should be seen
SELECT * FROM forcepushdown_schema.test_subquery ORDER BY 1;

-- Query with targetList greater than 1

-- Function from FROM clause is not delegated outside of a BEGIN (for now)
SELECT 1,2,3 FROM select_data(100);
BEGIN;
-- Function from FROM clause is delegated
SELECT 1,2,3 FROM select_data(100);
END;

-- Test prepared statements
CREATE TABLE table_test_prepare(i int, j bigint);
SELECT create_distributed_table('table_test_prepare', 'i', colocate_with := 'none');

DROP FUNCTION test_prepare(int, int);
CREATE OR REPLACE FUNCTION test_prepare(x int, y int)
RETURNS bigint
AS $$
DECLARE
BEGIN
    INSERT INTO forcepushdown_schema.table_test_prepare VALUES (x, y);
    INSERT INTO forcepushdown_schema.table_test_prepare VALUES (y, x);
	RETURN x + y;
END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function('test_prepare(int,int)','x',force_delegation :=true, colocate_with := 'table_test_prepare');

DROP FUNCTION outer_test_prepare(int, int);
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION outer_test_prepare(x int, y int)
RETURNS void
AS $$
DECLARE
    v int;
BEGIN
    PERFORM FROM test_prepare(x, y);
    PERFORM 1, 1 + a FROM test_prepare(x + 1, y + 1) a;
END;
$$ LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

-- First 5 get delegated and succeeds
BEGIN;
SELECT outer_test_prepare(1,1);
SELECT outer_test_prepare(1,1);
SELECT outer_test_prepare(1,1);
SELECT outer_test_prepare(1,1);
SELECT outer_test_prepare(1,1);
-- All the above gets delegated and should see 5 * 4 rows
SELECT COUNT(*) FROM table_test_prepare;
-- 6th execution will be generic plan and should get delegated
SELECT outer_test_prepare(1,1);
SELECT outer_test_prepare(1,1);
END;

-- Fails as expected
SELECT outer_test_prepare(1,2);

SELECT COUNT(*) FROM table_test_prepare;

CREATE TABLE test_perform(i int);
SELECT create_distributed_table('test_perform', 'i', colocate_with := 'none');

CREATE OR REPLACE FUNCTION test(x int)
RETURNS int
AS $$
DECLARE
BEGIN
    RAISE NOTICE 'INPUT %', x;
    RETURN x;
END;
$$ LANGUAGE plpgsql;

SELECT create_distributed_function('test(int)', 'x',
                colocate_with := 'test_perform', force_delegation := true);
DO $$
BEGIN
    PERFORM test(3);
END;
$$ LANGUAGE plpgsql;

RESET client_min_messages;
SET citus.log_remote_commands TO off;
DROP SCHEMA forcepushdown_schema CASCADE;
