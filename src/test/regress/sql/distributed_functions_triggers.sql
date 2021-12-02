SET citus.log_remote_commands TO OFF;
DROP SCHEMA IF EXISTS distributed_functions_triggers CASCADE;
CREATE SCHEMA distributed_functions_triggers;
SET search_path TO 'distributed_functions_triggers';
SET citus.shard_replication_factor = 1;
SET citus.shard_count = 32;
SET citus.next_shard_id TO 900000;

CREATE TABLE test_txn_dist(intcol int PRIMARY KEY, data char(50) default 'default');
SELECT create_distributed_table('test_txn_dist', 'intcol', colocate_with := 'none');

CREATE FUNCTION insert_data(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (a);
END;
$fn$;

CREATE FUNCTION insert_data_non_distarg(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (a+1);
END;
$fn$;

CREATE FUNCTION update_data_nonlocal(a integer)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	UPDATE distributed_functions_triggers.test_txn_dist SET data = 'non-default';
END;
$fn$;

SELECT create_distributed_function(
  'insert_data(int)', 'a',
  colocate_with := 'test_txn_dist',
  force_pushdown := true
);

SELECT create_distributed_function(
  'insert_data_non_distarg(int)', 'a',
  colocate_with := 'test_txn_dist',
  force_pushdown := true
);

SELECT create_distributed_function(
  'update_data_nonlocal(int)', 'a',
  colocate_with := 'test_txn_dist',
  force_pushdown := true
);

SET client_min_messages TO DEBUG1;
--SET citus.log_remote_commands TO on;

SELECT public.wait_until_metadata_sync(30000);

SELECT 'Transaction with no errors' Testing;
BEGIN;
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (1);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(2);
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (3);
COMMIT;

SELECT 'Transaction with duplicate error in the remote function' Testing;
BEGIN;
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (4);
-- This call will fail with duplicate error on the remote worker
SELECT insert_data(3);
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (5);
COMMIT;

SELECT 'Transaction with duplicate error in the local statement' Testing;
BEGIN;
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (6);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(7);
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (8);
-- This will fail
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (8);
COMMIT;

SELECT 'Transaction with function using non-distribution argument' Testing;
BEGIN;
-- This should fail
SELECT insert_data_non_distarg(9);
COMMIT;

SELECT 'Transaction with function doing remote connection' Testing;
BEGIN;
-- This statement will pass
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (11);
-- This call will try to update rows locally and on a different node
SELECT update_data_nonlocal(12);
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (13);
COMMIT;

SELECT 'Transaction with no errors but with a rollback' Testing;
BEGIN;
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (14);
-- This call will insert both the rows locally on the remote worker
SELECT insert_data(15);
INSERT INTO distributed_functions_triggers.test_txn_dist VALUES (16);
ROLLBACK;

--
-- Add function with pushdown=true in the targetList of a query
--
BEGIN;
SELECT insert_data(intcol+17) from test_txn_dist where intcol = 1;
SELECT insert_data(18);
COMMIT;

-- This should have only the first 3 rows as all other transactions were rolled back.
SELECT * FROM distributed_functions_triggers.test_txn_dist ORDER BY 1;

--
-- Nested call, function with pushdown=false calling function with pushdown=true
--
CREATE TABLE test_nested (id int, name text);
SELECT create_distributed_table('test_nested','id');
INSERT INTO test_nested VALUES (100,'hundred');
INSERT INTO test_nested VALUES (200,'twohundred');
INSERT INTO test_nested VALUES (300,'threehundred');
INSERT INTO test_nested VALUES (400,'fourhundred');

CREATE OR REPLACE FUNCTION inner_force_pushdown_function(int)
RETURNS NUMERIC AS $$
DECLARE ret_val NUMERIC;
BEGIN
        SELECT max(id)::numeric+1 INTO ret_val  FROM distributed_functions_triggers.test_nested WHERE id = $1;
	RAISE NOTICE 'inner_force_pushdown_function():%', ret_val;
        RETURN ret_val;
END;
$$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_calls_forcepush_func()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
BEGIN
	-- Constant distribution argument
	SELECT inner_force_pushdown_function INTO incremented_val FROM inner_force_pushdown_function(100);
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;

SELECT create_distributed_function('func_calls_forcepush_func()');
SELECT create_distributed_function('inner_force_pushdown_function(int)', '$1', colocate_with := 'test_nested', force_pushdown := true);
SELECT public.wait_until_metadata_sync(30000);

BEGIN;
SELECT func_calls_forcepush_func();
COMMIT;

SELECT func_calls_forcepush_func();

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
	SELECT inner_force_pushdown_function INTO incremented_val FROM inner_force_pushdown_function(add_val + 100);
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;

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
CREATE OR REPLACE FUNCTION func_calls_forcepush_func_intarget()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
DECLARE add_val INT;
BEGIN
	add_val := get_val();
	SELECT inner_force_pushdown_function(100 + 100) INTO incremented_val OFFSET 0;
	RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;

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
		var := distributed_functions_triggers.test_recursive(inp);
		RETURN var;
	else
		RETURN inp;
	END if;
END;
$$  LANGUAGE plpgsql;

SELECT create_distributed_function('test_recursive(int)', '$1', colocate_with := 'test_nested', force_pushdown := true);

BEGIN;
SELECT test_recursive(5);
END;

--
-- Non constant distribution arguments
--

-- Var node e.g. select fn(col) from table where col=150;
BEGIN;
SELECT inner_force_pushdown_function(id) FROM test_nested WHERE id = 300;
END;

-- Param(PARAM_EXEC) node e.g. SELECT fn((SELECT col from test_nested where col=val))
BEGIN;
SELECT inner_force_pushdown_function((SELECT id FROM test_nested WHERE id=400));
END;

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
CREATE OR REPLACE FUNCTION outer_emp()
RETURNS void
AS $$
DECLARE
BEGIN
    PERFORM inner_emp('hello');
END;
$$ LANGUAGE plpgsql;

SELECT create_distributed_function('inner_emp(text)','empname', force_pushdown := true);
SELECT outer_emp();
SELECT * from emp;

--
-- Test citus.enable_unsafe_triggers
-- Enables arbitrary triggers on distributed tables
--
/* CDC triggers with monotonically increasing change ID per shard key */

/* table containing all objects (collection) */
CREATE TABLE data (
    shard_key text not null,
    object_id text not null,
    value jsonb not null
);
ALTER TABLE data
ADD CONSTRAINT data_pk
PRIMARY KEY (shard_key, object_id);

/* table of changes */
CREATE TABLE data_changes (
    shard_key text not null,
    object_id text not null,
    change_id bigint not null,
    change_time timestamptz default now(),
    operation_type text not null,
    new_value jsonb
);
ALTER TABLE data_changes
ADD CONSTRAINT data_changes_pk
PRIMARY KEY (shard_key, object_id, change_id);

SELECT create_distributed_table('data', 'shard_key');
SELECT create_distributed_table('data_changes', 'shard_key', colocate_with := 'data');

SET citus.enable_unsafe_triggers TO true;
SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_unsafe_triggers TO true;');
SELECT run_command_on_workers('SELECT pg_reload_conf();');

/* trigger function that is called after any change */
CREATE OR REPLACE FUNCTION record_change()
RETURNS trigger
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    IF (TG_OP = 'DELETE') THEN
      /* get the last change ID for object key in OLD via index(-only) scan */
      SELECT change_id INTO last_change_id
      FROM distributed_functions_triggers.data_changes
      WHERE shard_key = OLD.shard_key AND object_id = OLD.object_id
      ORDER BY change_id DESC LIMIT 1;

      /* insert a change record for the delete */
      INSERT INTO distributed_functions_triggers.data_changes (shard_key, object_id, change_id, operation_type)
      VALUES (OLD.shard_key, OLD.object_id, COALESCE(last_change_id + 1, 1), TG_OP);
    ELSE
      /* get the last change ID for object key in NEW via index(-only) scan */
      SELECT change_id INTO last_change_id
      FROM distributed_functions_triggers.data_changes
      WHERE shard_key = NEW.shard_key AND object_id = NEW.object_id
      ORDER BY change_id DESC LIMIT 1;

      /* insert a change record for the insert/update */
      INSERT INTO distributed_functions_triggers.data_changes (shard_key, object_id, change_id, operation_type, new_value)
      VALUES (NEW.shard_key, NEW.object_id, COALESCE(last_change_id + 1, 1), TG_OP, NEW.value);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_functions_triggers.record_change();
SELECT public.wait_until_metadata_sync(30000);

INSERT INTO data VALUES ('hello','world','{"hello":"world"}');
INSERT INTO data VALUES ('hello2','world2','{"hello2":"world2"}');
DELETE FROM data where shard_key = 'hello';
BEGIN;
UPDATE data SET value = '{}';
END;

--
-- Run bad triggers
--
CREATE FUNCTION insert_document(key text, id text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO distributed_functions_triggers.data VALUES (key, id, '{"id1":"id2"}');
END;
$fn$;

SELECT create_distributed_function(
  'insert_document(text, text)', 'key',
  colocate_with := 'data',
  force_pushdown := true
);

CREATE OR REPLACE FUNCTION bad_shardkey_record_change()
RETURNS trigger
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    INSERT INTO distributed_functions_triggers.data_changes (shard_key, object_id, change_id, operation_type, new_value)
    VALUES ('BAD', NEW.object_id, COALESCE(last_change_id + 1, 1), TG_OP, NEW.value);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bad_shardkey_record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_functions_triggers.bad_shardkey_record_change();
SELECT public.wait_until_metadata_sync(30000);

SELECT insert_document('hello3', 'world3');

DROP TRIGGER bad_shardkey_record_change_trigger ON data;

CREATE OR REPLACE FUNCTION remote_shardkey_record_change()
RETURNS trigger
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    UPDATE distributed_functions_triggers.data_changes SET operation_type = TG_OP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER remote_shardkey_record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_functions_triggers.remote_shardkey_record_change();
SELECT public.wait_until_metadata_sync(30000);

BEGIN;
SELECT insert_document('hello4', 'world4');
END;

SELECT insert_document('hello4', 'world4');

SELECT * FROM data;
SELECT shard_key, object_id, change_id, operation_type, new_value FROM data_changes ORDER BY change_time;

--
-- Triggers which are not distributed
--
CREATE TABLE emptest (
    empname           text NOT NULL,
    salary            integer
);

CREATE TABLE emptest_audit(
    operation         char(1)   NOT NULL,
    stamp             timestamp NOT NULL,
    userid            text      NOT NULL,
    empname           text      NOT NULL,
    salary integer
);

SELECT create_distributed_table('emptest','empname');
SELECT create_distributed_table('emptest_audit','empname');

CREATE OR REPLACE FUNCTION process_emp_audit() RETURNS TRIGGER AS $emp_audit$
    BEGIN
        --
        -- Create a row in emp_audit to reflect the operation performed on emp,
        -- making use of the special variable TG_OP to work out the operation.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO distributed_functions_triggers.emptest_audit SELECT 'D', now(), user, OLD.*;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO distributed_functions_triggers.emptest_audit SELECT 'U', now(), user, NEW.*;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO distributed_functions_triggers.emptest_audit SELECT 'I', now(), user, NEW.*;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$emp_audit$ LANGUAGE plpgsql;

CREATE TRIGGER emptest_audit
AFTER INSERT OR UPDATE OR DELETE ON emptest
    FOR EACH ROW EXECUTE FUNCTION distributed_functions_triggers.process_emp_audit();

INSERT INTO emptest VALUES ('test', 1);
SELECT operation, userid, empname, salary FROM emptest_audit;
DELETE from emptest;
SELECT operation, userid, empname, salary FROM emptest_audit;

RESET client_min_messages;
SET citus.enable_unsafe_triggers TO false;
SET citus.log_remote_commands TO off;

DROP SCHEMA distributed_functions_triggers CASCADE;
