-- This test file includes tests to show that we do not allow triggers
-- on citus tables. Note that in other regression tests, we already test
-- the successfull citus table creation cases.

\set VERBOSITY terse

SET citus.next_shard_id TO 1505000;

CREATE SCHEMA table_triggers_schema;
SET search_path TO table_triggers_schema;

-------------------------------------------------------------
-- show that we do not allow trigger creation on citus tables
-------------------------------------------------------------

-- create a simple function to be invoked by triggers
CREATE FUNCTION update_value() RETURNS trigger AS $update_value$
BEGIN
    NEW.value := value+1 ;
    RETURN NEW;
END;
$update_value$ LANGUAGE plpgsql;

CREATE TABLE distributed_table (value int);
SELECT create_distributed_table('distributed_table', 'value');

CREATE TABLE reference_table (value int);
SELECT create_reference_table('reference_table');

-- below two should fail
CREATE TRIGGER update_value_dist
AFTER INSERT ON distributed_table
FOR EACH ROW EXECUTE PROCEDURE update_value();

CREATE TRIGGER update_value_ref
AFTER INSERT ON reference_table
FOR EACH ROW EXECUTE PROCEDURE update_value();

---------------------------------------------------------
-- show that we do not allow creating citus tables if the
-- table has already triggers
---------------------------------------------------------

CREATE TABLE distributed_table_1 (value int);

CREATE TRIGGER update_value_dist
AFTER INSERT ON distributed_table_1
FOR EACH ROW EXECUTE PROCEDURE update_value();

CREATE TABLE reference_table_1 (value int);

CREATE TRIGGER update_value_ref
AFTER INSERT ON reference_table_1
FOR EACH ROW EXECUTE PROCEDURE update_value();

-- below two should fail
SELECT create_distributed_table('distributed_table_1', 'value');
SELECT create_reference_table('reference_table_1');

-------------------------------------------------
-- test deparse logic for CREATE TRIGGER commands
-- via master_get_table_ddl_events
-------------------------------------------------

CREATE TABLE test_table (
    id int,
    text_number text,
    text_col text
);

CREATE FUNCTION test_table_trigger_function() RETURNS trigger AS $test_table_trigger_function$
BEGIN
    RAISE EXCEPTION 'a meaningless exception';
END;
$test_table_trigger_function$ LANGUAGE plpgsql;

-- in below two, use constraint triggers to test DEFERRABLE | NOT DEFERRABLE syntax
CREATE CONSTRAINT TRIGGER test_table_update
    AFTER UPDATE OF id ON test_table
    NOT DEFERRABLE
    FOR EACH ROW
    WHEN (OLD.* IS NOT DISTINCT FROM NEW.* AND OLD.text_number IS NOT NULL)
    EXECUTE FUNCTION test_table_trigger_function();

CREATE CONSTRAINT TRIGGER test_table_insert
    AFTER INSERT ON test_table
    DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW
    WHEN (NEW.id > 5 OR NEW.text_col IS NOT NULL AND NEW.id < to_number(NEW.text_number, '9999'))
    EXECUTE FUNCTION test_table_trigger_function();

CREATE TRIGGER test_table_delete
    AFTER DELETE ON test_table
    FOR EACH STATEMENT
    EXECUTE FUNCTION test_table_trigger_function();

SELECT master_get_table_ddl_events('test_table');

-- cleanup at exit
DROP SCHEMA table_triggers_schema CASCADE;
