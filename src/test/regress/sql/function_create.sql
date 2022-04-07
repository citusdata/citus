CREATE SCHEMA function_create;
SET search_path TO function_create;

-- helper function to verify the function of a coordinator is the same on all workers
CREATE OR REPLACE FUNCTION verify_function_is_same_on_workers(funcname text)
    RETURNS bool
    LANGUAGE plpgsql
AS $func$
DECLARE
    coordinatorSql text;
    workerSql text;
BEGIN
    SELECT pg_get_functiondef(funcname::regprocedure) INTO coordinatorSql;
    FOR workerSql IN SELECT result FROM run_command_on_workers('SELECT pg_get_functiondef(' || quote_literal(funcname) || '::regprocedure)') LOOP
            IF workerSql != coordinatorSql THEN
                RAISE INFO 'functions are different, coordinator:% worker:%', coordinatorSql, workerSql;
                RETURN false;
            END IF;
        END LOOP;

    RETURN true;
END;
$func$;

-- test delegating function calls
CREATE TABLE warnings (
    id int primary key,
    message text
);
SELECT create_distributed_table('warnings', 'id');
INSERT INTO warnings VALUES (1, 'hello arbitrary config tests');

CREATE FUNCTION warning(text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE WARNING '%', $1;
END;
$$;

SET client_min_messages TO WARNING;
SELECT warning(message) FROM warnings WHERE id = 1;
RESET client_min_messages;

-- verify that the function definition is consistent in the cluster
SELECT verify_function_is_same_on_workers('function_create.warning(text)');

-- Create and distribute a simple function
CREATE FUNCTION eq(macaddr, macaddr) RETURNS bool
    AS 'select $1 = $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- testing alter statements for a distributed function
-- ROWS 5, untested because;
-- ERROR:  ROWS is not applicable when function does not return a set
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) CALLED ON NULL INPUT IMMUTABLE SECURITY INVOKER PARALLEL UNSAFE LEAKPROOF COST 5;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RETURNS NULL ON NULL INPUT STABLE SECURITY DEFINER PARALLEL RESTRICTED;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) STRICT VOLATILE PARALLEL SAFE;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');

-- Test SET/RESET for alter function
ALTER ROUTINE eq(macaddr,macaddr) SET client_min_messages TO debug;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET client_min_messages;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET search_path TO 'sch'';ma', public;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET search_path;

-- SET ... FROM CURRENT is not supported, verify the query fails with a descriptive error irregardless of where in the action list the statement occurs
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages FROM CURRENT;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RETURNS NULL ON NULL INPUT SET client_min_messages FROM CURRENT;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages FROM CURRENT SECURITY DEFINER;
SELECT verify_function_is_same_on_workers('function_create.eq(macaddr,macaddr)');

-- rename function and make sure the new name can be used on the workers
ALTER FUNCTION eq(macaddr,macaddr) RENAME TO eq2;
SELECT verify_function_is_same_on_workers('function_create.eq2(macaddr,macaddr)');
