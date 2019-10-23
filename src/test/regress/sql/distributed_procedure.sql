SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 11 AS server_verion_eleven_and_above
    \gset
\if :server_verion_eleven_and_above
\else
\q
\endif

SET citus.next_shard_id TO 20030000;

CREATE USER procedureuser;
SELECT run_command_on_workers($$CREATE USER procedureuser;$$);

CREATE SCHEMA procedure_tests AUTHORIZATION procedureuser;
CREATE SCHEMA procedure_tests2 AUTHORIZATION procedureuser;

SET search_path TO procedure_tests;
SET citus.shard_count TO 4;

-- Create and distribute a simple function
CREATE OR REPLACE PROCEDURE raise_info(text)
    LANGUAGE PLPGSQL AS $proc$
BEGIN
    RAISE INFO 'information message %', $1;
END;
$proc$;

-- procedures are distributed by text arguments, when run in isolation it is not guaranteed a table actually exists.
CREATE TABLE colocation_table(id text);
SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('colocation_table','id');

SELECT create_distributed_function('raise_info(text)', '$1', colocate_with := 'colocation_table');
SELECT * FROM run_command_on_workers($$CALL procedure_tests.raise_info('hello');$$) ORDER BY 1,2;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');

-- testing alter statements for a distributed function
-- ROWS 5, untested because;
-- ERROR:  ROWS is not applicable when function does not return a set
ALTER PROCEDURE raise_info(text) SECURITY INVOKER;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');
ALTER PROCEDURE raise_info(text) SECURITY DEFINER;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');

-- Test SET/RESET for alter procedure
ALTER PROCEDURE raise_info(text) SET client_min_messages TO warning;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');
ALTER PROCEDURE raise_info(text) SET client_min_messages TO error;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');
ALTER PROCEDURE raise_info(text) SET client_min_messages TO debug;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');
ALTER PROCEDURE raise_info(text) RESET client_min_messages;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');

-- rename function and make sure the new name can be used on the workers while the old name can't
ALTER PROCEDURE raise_info(text) RENAME TO raise_info2;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info2(text)');
SELECT * FROM run_command_on_workers($$CALL procedure_tests.raise_info('hello');$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$CALL procedure_tests.raise_info2('hello');$$) ORDER BY 1,2;
ALTER PROCEDURE raise_info2(text) RENAME TO raise_info;

-- change the owner of the function and verify the owner has been changed on the workers
ALTER PROCEDURE raise_info(text) OWNER TO procedureuser;
SELECT public.verify_function_is_same_on_workers('procedure_tests.raise_info(text)');
SELECT run_command_on_workers($$
SELECT row(usename, nspname, proname)
FROM pg_proc
JOIN pg_user ON (usesysid = proowner)
JOIN pg_namespace ON (pg_namespace.oid = pronamespace)
WHERE proname = 'raise_info';
$$);

-- change the schema of the procedure and verify the old schema doesn't exist anymore while
-- the new schema has the function.
ALTER PROCEDURE raise_info(text) SET SCHEMA procedure_tests2;
SELECT public.verify_function_is_same_on_workers('procedure_tests2.raise_info(text)');
SELECT * FROM run_command_on_workers($$CALL procedure_tests.raise_info('hello');$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$CALL procedure_tests2.raise_info('hello');$$) ORDER BY 1,2;
ALTER PROCEDURE procedure_tests2.raise_info(text) SET SCHEMA procedure_tests;

DROP PROCEDURE raise_info(text);
-- call should fail as procedure should have been dropped
SELECT * FROM run_command_on_workers($$CALL procedure_tests.raise_info('hello');$$) ORDER BY 1,2;

SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA procedure_tests CASCADE;
SELECT run_command_on_workers($$DROP SCHEMA procedure_tests CASCADE;$$);
DROP SCHEMA procedure_tests2 CASCADE;
SELECT run_command_on_workers($$DROP SCHEMA procedure_tests2 CASCADE;$$);
DROP USER procedureuser;
SELECT run_command_on_workers($$DROP USER procedureuser;$$);
