SET citus.next_shard_id TO 20020000;

CREATE USER functionuser;
SELECT run_command_on_workers($$CREATE USER functionuser;$$);

CREATE SCHEMA function_tests AUTHORIZATION functionuser;

SET search_path TO function_tests;
SET citus.shard_count TO 4;


-- Create and distribute a simple function
CREATE FUNCTION add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
SELECT create_distributed_function('add(int,int)');
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;


-- Test some combination of functions without ddl propagation
-- This will prevent the workers from having those types created. They are
-- created just-in-time on function distribution
SET citus.enable_ddl_propagation TO off;

CREATE TYPE dup_result AS (f1 int, f2 text);

CREATE FUNCTION dup(int) RETURNS dup_result
    AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
    LANGUAGE SQL;

SELECT create_distributed_function('dup(int)');
SELECT * FROM run_command_on_workers('SELECT function_tests.dup(42);') ORDER BY 1,2;

-- clear objects
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
SELECT run_command_on_workers($$DROP SCHEMA function_tests CASCADE;$$);
DROP USER functionuser;
SELECT run_command_on_workers($$DROP USER functionuser;$$);