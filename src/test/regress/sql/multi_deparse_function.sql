--
-- Regression tests for deparsing ALTER/DROP FUNCTION Queries
--
-- This test implements all the possible queries as of Postgres 11
-- in the order they are listed in the docs
-- 
-- ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     action [ ... ] [ RESTRICT ]
-- ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     RENAME TO new_name
-- ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
-- ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     SET SCHEMA new_schema
-- ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     DEPENDS ON EXTENSION extension_name
-- 
-- where action is one of:
-- 
--     CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
--     IMMUTABLE | STABLE | VOLATILE | [ NOT ] LEAKPROOF
--     [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
--     PARALLEL { UNSAFE | RESTRICTED | SAFE }
--     COST execution_cost
--     ROWS result_rows
--     SET configuration_parameter { TO | = } { value | DEFAULT }
--     SET configuration_parameter FROM CURRENT
--     RESET configuration_parameter
--     RESET ALL
-- 
-- DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
--     [ CASCADE | RESTRICT ]

SET citus.next_shard_id TO 20020000;

CREATE SCHEMA function_tests;
SET search_path TO function_tests;
SET citus.shard_count TO 4;
SET client_min_messages TO INFO;

CREATE FUNCTION deparse_test(text)
	RETURNS text
	AS 'citus'
 	LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION deparse_and_run_on_workers(IN query text,
                                                     OUT nodename text,
                                                     OUT nodeport int,
                                                     OUT success bool,
                                                     OUT result text)
    RETURNS SETOF record
    LANGUAGE PLPGSQL AS $fnc$
    DECLARE
        deparsed_query character varying(255);
    BEGIN
        deparsed_query := ( SELECT deparse_test($1) );
        RAISE INFO 'Propagating deparsed query: %', deparsed_query;
        RETURN QUERY SELECT * FROM run_command_on_workers(deparsed_query);
    END;
    $fnc$;


-- Create a simple function and distribute it
CREATE FUNCTION add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
SELECT create_distributed_function('add(int,int)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION  add CALLED ON NULL INPUT
$cmd$);

-- RETURNS NULL ON NULL INPUT and STRICT are synonyms and can be used interchangeably
-- RETURNS NULL ON NULL INPUT is actually stored as STRICT in the query parse tree
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add RETURNS NULL ON NULL INPUT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add STRICT
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add IMMUTABLE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add STABLE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add VOLATILE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add LEAKPROOF
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add NOT LEAKPROOF
$cmd$);


-- EXTERNAL keyword is ignored by Postgres Parser. It is allowed only for SQL conformance
-- The following queries will not have the EXTERNAL keyword after deparsing
SELECT deparse_and_run_on_workers($cmd$
ALTER  FUNCTION add EXTERNAL SECURITY INVOKER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add SECURITY INVOKER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER  FUNCTION add EXTERNAL SECURITY DEFINER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add SECURITY DEFINER
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add PARALLEL UNSAFE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add PARALLEL RESTRICTED
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add PARALLEL SAFE
$cmd$);


-- The COST arguments should always be numeric
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add COST 1234
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER  FUNCTION add COST 1234.5
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION  add SET log_min_messages = ERROR
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION  add SET log_min_messages TO DEFAULT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION  add SET log_min_messages FROM CURRENT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add RESET log_min_messages
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add RESET ALL
$cmd$);

-- Rename the function in the workers
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add RENAME TO summation
$cmd$);

-- Rename the function inb the coordinator as well.
-- This is needed so the next query is parsed on the coordinator
ALTER FUNCTION add RENAME TO summation;

-- Rename it back to the original so that the next tests can pass
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION summation RENAME TO add
$cmd$);

-- Rename the function back to the original name in the coordinator
ALTER FUNCTION summation RENAME TO add;

CREATE ROLE function_role;
SELECT run_command_on_workers('CREATE ROLE function_role');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add OWNER TO function_role
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add OWNER TO missing_role
$cmd$);

-- SET the schema in workers as well as the coordinator so that it remains in the same schema
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add SET SCHEMA public
$cmd$);
ALTER FUNCTION add SET SCHEMA public;

-- Revert the schema back
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION public.add SET SCHEMA function_tests
$cmd$);
ALTER FUNCTION public.add SET SCHEMA function_tests;

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add DEPENDS ON EXTENSION citus
$cmd$);

-- make sure "any" type is correctly deparsed
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION pg_catalog.get_shard_id_for_distribution_column(table_name regclass, distribution_value "any") PARALLEL SAFE;
$cmd$);

-- Do not run valid drop queries in the workers
SELECT deparse_test($cmd$
DROP FUNCTION add(int,int);
$cmd$);

-- have multiple actions in a single query
SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION add volatile leakproof SECURITY DEFINER PARALLEL unsafe;
$cmd$);

-- Check that an invalid function name is still parsed correctly
-- Test that it fails when run without IF EXISTS clause
SELECT deparse_and_run_on_workers($cmd$
DROP FUNCTION missing_function(int, text);
$cmd$);

-- Check that an invalid function name is still parsed correctly
-- Test that it is successful when run with IF EXISTS clause
SELECT deparse_and_run_on_workers($cmd$
DROP FUNCTION IF EXISTS missing_function(int, text);
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
DROP FUNCTION IF EXISTS missing_schema.missing_function(int,float);
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
DROP FUNCTION IF EXISTS missing_func_without_args;
$cmd$);

-- create schema with weird names
CREATE SCHEMA "CiTuS.TeeN";
CREATE SCHEMA "CiTUS.TEEN2";

SELECT run_command_on_workers($$
    CREATE SCHEMA IF NOT EXISTS "CiTuS.TeeN";
    CREATE SCHEMA IF NOT EXISTS "CiTUS.TEEN2";
$$);

-- create table with weird names
CREATE FUNCTION "CiTuS.TeeN"."TeeNFunCT10N.1!?!"() RETURNS TEXT
    AS $$ SELECT 'test function without params' $$
    LANGUAGE SQL;

CREATE FUNCTION "CiTuS.TeeN"."TeeNFunCT10N.1!?!"(text) RETURNS TEXT
    AS $$ SELECT 'Overloaded function called with param: ' || $1 $$
    LANGUAGE SQL;

SELECT create_distributed_function('"CiTuS.TeeN"."TeeNFunCT10N.1!?!"()');
SELECT create_distributed_function('"CiTuS.TeeN"."TeeNFunCT10N.1!?!"(text)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION "CiTuS.TeeN"."TeeNFunCT10N.1!?!"() SET SCHEMA "CiTUS.TEEN2"
$cmd$);

-- drop 2 functions at the same time
SELECT deparse_and_run_on_workers($cmd$
DROP FUNCTION "CiTUS.TEEN2"."TeeNFunCT10N.1!?!"(),"CiTuS.TeeN"."TeeNFunCT10N.1!?!"(text);
$cmd$);

-- a function with a default parameter
CREATE FUNCTION func_default_param(param INT DEFAULT 0) RETURNS TEXT
    AS $$ SELECT 'supplied param is : ' || param; $$
    LANGUAGE SQL;
SELECT create_distributed_function('func_default_param(INT)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION func_default_param RENAME TO func_with_default_param;
$cmd$);

-- a function with IN and OUT parameters
CREATE FUNCTION func_out_param(IN param INT, OUT result TEXT)
    AS $$ SELECT 'supplied param is : ' || param; $$
    LANGUAGE SQL;
SELECT create_distributed_function('func_out_param(INT)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION func_out_param RENAME TO func_in_and_out_param;
$cmd$);

-- a function with INOUT parameter
CREATE FUNCTION square(INOUT a NUMERIC)
AS $$
BEGIN
   a := a * a;
END; $$
LANGUAGE plpgsql;
SELECT create_distributed_function('square(NUMERIC)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION square SET search_path TO DEFAULT;
$cmd$);

-- a function with variadic input. 
CREATE FUNCTION sum_avg(
   VARIADIC list NUMERIC[],
   OUT total NUMERIC,
   OUT average NUMERIC)
AS $$
BEGIN
   SELECT INTO total SUM(list[i])
   FROM generate_subscripts(list, 1) g(i);
 
   SELECT INTO average AVG(list[i])
   FROM generate_subscripts(list, 1) g(i);
END; $$
LANGUAGE plpgsql;
SELECT create_distributed_function('sum_avg(NUMERIC[])');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION sum_avg COST 10000;
$cmd$);

-- a function with a custom type IN parameter 
CREATE TYPE intpair AS (x int, y int);
CREATE FUNCTION func_custom_param(IN param intpair, OUT total INT)
    AS $$ SELECT param.x + param.y $$
    LANGUAGE SQL;
SELECT create_distributed_function('func_custom_param(intpair)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION func_custom_param RENAME TO func_with_custom_param;
$cmd$);


-- a function that returns TABLE
CREATE FUNCTION func_returns_table(IN count INT)
    RETURNS TABLE (x INT, y INT)
    AS $$ SELECT i,i FROM generate_series(1,count) i $$
    LANGUAGE SQL;
SELECT create_distributed_function('func_returns_table(INT)');

SELECT deparse_and_run_on_workers($cmd$
ALTER FUNCTION func_returns_table ROWS 100;
$cmd$);

-- clear objects
SET client_min_messages TO WARNING; -- suppress cascading objects dropping

DROP SCHEMA "CiTuS.TeeN" CASCADE;
DROP SCHEMA "CiTUS.TEEN2" CASCADE;
DROP SCHEMA function_tests CASCADE;

SELECT run_command_on_workers($$
    DROP SCHEMA "CiTuS.TeeN" CASCADE;
    DROP SCHEMA "CiTUS.TEEN2" CASCADE;
    DROP SCHEMA function_tests CASCADE;
$$);

DROP ROLE function_role;
