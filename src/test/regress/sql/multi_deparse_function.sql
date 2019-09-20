--
-- Regression tests for deparsing ALTER/DROP TABLE Queries
--
-- This test implements all the possible queries as of Postgres 11:
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
SET client_min_messages TO DEBUG;

CREATE FUNCTION deparse_test(text)
	RETURNS text
	AS 'citus'
 	LANGUAGE C STRICT;

-- Create a simple function
CREATE FUNCTION add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

SELECT deparse_test($cmd$
ALTER FUNCTION  add CALLED ON NULL INPUT
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add  RETURNS NULL ON NULL INPUT
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add STRICT
$cmd$);


SELECT deparse_test($cmd$
ALTER FUNCTION add IMMUTABLE
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add STABLE
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add VOLATILE
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add LEAKPROOF
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add NOT LEAKPROOF
$cmd$);


SELECT deparse_test($cmd$
ALTER  FUNCTION add EXTERNAL SECURITY INVOKER
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add SECURITY INVOKER
$cmd$);

SELECT deparse_test($cmd$
ALTER  FUNCTION add EXTERNAL SECURITY DEFINER
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add SECURITY DEFINER
$cmd$);


SELECT deparse_test($cmd$
ALTER FUNCTION add PARALLEL UNSAFE
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add PARALLEL RESTRICTED
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add PARALLEL SAFE
$cmd$);


-- The COST/ROWS arguments should always be numeric
SELECT deparse_test($cmd$
ALTER FUNCTION add COST 1234
$cmd$);

SELECT deparse_test($cmd$
ALTER  FUNCTION add COST 1234.5
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add ROWS 10
$cmd$);

SELECT deparse_test($cmd$
ALTER  FUNCTION add ROWS 10.8
$cmd$);


SELECT deparse_test($cmd$
ALTER FUNCTION  add SET log_min_messages = ERROR
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION  add SET log_min_messages TO DEFAULT
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION  add SET log_min_messages FROM CURRENT
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add RESET log_min_messages
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add RESET ALL
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add RENAME TO summation
$cmd$);

CREATE ROLE function_role;
SELECT run_command_on_workers('CREATE ROLE function_role');

SELECT deparse_test($cmd$
ALTER FUNCTION add OWNER TO function_role
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add SET SCHEMA public
$cmd$);

SELECT deparse_test($cmd$
ALTER FUNCTION add DEPENDS ON EXTENSION citus
$cmd$);

SELECT deparse_test($cmd$
DROP FUNCTION IF EXISTS add(int,int);
$cmd$);

-- Check that an invalid function name is still parsed correctly
SELECT deparse_test($cmd$
DROP FUNCTION IF EXISTS missing_function(int, text);
$cmd$);

SELECT deparse_test($cmd$
DROP FUNCTION IF EXISTS missing_schema.missing_function(int,float);
$cmd$);

-- clear objects
SET client_min_messages TO FATAL; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
DROP ROLE function_role;
SELECT result FROM run_command_on_workers('DROP ROLE function_role');