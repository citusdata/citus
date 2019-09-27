--
-- Regression tests for deparsing ALTER/DROP PROCEDURE Queries
--
-- ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     action [ ... ] [ RESTRICT ]
-- ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     RENAME TO new_name
-- ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
-- ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     SET SCHEMA new_schema
-- ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
--     DEPENDS ON EXTENSION extension_name

-- where action is one of:

--     [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
--     SET configuration_parameter { TO | = } { value | DEFAULT }
--     SET configuration_parameter FROM CURRENT
--     RESET configuration_parameter
--     RESET ALL
-- 
-- DROP PROCEDURE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
--     [ CASCADE | RESTRICT ]
-- 
-- Please note that current deparser does not return errors on some invalid queries.
-- 
-- For example CALLED ON NULL INPUT action is valid only for FUNCTIONS, but we still 
-- allow deparsing them here.

SET citus.next_shard_id TO 20030000;

CREATE SCHEMA procedure_tests;
SET search_path TO procedure_tests;
SET citus.shard_count TO 4;
SET client_min_messages TO INFO;

-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

CREATE FUNCTION deparse_test(text)
	RETURNS text
	AS 'citus'
 	LANGUAGE C STRICT;

CREATE FUNCTION deparse_and_run_on_workers(text)
    RETURNS SETOF record
    AS $fnc$
    WITH deparsed_query AS ( SELECT deparse_test($1) qualified_query )
    SELECT run_command_on_workers(qualified_query) FROM deparsed_query d 
    $fnc$
    LANGUAGE SQL;

-- Create a simple PROCEDURE and distribute it
CREATE OR REPLACE PROCEDURE raise_info(text)
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE INFO 'information message %', $1;
END;
$proc$;
SELECT create_distributed_function('raise_info(text)');

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE  raise_info CALLED ON NULL INPUT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info RETURNS NULL ON NULL INPUT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info STRICT
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info IMMUTABLE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info STABLE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info VOLATILE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info LEAKPROOF
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info NOT LEAKPROOF
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER  PROCEDURE raise_info EXTERNAL SECURITY INVOKER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info SECURITY INVOKER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER  PROCEDURE raise_info EXTERNAL SECURITY DEFINER
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info SECURITY DEFINER
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info PARALLEL UNSAFE
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info PARALLEL RESTRICTED
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info PARALLEL SAFE
$cmd$);


-- The COST/ROWS arguments should always be numeric
SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info COST 1234
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER  PROCEDURE raise_info COST 1234.5
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info ROWS 10
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER  PROCEDURE raise_info ROWS 10.8
$cmd$);


SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE  raise_info SET log_min_messages = ERROR
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE  raise_info SET log_min_messages TO DEFAULT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE  raise_info SET log_min_messages FROM CURRENT
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info RESET log_min_messages
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info RESET ALL
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info RENAME TO summation
$cmd$);

CREATE ROLE PROCEDURE_role;

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info OWNER TO PROCEDURE_role
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info OWNER TO missing_role
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info SET SCHEMA public
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
ALTER PROCEDURE raise_info DEPENDS ON EXTENSION citus
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
DROP PROCEDURE IF EXISTS raise_info(int,int);
$cmd$);

-- Check that an invalid PROCEDURE name is still parsed correctly
SELECT deparse_and_run_on_workers($cmd$
DROP PROCEDURE IF EXISTS missing_PROCEDURE(int, text);
$cmd$);

SELECT deparse_and_run_on_workers($cmd$
DROP PROCEDURE IF EXISTS missing_schema.missing_PROCEDURE(int,float);
$cmd$);

-- clear objects
SET client_min_messages TO WARNING; -- suppress cascading objects dropping
DROP SCHEMA procedure_tests CASCADE;
DROP ROLE PROCEDURE_role;
