SET citus.next_shard_id TO 20020000;

CREATE USER functionuser;
SELECT run_command_on_workers($$CREATE USER functionuser;$$);

CREATE SCHEMA function_tests AUTHORIZATION functionuser;
CREATE SCHEMA function_tests2 AUTHORIZATION functionuser;

SET search_path TO function_tests;
SET citus.shard_count TO 4;

-- set sync intervals to less than 15s so wait_until_metadata_sync never times out
ALTER SYSTEM SET citus.metadata_sync_interval TO 3000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 500;
SELECT pg_reload_conf();

CREATE OR REPLACE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

-- Create and distribute a simple function
CREATE FUNCTION add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION add_numeric(numeric, numeric) RETURNS numeric
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- $function$ is what postgres escapes functions with when deparsing
-- make sure $function$ doesn't cause invalid syntax
CREATE FUNCTION add_text(text, text) RETURNS text
    AS 'select $function$test$function$ || $1::int || $2::int;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION add_polygons(polygon, polygon) RETURNS int
    AS 'select 1'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- Test some combination of functions without ddl propagation
-- This will prevent the workers from having those types created. They are
-- created just-in-time on function distribution
SET citus.enable_ddl_propagation TO off;

CREATE TYPE dup_result AS (f1 int, f2 text);

CREATE FUNCTION dup(int) RETURNS dup_result
    AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
    LANGUAGE SQL;

CREATE FUNCTION add_with_param_names(val1 integer, val2 integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION add_without_param_names(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION add_mixed_param_names(integer, val1 integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- Include aggregate function case
CREATE FUNCTION agg_sfunc(state int, item int)
RETURNS int IMMUTABLE LANGUAGE plpgsql AS $$
begin
    return state + item;
end;
$$;

CREATE FUNCTION agg_invfunc(state int, item int)
RETURNS int IMMUTABLE LANGUAGE plpgsql AS $$
begin
    return state - item;
end;
$$;

CREATE FUNCTION agg_finalfunc(state int, extra int)
RETURNS int IMMUTABLE LANGUAGE plpgsql AS $$
begin
        return state * 2;
end;
$$;

CREATE AGGREGATE sum2(int) (
    sfunc = agg_sfunc,
    stype = int,
    sspace = 8,
    finalfunc = agg_finalfunc,
    finalfunc_extra,
    initcond = '5',
    msfunc = agg_sfunc,
    mstype = int,
    msspace = 12,
    minvfunc = agg_invfunc,
    mfinalfunc = agg_finalfunc,
    mfinalfunc_extra,
    minitcond = '1',
    sortop = ">"
);

-- make sure to propagate ddl propagation after we have setup our functions, this will
-- allow alter statements to be propagated and keep the functions in sync across machines
SET citus.enable_ddl_propagation TO on;

-- functions are distributed by int arguments, when run in isolation it is not guaranteed a table actually exists.
CREATE TABLE colocation_table(id int);
SELECT create_distributed_table('colocation_table','id');

-- make sure that none of the active and primary nodes hasmetadata
-- at the start of the test
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- if not paremeters are supplied, we'd see that function doesn't have
-- distribution_argument_index and colocationid
SELECT create_distributed_function('add_mixed_param_names(int, int)');
SELECT distribution_argument_index is NULL, colocationid is NULL from citus.pg_dist_object
WHERE objid = 'add_mixed_param_names(int, int)'::regprocedure;

-- also show that we can use the function
SELECT * FROM run_command_on_workers('SELECT function_tests.add_mixed_param_names(2,3);') ORDER BY 1,2;

-- make sure that none of the active and primary nodes hasmetadata
-- since the function doesn't have a parameter
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

SELECT create_distributed_function('dup(int)', '$1');
SELECT * FROM run_command_on_workers('SELECT function_tests.dup(42);') ORDER BY 1,2;

SELECT create_distributed_function('add(int,int)', '$1');
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');

-- distribute aggregate
SELECT create_distributed_function('sum2(int)');

-- testing alter statements for a distributed function
-- ROWS 5, untested because;
-- ERROR:  ROWS is not applicable when function does not return a set
ALTER FUNCTION add(int,int) CALLED ON NULL INPUT IMMUTABLE SECURITY INVOKER PARALLEL UNSAFE LEAKPROOF COST 5;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) RETURNS NULL ON NULL INPUT STABLE SECURITY DEFINER PARALLEL RESTRICTED;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) STRICT VOLATILE PARALLEL SAFE;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');

-- Test SET/RESET for alter function
ALTER FUNCTION add(int,int) SET client_min_messages TO warning;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) SET client_min_messages TO error;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) SET client_min_messages TO debug;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) RESET client_min_messages;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');

-- SET ... FROM CURRENT is not supported, verify the query fails with a descriptive error irregardless of where in the action list the statement occurs
ALTER FUNCTION add(int,int) SET client_min_messages FROM CURRENT;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) RETURNS NULL ON NULL INPUT SET client_min_messages FROM CURRENT;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER FUNCTION add(int,int) SET client_min_messages FROM CURRENT SECURITY DEFINER;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');

-- rename function and make sure the new name can be used on the workers while the old name can't
ALTER FUNCTION add(int,int) RENAME TO add2;
SELECT public.verify_function_is_same_on_workers('function_tests.add2(int,int)');
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;
SELECT * FROM run_command_on_workers('SELECT function_tests.add2(2,3);') ORDER BY 1,2;
ALTER FUNCTION add2(int,int) RENAME TO add;

ALTER AGGREGATE sum2(int) RENAME TO sum27;
SELECT * FROM run_command_on_workers($$SELECT 1 from pg_proc where proname = 'sum27';$$) ORDER BY 1,2;
ALTER AGGREGATE sum27(int) RENAME TO sum2;

-- change the owner of the function and verify the owner has been changed on the workers
ALTER FUNCTION add(int,int) OWNER TO functionuser;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
ALTER AGGREGATE sum2(int) OWNER TO functionuser;
SELECT run_command_on_workers($$
SELECT row(usename, nspname, proname)
FROM pg_proc
JOIN pg_user ON (usesysid = proowner)
JOIN pg_namespace ON (pg_namespace.oid = pronamespace)
WHERE proname = 'add';
$$);
SELECT run_command_on_workers($$
SELECT row(usename, nspname, proname)
FROM pg_proc
JOIN pg_user ON (usesysid = proowner)
JOIN pg_namespace ON (pg_namespace.oid = pronamespace)
WHERE proname = 'sum2';
$$);

-- change the schema of the function and verify the old schema doesn't exist anymore while
-- the new schema has the function.
ALTER FUNCTION add(int,int) SET SCHEMA function_tests2;
SELECT public.verify_function_is_same_on_workers('function_tests2.add(int,int)');
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;
SELECT * FROM run_command_on_workers('SELECT function_tests2.add(2,3);') ORDER BY 1,2;
ALTER FUNCTION function_tests2.add(int,int) SET SCHEMA function_tests;

ALTER AGGREGATE sum2(int) SET SCHEMA function_tests2;

-- when a function is distributed and we create or replace the function we need to propagate the statement to the worker to keep it in sync with the coordinator
CREATE OR REPLACE FUNCTION add(integer, integer) RETURNS integer
AS 'select $1 * $2;' -- I know, this is not an add, but the output will tell us if the update succeeded
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
SELECT public.verify_function_is_same_on_workers('function_tests.add(int,int)');
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;

-- distributed functions should not be allowed to depend on an extension, also functions
-- that depend on an extension should not be allowed to be distributed.
ALTER FUNCTION add(int,int) DEPENDS ON EXTENSION citus;
SELECT create_distributed_function('pg_catalog.citus_drop_trigger()');

DROP FUNCTION add(int,int);
-- call should fail as function should have been dropped
SELECT * FROM run_command_on_workers('SELECT function_tests.add(2,3);') ORDER BY 1,2;

DROP AGGREGATE function_tests2.sum2(int);
-- call should fail as aggregate should have been dropped
SELECT * FROM run_command_on_workers('SELECT function_tests2.sum2(id) FROM (select 1 id, 2) subq;') ORDER BY 1,2;

-- postgres doesn't accept parameter names in the regprocedure input
SELECT create_distributed_function('add_with_param_names(val1 int, int)', 'val1');

-- invalid distribution_arg_name
SELECT create_distributed_function('add_with_param_names(int, int)', distribution_arg_name:='test');
SELECT create_distributed_function('add_with_param_names(int, int)', distribution_arg_name:='int');

-- invalid distribution_arg_index
SELECT create_distributed_function('add_with_param_names(int, int)', '$0');
SELECT create_distributed_function('add_with_param_names(int, int)', '$-1');
SELECT create_distributed_function('add_with_param_names(int, int)', '$-10');
SELECT create_distributed_function('add_with_param_names(int, int)', '$3');
SELECT create_distributed_function('add_with_param_names(int, int)', '$1a');

-- non existing column name
SELECT create_distributed_function('add_with_param_names(int, int)', 'aaa');

-- NULL function
SELECT create_distributed_function(NULL);

-- NULL colocate_with
SELECT create_distributed_function('add_with_param_names(int, int)', '$1', NULL);

-- empty string distribution_arg_index
SELECT create_distributed_function('add_with_param_names(int, int)', '');

-- The first distributed function syncs the metadata to nodes
-- and metadata syncing is not supported within transaction blocks
BEGIN;
	SELECT create_distributed_function('add_with_param_names(int, int)', distribution_arg_name:='val1');
ROLLBACK;

-- make sure that none of the nodes have the function because we've rollbacked
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='add_with_param_names';$$);

-- make sure that none of the active and primary nodes hasmetadata
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- valid distribution with distribution_arg_name
SELECT create_distributed_function('add_with_param_names(int, int)', distribution_arg_name:='val1');

-- make sure that the primary nodes are now metadata synced
select bool_and(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- make sure that both of the nodes have the function because we've succeeded
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='add_with_param_names';$$);

-- valid distribution with distribution_arg_name -- case insensitive
SELECT create_distributed_function('add_with_param_names(int, int)', distribution_arg_name:='VaL1');

-- valid distribution with distribution_arg_index
SELECT create_distributed_function('add_with_param_names(int, int)','$1');

-- a function cannot be colocated with a table that is not "streaming" replicated
SET citus.shard_replication_factor TO 2;
CREATE TABLE replicated_table_func_test (a int);
SET citus.replication_model TO "statement";
SELECT create_distributed_table('replicated_table_func_test', 'a');
SELECT create_distributed_function('add_with_param_names(int, int)', '$1', colocate_with:='replicated_table_func_test');

-- a function can be colocated with a different distribution argument type
-- as long as there is a coercion path
SET citus.shard_replication_factor TO 1;
CREATE TABLE replicated_table_func_test_2 (a bigint);
SET citus.replication_model TO "streaming";
SELECT create_distributed_table('replicated_table_func_test_2', 'a');
SELECT create_distributed_function('add_with_param_names(int, int)', 'val1', colocate_with:='replicated_table_func_test_2');

-- colocate_with cannot be used without distribution key
SELECT create_distributed_function('add_with_param_names(int, int)', colocate_with:='replicated_table_func_test_2');

-- a function cannot be colocated with a local table
CREATE TABLE replicated_table_func_test_3 (a bigint);
SELECT create_distributed_function('add_with_param_names(int, int)', 'val1', colocate_with:='replicated_table_func_test_3');

-- a function cannot be colocated with a reference table
SELECT create_reference_table('replicated_table_func_test_3');
SELECT create_distributed_function('add_with_param_names(int, int)', 'val1', colocate_with:='replicated_table_func_test_3');

-- finally, colocate the function with a distributed table
SET citus.shard_replication_factor TO 1;
CREATE TABLE replicated_table_func_test_4 (a int);
SET citus.replication_model TO "streaming";
SELECT create_distributed_table('replicated_table_func_test_4', 'a');
SELECT create_distributed_function('add_with_param_names(int, int)', '$1', colocate_with:='replicated_table_func_test_4');

-- show that the colocationIds are the same
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'add_with_param_names(int, int)'::regprocedure;

-- now, re-distributed with the default colocation option, we should still see that the same colocation
-- group preserved, because we're using the default shard creation settings
SELECT create_distributed_function('add_with_param_names(int, int)', 'val1');
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'add_with_param_names(int, int)'::regprocedure;

-- function with a numeric dist. arg can be colocated with int
-- column of a distributed table. In general, if there is a coercion
-- path, we rely on postgres for implicit coersions, and users for explicit coersions
-- to coerce the values
SELECT create_distributed_function('add_numeric(numeric, numeric)', '$1', colocate_with:='replicated_table_func_test_4');
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'add_numeric(numeric, numeric)'::regprocedure;

SELECT create_distributed_function('add_text(text, text)', '$1', colocate_with:='replicated_table_func_test_4');
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'add_text(text, text)'::regprocedure;

-- cannot distribute function because there is no
-- coercion path from polygon to int
SELECT create_distributed_function('add_polygons(polygon,polygon)', '$1', colocate_with:='replicated_table_func_test_4');

-- without the colocate_with, the function errors out since there is no
-- default colocation group
SET citus.shard_count TO 55;
SELECT create_distributed_function('add_with_param_names(int, int)', 'val1');

-- sync metadata to workers for consistent results when clearing objects
SELECT wait_until_metadata_sync();

-- clear objects
SELECT stop_metadata_sync_to_node(nodename,nodeport) FROM pg_dist_node WHERE isactive AND noderole = 'primary';

SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;

-- This is hacky, but we should clean-up the resources as below

\c - - - :worker_1_port
SET client_min_messages TO error; -- suppress cascading objects dropping
UPDATE pg_dist_local_group SET groupid = 0;
SELECT worker_drop_distributed_table(logicalrelid::text) FROM pg_dist_partition WHERE logicalrelid::text ILIKE '%replicated_table_func_test%';
TRUNCATE pg_dist_node;
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;

\c - - - :worker_2_port
SET client_min_messages TO error; -- suppress cascading objects dropping
UPDATE pg_dist_local_group SET groupid = 0;
SELECT worker_drop_distributed_table(logicalrelid::text) FROM pg_dist_partition WHERE logicalrelid::text ILIKE '%replicated_table_func_test%';
TRUNCATE pg_dist_node;
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;

\c - - - :master_port

DROP USER functionuser;
SELECT run_command_on_workers($$DROP USER functionuser;$$);
