SET citus.next_shard_id TO 20020000;

CREATE USER functionuser;
SELECT run_command_on_workers($$CREATE USER functionuser;$$);

CREATE SCHEMA function_tests AUTHORIZATION functionuser;
CREATE SCHEMA function_tests2 AUTHORIZATION functionuser;

SET search_path TO function_tests;
SET citus.shard_count TO 4;

-- Create and distribute a simple function
CREATE FUNCTION eq(macaddr, macaddr) RETURNS bool
    AS 'select $1 = $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION eq8(macaddr8, macaddr8) RETURNS bool
    AS 'select $1 = $2;'
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

CREATE TYPE dup_result AS (f1 macaddr, f2 text);

CREATE FUNCTION dup(macaddr) RETURNS dup_result
    AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
    LANGUAGE SQL;

CREATE FUNCTION increment(int2) RETURNS int
    AS $$ SELECT $1 + 1$$
    LANGUAGE SQL;

CREATE FUNCTION eq_with_param_names(val1 macaddr, val2 macaddr) RETURNS bool
    AS 'select $1 = $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION add_without_param_names(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION "eq_mi'xed_param_names"(macaddr, "va'l1" macaddr) RETURNS bool
    AS 'select $1 = $2;'
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

-- Test VARIADIC, example taken from postgres test suite
CREATE AGGREGATE my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
    stype = internal,
    sfunc = ordered_set_transition_multi,
    finalfunc = rank_final,
    finalfunc_extra,
    hypothetical
);

-- Test deparsing multiple parameters with names
CREATE FUNCTION agg_names_sfunc(state dup_result, x dup_result, yz dup_result)
RETURNS dup_result IMMUTABLE STRICT LANGUAGE sql AS $$
    select x.f1 | yz.f1, x.f2 || yz.f2;
$$;

CREATE FUNCTION agg_names_finalfunc(x dup_result)
RETURNS int IMMUTABLE STRICT LANGUAGE plpgsql AS $$
begin
    return x.f1;
end;
$$;


CREATE AGGREGATE agg_names(x dup_result, yz dup_result) (
    stype = dup_result,
    sfunc = agg_names_sfunc,
    finalfunc = agg_names_finalfunc,
    finalfunc_modify = shareable
);

-- make sure to propagate ddl propagation after we have setup our functions, this will
-- allow alter statements to be propagated and keep the functions in sync across machines
SET citus.enable_ddl_propagation TO on;

-- use an unusual type to force a new colocation group
CREATE TABLE statement_table(id int2);
SET citus.replication_model TO 'statement';
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('statement_table','id');

-- create a table uses streaming-based replication (can be synced)
CREATE TABLE streaming_table(id macaddr);
SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('streaming_table','id');

-- make sure that none of the active and primary nodes hasmetadata
-- at the start of the test
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- if not paremeters are supplied, we'd see that function doesn't have
-- distribution_argument_index and colocationid
SELECT create_distributed_function('"eq_mi''xed_param_names"(macaddr, macaddr)');
SELECT distribution_argument_index is NULL, colocationid is NULL from citus.pg_dist_object
WHERE objid = 'eq_mi''xed_param_names(macaddr, macaddr)'::regprocedure;

-- also show that we can use the function
SELECT * FROM run_command_on_workers($$SELECT function_tests."eq_mi'xed_param_names"('0123456789ab','ba9876543210');$$) ORDER BY 1,2;

-- make sure that none of the active and primary nodes hasmetadata
-- since the function doesn't have a parameter
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- try to co-locate with a table that uses statement-based replication
SELECT create_distributed_function('increment(int2)', '$1');
SELECT create_distributed_function('increment(int2)', '$1', colocate_with := 'statement_table');
BEGIN;
SET LOCAL citus.replication_model TO 'statement';
DROP TABLE statement_table;
SELECT create_distributed_function('increment(int2)', '$1');
END;

-- try to co-locate with a table that uses streaming replication
SELECT create_distributed_function('dup(macaddr)', '$1', colocate_with := 'streaming_table');
SELECT * FROM run_command_on_workers($$SELECT function_tests.dup('0123456789ab');$$) ORDER BY 1,2;

SELECT create_distributed_function('eq(macaddr,macaddr)', '$1', colocate_with := 'streaming_table');
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('012345689ab','0123456789ab');$$) ORDER BY 1,2;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');

-- distribute aggregate
SELECT create_distributed_function('sum2(int)');
SELECT create_distributed_function('my_rank("any")');
SELECT create_distributed_function('agg_names(dup_result,dup_result)');


-- testing alter statements for a distributed function
-- ROWS 5, untested because;
-- ERROR:  ROWS is not applicable when function does not return a set
ALTER FUNCTION eq(macaddr,macaddr) CALLED ON NULL INPUT IMMUTABLE SECURITY INVOKER PARALLEL UNSAFE LEAKPROOF COST 5;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RETURNS NULL ON NULL INPUT STABLE SECURITY DEFINER PARALLEL RESTRICTED;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) STRICT VOLATILE PARALLEL SAFE;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');

-- Test SET/RESET for alter function
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages TO warning;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages TO error;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER ROUTINE eq(macaddr,macaddr) SET client_min_messages TO debug;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET client_min_messages;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET "citus.setting;'" TO 'hello '' world';
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET "citus.setting;'";
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET search_path TO 'sch'';ma', public;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RESET search_path;

-- SET ... FROM CURRENT is not supported, verify the query fails with a descriptive error irregardless of where in the action list the statement occurs
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages FROM CURRENT;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) RETURNS NULL ON NULL INPUT SET client_min_messages FROM CURRENT;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER FUNCTION eq(macaddr,macaddr) SET client_min_messages FROM CURRENT SECURITY DEFINER;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');

-- rename function and make sure the new name can be used on the workers while the old name can't
ALTER FUNCTION eq(macaddr,macaddr) RENAME TO eq2;
SELECT public.verify_function_is_same_on_workers('function_tests.eq2(macaddr,macaddr)');
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('012346789ab','012345689ab');$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq2('012345689ab','012345689ab');$$) ORDER BY 1,2;
ALTER ROUTINE eq2(macaddr,macaddr) RENAME TO eq;

ALTER AGGREGATE sum2(int) RENAME TO sum27;
SELECT * FROM run_command_on_workers($$SELECT 1 from pg_proc where proname = 'sum27';$$) ORDER BY 1,2;
ALTER AGGREGATE sum27(int) RENAME TO sum2;

-- change the owner of the function and verify the owner has been changed on the workers
ALTER FUNCTION eq(macaddr,macaddr) OWNER TO functionuser;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
ALTER AGGREGATE sum2(int) OWNER TO functionuser;
ALTER ROUTINE my_rank("any") OWNER TO functionuser;
ALTER AGGREGATE my_rank("any") OWNER TO functionuser;
SELECT run_command_on_workers($$
SELECT array_agg(row(usename, nspname, proname) order by proname)
FROM pg_proc
JOIN pg_user ON (usesysid = proowner)
JOIN pg_namespace ON (pg_namespace.oid = pronamespace and nspname = 'function_tests')
WHERE proname IN ('eq', 'sum2', 'my_rank');
$$);

-- change the schema of the function and verify the old schema doesn't exist anymore while
-- the new schema has the function.
ALTER FUNCTION eq(macaddr,macaddr) SET SCHEMA function_tests2;
SELECT public.verify_function_is_same_on_workers('function_tests2.eq(macaddr,macaddr)');
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('0123456789ab','ba9876543210');$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT function_tests2.eq('012345689ab','ba9876543210');$$) ORDER BY 1,2;
ALTER ROUTINE function_tests2.eq(macaddr,macaddr) SET SCHEMA function_tests;

ALTER AGGREGATE sum2(int) SET SCHEMA function_tests2;

-- when a function is distributed and we create or replace the function we need to propagate the statement to the worker to keep it in sync with the coordinator
CREATE OR REPLACE FUNCTION eq(macaddr, macaddr) RETURNS bool
AS 'select $1 <> $2;' -- I know, this is not an add, but the output will tell us if the update succeeded
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
SELECT public.verify_function_is_same_on_workers('function_tests.eq(macaddr,macaddr)');
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('012345689ab','012345689ab');$$) ORDER BY 1,2;

-- distributed functions should not be allowed to depend on an extension, also functions
-- that depend on an extension should not be allowed to be distributed.
ALTER FUNCTION eq(macaddr,macaddr) DEPENDS ON EXTENSION citus;
SELECT create_distributed_function('pg_catalog.citus_drop_trigger()');

DROP FUNCTION eq(macaddr,macaddr);
-- call should fail as function should have been dropped
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('0123456789ab','ba9876543210');$$) ORDER BY 1,2;

-- Test DROP for ROUTINE
CREATE OR REPLACE FUNCTION eq(macaddr, macaddr) RETURNS bool
AS 'select $1 = $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
select create_distributed_function('eq(macaddr,macaddr)');

DROP ROUTINE eq(macaddr, macaddr);
-- call should fail as function should have been dropped
SELECT * FROM run_command_on_workers($$SELECT function_tests.eq('0123456789ab','ba9876543210');$$) ORDER BY 1,2;

DROP AGGREGATE function_tests2.sum2(int);
-- call should fail as aggregate should have been dropped
SELECT * FROM run_command_on_workers('SELECT function_tests2.sum2(id) FROM (select 1 id, 2) subq;') ORDER BY 1,2;

-- postgres doesn't accept parameter names in the regprocedure input
SELECT create_distributed_function('eq_with_param_names(val1 macaddr, macaddr)', 'val1');

-- invalid distribution_arg_name
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', distribution_arg_name:='test');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', distribution_arg_name:='int');

-- invalid distribution_arg_index
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$0');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$-1');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$-10');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$3');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$1a');

-- non existing column name
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'aaa');

-- NULL function
SELECT create_distributed_function(NULL);

-- NULL colocate_with
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$1', NULL);

-- empty string distribution_arg_index
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '');

-- The first distributed function syncs the metadata to nodes
-- and metadata syncing is not supported within transaction blocks
BEGIN;
	SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', distribution_arg_name:='val1');
ROLLBACK;

-- make sure that none of the nodes have the function because we've rollbacked
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='eq_with_param_names';$$);

-- make sure that none of the active and primary nodes hasmetadata
select bool_or(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- valid distribution with distribution_arg_name
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', distribution_arg_name:='val1');

-- make sure that the primary nodes are now metadata synced
select bool_and(hasmetadata) from pg_dist_node WHERE isactive AND  noderole = 'primary';

-- make sure that both of the nodes have the function because we've succeeded
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='eq_with_param_names';$$);

-- valid distribution with distribution_arg_name -- case insensitive
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', distribution_arg_name:='VaL1');

-- valid distribution with distribution_arg_index
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)','$1');

-- a function cannot be colocated with a table that is not "streaming" replicated
SET citus.shard_replication_factor TO 2;
CREATE TABLE replicated_table_func_test (a macaddr);
SET citus.replication_model TO "statement";
SELECT create_distributed_table('replicated_table_func_test', 'a');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$1', colocate_with:='replicated_table_func_test');

SELECT public.wait_until_metadata_sync();

-- a function can be colocated with a different distribution argument type
-- as long as there is a coercion path
SET citus.shard_replication_factor TO 1;
CREATE TABLE replicated_table_func_test_2 (a macaddr8);
SET citus.replication_model TO "streaming";
SELECT create_distributed_table('replicated_table_func_test_2', 'a');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'val1', colocate_with:='replicated_table_func_test_2');

-- colocate_with cannot be used without distribution key
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', colocate_with:='replicated_table_func_test_2');

-- a function cannot be colocated with a local table
CREATE TABLE replicated_table_func_test_3 (a macaddr8);
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'val1', colocate_with:='replicated_table_func_test_3');

-- a function cannot be colocated with a reference table
SELECT create_reference_table('replicated_table_func_test_3');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'val1', colocate_with:='replicated_table_func_test_3');

-- finally, colocate the function with a distributed table
SET citus.shard_replication_factor TO 1;
CREATE TABLE replicated_table_func_test_4 (a macaddr);
SET citus.replication_model TO "streaming";
SELECT create_distributed_table('replicated_table_func_test_4', 'a');
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', '$1', colocate_with:='replicated_table_func_test_4');

-- show that the colocationIds are the same
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'eq_with_param_names(macaddr, macaddr)'::regprocedure;

-- now, redistributed with the default colocation option, we should still see that the same colocation
-- group preserved, because we're using the default shard creation settings
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'val1');
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'eq_with_param_names(macaddr, macaddr)'::regprocedure;

-- function with a macaddr8 dist. arg can be colocated with macaddr
-- column of a distributed table. In general, if there is a coercion
-- path, we rely on postgres for implicit coersions, and users for explicit coersions
-- to coerce the values
SELECT create_distributed_function('eq8(macaddr8, macaddr8)', '$1', colocate_with:='replicated_table_func_test_4');
SELECT pg_dist_partition.colocationid = objects.colocationid as table_and_function_colocated
FROM pg_dist_partition, citus.pg_dist_object as objects
WHERE pg_dist_partition.logicalrelid = 'replicated_table_func_test_4'::regclass AND
	  objects.objid = 'eq8(macaddr8, macaddr8)'::regprocedure;

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
SELECT create_distributed_function('eq_with_param_names(macaddr, macaddr)', 'val1');

-- sync metadata to workers for consistent results when clearing objects
SELECT public.wait_until_metadata_sync();


SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
CREATE TABLE test (id int, name text);
SELECT create_distributed_table('test','id');
INSERT INTO test VALUES (3,'three');

CREATE OR REPLACE FUNCTION increment(int)
RETURNS NUMERIC AS $$
DECLARE ret_val NUMERIC;
BEGIN
        SELECT max(id)::numeric+1 INTO ret_val  FROM test WHERE id = $1;
        RETURN ret_val;
END;
$$  LANGUAGE plpgsql;

SELECT create_distributed_function('increment(int)', '$1', colocate_with := 'test');

-- call a distributed function inside a pl/pgsql function

CREATE OR REPLACE FUNCTION test_func_calls_dist_func()
RETURNS NUMERIC AS $$
DECLARE incremented_val NUMERIC;
BEGIN
        SELECT INTO incremented_val increment(1);
        RETURN incremented_val;
END;
$$  LANGUAGE plpgsql;

SELECT test_func_calls_dist_func();
SELECT test_func_calls_dist_func();

-- test an INSERT..SELECT via the coordinator just because it is kind of funky
INSERT INTO test SELECT increment(3);
SELECT * FROM test ORDER BY id;

DROP TABLE test;

SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;

-- clear objects
SELECT stop_metadata_sync_to_node(nodename,nodeport) FROM pg_dist_node WHERE isactive AND noderole = 'primary';
-- This is hacky, but we should clean-up the resources as below

\c - - :public_worker_1_host :worker_1_port
UPDATE pg_dist_local_group SET groupid = 0;
TRUNCATE pg_dist_node;
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;
SET search_path TO function_tests, function_tests2;
\c - - :public_worker_2_host :worker_2_port
UPDATE pg_dist_local_group SET groupid = 0;
TRUNCATE pg_dist_node;
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA function_tests CASCADE;
DROP SCHEMA function_tests2 CASCADE;
\c - - :master_host :master_port

DROP USER functionuser;
SELECT run_command_on_workers($$DROP USER functionuser$$);
