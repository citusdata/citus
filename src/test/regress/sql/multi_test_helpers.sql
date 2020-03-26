-- File to create functions and helpers needed for subsequent tests

-- Some of the tests fail but without them some other tests fail. So failing parts
-- are removed to create shadow tests. This is one of them.

-- create a helper function to create objects on each node
CREATE OR REPLACE FUNCTION run_command_on_master_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- Create a function to make sure that queries returning the same result
CREATE OR REPLACE FUNCTION raise_failed_execution(query text) RETURNS void AS $$
BEGIN
	EXECUTE query;
	EXCEPTION WHEN OTHERS THEN
	IF SQLERRM LIKE 'failed to execute task%' THEN
		RAISE 'Task failed to execute';
	END IF;
END;
$$LANGUAGE plpgsql;

-- Create a function to ignore worker plans in explain output
CREATE OR REPLACE FUNCTION coordinator_plan(explain_commmand text, out query_plan text)
RETURNS SETOF TEXT AS $$
BEGIN
  FOR query_plan IN execute explain_commmand LOOP
    RETURN next;
    IF query_plan LIKE '%Task Count:%'
    THEN
        RETURN;
    END IF;
  END LOOP;
  RETURN;
END; $$ language plpgsql;

-- Is a distributed plan?
CREATE OR REPLACE FUNCTION plan_is_distributed(explain_commmand text)
RETURNS BOOLEAN AS $$
DECLARE
    query_plan TEXT;
BEGIN
  FOR query_plan IN execute explain_commmand LOOP
    IF query_plan LIKE '%Task Count:%'
    THEN
        RETURN TRUE;
    END IF;
  END LOOP;
  RETURN FALSE;
END; $$ language plpgsql;

-- helper function to quickly run SQL on the whole cluster
CREATE OR REPLACE FUNCTION run_command_on_coordinator_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- 1. Marks the given procedure as colocated with the given table.
-- 2. Marks the argument index with which we route the procedure.
CREATE OR REPLACE FUNCTION colocate_proc_with_table(procname text, tablerelid regclass, argument_index int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    update citus.pg_dist_object
    set distribution_argument_index = argument_index, colocationid = pg_dist_partition.colocationid
    from pg_proc, pg_dist_partition
    where proname = procname and oid = objid and pg_dist_partition.logicalrelid = tablerelid;
END;$$;

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

--
-- Procedure for creating shards for range partitioned distributed table.
--
CREATE OR REPLACE PROCEDURE create_range_partitioned_shards(rel regclass, minvalues text[], maxvalues text[])
AS $$
DECLARE
  new_shardid bigint;
  idx int;
BEGIN
  FOR idx IN SELECT * FROM generate_series(1, array_length(minvalues, 1))
  LOOP
    SELECT master_create_empty_shard(rel::text) INTO new_shardid;
    UPDATE pg_dist_shard SET shardminvalue=minvalues[idx], shardmaxvalue=maxvalues[idx] WHERE shardid=new_shardid;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
