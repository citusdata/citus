-- File to create functions and helpers needed for subsequent tests

-- create a helper function to create objects on each node
CREATE FUNCTION run_command_on_master_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- Create a function to make sure that queries returning the same result
CREATE FUNCTION raise_failed_execution(query text) RETURNS void AS $$
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

-- helper function to quickly run SQL on the whole cluster
CREATE FUNCTION run_command_on_coordinator_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- 1. Marks the given procedure as colocated with the given table.
-- 2. Marks the argument index with which we route the procedure.
CREATE FUNCTION colocate_proc_with_table(procname text, tablerelid regclass, argument_index int)
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

CREATE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

-- set sync intervals to less than 15s so wait_until_metadata_sync never times out
ALTER SYSTEM SET citus.metadata_sync_interval TO 3000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 500;
SELECT pg_reload_conf();

-- Verifies pg_dist_node and pg_dist_palcement in the given worker matches the ones in coordinator
CREATE FUNCTION verify_metadata(hostname TEXT, port INTEGER, master_port INTEGER DEFAULT 57636)
    RETURNS BOOLEAN
    LANGUAGE sql
    AS $$
SELECT wait_until_metadata_sync();
WITH dist_node_summary AS (
    SELECT 'SELECT jsonb_agg(ROW(nodeid, groupid, nodename, nodeport, isactive) ORDER BY nodeid) FROM  pg_dist_node' as query
), dist_node_check AS (
    SELECT count(distinct result) = 1 AS matches
    FROM dist_node_summary CROSS JOIN LATERAL
        master_run_on_worker(ARRAY[hostname, 'localhost'], ARRAY[port, master_port],
                            ARRAY[dist_node_summary.query, dist_node_summary.query],
                            false)
), dist_placement_summary AS (
    SELECT 'SELECT jsonb_agg(pg_dist_placement ORDER BY shardid) FROM pg_dist_placement)' AS query
), dist_placement_check AS (
    SELECT count(distinct result) = 1 AS matches
    FROM dist_placement_summary CROSS JOIN LATERAL
        master_run_on_worker(ARRAY[hostname, 'localhost'], ARRAY[port, master_port],
                            ARRAY[dist_placement_summary.query, dist_placement_summary.query],
                            false)
)
SELECT dist_node_check.matches AND dist_placement_check.matches
FROM dist_node_check CROSS JOIN dist_placement_check
$$;
