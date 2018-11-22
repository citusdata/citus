/* citus--8.0-9--8.0-10 */
SET search_path = 'pg_catalog';

CREATE FUNCTION worker_execute_sql_task(jobid bigint, taskid integer, query text, binary bool)
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_execute_sql_task$$;
COMMENT ON FUNCTION worker_execute_sql_task(bigint, integer, text, bool)
IS 'execute a query and write the results to a task file';

RESET search_path;
