/* citus--8.2-3--8.2-4 */

CREATE OR REPLACE FUNCTION pg_catalog.citus_executor_name(executor_type int)
RETURNS text
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_executor_name$$;
COMMENT ON FUNCTION pg_catalog.citus_executor_name(int)
IS 'return the name of the external based for the value in citus_stat_statements() output';
