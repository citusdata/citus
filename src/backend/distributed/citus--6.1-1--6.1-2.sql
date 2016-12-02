/* citus--6.1-1--6.1-2.sql */

SET search_path = 'pg_catalog';
    
CREATE FUNCTION worker_create_truncate_trigger(table_name regclass)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$worker_create_truncate_trigger$$;
COMMENT ON FUNCTION worker_create_truncate_trigger(tablename regclass)
	IS 'create truncate trigger for distributed table';
    
RESET search_path;
