/* citus--8.0-7--8.0-8 */
SET search_path = 'pg_catalog';

DROP FUNCTION IF EXISTS pg_catalog.worker_drop_distributed_table(logicalrelid Oid);

	
CREATE FUNCTION worker_drop_distributed_table(table_name text)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_distributed_table$$;
    
COMMENT ON FUNCTION worker_drop_distributed_table(table_name text)
    IS 'drop the distributed table and its reference from metadata tables';

RESET search_path;
