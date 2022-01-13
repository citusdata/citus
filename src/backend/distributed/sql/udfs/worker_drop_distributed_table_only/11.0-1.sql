CREATE FUNCTION pg_catalog.worker_drop_distributed_table_only(table_name text)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_distributed_table_only$$;
    
COMMENT ON FUNCTION worker_drop_distributed_table_only(table_name text)
    IS 'drop the distributed table only without the metadata';