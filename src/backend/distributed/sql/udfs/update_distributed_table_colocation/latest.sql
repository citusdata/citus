CREATE OR REPLACE FUNCTION pg_catalog.update_distributed_table_colocation(table_name regclass, colocate_with text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$update_distributed_table_colocation$$;
COMMENT ON FUNCTION pg_catalog.update_distributed_table_colocation(table_name regclass, colocate_with text)
    IS 'updates colocation of a table';
