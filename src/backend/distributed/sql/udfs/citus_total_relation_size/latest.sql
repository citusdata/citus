CREATE FUNCTION pg_catalog.citus_total_relation_size(logicalrelid regclass, fail_on_error boolean default true)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_total_relation_size$$;
COMMENT ON FUNCTION pg_catalog.citus_total_relation_size(logicalrelid regclass, boolean)
    IS 'get total disk space used by the specified table';
