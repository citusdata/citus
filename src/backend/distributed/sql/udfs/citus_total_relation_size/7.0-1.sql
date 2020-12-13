CREATE FUNCTION pg_catalog.citus_total_relation_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_total_relation_size$$;
COMMENT ON FUNCTION pg_catalog.citus_total_relation_size(logicalrelid regclass)
    IS 'get total disk space used by the specified table';
