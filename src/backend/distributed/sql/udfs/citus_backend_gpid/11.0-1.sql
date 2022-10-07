CREATE FUNCTION pg_catalog.citus_backend_gpid(pid int default NULL)
    RETURNS BIGINT
    LANGUAGE C
    AS 'MODULE_PATHNAME',$$citus_backend_gpid$$;
COMMENT ON FUNCTION pg_catalog.citus_backend_gpid(int)
    IS 'returns gpid of the current backend';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_backend_gpid(int) TO PUBLIC;
