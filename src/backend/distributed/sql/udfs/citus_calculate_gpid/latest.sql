CREATE FUNCTION pg_catalog.citus_calculate_gpid(nodeid integer,
                                                pid integer)
    RETURNS BIGINT
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$citus_calculate_gpid$$;
COMMENT ON FUNCTION pg_catalog.citus_calculate_gpid(nodeid integer, pid integer)
    IS 'calculate gpid of a backend running on any node';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_calculate_gpid(integer, integer) TO PUBLIC;
