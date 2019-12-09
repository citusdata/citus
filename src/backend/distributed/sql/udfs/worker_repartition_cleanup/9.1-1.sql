CREATE FUNCTION pg_catalog.worker_repartition_cleanup(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_repartition_cleanup$$;
COMMENT ON FUNCTION pg_catalog.worker_repartition_cleanup(bigint)
    IS 'remove job in remote node';

REVOKE ALL ON FUNCTION pg_catalog.worker_repartition_cleanup(bigint) FROM PUBLIC;
