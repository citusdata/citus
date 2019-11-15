CREATE FUNCTION pg_catalog.worker_create_schema(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_create_schema$$;
COMMENT ON FUNCTION pg_catalog.worker_create_schema(bigint)
    IS 'create schema in remote node';   


CREATE FUNCTION pg_catalog.worker_remove_jobdir(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_remove_jobdir$$;
COMMENT ON FUNCTION pg_catalog.worker_remove_jobdir(bigint)
    IS 'remove job in remote node';   