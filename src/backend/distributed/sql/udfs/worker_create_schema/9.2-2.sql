DROP FUNCTION IF EXISTS pg_catalog.worker_create_schema(jobid bigint);

CREATE FUNCTION pg_catalog.worker_create_schema(jobid bigint, username text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_create_schema$$;
COMMENT ON FUNCTION pg_catalog.worker_create_schema(bigint, text)
    IS 'create schema in remote node';

REVOKE ALL ON FUNCTION pg_catalog.worker_create_schema(bigint, text) FROM PUBLIC;
