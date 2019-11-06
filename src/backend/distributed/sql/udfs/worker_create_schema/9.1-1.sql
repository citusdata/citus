SET search_path = 'pg_catalog';
CREATE FUNCTION worker_create_schema(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_create_schema$$;
COMMENT ON FUNCTION worker_create_schema(bigint)
    IS 'create schema in remote node';   
