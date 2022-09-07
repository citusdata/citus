DROP FUNCTION IF EXISTS pg_catalog.pg_terminate_backend(global_pid bigint, timeout bigint) CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pg_terminate_backend(global_pid bigint, timeout bigint DEFAULT 0)
    RETURNS BOOL
    LANGUAGE C
AS 'MODULE_PATHNAME', $$citus_terminate_backend$$;

COMMENT ON FUNCTION pg_catalog.pg_terminate_backend(global_pid bigint, timeout bigint)
    IS 'terminates a Citus query which might be on any node in the Citus cluster';
