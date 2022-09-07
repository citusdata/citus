DROP FUNCTION IF EXISTS pg_catalog.pg_cancel_backend(global_pid bigint) CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pg_cancel_backend(global_pid bigint)
    RETURNS BOOL
    LANGUAGE C
AS 'MODULE_PATHNAME', $$citus_cancel_backend$$;

COMMENT ON FUNCTION pg_catalog.pg_cancel_backend(global_pid bigint)
    IS 'cancels a Citus query which might be on any node in the Citus cluster';
