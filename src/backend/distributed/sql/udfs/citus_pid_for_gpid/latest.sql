CREATE OR REPLACE FUNCTION pg_catalog.citus_pid_for_gpid(global_pid bigint)
    RETURNS integer
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_pid_for_gpid$$;

COMMENT ON FUNCTION pg_catalog.citus_pid_for_gpid(global_pid bigint)
    IS 'returns process id for the global process with given global pid';
