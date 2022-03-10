CREATE OR REPLACE FUNCTION pg_catalog.citus_nodeid_for_gpid(global_pid bigint)
    RETURNS integer
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_nodeid_for_gpid$$;

COMMENT ON FUNCTION pg_catalog.citus_nodeid_for_gpid(global_pid bigint)
    IS 'returns node id for the global process with given global pid';
