CREATE OR REPLACE FUNCTION pg_catalog.citus_coordinator_nodeid()
    RETURNS integer
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_coordinator_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_coordinator_nodeid()
    IS 'returns node id of the coordinator node';
