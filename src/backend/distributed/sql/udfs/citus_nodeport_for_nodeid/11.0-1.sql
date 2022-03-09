CREATE OR REPLACE FUNCTION pg_catalog.citus_nodeport_for_nodeid(nodeid integer)
    RETURNS integer
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_nodeport_for_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_nodeport_for_nodeid(nodeid integer)
    IS 'returns node port for the node with given node id';
