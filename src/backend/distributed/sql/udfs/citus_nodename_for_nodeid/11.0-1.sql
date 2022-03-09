CREATE OR REPLACE FUNCTION pg_catalog.citus_nodename_for_nodeid(nodeid integer)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_nodename_for_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_nodename_for_nodeid(nodeid integer)
    IS 'returns node name for the node with given node id';
