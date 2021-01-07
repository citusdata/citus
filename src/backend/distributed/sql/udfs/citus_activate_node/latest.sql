CREATE FUNCTION pg_catalog.citus_activate_node(nodename text,
                                               nodeport integer)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$citus_activate_node$$;
COMMENT ON FUNCTION pg_catalog.citus_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

REVOKE ALL ON FUNCTION pg_catalog.citus_activate_node(text, integer) FROM PUBLIC;
