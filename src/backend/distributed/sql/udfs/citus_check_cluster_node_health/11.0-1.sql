CREATE FUNCTION pg_catalog.citus_check_cluster_node_health (
    OUT from_nodename text,
    OUT from_nodeport int,
    OUT to_nodename text,
    OUT to_nodeport int,
    OUT result bool )
    RETURNS SETOF RECORD
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME', $$citus_check_cluster_node_health$$;

COMMENT ON FUNCTION pg_catalog.citus_check_cluster_node_health ()
    IS 'checks connections between all nodes in the cluster';
