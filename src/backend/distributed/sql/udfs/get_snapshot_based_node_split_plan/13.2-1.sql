CREATE OR REPLACE FUNCTION pg_catalog.get_snapshot_based_node_split_plan(
    primary_node_name text,
    primary_node_port integer,
    replica_node_name text,
    replica_node_port integer,
    rebalance_strategy name DEFAULT NULL
    )
    RETURNS TABLE (table_name regclass,
                   shardid bigint,
                   shard_size bigint,
                   placement_node text)
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.get_snapshot_based_node_split_plan(text, int, text, int, name)
    IS 'shows the shard placements to balance shards between primary and replica worker nodes';

REVOKE ALL ON FUNCTION pg_catalog.get_snapshot_based_node_split_plan(text, int, text, int, name) FROM PUBLIC;
