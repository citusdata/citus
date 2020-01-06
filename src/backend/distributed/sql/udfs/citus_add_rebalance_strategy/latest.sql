CREATE OR REPLACE FUNCTION pg_catalog.citus_add_rebalance_strategy(
    name name,
    shard_cost_function regproc,
    node_capacity_function regproc,
    shard_allowed_on_node_function regproc,
    default_threshold float4,
    minimum_threshold float4 DEFAULT 0
)
    RETURNS VOID AS $$
    INSERT INTO
        pg_catalog.pg_dist_rebalance_strategy(
            name,
            shard_cost_function,
            node_capacity_function,
            shard_allowed_on_node_function,
            default_threshold,
            minimum_threshold
        ) VALUES (
            name,
            shard_cost_function,
            node_capacity_function,
            shard_allowed_on_node_function,
            default_threshold,
            minimum_threshold
        );
    $$ LANGUAGE sql;
COMMENT ON FUNCTION pg_catalog.citus_add_rebalance_strategy(name,regproc,regproc,regproc,float4, float4)
  IS 'adds a new rebalance strategy which can be used when rebalancing shards or draining nodes';
