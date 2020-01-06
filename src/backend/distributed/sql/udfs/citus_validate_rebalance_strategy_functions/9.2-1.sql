CREATE OR REPLACE FUNCTION pg_catalog.citus_validate_rebalance_strategy_functions(
    shard_cost_function regproc,
    node_capacity_function regproc,
    shard_allowed_on_node_function regproc
)
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_validate_rebalance_strategy_functions(regproc,regproc,regproc)
  IS 'internal function used by citus to validate signatures of functions used in rebalance strategy';
