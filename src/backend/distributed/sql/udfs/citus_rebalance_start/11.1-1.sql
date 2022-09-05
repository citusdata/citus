CREATE OR REPLACE FUNCTION pg_catalog.citus_rebalance_start(
        rebalance_strategy name default NULL,
        shard_transfer_mode citus.shard_transfer_mode default 'auto'
    )
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_rebalance_start(name, citus.shard_transfer_mode)
    IS 'rebalance the shards in the cluster in the background';
GRANT EXECUTE ON FUNCTION pg_catalog.citus_rebalance_start(name, citus.shard_transfer_mode) TO PUBLIC;
