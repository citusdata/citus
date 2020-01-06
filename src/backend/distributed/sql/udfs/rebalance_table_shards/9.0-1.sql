-- rebalance_table_shards uses the shard rebalancer's C UDF functions to rebalance
-- shards of the given relation.
--
CREATE OR REPLACE FUNCTION pg_catalog.rebalance_table_shards(
        relation regclass,
        threshold float4 default 0,
        max_shard_moves int default 1000000,
        excluded_shard_list bigint[] default '{}',
        shard_transfer_mode citus.shard_transfer_mode default 'auto')
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.rebalance_table_shards(regclass, float4, int, bigint[], citus.shard_transfer_mode)
    IS 'rebalance the shards of the given table across the worker nodes (including colocated shards of other tables)';
