-- replicate_table_shards uses the shard rebalancer's C UDF functions to replicate
-- under-replicated shards of the given table.
--
CREATE FUNCTION pg_catalog.replicate_table_shards(
        relation regclass,
        shard_replication_factor int default current_setting('citus.shard_replication_factor')::int,
        max_shard_copies int default 1000000,
        excluded_shard_list bigint[] default '{}',
        shard_transfer_mode citus.shard_transfer_mode default 'auto')
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;
COMMENT ON FUNCTION pg_catalog.replicate_table_shards(regclass, int, int, bigint[], citus.shard_transfer_mode)
    IS 'replicates under replicated shards of the the given table';
