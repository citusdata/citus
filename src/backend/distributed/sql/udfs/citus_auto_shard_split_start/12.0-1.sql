CREATE OR REPLACE FUNCTION pg_catalog.citus_auto_shard_split_start(
    shard_transfer_mode citus.shard_transfer_mode default 'auto'

    )
    RETURNS bigint

    AS 'MODULE_PATHNAME'

    LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_auto_shard_split_start(citus.shard_transfer_mode)

    IS 'automatically split the necessary shards in the cluster in the background';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_auto_shard_split_start(citus.shard_transfer_mode) TO PUBLIC;