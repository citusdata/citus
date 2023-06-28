CREATE OR REPLACE FUNCTION pg_catalog.citus_auto_shard_split_start(
    shard_transfer_mode citus.shard_transfer_mode default 'auto'
    )
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_auto_shard_split_start(citus.shard_transfer_mode)
    IS 'automatically split the necessary shards in the cluster in the background';
GRANT EXECUTE ON FUNCTION pg_catalog.citus_auto_shard_split_start(citus.shard_transfer_mode) TO PUBLIC;

CREATE OR REPLACE FUNCTION pg_catalog.citus_find_shard_split_points(
    shard_id bigint,
    shard_size bigint,
    shard_group_size bigint
    )
    RETURNS SETOF bigint[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_find_shard_split_points(shard_id bigint , shard_size bigint , shard_group_size bigint)
    IS 'creates split points for shards';
GRANT EXECUTE ON FUNCTION pg_catalog.citus_find_shard_split_points(shard_id bigint , shard_size bigint , shard_group_size bigint) TO PUBLIC;
