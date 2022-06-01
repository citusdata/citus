CREATE OR REPLACE FUNCTION pg_catalog.split_shard_replication_setup(
    shardInfo bigint[][])
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.split_shard_replication_setup(shardInfo bigint[][])
    IS 'Replication setup for splitting a shard'
