CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_replication_setup(
    shardInfo bigint[][])
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_replication_setup(shardInfo bigint[][])
    IS 'Replication setup for splitting a shard'
