DROP FUNCTION pg_catalog.worker_split_shard_replication_setup(pg_catalog.split_shard_info[]);

CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_replication_setup(
    splitShardInfo pg_catalog.split_shard_info[], operation_id bigint)
RETURNS setof pg_catalog.replication_slot_info
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_replication_setup(splitShardInfo pg_catalog.split_shard_info[], operation_id bigint)
    IS 'Replication setup for splitting a shard';

REVOKE ALL ON FUNCTION pg_catalog.worker_split_shard_replication_setup(pg_catalog.split_shard_info[], bigint) FROM PUBLIC;
