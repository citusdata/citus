CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_cost_by_disk_size(bigint)
    RETURNS float4
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_shard_cost_by_disk_size(bigint)
  IS 'a shard cost function for use by the rebalance algorithm that returns the disk size in bytes for the specified shard and the shards that are colocated with it';
