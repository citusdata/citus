CREATE FUNCTION pg_catalog.worker_fix_partition_shard_index_names(parent_index_name regclass)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_fix_partition_shard_index_names$$;
COMMENT ON FUNCTION pg_catalog.worker_fix_partition_shard_index_names(parent_index_name regclass)
  IS 'fix index names on partition shards on worker nodes';
