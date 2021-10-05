CREATE FUNCTION pg_catalog.fix_partition_shard_index_names(table_name regclass)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$fix_partition_shard_index_names$$;
COMMENT ON FUNCTION pg_catalog.fix_partition_shard_index_names(table_name regclass)
  IS 'fix index names on partition shards of given table';
