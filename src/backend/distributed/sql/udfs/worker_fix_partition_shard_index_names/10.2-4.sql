CREATE FUNCTION pg_catalog.worker_fix_partition_shard_index_names(parent_shard_index regclass,
                                                                  partition_shard text,
                                                                  new_partition_shard_index_name text)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_fix_partition_shard_index_names$$;
COMMENT ON FUNCTION pg_catalog.worker_fix_partition_shard_index_names(parent_shard_index regclass,
                                                                      partition_shard text,
                                                                      new_partition_shard_index_name text)
  IS 'fix the name of the index on given partition shard that is child of given parent_index';
