CREATE FUNCTION pg_catalog.create_distributed_table_concurrently(table_name regclass,
                                                                 distribution_column text,
                                                                 distribution_type citus.distribution_type DEFAULT 'hash',
                                                                 colocate_with text DEFAULT 'default',
                                                                 shard_count int DEFAULT NULL)
  RETURNS void
  LANGUAGE C
  AS 'MODULE_PATHNAME', $$create_distributed_table_concurrently$$;
COMMENT ON FUNCTION pg_catalog.create_distributed_table_concurrently(table_name regclass,
                                                                     distribution_column text,
                                                                     distribution_type citus.distribution_type,
                                                                     colocate_with text,
                                                                     shard_count int)
    IS 'creates a distributed table and avoids blocking writes';
