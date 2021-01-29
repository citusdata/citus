CREATE FUNCTION pg_catalog.worker_fix_pre_citus10_partitioned_table_constraint_names(table_name regclass,
															shardid bigint,
															constraint_name text)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_fix_pre_citus10_partitioned_table_constraint_names$$;
COMMENT ON FUNCTION pg_catalog.worker_fix_pre_citus10_partitioned_table_constraint_names(table_name regclass,
																shardid bigint,
																constraint_name text)
  IS 'fix constraint names on partition shards on worker nodes';
