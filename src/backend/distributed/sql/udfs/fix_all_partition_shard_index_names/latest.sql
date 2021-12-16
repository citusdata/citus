CREATE OR REPLACE FUNCTION pg_catalog.fix_all_partition_shard_index_names()
  RETURNS SETOF regclass
  LANGUAGE plpgsql
  AS $$
DECLARE
	dist_partitioned_table_name regclass;
BEGIN
  FOR dist_partitioned_table_name IN SELECT p.logicalrelid
                                     FROM pg_dist_partition p
                                     JOIN pg_class c ON p.logicalrelid = c.oid
                                     WHERE c.relkind = 'p'
		                                 ORDER BY c.relname, c.oid
    LOOP
      EXECUTE 'SELECT fix_partition_shard_index_names( ' || quote_literal(dist_partitioned_table_name) || ' )';
      RETURN NEXT dist_partitioned_table_name;
    END LOOP;
  RETURN;
END;
$$;
COMMENT ON FUNCTION pg_catalog.fix_all_partition_shard_index_names()
  IS 'fix index names on partition shards of all tables';
