CREATE FUNCTION pg_catalog.citus_update_table_statistics(relation regclass)
RETURNS VOID AS $$
DECLARE
	colocated_tables regclass[];
BEGIN
	SELECT get_colocated_table_array(relation) INTO colocated_tables;

	PERFORM
		master_update_shard_statistics(shardid)
	FROM
		pg_dist_shard
	WHERE
		logicalrelid = ANY (colocated_tables);
END;
$$ LANGUAGE 'plpgsql';
COMMENT ON FUNCTION pg_catalog.citus_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table and its colocated tables';
