-- citus--9.4-3--9.4-2
-- This is a downgrade path that will revert the changes made in citus--9.4-2--9.4-3.sql
-- 9.4-2--9.4-3 was added later as a patch to improve master_update_table_statistics.
-- We have this downgrade script so that we can continue from the main upgrade path
-- when upgrading to later versions.
CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
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
COMMENT ON FUNCTION master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table and its colocated tables';
