-- citus--10.0-3--10.0-2
-- this is a downgrade path that will revert the changes made in citus--10.0-2--10.0-3.sql

DROP FUNCTION pg_catalog.citus_update_table_statistics(regclass);

#include "../udfs/citus_update_table_statistics/10.0-1.sql"

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

DROP FUNCTION pg_catalog.citus_get_active_worker_nodes(OUT text, OUT bigint);
