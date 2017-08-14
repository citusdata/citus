/* citus-7.0-10--7.0-11 */

SET search_path = 'pg_catalog';

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
	
CREATE OR REPLACE FUNCTION get_colocated_shard_array(bigint)
	RETURNS BIGINT[]
	LANGUAGE C STRICT
	AS 'citus', $$get_colocated_shard_array$$;
COMMENT ON FUNCTION get_colocated_shard_array(bigint)
	IS 'returns the array of colocated shards of the given shard';

RESET search_path;
