CREATE OR REPLACE FUNCTION worker_partitioned_relation_total_size(relation regclass)
RETURNS bigint AS $$
    SELECT sum(pg_total_relation_size(relid))::bigint
					  FROM (SELECT relid from pg_partition_tree(relation)) partition_tree;
$$ LANGUAGE SQL;
COMMENT ON FUNCTION worker_partitioned_relation_total_size(regclass)
	IS 'Calculates and returns the total size of a partitioned relation';
