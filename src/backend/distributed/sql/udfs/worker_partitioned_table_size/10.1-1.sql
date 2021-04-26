CREATE OR REPLACE FUNCTION worker_partitioned_table_size(relation regclass)
RETURNS bigint AS $$
    SELECT sum(pg_table_size(relid))::bigint
					  FROM (SELECT relid from pg_partition_tree(relation)) partition_tree;
$$ LANGUAGE SQL;
COMMENT ON FUNCTION pg_catalog.worker_partitioned_table_size(regclass)
    IS 'Calculates and returns the size of a partitioned table';
