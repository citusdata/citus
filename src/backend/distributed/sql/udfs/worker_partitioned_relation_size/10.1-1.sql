CREATE OR REPLACE FUNCTION worker_partitioned_relation_size(relation text)
RETURNS bigint AS $$
    SELECT sum(pg_relation_size(relid))::bigint
					  FROM (SELECT relid from pg_partition_tree(relation)) partition_tree;
$$ LANGUAGE SQL;
COMMENT ON FUNCTION pg_catalog.worker_partitioned_relation_size(text)
    IS 'Calculates and returns the size of a partitioned relation';
