CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_colocation_metadata(
							colocation_id int,
                            shard_count int,
                            replication_factor int,
							distribution_column_type regtype,
                            distribution_column_collation oid)
    RETURNS void
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_add_colocation_metadata(int,int,int,regtype,oid) IS
    'Inserts a co-location group into pg_dist_colocation';
