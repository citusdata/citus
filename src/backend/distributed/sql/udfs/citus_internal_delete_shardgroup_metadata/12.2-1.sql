CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(shardgroupid bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(bigint) IS
    'Deletes rows from pg_dist_shardgroup';
