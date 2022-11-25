CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(colocationid int)
    RETURNS void
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(int) IS
    'deletes a shardgroups from pg_dist_shardgroup based on the colocation id they belong to';
