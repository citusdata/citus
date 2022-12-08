CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(
    shardgroupid bigint DEFAULT NULL,
    colocationid int DEFAULT NULL)
    RETURNS void
    LANGUAGE C
    CALLED ON NULL INPUT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(bigint, int) IS
    'deletes a shardgroups from pg_dist_shardgroup, expect exactly one argument to be passed as an indicator what shardgroup(s) to remove';
