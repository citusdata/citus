CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_shardgroup_metadata(
							shardgroupid bigint, colocationid integer,
							shardminvalue text, shardmaxvalue text)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_add_shardgroup_metadata(bigint, integer, text, text) IS
    'Inserts into pg_dist_shardgroup with user checks';
