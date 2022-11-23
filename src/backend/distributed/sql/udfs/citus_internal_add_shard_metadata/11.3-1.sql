CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_shard_metadata(
							relation_id regclass, shard_id bigint,
							storage_type "char", shard_min_value text,
							shard_max_value text, shardgroup_id bigint
							)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, bigint) IS
    'Inserts into pg_dist_shard with user checks';
