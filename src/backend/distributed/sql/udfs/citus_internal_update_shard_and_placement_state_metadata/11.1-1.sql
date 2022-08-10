CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_update_shard_and_placement_state_metadata(
							shard_id bigint,
							shardState integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_update_shard_and_placement_state_metadata(bigint, integer) IS
    'Updates into pg_dist_shard and pg_dist_placement with user checks';
