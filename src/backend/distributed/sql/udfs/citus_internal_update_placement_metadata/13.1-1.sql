CREATE OR REPLACE FUNCTION citus_internal.update_placement_metadata(
							shard_id bigint, source_group_id integer,
							target_group_id integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_update_placement_metadata$$;

COMMENT ON FUNCTION citus_internal.update_placement_metadata(bigint, integer, integer) IS
    'Updates into pg_dist_placement with user checks';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_update_placement_metadata(
							shard_id bigint, source_group_id integer,
							target_group_id integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_update_placement_metadata(bigint, integer, integer) IS
    'Updates into pg_dist_placement with user checks';
