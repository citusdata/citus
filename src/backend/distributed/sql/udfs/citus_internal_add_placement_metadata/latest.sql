-- create a new function, without shardstate
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_placement_metadata(
							shard_id bigint,
							shard_length bigint, group_id integer,
							placement_id bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_add_placement_metadata$$;

COMMENT ON FUNCTION pg_catalog.citus_internal_add_placement_metadata(bigint, bigint, integer, bigint) IS
    'Inserts into pg_dist_shard_placement with user checks';

-- replace the old one so it would call the old C function with shard_state
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_placement_metadata(
							shard_id bigint, shard_state integer,
							shard_length bigint, group_id integer,
							placement_id bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_add_placement_metadata_legacy$$;

COMMENT ON FUNCTION pg_catalog.citus_internal_add_placement_metadata(bigint, integer, bigint, integer, bigint) IS
    'Inserts into pg_dist_shard_placement with user checks';

