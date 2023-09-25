-- create a new function, with needs_isolated_node
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_shard_metadata(
							relation_id regclass, shard_id bigint,
							storage_type "char", shard_min_value text,
							shard_max_value text, needs_isolated_node boolean
							)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_internal_add_shard_metadata$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, boolean) IS
    'Inserts into pg_dist_shard with user checks';

-- replace the old one so it would call the old C function without needs_isolated_node
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_shard_metadata(
							relation_id regclass, shard_id bigint,
							storage_type "char", shard_min_value text,
							shard_max_value text
							)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_internal_add_shard_metadata_legacy$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text) IS
    'Inserts into pg_dist_shard with user checks';
