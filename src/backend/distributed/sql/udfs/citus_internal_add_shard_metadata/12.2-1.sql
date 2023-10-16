-- create a new function, with needs_separate_node
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_shard_metadata(
							relation_id regclass, shard_id bigint,
							storage_type "char", shard_min_value text,
							shard_max_value text,
                            needs_separate_node boolean default false
							)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_internal_add_shard_metadata$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, boolean) IS
    'Inserts into pg_dist_shard with user checks';
