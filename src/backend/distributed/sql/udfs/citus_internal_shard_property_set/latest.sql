CREATE OR REPLACE FUNCTION citus_internal.shard_property_set(
                            shard_id bigint,
                            needs_separate_node boolean)
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$citus_internal_shard_property_set$$;
