CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_property_set(shard_id bigint, anti_affinity boolean default null)
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$citus_shard_property_set$$;
COMMENT ON FUNCTION pg_catalog.citus_shard_property_set(bigint, boolean) IS
    'Allows setting shard properties for all the shards within the shard group that given shard belongs to.';
