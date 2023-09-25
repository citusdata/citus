CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_unset_isolated(shard_id bigint)
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$citus_shard_unset_isolated$$;
COMMENT ON FUNCTION pg_catalog.citus_shard_unset_isolated(bigint) IS
    'Sets the needsisolatednode flag to false for all the shards in the shard group of the given shard.';
