CREATE FUNCTION pg_catalog.citus_update_shard_statistics(shard_id bigint)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_update_shard_statistics$$;
COMMENT ON FUNCTION pg_catalog.citus_update_shard_statistics(bigint)
    IS 'updates shard statistics and returns the updated shard size';
