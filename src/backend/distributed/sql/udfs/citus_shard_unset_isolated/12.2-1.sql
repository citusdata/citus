CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_unset_isolated(shard_id bigint)
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$citus_shard_unset_isolated$$;
