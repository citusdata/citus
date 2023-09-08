CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_shard_group_set_needsisolatednode(
                            shard_id bigint,
                            enabled boolean)
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$citus_internal_shard_group_set_needsisolatednode$$;
