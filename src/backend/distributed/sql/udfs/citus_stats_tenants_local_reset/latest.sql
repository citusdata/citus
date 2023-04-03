CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_tenants_local_reset()
    RETURNS VOID
    LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_stat_tenants_local_reset$$;

COMMENT ON FUNCTION pg_catalog.citus_stat_tenants_local_reset()
    IS 'resets the local tenant statistics';
