CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters_reset(database_oid oid DEFAULT 0)
RETURNS VOID
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters_reset$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters_reset(oid) IS 'Resets Citus stat counters for the given database OID, or for all databases if 0 is passed.';

REVOKE ALL ON FUNCTION pg_catalog.citus_stat_counters_reset(oid) FROM PUBLIC;
