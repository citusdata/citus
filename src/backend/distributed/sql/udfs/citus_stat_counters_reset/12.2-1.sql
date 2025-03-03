CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters_reset()
RETURNS VOID
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters_reset$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters_reset() IS 'Resets Citus stat counters';

REVOKE ALL ON FUNCTION citus_stat_counters_reset() FROM PUBLIC;
