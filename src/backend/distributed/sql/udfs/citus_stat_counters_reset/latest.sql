CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters_reset(database_oid oid DEFAULT 0)
RETURNS VOID
LANGUAGE C STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters_reset$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters_reset(oid) IS 'Resets Citus stat counters for the given database OID or for the current database if nothing or 0 is provided.';

-- Rather than using explicit superuser() check in the function, we use
-- the GRANT system to REVOKE access to it when creating the extension.
-- Administrators can later change who can access it, or leave them as
-- only available to superuser / database cluster owner, if they choose.
REVOKE ALL ON FUNCTION pg_catalog.citus_stat_counters_reset(oid) FROM PUBLIC;
