CREATE OR REPLACE FUNCTION pg_catalog.citus_use_snapshot()
 RETURNS void
 LANGUAGE c
 STRICT
AS '$libdir/citus', $function$citus_use_snapshot$function$;
COMMENT ON FUNCTION pg_catalog.citus_use_snapshot()
    IS 'use a consistent a consistent distributed snapshot for the remainder of the transaction';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_use_snapshot() TO PUBLIC;
