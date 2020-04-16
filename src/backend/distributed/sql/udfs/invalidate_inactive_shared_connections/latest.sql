CREATE OR REPLACE FUNCTION pg_catalog.invalidate_inactive_shared_connections()
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$invalidate_inactive_shared_connections$$;

COMMENT ON FUNCTION pg_catalog.invalidate_inactive_shared_connections()
     IS 'invalidated inactive shared connections';

REVOKE ALL ON FUNCTION pg_catalog.invalidate_inactive_shared_connections()
FROM PUBLIC;
