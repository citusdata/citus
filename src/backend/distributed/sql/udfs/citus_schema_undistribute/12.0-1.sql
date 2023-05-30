CREATE OR REPLACE FUNCTION pg_catalog.citus_schema_undistribute(schemaname regnamespace)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_schema_undistribute$$;
COMMENT ON FUNCTION pg_catalog.citus_schema_undistribute(schemaname regnamespace)
	IS 'converts a tenant schema back to a regular schema';
