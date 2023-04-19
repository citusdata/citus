CREATE OR REPLACE FUNCTION pg_catalog.citus_schema_tenant_unset(schemaname regnamespace)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_schema_tenant_unset$$;
COMMENT ON FUNCTION pg_catalog.citus_schema_tenant_unset(schemaname regnamespace)
	IS 'converts a tenant schema back to a regular schema';
REVOKE ALL ON FUNCTION pg_catalog.citus_schema_tenant_unset(regnamespace) FROM PUBLIC;
