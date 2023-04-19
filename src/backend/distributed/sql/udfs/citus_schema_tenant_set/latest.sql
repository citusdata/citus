CREATE OR REPLACE FUNCTION pg_catalog.citus_schema_tenant_set(schemaname regnamespace)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_schema_tenant_set$$;
COMMENT ON FUNCTION pg_catalog.citus_schema_tenant_set(schemaname regnamespace)
	IS 'converts a regular schema into a tenant schema';
REVOKE ALL ON FUNCTION pg_catalog.citus_schema_tenant_set(regnamespace) FROM PUBLIC;
