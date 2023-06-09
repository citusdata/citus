CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_unregister_tenant_schema_globally(schema_id Oid, schema_name text)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_unregister_tenant_schema_globally(schema_id Oid, schema_name text) IS
    'Delete a tenant schema and the corresponding colocation group from metadata tables.';
