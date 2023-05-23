CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_unregister_tenant_schema(schema_id Oid, schema_name text)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_unregister_tenant_schema(schema_id Oid, schema_name text) IS
    'Unregister a tenant schema from the catalog..';
