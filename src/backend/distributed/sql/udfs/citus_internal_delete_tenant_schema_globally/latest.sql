CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_tenant_schema_globally(schema_id Oid, schema_name text)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_tenant_schema_globally(Oid, text) IS
    'delete given tenant schema from pg_dist_tenant_schema globally';
