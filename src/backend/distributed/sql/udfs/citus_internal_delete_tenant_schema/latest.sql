CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_tenant_schema(schema_id Oid)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_tenant_schema(Oid) IS
    'delete given tenant schema from pg_dist_schema';
