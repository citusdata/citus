CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_tenant_schema(schema_id Oid, colocation_id int)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_add_tenant_schema(Oid, int) IS
    'insert given tenant schema into pg_dist_schema with given colocation id';
