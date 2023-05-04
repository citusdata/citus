CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_set_tenant_schema_colocation_id(schema_id Oid, colocation_id int)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_set_tenant_schema_colocation_id(Oid, int) IS
    'set colocation id of given tenant schema in pg_dist_tenant_schema';
