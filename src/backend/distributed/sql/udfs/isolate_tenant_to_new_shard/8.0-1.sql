CREATE FUNCTION pg_catalog.isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text DEFAULT '')
	RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$isolate_tenant_to_new_shard$$;

COMMENT ON FUNCTION pg_catalog.isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text)
    IS 'isolate a tenant to its own shard and return the new shard id';
