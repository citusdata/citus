CREATE FUNCTION pg_catalog.isolate_tenant_to_new_shard(
  table_name regclass,
  tenant_id "any",
  cascade_option text DEFAULT '',
  shard_transfer_mode citus.shard_transfer_mode DEFAULT 'auto')
RETURNS bigint LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$isolate_tenant_to_new_shard$$;

COMMENT ON FUNCTION pg_catalog.isolate_tenant_to_new_shard(
  table_name regclass,
  tenant_id "any",
  cascade_option text,
  shard_transfer_mode citus.shard_transfer_mode)
IS 'isolate a tenant to its own shard and return the new shard id';
