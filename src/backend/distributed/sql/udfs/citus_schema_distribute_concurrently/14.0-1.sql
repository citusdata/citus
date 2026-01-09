CREATE OR REPLACE FUNCTION pg_catalog.citus_schema_distribute_concurrently(schemaname regnamespace)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_schema_distribute_concurrently$$;
COMMENT ON FUNCTION pg_catalog.citus_schema_distribute_concurrently(schemaname regnamespace)
	IS 'distributes a schema without blocking writes, using logical replication';
