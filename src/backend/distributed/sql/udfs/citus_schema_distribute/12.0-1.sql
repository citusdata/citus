CREATE OR REPLACE FUNCTION pg_catalog.citus_schema_distribute(schemaname regnamespace)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_schema_distribute$$;
COMMENT ON FUNCTION pg_catalog.citus_schema_distribute(schemaname regnamespace)
	IS 'distributes a schema, allowing it to move between nodes';
