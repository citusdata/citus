CREATE OR REPLACE FUNCTION pg_catalog.citus_database_size(
	dbname name default current_database())
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_database_size$$;
COMMENT ON FUNCTION pg_catalog.citus_database_size(name)
IS 'returns the size of the database across all nodes';
