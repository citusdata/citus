CREATE OR REPLACE FUNCTION pg_catalog.citus_node_database_size(
	nodename text,
	nodeport int default 5432,
	dbname name default current_database())
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_node_database_size$$;
COMMENT ON FUNCTION pg_catalog.citus_node_database_size(text, int, name)
IS 'returns the size of the database on the given node';
