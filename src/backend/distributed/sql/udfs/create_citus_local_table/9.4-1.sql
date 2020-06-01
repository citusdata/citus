CREATE OR REPLACE FUNCTION pg_catalog.create_citus_local_table(table_name regclass)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$create_citus_local_table$$;
COMMENT ON FUNCTION create_citus_local_table(table_name regclass)
	IS 'create a citus local table';
