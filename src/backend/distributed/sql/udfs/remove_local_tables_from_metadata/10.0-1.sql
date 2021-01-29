CREATE OR REPLACE FUNCTION pg_catalog.remove_local_tables_from_metadata()
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$remove_local_tables_from_metadata$$;
COMMENT ON FUNCTION pg_catalog.remove_local_tables_from_metadata()
	IS 'undistribute citus local tables that are not chained with any reference tables via foreign keys';
