DROP FUNCTION pg_catalog.create_citus_local_table(regclass);
CREATE OR REPLACE FUNCTION pg_catalog.citus_add_local_table_to_metadata(table_name regclass, cascade_via_foreign_keys boolean default false)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_add_local_table_to_metadata$$;
COMMENT ON FUNCTION pg_catalog.citus_add_local_table_to_metadata(table_name regclass, cascade_via_foreign_keys boolean)
	IS 'create a citus local table';
