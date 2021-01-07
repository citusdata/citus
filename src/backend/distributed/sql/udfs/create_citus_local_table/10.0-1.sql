DROP FUNCTION pg_catalog.create_citus_local_table(regclass);
CREATE OR REPLACE FUNCTION pg_catalog.create_citus_local_table(table_name regclass, cascade_via_foreign_keys boolean default false)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$create_citus_local_table$$;
COMMENT ON FUNCTION pg_catalog.create_citus_local_table(table_name regclass, cascade_via_foreign_keys boolean)
	IS 'create a citus local table';
