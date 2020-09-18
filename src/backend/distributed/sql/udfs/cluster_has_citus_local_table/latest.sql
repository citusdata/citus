CREATE OR REPLACE FUNCTION pg_catalog.cluster_has_citus_local_table()
	RETURNS boolean
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$cluster_has_citus_local_table$$;
COMMENT ON FUNCTION pg_catalog.cluster_has_citus_local_table()
	IS 'return true if cluster has citus local table';
