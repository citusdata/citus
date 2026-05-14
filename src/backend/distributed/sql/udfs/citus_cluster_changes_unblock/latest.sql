CREATE OR REPLACE FUNCTION pg_catalog.citus_cluster_changes_unblock()
RETURNS boolean
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_cluster_changes_unblock$$;
COMMENT ON FUNCTION pg_catalog.citus_cluster_changes_unblock()
IS 'release the cluster changes block';
REVOKE ALL ON FUNCTION pg_catalog.citus_cluster_changes_unblock() FROM PUBLIC;
