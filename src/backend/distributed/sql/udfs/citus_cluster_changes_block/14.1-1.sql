CREATE OR REPLACE FUNCTION pg_catalog.citus_cluster_changes_block(
    timeout_ms int DEFAULT 300000)
RETURNS boolean
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_cluster_changes_block$$;
COMMENT ON FUNCTION pg_catalog.citus_cluster_changes_block(int)
IS 'block distributed write transactions, DDL, and topology changes across the Citus cluster';
REVOKE ALL ON FUNCTION pg_catalog.citus_cluster_changes_block(int) FROM PUBLIC;
