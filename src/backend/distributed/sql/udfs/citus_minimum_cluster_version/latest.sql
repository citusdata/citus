CREATE OR REPLACE FUNCTION pg_catalog.citus_minimum_cluster_version()
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_minimum_cluster_version$$;
COMMENT ON FUNCTION pg_catalog.citus_minimum_cluster_version()
    IS 'oldest (minimum) Citus version running on any active primary node in the cluster';
