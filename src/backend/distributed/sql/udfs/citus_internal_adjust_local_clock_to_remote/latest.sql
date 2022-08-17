CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_adjust_local_clock_to_remote(pg_catalog.cluster_clock)
    RETURNS void
    LANGUAGE C STABLE PARALLEL SAFE STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_adjust_local_clock_to_remote$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_adjust_local_clock_to_remote(pg_catalog.cluster_clock)
    IS 'Internal UDF used to adjust the local clock to the maximum of nodes in the cluster';

REVOKE ALL ON FUNCTION pg_catalog.citus_internal_adjust_local_clock_to_remote(pg_catalog.cluster_clock) FROM PUBLIC;
