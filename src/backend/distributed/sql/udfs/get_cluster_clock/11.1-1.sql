CREATE OR REPLACE FUNCTION pg_catalog.citus_get_cluster_clock()
  RETURNS BIGINT
  LANGUAGE C STABLE PARALLEL SAFE STRICT
  AS 'MODULE_PATHNAME', $$citus_get_cluster_clock$$;
COMMENT ON FUNCTION pg_catalog.citus_get_cluster_clock()
     IS 'returns monotonically increasing logical clock value as close to epoch value (in milli seconds) possible, with the guarantee that it will never go back from its current value even after restart and crashes';
