
CREATE FUNCTION pg_catalog.worker_binary_partial_agg_ffunc(internal)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc(internal)
    IS 'finalizer for worker_binary_partial_agg';

REVOKE ALL ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc TO PUBLIC;