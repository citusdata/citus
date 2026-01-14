
-- similar to worker_partial_agg but returns binary representation of the state
CREATE AGGREGATE pg_catalog.worker_binary_partial_agg(oid, anyelement) (
    STYPE = internal,
    SFUNC = pg_catalog.worker_partial_agg_sfunc,
    FINALFUNC = pg_catalog.worker_binary_partial_agg_ffunc
);
COMMENT ON AGGREGATE pg_catalog.worker_binary_partial_agg(oid, anyelement)
    IS 'support aggregate for implementing partial binary aggregation on workers';
REVOKE ALL ON FUNCTION pg_catalog.worker_binary_partial_agg FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.worker_binary_partial_agg TO PUBLIC;
