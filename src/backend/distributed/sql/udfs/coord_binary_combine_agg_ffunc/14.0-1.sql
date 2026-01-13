
CREATE FUNCTION pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement)
    IS 'finalizer for coord_binary_combine_agg';

REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc TO PUBLIC;
