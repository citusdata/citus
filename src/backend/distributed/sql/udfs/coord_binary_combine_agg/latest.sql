
-- select coord_binary_combine_agg(agg, col) is similar to coord_combine_agg but
-- takes binary representation of the state as input
CREATE AGGREGATE pg_catalog.coord_binary_combine_agg(oid, bytea, anyelement) (
    STYPE = internal,
    SFUNC = pg_catalog.coord_binary_combine_agg_sfunc,
    FINALFUNC = pg_catalog.coord_binary_combine_agg_ffunc,
    FINALFUNC_EXTRA
);
COMMENT ON AGGREGATE pg_catalog.coord_binary_combine_agg(oid, bytea, anyelement)
    IS 'support aggregate for implementing combining partial aggregate results from workers';

REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg TO PUBLIC;
