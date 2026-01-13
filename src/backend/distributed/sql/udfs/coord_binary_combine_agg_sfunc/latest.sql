
CREATE FUNCTION pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement)
    IS 'transition function for coord_binary_combine_agg';

REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc TO PUBLIC;