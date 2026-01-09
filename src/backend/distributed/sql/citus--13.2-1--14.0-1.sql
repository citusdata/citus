-- citus--13.2-1--14.0-1
-- bump version to 14.0-1

#include "udfs/citus_prepare_pg_upgrade/14.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/14.0-1.sql"


CREATE FUNCTION pg_catalog.worker_binary_partial_agg_ffunc(internal)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc(internal)
    IS 'finalizer for worker_binary_partial_agg';

CREATE FUNCTION pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement)
    IS 'transition function for coord_binary_combine_agg';

CREATE FUNCTION pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement)
    IS 'finalizer for coord_binary_combine_agg';

-- similar to worker_partial_agg but returns binary representation of the state
CREATE AGGREGATE pg_catalog.worker_binary_partial_agg(oid, anyelement) (
    STYPE = internal,
    SFUNC = pg_catalog.worker_partial_agg_sfunc,
    FINALFUNC = pg_catalog.worker_binary_partial_agg_ffunc
);
COMMENT ON AGGREGATE pg_catalog.worker_binary_partial_agg(oid, anyelement)
    IS 'support aggregate for implementing partial binary aggregation on workers';

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


REVOKE ALL ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.worker_binary_partial_agg FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.coord_binary_combine_agg FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_catalog.worker_binary_partial_agg_ffunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg_ffunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg_sfunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.worker_binary_partial_agg TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.coord_binary_combine_agg TO PUBLIC;