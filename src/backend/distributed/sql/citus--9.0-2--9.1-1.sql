ALTER TABLE pg_catalog.pg_dist_node ADD shouldhaveshards bool NOT NULL DEFAULT true;
COMMENT ON COLUMN pg_catalog.pg_dist_node.shouldhaveshards IS
    'indicates whether the node is eligible to contain data from distributed tables';

#include "udfs/master_set_node_property/9.1-1.sql"
#include "udfs/master_drain_node/9.1-1.sql"

-- we don't maintain replication factor of reference tables anymore and just
-- use -1 instead.
UPDATE pg_dist_colocation SET replicationfactor = -1 WHERE distributioncolumntype = 0;

#include "udfs/any_value/9.1-1.sql"

-- drop function which was used for upgrading from 6.0
-- creation was removed from citus--7.0-1.sql
DROP FUNCTION IF EXISTS pg_catalog.master_initialize_node_metadata;

-- Support infrastructure for distributing aggregation
CREATE FUNCTION citus.worker_partial_agg_sfunc(internal, oid, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION citus.worker_partial_agg_sfunc(internal, oid, anyelement)
    IS 'transition function for worker_partial_agg';

CREATE FUNCTION citus.worker_partial_agg_ffunc(internal)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION citus.worker_partial_agg_ffunc(internal)
    IS 'finalizer for worker_partial_agg';

CREATE FUNCTION citus.coord_combine_agg_sfunc(internal, oid, cstring, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION citus.coord_combine_agg_sfunc(internal, oid, cstring, anyelement)
    IS 'transition function for coord_combine_agg';

CREATE FUNCTION citus.coord_combine_agg_ffunc(internal, oid, cstring, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
COMMENT ON FUNCTION citus.coord_combine_agg_ffunc(internal, oid, cstring, anyelement)
    IS 'finalizer for coord_combine_agg';

-- select worker_partial_agg(agg, ...)
-- equivalent to
-- select to_cstring(agg_without_ffunc(...))
CREATE AGGREGATE citus.worker_partial_agg(oid, anyelement) (
    STYPE = internal,
    SFUNC = citus.worker_partial_agg_sfunc,
    FINALFUNC = citus.worker_partial_agg_ffunc
);
COMMENT ON AGGREGATE citus.worker_partial_agg(oid, anyelement)
    IS 'support aggregate for implementing partial aggregation on workers';

-- select coord_combine_agg(agg, col)
-- equivalent to
-- select agg_ffunc(agg_combine(from_cstring(col)))
CREATE AGGREGATE citus.coord_combine_agg(oid, cstring, anyelement) (
    STYPE = internal,
    SFUNC = citus.coord_combine_agg_sfunc,
    FINALFUNC = citus.coord_combine_agg_ffunc,
    FINALFUNC_EXTRA
);
COMMENT ON AGGREGATE citus.coord_combine_agg(oid, cstring, anyelement)
    IS 'support aggregate for implementing combining partial aggregate results from workers';

