SET search_path = 'pg_catalog';

CREATE FUNCTION mark_aggregate_for_distributed_execution(regprocedure, strategy text)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

SET search_path = 'citus';

CREATE FUNCTION citus_stype_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION citus_stype_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION citus_stype_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION worker_partial_agg_ffunc(internal)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_sfunc(internal, oid, bytea, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_ffunc(internal, oid, bytea, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

ALTER TABLE pg_dist_object ADD aggregation_strategy int;

#include "udfs/citus_finish_pg_upgrade/9.0-customagg.sql"

RESET search_path;
