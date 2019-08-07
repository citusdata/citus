SET search_path = 'pg_catalog';

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

CREATE FUNCTION worker_partial_agg_sfunc(internal, oid, anyelement)
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

-- select worker_partial_agg(agg, ...)
-- equivalent to
-- select serialize_stype(agg_without_ffunc(...))
CREATE AGGREGATE worker_partial_agg(oid, anyelement) (
	STYPE = internal,
	SFUNC = worker_partial_agg_sfunc,
	FINALFUNC = worker_partial_agg_ffunc,
	COMBINEFUNC = citus_stype_combine,
	SERIALFUNC = citus_stype_serialize,
	DESERIALFUNC = citus_stype_deserialize,
	PARALLEL = SAFE
);

-- select coord_combine_agg(agg, col)
-- equivalent to
-- select agg_ffunc(agg_combine(col))
CREATE AGGREGATE coord_combine_agg(oid, bytea, anyelement) (
	STYPE = internal,
	SFUNC = coord_combine_agg_sfunc,
	FINALFUNC = coord_combine_agg_ffunc,
	FINALFUNC_EXTRA,
	COMBINEFUNC = citus_stype_combine,
	SERIALFUNC = citus_stype_serialize,
	DESERIALFUNC = citus_stype_deserialize,
	PARALLEL = SAFE
);

RESET search_path;
