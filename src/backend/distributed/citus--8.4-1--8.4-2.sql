CREATE FUNCTION stype_serialize(internal, oid, ...)serial
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION stype_deserialize(internal, oid, ...)serial
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION stype_combine(internal, oid, ...)serial
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION worker_partial_agg_sfunc(internal, oid, ...)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION worker_partial_agg_ffunc(internal, oid, ...)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_sfunc(internal, oid, ...)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION coord_combine_agg_ffunc(internal, oid, ...)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- select worker_partial_agg(agg, ...)
-- equivalent to
-- select serialize_stype(agg_without_ffunc(...))
CREATE AGGREGATE worker_partial_agg(oid, ...) (
	STYPE = internal,
	SFUNC = worker_partial_agg_sfunc,
	FINALFUNC = worker_partial_agg_ffunc,
	COMBINEFUNC = stypebox_combine,
	SERIALFUNC = stypebox_serialize,
	DESERIALFUNC = stypebox_deserialize,
	PARALLEL = SAFE
)

-- select coord_combine_agg(agg, col)
-- equivalent to
-- select agg_ffunc(agg_combine(col))
CREATE AGGREGATE coord_combine_agg(oid, ...) (
	STYPE = internal,
	SFUNC = coord_combine_sfunc,
	FINALFUNC = coord_combine_agg_ffunc,
	FINALFUNC_EXTRA,
	COMBINEFUNC = stypebox_combine,
	SERIALFUNC = stypebox_serialize,
	DESERIALFUNC = stypebox_deserialize,
	PARALLEL = SAFE
)