CREATE FUNCTION write_intermediate_result_sfunc(state INTERNAL, filename TEXT, tuple RECORD)
RETURNS INTERNAL
LANGUAGE C
CALLED ON NULL INPUT
AS 'MODULE_PATHNAME', $$write_intermediate_result_sfunc$$;

CREATE FUNCTION write_intermediate_result_final(state INTERNAL)
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$write_intermediate_result_final$$;

CREATE AGGREGATE write_intermediate_result(filename TEXT, tuple RECORD) (
    STYPE=INTERNAL,
    SFUNC=write_intermediate_result_sfunc,
    FINALFUNC=write_intermediate_result_final
);
