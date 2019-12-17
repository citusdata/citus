CREATE OR REPLACE FUNCTION pg_catalog.read_intermediate_results(
    result_ids text[],
    format pg_catalog.citus_copy_format default 'csv')
RETURNS SETOF record
LANGUAGE C STRICT VOLATILE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$read_intermediate_result_array$$;
COMMENT ON FUNCTION pg_catalog.read_intermediate_results(text[],pg_catalog.citus_copy_format)
IS 'read a set files and return them as a set of records';
