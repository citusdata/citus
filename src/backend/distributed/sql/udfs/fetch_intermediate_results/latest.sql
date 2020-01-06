CREATE OR REPLACE FUNCTION pg_catalog.fetch_intermediate_results(
    result_ids text[],
    node_name text,
    node_port int)
RETURNS bigint
LANGUAGE C STRICT VOLATILE
AS 'MODULE_PATHNAME', $$fetch_intermediate_results$$;
COMMENT ON FUNCTION pg_catalog.fetch_intermediate_results(text[],text,int)
IS 'fetch array of intermediate results from a remote node. returns number of bytes read.';
