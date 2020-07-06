
CREATE OR REPLACE FUNCTION pg_catalog.worker_save_query_explain_analyze(
      query text, options jsonb)
    RETURNS SETOF record
    LANGUAGE C STRICT
    AS 'citus';

COMMENT ON FUNCTION pg_catalog.worker_save_query_explain_analyze(text, jsonb) IS
    'Executes and returns results of query while saving its EXPLAIN ANALYZE to be fetched later';
