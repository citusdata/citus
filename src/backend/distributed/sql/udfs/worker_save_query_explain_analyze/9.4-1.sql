
CREATE OR REPLACE FUNCTION pg_catalog.worker_save_query_explain_analyze(
      save_enabled boolean, verbose boolean, costs boolean,
      timing boolean, summary boolean, query boolean,
      format integer)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'citus';

COMMENT ON FUNCTION pg_catalog.worker_save_query_explain_analyze(boolean, boolean, boolean, boolean, boolean, boolean, integer) IS
    'Sets parameters for saving of explain analyze output of task queries';
