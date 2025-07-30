
DROP FUNCTION pg_catalog.worker_last_saved_explain_analyze();

CREATE OR REPLACE FUNCTION pg_catalog.worker_last_saved_explain_analyze()
    RETURNS TABLE(explain_analyze_output TEXT, execution_duration DOUBLE PRECISION,
        execution_ntuples DOUBLE PRECISION, execution_nloops DOUBLE PRECISION)
    LANGUAGE C STRICT
    AS 'citus';
COMMENT ON FUNCTION pg_catalog.worker_last_saved_explain_analyze() IS
    'Returns the saved explain analyze output for the last run query';
