-- citus--9.4-1--9.3-2
-- this is a downgrade path that will revert the changes made in citus--9.3-2--9.4-1.sql

DROP FUNCTION pg_catalog.worker_last_saved_explain_analyze();
DROP FUNCTION pg_catalog.worker_save_query_explain_analyze(query text,
                                                           options jsonb);
