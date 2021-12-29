DROP FUNCTION IF EXISTS pg_catalog.worker_drop_sequence_dependency(table_name text);

CREATE OR REPLACE FUNCTION pg_catalog.worker_drop_sequence_dependency(table_name text)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_sequence_dependency$$;
COMMENT ON FUNCTION pg_catalog.worker_drop_sequence_dependency(table_name text)
    IS 'drop the Citus tables sequence dependency';
