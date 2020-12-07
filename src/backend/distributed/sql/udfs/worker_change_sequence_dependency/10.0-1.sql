CREATE OR REPLACE FUNCTION pg_catalog.worker_change_sequence_dependency(
    sequence regclass,
    source_table regclass,
    target_table regclass)
    RETURNS VOID
    LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_change_sequence_dependency$$;

COMMENT ON FUNCTION pg_catalog.worker_change_sequence_dependency(
    sequence regclass,
    source_table regclass,
    target_table regclass)
    IS 'changes sequence''s dependency from source table to target table';
