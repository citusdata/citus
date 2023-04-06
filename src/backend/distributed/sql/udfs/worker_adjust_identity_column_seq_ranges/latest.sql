CREATE OR REPLACE FUNCTION pg_catalog.worker_adjust_identity_column_seq_ranges(regclass)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_adjust_identity_column_seq_ranges$$;
COMMENT ON FUNCTION pg_catalog.worker_adjust_identity_column_seq_ranges(regclass)
    IS 'modify identity column seq ranges to produce globally unique values';

