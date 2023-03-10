CREATE OR REPLACE FUNCTION pg_catalog.worker_modify_identity_columns(regclass)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_modify_identity_columns$$;
COMMENT ON FUNCTION pg_catalog.worker_modify_identity_columns(regclass)
    IS 'modify identity columns to produce globally unique values';

