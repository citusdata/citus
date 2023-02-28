-- citus--11.2-1--11.3-1

CREATE FUNCTION worker_modify_identity_columns(regclass)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_modify_identity_columns$$;
COMMENT ON FUNCTION worker_modify_identity_columns(regclass)
    IS 'modify identity columns to produce globally unique values';

