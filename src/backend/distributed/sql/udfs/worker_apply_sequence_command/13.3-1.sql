CREATE OR REPLACE FUNCTION pg_catalog.worker_apply_sequence_command(create_sequence_command text,
                                                                    sequence_type_id regtype,
                                                                    last_value bigint,
                                                                    is_called boolean)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_sequence_command$$;
COMMENT ON FUNCTION pg_catalog.worker_apply_sequence_command(text,regtype,bigint,boolean)
    IS 'create a sequence which produces globally unique values';
