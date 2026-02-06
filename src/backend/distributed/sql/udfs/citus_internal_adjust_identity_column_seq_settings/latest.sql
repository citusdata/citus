CREATE OR REPLACE FUNCTION citus_internal.adjust_identity_column_seq_settings(sequence_id regclass,
                                                                              last_value bigint default null)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_adjust_identity_column_seq_settings$$;
COMMENT ON FUNCTION citus_internal.adjust_identity_column_seq_settings(regclass, bigint)
    IS 'modify identity column sequence settings to produce globally unique values';
