CREATE FUNCTION pg_catalog.upgrade_to_reference_table(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$upgrade_to_reference_table$$;
COMMENT ON FUNCTION pg_catalog.upgrade_to_reference_table(table_name regclass)
    IS 'upgrades an existing broadcast table to a reference table';
