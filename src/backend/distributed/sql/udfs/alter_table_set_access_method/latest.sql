CREATE OR REPLACE FUNCTION pg_catalog.alter_table_set_access_method(
    table_name regclass, access_method text)
    RETURNS VOID
    LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$alter_table_set_access_method$$;

COMMENT ON FUNCTION pg_catalog.alter_table_set_access_method(
    table_name regclass, access_method text)
    IS 'alters a table''s access method';
