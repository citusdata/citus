CREATE OR REPLACE FUNCTION pg_catalog.drop_timeseries_table_metadata(
    logicalrelid regclass)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'drop_timeseries_table_metadata';

COMMENT ON FUNCTION pg_catalog.drop_timeseries_table_metadata(
    logicalrelid regclass)
IS 'drops citus timeseries table''s metadata';
