CREATE OR REPLACE FUNCTION pg_catalog.drop_timeseries_table(
    logicalrelid regclass)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'drop_timeseries_table';

COMMENT ON FUNCTION pg_catalog.drop_timeseries_table(
    logicalrelid regclass)
IS 'drops a citus timeseries table by removing metadata';
-- TODO: Update comment for unscheduling
