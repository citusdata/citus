CREATE OR REPLACE FUNCTION pg_catalog.create_timeseries_table(
    table_name regclass,
    partition_interval INTERVAL,
    premake_interval_count int DEFAULT 7,
    postmake_interval_count int DEFAULT 7,
    compression_threshold INTERVAL DEFAULT NULL,
    retention_threshold INTERVAL DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'create_timeseries_table';

COMMENT ON FUNCTION pg_catalog.create_timeseries_table(
    table_name regclass,
    partition_interval INTERVAL,
    premake_interval_count int,
    postmake_interval_count int,
    compression_threshold INTERVAL,
    retention_threshold INTERVAL)
IS 'creates a citus timeseries table which will be autopartitioned';
