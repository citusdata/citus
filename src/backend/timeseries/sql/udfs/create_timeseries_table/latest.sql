CREATE OR REPLACE FUNCTION pg_catalog.create_timeseries_table(
    table_name regclass,
    partition_interval INTERVAL,
    postmake_interval_count int DEFAULT 7,
    premake_interval_count int DEFAULT NULL,
    start_from timestamptz DEFAULT NULL,
    compression_threshold INTERVAL DEFAULT NULL,
    retention_threshold INTERVAL DEFAULT NULL) -- can change the order with compression, raise a message about dropping data
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'create_timeseries_table';


COMMENT ON FUNCTION pg_catalog.create_timeseries_table(
    table_name regclass,
    partition_interval INTERVAL,
    postmake_interval_count int,
    premake_interval_count int,
    start_from timestamptz,
    compression_threshold INTERVAL,
    retention_threshold INTERVAL)
IS 'creates a citus timeseries table which will be autopartitioned';
