CREATE OR REPLACE FUNCTION pg_catalog.time_partition_range(
    table_name regclass,
    OUT lower_bound text,
    OUT upper_bound text)
RETURNS record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$time_partition_range$$;

COMMENT ON FUNCTION pg_catalog.time_partition_range(regclass)
IS 'returns the start and end of partition boundaries';
