CREATE OR REPLACE FUNCTION pg_catalog.create_missing_partitions(
    table_name regclass,
    to_date timestamptz,
    start_from timestamptz DEFAULT NULL)
returns boolean
LANGUAGE plpgsql
AS $$
DECLARE
    missing_partition_record record;
BEGIN

    /*
     * Get missing partition range info using the get_missing_partition_ranges
     * and create partitions using that info. Since timeseries table related checks are
     * handled by get_missing_partition_range we don't check them here again.
     *
     * TODO: Create that using attach partition, which can be implemented with another UDF to make sure that it gets right locks.
     * Consider different types of tables while implemented that (Task 1.3 on sheet)
     */

    FOR missing_partition_record IN
        SELECT *
        FROM get_missing_partition_ranges(table_name, to_date, start_from)
    LOOP
        EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (''%s'') TO (''%s'')', missing_partition_record.partition_name, table_name::text, missing_partition_record.range_from_value, missing_partition_record.range_to_value);
    END LOOP;

    RETURN true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_missing_partitions(
    table_name regclass,
    to_date timestamptz,
    start_from timestamptz)
IS 'create missing partitions for the given timeseries table and range';
