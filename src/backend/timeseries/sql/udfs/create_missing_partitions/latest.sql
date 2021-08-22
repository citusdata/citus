CREATE OR REPLACE FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz DEFAULT NULL)
returns boolean
LANGUAGE plpgsql
AS $$
DECLARE
    range_values_record record;
    current_partition_name text;
    current_partition_count int;
BEGIN

    /*
     * Get the partition ranges using the get_missing_partition_ranges and create
     * partitions for those ranges. Since timeseries table related checks are
     * handled by get_missing_partition_range we don't check them here again.
     *
     * TODO: Create that using attach partition, which can be implemented with another UDF to make sure that it gets right locks.
     * Consider different types of tables while implemented that (Task 1.3 on sheet)
     */

    SELECT count(*)
    INTO current_partition_count
    FROM pg_catalog.time_partitions
    WHERE parent_table = table_name;

    FOR range_values_record IN
        SELECT *
        FROM get_missing_partition_ranges(table_name, to_date, start_from)
    LOOP
        current_partition_name := table_name::text || '_' || current_partition_count::text; 
        EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (''%I'') TO (''%I'')', current_partition_name, table_name::text, range_values_record.range_from_value, range_values_record.range_to_value);
        current_partition_count := current_partition_count + 1;
    END LOOP;

    RETURN true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz)
IS 'create missing partitions for the given timeseries table and range';
