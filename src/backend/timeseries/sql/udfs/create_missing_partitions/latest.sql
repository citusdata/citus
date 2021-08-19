CREATE OR REPLACE FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz DEFAULT NULL)
returns boolean
LANGUAGE plpgsql
AS $$
DECLARE
    table_partition_interval INTERVAL;
    current_range_from_value timestamptz;
    current_range_to_value timestamptz;
    current_partition_name text;
    current_partition_count int;
BEGIN
    /*
     * TODO: Check whether the table is timeseries table with INTERVAL partition range
     * and timestamptz partition column
     */
    
    /*
     * Then check if start_from is given. If it is, create the partition for that time
     * and let remaining of the function fill the gap from start_from to to_date.
     *
     * TODO: That part is implemented by assuming that there is no partition exist for
     * the given table, in other words that function will be called via create_timeseries_table
     * only.
     * 
     * TODO: Handle date trunc according to given interval of the timeseries table
     */

    SELECT partitioninterval
    INTO table_partition_interval
    FROM citus_timeseries.citus_timeseries_tables
    WHERE logicalrelid = table_name;

    IF start_from IS NOT NULL THEN
        RAISE NOTICE 'IN START FROM';
        current_partition_count := 0;
        current_range_from_value := start_from;
        current_range_to_value := start_from + table_partition_interval;
        current_partition_name := table_name::text || '_' || current_partition_count::text; 

        EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (''%I'') TO (''%I'')', current_partition_name, table_name::text, current_range_from_value::text, current_range_to_value::text);
    END IF;

    /*
     * At this point, it is assumed that initial partition of the timeseries table
     * exists. Remaining partitions till to_date will be created if any partition
     * missing
     */
    SELECT from_value::timestamptz, to_value::timestamptz
    INTO current_range_from_value, current_range_to_value
    FROM pg_catalog.time_partitions
    WHERE parent_table = table_name
    
    ORDER BY from_value::timestamptz;

    SELECT count(*)
    INTO current_partition_count
    FROM pg_catalog.time_partitions
    WHERE parent_table = table_name;

    RAISE NOTICE 'current_range_from_value %', current_range_from_value;

    WHILE current_range_from_value < to_date LOOP
        current_range_from_value := current_range_to_value;
        current_range_to_value := current_range_to_value + table_partition_interval;
        current_partition_name := table_name::text || '_' || current_partition_count::text; 

        -- TODO: Create that using attach partition, which can be implemented with another UDF to make sure that it gets light locks.
        -- TODO: Add exception handling or partition check
        BEGIN
            EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (''%I'') TO (''%I'')', current_partition_name, table_name::text, current_range_from_value::text, current_range_to_value::text);
        EXCEPTION WHEN OTHERS THEN
            raise notice 'oops %', sqlstate;
        END;

        current_partition_count := current_partition_count + 1;
    END LOOP;

    return true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz)
IS 'create missing partitions for the given timeseries table';