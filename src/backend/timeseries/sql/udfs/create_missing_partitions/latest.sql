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
     * First check whether such timeseries table exists. If not, error out.
     *
     * Then check if start_from is given. If it is, create the partition for that time
     * and let remaining of the function fill the gap from start_from to to_date.
     *
     * TODO: That part is implemented by assuming that there is no partition exist for
     * the given table, in other words that function will be called via create_timeseries_table
     * only. That will be handled while adding starting time for old data ingestion (Task 1.3)
     */

    SELECT partitioninterval
    INTO table_partition_interval
    FROM citus_timeseries.citus_timeseries_tables
    WHERE logicalrelid = table_name;

    IF NOT found THEN
        RAISE '% must be timeseries table', table_name;
    END IF;

    IF start_from IS NOT NULL THEN
        /*
         * Decide on the current_range_from_value of the initial partition according to interval of the timeseries table.
         * Since we will create all other partitions by adding intervals, truncating given start time will provide
         * more intuitive interval ranges.
         */
        IF table_partition_interval < INTERVAL '1 hour' THEN
            current_range_from_value = date_trunc('minute', start_from);
        ELSIF table_partition_interval < INTERVAL '1 day' THEN
            current_range_from_value = date_trunc('hour', start_from);
        ELSIF table_partition_interval < INTERVAL '1 week' THEN
            current_range_from_value = date_trunc('day', start_from);
        ELSIF table_partition_interval < INTERVAL '1 month' THEN
            current_range_from_value = date_trunc('week', start_from);
        ELSIF table_partition_interval < INTERVAL '1 year' THEN
            current_range_from_value = date_trunc('month', start_from);
        ELSE
            current_range_from_value = date_trunc('year', start_from);
        END IF;

        current_partition_count := 0;
        current_range_to_value := current_range_from_value + table_partition_interval;
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

    WHILE current_range_to_value < to_date LOOP
        current_range_from_value := current_range_to_value;
        current_range_to_value := current_range_to_value + table_partition_interval;
        current_partition_name := table_name::text || '_' || current_partition_count::text; 

        -- TODO: Create that using attach partition, which can be implemented with another UDF to make sure that it gets light locks.
        BEGIN
            EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (''%I'') TO (''%I'')', current_partition_name, table_name::text, current_range_from_value::text, current_range_to_value::text);
        EXCEPTION WHEN OTHERS THEN
            raise DEBUG3 'Partition has already been created for the range from % to %', current_range_from_value::text, current_range_to_value::text;
        END;

        current_partition_count := current_partition_count + 1;
    END LOOP;

    RETURN true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz)
IS 'create missing partitions for the given timeseries table';