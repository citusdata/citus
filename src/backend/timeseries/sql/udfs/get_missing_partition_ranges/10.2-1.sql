CREATE OR REPLACE FUNCTION pg_catalog.get_missing_partition_ranges(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz DEFAULT NULL)
returns table(
    range_from_value text,
    range_to_value text
)
LANGUAGE plpgsql
AS $$
DECLARE
    table_partition_interval INTERVAL;
    table_partition_column_type_name text;
    current_range_from_value timestamptz := NULL;
    current_range_to_value timestamptz := NULL;
BEGIN
    /*
     * First check whether such timeseries table exists. If not, error out.
     *
     * Then check if start_from is given. If it is, create the partition for that time
     * and let remaining of the function fill the gap from start_from to to_date.
     *
     * TODO: That part is implemented by assuming that there is no partition exist for
     * the given table, in other words that function will be called via create_timeseries_table
     * only when start_from is given. That will be handled while adding starting time for old
     * data ingestion. That part will be implemented with Task 1.2.
     *
     * TODO: Add tests to cover start_from for the PR to handle Task 1.2.
     */

    SELECT partitioninterval
    INTO table_partition_interval
    FROM citus_timeseries.citus_timeseries_tables
    WHERE logicalrelid = table_name;

    IF NOT found THEN
        RAISE '% must be timeseries table', table_name;
    END IF;

    -- Get datatype here to generate range values in the right data format
    -- Since we already check that timeseries tables have single column to partition the table
    -- we can directly get the 0th element of the partattrs column
    SELECT atttypid::regtype::text INTO table_partition_column_type_name
    FROM pg_attribute 
    WHERE attrelid = table_name::oid 
    AND attnum = (select partattrs[0] from pg_partitioned_table where partrelid = table_name::oid);

    IF start_from IS NOT NULL THEN
        /*
         * Decide on the current_range_from_value of the initial partition according to interval of the timeseries table.
         * Since we will create all other partitions by adding intervals, truncating given start time will provide
         * more intuitive interval ranges, instead of starting from start_from directly.
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

        current_range_to_value := current_range_from_value + table_partition_interval;

        -- TODO: Check for dynamic way to do it or create a function for it to use for the call at the end
        IF table_partition_column_type_name = 'date' THEN
            RETURN QUERY SELECT current_range_from_value::date::text, current_range_to_value::date::text;
        ELSIF table_partition_column_type_name = 'timestamp without time zone' THEN
            RETURN QUERY SELECT current_range_from_value::timestamp::text, current_range_to_value::timestamp::text;
        ELSIF table_partition_column_type_name = 'timestamp with time zone' THEN
            RETURN QUERY SELECT current_range_from_value::timestamptz::text, current_range_to_value::timestamptz::text;
        ELSE
            RAISE 'type of the partition column of the table % must be date, timestamp or timestamptz', table_name;
        END IF;
    END IF;

    /*
     * To be able to fill any gaps after the initial partition of the timeseries table,
     * we are starting from the first partition instead of the last.
     *
     * Also note that we must have either start_from or an initial partition for the timeseries
     * table, as we call that function while creating timeseries table first.
     */
    IF current_range_from_value IS NULL AND current_range_to_value IS NULL THEN
        SELECT from_value::timestamptz, to_value::timestamptz
        INTO current_range_from_value, current_range_to_value
        FROM pg_catalog.time_partitions
        WHERE parent_table = table_name
        ORDER BY from_value::timestamptz;
    END IF;

    WHILE current_range_to_value < to_date LOOP
        current_range_from_value := current_range_to_value;
        current_range_to_value := current_range_to_value + table_partition_interval;

        -- Check whether partition with given range has already been created
        -- Since partition interval can be given as, we are converting all variables to timestamptz to make sure
        -- that we are comparing same type of parameters
        PERFORM * FROM pg_catalog.time_partitions WHERE from_value::timestamptz = current_range_from_value::timestamptz AND to_value::timestamptz = current_range_to_value::timestamptz;
        IF found THEN
            CONTINUE;
        END IF;

        -- Check whether any other partition covers from_value or to_value
        -- That means some partitions have been created manually and we must error out.
        PERFORM * FROM pg_catalog.time_partitions 
        WHERE (current_range_from_value::timestamptz >= from_value::timestamptz AND current_range_from_value < to_value::timestamptz) OR 
              (current_range_to_value::timestamptz >= from_value::timestamptz AND current_range_to_value::timestamptz < to_value::timestamptz);
        IF found THEN
            RAISE 'For the table % manual partition(s) has been created, Please remove them to continue using that table as timeseries table', table_name;
        END IF;

        IF table_partition_column_type_name = 'date' THEN
            RETURN QUERY SELECT current_range_from_value::date::text, current_range_to_value::date::text;
        ELSIF table_partition_column_type_name = 'timestamp without time zone' THEN
            RETURN QUERY SELECT current_range_from_value::timestamp::text, current_range_to_value::timestamp::text;
        ELSIF table_partition_column_type_name = 'timestamp with time zone' THEN
            RETURN QUERY SELECT current_range_from_value::timestamptz::text, current_range_to_value::timestamptz::text;
        ELSE
            RAISE 'type of the partition column of the table % must be date, timestamp or timestamptz', table_name;
        END IF;
    END LOOP;

    RETURN;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_missing_partitions(
		table_name regclass,
        to_date timestamptz,
        start_from timestamptz)
IS 'create missing partitions for the given timeseries table';
