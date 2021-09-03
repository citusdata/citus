CREATE OR REPLACE FUNCTION pg_catalog.get_missing_time_partition_ranges(
    table_name regclass,
    partition_interval INTERVAL,
    to_value timestamptz,
    from_value timestamptz DEFAULT now())
returns table(
    partition_name text,
    range_from_value text,
    range_to_value text)
LANGUAGE plpgsql
AS $$
DECLARE
    -- properties of the partitioned table
    table_name_text text;
    table_schema_text text;
    number_of_partition_columns int;
    partition_column_index int;
    partition_column_type regtype;

    -- used for generating time ranges
    current_range_from_value timestamptz := NULL;
    current_range_to_value timestamptz := NULL;
    current_range_from_value_text text;
    current_range_to_value_text text;

    -- used to check whether there are misaligned (manually created) partitions
    manual_partition regclass;
    manual_partition_from_value_text text;
    manual_partition_to_value_text text;

    -- used for partition naming
    partition_name_format text;
    max_table_name_length int := current_setting('max_identifier_length');

    -- used to determine whether the partition_interval is a day multiple
    is_day_multiple boolean;
BEGIN
    -- check whether the table is time partitioned table, if not error out
    SELECT relname, nspname, partnatts, partattrs[0]
    INTO table_name_text, table_schema_text, number_of_partition_columns, partition_column_index
    FROM pg_catalog.pg_partitioned_table, pg_catalog.pg_class c, pg_catalog.pg_namespace n
    WHERE partrelid = c.oid AND c.oid = table_name
    AND c.relnamespace = n.oid;
    IF NOT FOUND THEN
        RAISE '% is not partitioned', table_name;
    ELSIF number_of_partition_columns <> 1 THEN
        RAISE 'partitioned tables with multiple partition columns are not supported';
    END IF;

    -- to not to have partitions to be created in parallel
    EXECUTE format('LOCK TABLE %I.%I IN SHARE UPDATE EXCLUSIVE MODE', table_schema_text, table_name_text);

    -- get datatype here to check interval-table type alignment and generate range values in the right data format
    SELECT atttypid
    INTO partition_column_type
    FROM pg_attribute
    WHERE attrelid = table_name::oid
    AND attnum = partition_column_index;

    -- we currently only support partitioning by date, timestamp, and timestamptz
    IF partition_column_type <> 'date'::regtype
    AND partition_column_type <> 'timestamp'::regtype
    AND partition_column_type <> 'timestamptz'::regtype  THEN
        RAISE 'type of the partition column of the table % must be date, timestamp or timestamptz', table_name;
    END IF;

    IF partition_column_type = 'date'::regtype AND partition_interval IS NOT NULL THEN
        SELECT date_trunc('day', partition_interval) = partition_interval
        INTO is_day_multiple;

        IF NOT is_day_multiple THEN
            RAISE 'partition interval of date partitioned table must be day or multiple days';
        END IF;
    END IF;

    -- If no partition exists, truncate from_value to find intuitive initial value.
    -- If any partition exist, use the initial partition as the pivot partition.
    -- tp.to_value and tp.from_value are equal to '', if default partition exists.
    SELECT tp.from_value::timestamptz, tp.to_value::timestamptz
    INTO current_range_from_value, current_range_to_value
    FROM pg_catalog.time_partitions tp
    WHERE parent_table = table_name AND tp.to_value <> '' AND tp.from_value <> ''
    ORDER BY tp.from_value::timestamptz ASC
    LIMIT 1;

    IF NOT FOUND THEN
        -- Decide on the current_range_from_value of the initial partition according to interval of the table.
        -- Since we will create all other partitions by adding intervals, truncating given start time will provide
        -- more intuitive interval ranges, instead of starting from from_value directly.
        IF partition_interval < INTERVAL '1 hour' THEN
            current_range_from_value = date_trunc('minute', from_value);
        ELSIF partition_interval < INTERVAL '1 day' THEN
            current_range_from_value = date_trunc('hour', from_value);
        ELSIF partition_interval < INTERVAL '1 week' THEN
            current_range_from_value = date_trunc('day', from_value);
        ELSIF partition_interval < INTERVAL '1 month' THEN
            current_range_from_value = date_trunc('week', from_value);
        ELSIF partition_interval = INTERVAL '3 months' THEN
            current_range_from_value = date_trunc('quarter', from_value);
        ELSIF partition_interval < INTERVAL '1 year' THEN
            current_range_from_value = date_trunc('month', from_value);
        ELSE
            current_range_from_value = date_trunc('year', from_value);
        END IF;

        current_range_to_value := current_range_from_value + partition_interval;
    ELSE
        -- if from_value is newer than pivot's from value, go forward, else go backward
        IF from_value >= current_range_from_value THEN
            WHILE current_range_from_value < from_value LOOP
                    current_range_from_value := current_range_from_value + partition_interval;
            END LOOP;
        ELSE
            WHILE current_range_from_value > from_value LOOP
                    current_range_from_value := current_range_from_value - partition_interval;
            END LOOP;
        END IF;
        current_range_to_value := current_range_from_value + partition_interval;
    END IF;

    -- reuse pg_partman naming scheme for back-and-forth migration
    IF partition_interval = INTERVAL '3 months' THEN
        -- include quarter in partition name
        partition_name_format = 'YYYY"q"Q';
    ELSIF partition_interval = INTERVAL '1 week' THEN
        -- include week number in partition name
        partition_name_format := 'IYYY"w"IW';
    ELSE
        -- always start with the year
        partition_name_format := 'YYYY';

        IF partition_interval < INTERVAL '1 year' THEN
            -- include month in partition name
            partition_name_format := partition_name_format || '_MM';
        END IF;

        IF partition_interval < INTERVAL '1 month' THEN
            -- include day of month in partition name
            partition_name_format := partition_name_format || '_DD';
        END IF;

        IF partition_interval < INTERVAL '1 day' THEN
            -- include time of day in partition name
            partition_name_format := partition_name_format || '_HH24MI';
        END IF;

        IF partition_interval < INTERVAL '1 minute' THEN
             -- include seconds in time of day in partition name
             partition_name_format := partition_name_format || 'SS';
        END IF;
    END IF;

    WHILE current_range_from_value < to_value LOOP
        -- Check whether partition with given range has already been created
        -- Since partition interval can be given with different types, we are converting
        -- all variables to timestamptz to make sure that we are comparing same type of parameters
        PERFORM * FROM pg_catalog.time_partitions tp
        WHERE
            tp.from_value::timestamptz = current_range_from_value::timestamptz AND
            tp.to_value::timestamptz = current_range_to_value::timestamptz AND
            parent_table = table_name;
        IF found THEN
            current_range_from_value := current_range_to_value;
            current_range_to_value := current_range_to_value + partition_interval;
            CONTINUE;
        END IF;

        -- Check whether any other partition covers from_value or to_value
        -- That means some partitions doesn't align with the initial partition.
        -- In other words, gap(s) exist between partitions which is not multiple of intervals.
        SELECT partition, tp.from_value::text, tp.to_value::text
        INTO manual_partition, manual_partition_from_value_text, manual_partition_to_value_text
        FROM pg_catalog.time_partitions tp
        WHERE
            ((current_range_from_value::timestamptz >= tp.from_value::timestamptz AND current_range_from_value < tp.to_value::timestamptz) OR
            (current_range_to_value::timestamptz > tp.from_value::timestamptz AND current_range_to_value::timestamptz < tp.to_value::timestamptz)) AND
            parent_table = table_name;

        IF found THEN
            RAISE 'partition % with the range from % to % does not align with the initial partition given the partition interval',
            manual_partition::text,
            manual_partition_from_value_text,
            manual_partition_to_value_text
			USING HINT = 'Only use partitions of the same size, without gaps between partitions.';
        END IF;

        IF partition_column_type = 'date'::regtype THEN
            SELECT current_range_from_value::date::text INTO current_range_from_value_text;
            SELECT current_range_to_value::date::text INTO current_range_to_value_text;
        ELSIF partition_column_type = 'timestamp without time zone'::regtype THEN
            SELECT current_range_from_value::timestamp::text INTO current_range_from_value_text;
            SELECT current_range_to_value::timestamp::text INTO current_range_to_value_text;
        ELSIF partition_column_type = 'timestamp with time zone'::regtype THEN
            SELECT current_range_from_value::timestamptz::text INTO current_range_from_value_text;
            SELECT current_range_to_value::timestamptz::text INTO current_range_to_value_text;
        ELSE
            RAISE 'type of the partition column of the table % must be date, timestamp or timestamptz', table_name;
        END IF;

        -- use range values within the name of partition to have unique partition names
        RETURN QUERY
        SELECT
            substring(table_name_text, 0, max_table_name_length - length(to_char(current_range_from_value, partition_name_format)) - 1) || '_p' ||
            to_char(current_range_from_value, partition_name_format),
            current_range_from_value_text,
            current_range_to_value_text;

        current_range_from_value := current_range_to_value;
        current_range_to_value := current_range_to_value + partition_interval;
    END LOOP;
    RETURN;
END;
$$;
COMMENT ON FUNCTION pg_catalog.get_missing_time_partition_ranges(
	table_name regclass,
    partition_interval INTERVAL,
    to_value timestamptz,
    from_value timestamptz)
IS 'get missing partitions ranges for table within the range using the given interval';
