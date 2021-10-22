CREATE OR REPLACE FUNCTION pg_catalog.create_time_partitions(
    table_name regclass,
    partition_interval INTERVAL,
    end_at timestamptz,
    start_from timestamptz DEFAULT now())
returns boolean
LANGUAGE plpgsql
AS $$
DECLARE
    -- partitioned table name
    schema_name_text name;
    table_name_text name;

    -- record for to-be-created partition
    missing_partition_record record;

    -- result indiciates whether any partitions were created
    partition_created bool := false;
BEGIN
    IF start_from >= end_at THEN
        RAISE 'start_from (%) must be older than end_at (%)', start_from, end_at;
    END IF;

    SELECT nspname, relname
    INTO schema_name_text, table_name_text
    FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
    WHERE pg_class.oid = table_name::oid;

    -- Get missing partition range info using the get_missing_partition_ranges
    -- and create partitions using that info.
    FOR missing_partition_record IN
        SELECT *
        FROM get_missing_time_partition_ranges(table_name, partition_interval, end_at, start_from)
    LOOP
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
        schema_name_text,
        missing_partition_record.partition_name,
        schema_name_text,
        table_name_text,
        missing_partition_record.range_from_value,
        missing_partition_record.range_to_value);

        partition_created := true;
    END LOOP;

    RETURN partition_created;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_time_partitions(
    table_name regclass,
    partition_interval INTERVAL,
    end_at timestamptz,
    start_from timestamptz)
IS 'create time partitions for the given range';
