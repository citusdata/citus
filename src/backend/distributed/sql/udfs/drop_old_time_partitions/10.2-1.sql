CREATE OR REPLACE PROCEDURE pg_catalog.drop_old_time_partitions(
	table_name regclass,
	older_than timestamptz)
LANGUAGE plpgsql
AS $$
DECLARE
    -- properties of the partitioned table
    number_of_partition_columns int;
    partition_column_index int;
    partition_column_type regtype;

    r record;
BEGIN
    -- check whether the table is time partitioned table, if not error out
    SELECT partnatts, partattrs[0]
    INTO number_of_partition_columns, partition_column_index
    FROM pg_catalog.pg_partitioned_table
    WHERE partrelid = table_name;

    IF NOT FOUND THEN
        RAISE '% is not partitioned', table_name::text;
    ELSIF number_of_partition_columns <> 1 THEN
        RAISE 'partitioned tables with multiple partition columns are not supported';
    END IF;

    -- get datatype here to check interval-table type
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

    FOR r IN
		SELECT partition, nspname AS schema_name, relname AS table_name, from_value, to_value
		FROM pg_catalog.time_partitions, pg_catalog.pg_class c, pg_catalog.pg_namespace n
		WHERE parent_table = table_name AND partition = c.oid AND c.relnamespace = n.oid
		AND to_value IS NOT NULL
		AND to_value::timestamptz <= older_than
		ORDER BY to_value::timestamptz
    LOOP
        RAISE NOTICE 'dropping % with start time % and end time %', r.partition, r.from_value, r.to_value;
        EXECUTE format('DROP TABLE %I.%I', r.schema_name, r.table_name);
    END LOOP;
END;
$$;
COMMENT ON PROCEDURE pg_catalog.drop_old_time_partitions(
	table_name regclass,
	older_than timestamptz)
IS 'drop old partitions of a time-partitioned table';
