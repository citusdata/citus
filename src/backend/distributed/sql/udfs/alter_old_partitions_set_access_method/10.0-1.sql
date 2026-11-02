CREATE OR REPLACE PROCEDURE pg_catalog.alter_old_partitions_set_access_method(
		parent_table_name regclass,
		older_than timestamptz,
		new_access_method name)
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
    parent_table_name_text text;
BEGIN
    SELECT pg_catalog.quote_ident(relname)
    INTO parent_table_name_text
    FROM pg_catalog.pg_class
    WHERE oid = parent_table_name;
	-- first check whether we can convert all the to_value's to timestamptz
	BEGIN
		PERFORM
		FROM pg_catalog.time_partitions
		WHERE parent_table = parent_table_name
		AND to_value IS NOT NULL
		AND to_value::timestamptz <= older_than
		AND access_method <> new_access_method;
	EXCEPTION WHEN invalid_datetime_format THEN
		RAISE 'partition column of % cannot be cast to a timestamptz', parent_table_name_text;
	END;

	-- now convert the partitions in separate transactions
    FOR r IN
		SELECT partition, c.relname AS partition_name, from_value, to_value
		FROM pg_catalog.time_partitions tp
        JOIN pg_catalog.pg_class c ON tp.partition = c.oid
		WHERE parent_table = parent_table_name
		AND to_value IS NOT NULL
		AND to_value::timestamptz <= older_than
		AND access_method <> new_access_method
		ORDER BY to_value::timestamptz
    LOOP
        RAISE NOTICE 'converting % with start time % and end time %', pg_catalog.quote_ident(r.partition_name), r.from_value, r.to_value;
        PERFORM pg_catalog.alter_table_set_access_method(r.partition, new_access_method);
        COMMIT;
    END LOOP;
END;
$$;
COMMENT ON PROCEDURE pg_catalog.alter_old_partitions_set_access_method(
		parent_table_name regclass,
		older_than timestamptz,
		new_access_method name)
IS 'convert old partitions of a time-partitioned table to a new access method';
