-- Heavily inspired by the procedure alter_old_partitions_set_access_method
CREATE OR REPLACE PROCEDURE pg_catalog.expire_old_time_partitions(
		parent_table_name regclass,
		older_than timestamptz)
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
BEGIN
	/* first check whether we can convert all the to_value's to timestamptz */
	BEGIN
		PERFORM
		FROM pg_catalog.time_partitions
		WHERE parent_table = parent_table_name
		AND to_value IS NOT NULL
		AND to_value::timestamptz <= older_than;
	EXCEPTION WHEN invalid_datetime_format THEN
		RAISE 'partition column of % cannot be cast to a timestamptz', parent_table_name;
	END;

	/* now drop the partitions in separate transactions */
    FOR r IN
		SELECT partition, from_value, to_value
		FROM pg_catalog.time_partitions
		WHERE parent_table = parent_table_name
		AND to_value IS NOT NULL
		AND to_value::timestamptz <= older_than
		ORDER BY to_value::timestamptz
    LOOP
        RAISE NOTICE 'dropping % with start time % and end time %', r.partition, r.from_value, r.to_value;
        EXECUTE format('DROP TABLE %I', r.partition);
        COMMIT;
    END LOOP;
END;
$$;
COMMENT ON PROCEDURE pg_catalog.expire_old_time_partitions(
		parent_table_name regclass,
		older_than timestamptz)
IS 'drop old partitions of a time-partitioned table';
