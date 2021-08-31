-- Show get_missing_partition_ranges function can be only callede for timeseries tables
CREATE TABLE date_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '15 days');

-- Create missing partitions for various ranges on date partitioned table
BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '15 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '10 days', now() - INTERVAL '10 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '5 days');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '45 days', now() - INTERVAL '45 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

-- Show start from date must be before any of existing partition ranges
BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE date_partitioned_table;

-- Create missing partitions for various ranges on timestamptz partitioned table
CREATE TABLE tstz_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 hour');
    SELECT create_missing_partitions('tstz_partitioned_table', now() + INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp with time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tstz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 hour');
    SELECT create_missing_partitions('tstz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp with time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tstz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '6 hours');
    SELECT create_missing_partitions('tstz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp with time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tstz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

-- Show start from date must be before any of existing partition ranges
BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('tstz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days');
    SELECT
        date_trunc('day', now()) - from_value::timestamp with time zone as from_diff,
        date_trunc('day', now()) - to_value::timestamp with time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tstz_partitioned_table'::regclass;
ROLLBACK;

DROP TABLE tstz_partitioned_table;

-- Show range values for timestamp without time zone partitioned table
CREATE TABLE tswtz_partitioned_table(
    measureid integer,
    eventdatetime timestamp without time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 hour');
    SELECT create_missing_partitions('tswtz_partitioned_table', now() + INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp without time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tswtz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 hour');
    SELECT create_missing_partitions('tswtz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp without time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tswtz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '6 hours');
    SELECT create_missing_partitions('tswtz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp without time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tswtz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

-- Show start from date must be before any of existing partition ranges
BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('tswtz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days');
    SELECT
        date_trunc('day', now()) - from_value::timestamp without time zone as from_diff,
        date_trunc('day', now()) - to_value::timestamp without time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tswtz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE tswtz_partitioned_table;

-- Create missing partitions for a table with schema given
CREATE SCHEMA timeseries_test_schema;

CREATE TABLE timeseries_test_schema.schema_test_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

BEGIN;
    SELECT create_timeseries_table('timeseries_test_schema.schema_test_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('timeseries_test_schema.schema_test_partitioned_table', now() + INTERVAL '15 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'timeseries_test_schema.schema_test_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('timeseries_test_schema.schema_test_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('timeseries_test_schema.schema_test_partitioned_table', now() + INTERVAL '10 days', now() - INTERVAL '10 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'timeseries_test_schema.schema_test_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE timeseries_test_schema.schema_test_partitioned_table;
DROP SCHEMA timeseries_test_schema;

-- Test with absolute time results
CREATE TABLE absolute_times_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

BEGIN;
    SELECT create_timeseries_table('absolute_times_partitioned_table', INTERVAL '1 month');
    SELECT create_missing_partitions('absolute_times_partitioned_table', '2030-01-01'::date, '2020-01-01'::date);

    SELECT *
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'absolute_times_partitioned_table'::regclass
    ORDER BY 1,2,3;
ROLLBACK;

DROP TABLE absolute_times_partitioned_table;
