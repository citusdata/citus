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
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '10 days', now() + INTERVAL '10 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '5 days');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '45 days', now() + INTERVAL '45 days');
    SELECT
        date_trunc('day', now()) - from_value::date as from_diff,
        date_trunc('day', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 week');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '85 days', now() + INTERVAL '85 days');
    SELECT
        date_trunc('week', now()) - from_value::date as from_diff,
        date_trunc('week', now()) - to_value::date as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'date_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT create_missing_partitions('date_partitioned_table', now() + INTERVAL '5 days', now() + INTERVAL '5 days');
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
    SELECT create_missing_partitions('tstz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp with time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tstz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

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
    SELECT create_missing_partitions('tswtz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day');
    SELECT
        date_trunc('hour', now()) - from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - to_value::timestamp without time zone as to_diff
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'tswtz_partitioned_table'::regclass
    ORDER BY 1,2;
ROLLBACK;

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
