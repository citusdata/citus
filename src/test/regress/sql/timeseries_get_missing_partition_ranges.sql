-- Show get_missing_partition_ranges function can be only callede for timeseries tables
CREATE TABLE date_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

SELECT get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '15 days');

-- Show range values for data partitioned table
BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT 
        date_trunc('day', now()) - range_from_value::date as from_diff,
        date_trunc('day', now()) - range_to_value::date as to_diff
    FROM get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '15 days')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT 
        date_trunc('day', now()) - range_from_value::date as from_diff,
        date_trunc('day', now()) - range_to_value::date as to_diff
    FROM get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '15 days', now() - INTERVAL '15 days')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '5 days');
    SELECT 
        date_trunc('day', now()) - range_from_value::date as from_diff,
        date_trunc('day', now()) - range_to_value::date as to_diff
    FROM get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '45 days', now() - INTERVAL '45 days')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 week');
    SELECT 
        date_trunc('week', now()) - range_from_value::date as from_diff,
        date_trunc('week', now()) - range_to_value::date as to_diff
    FROM get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '65 days', now() - INTERVAL '65 days')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day');
    SELECT 
        date_trunc('day', now()) - range_from_value::date as from_diff,
        date_trunc('day', now()) - range_to_value::date as to_diff
    FROM get_missing_partition_ranges('date_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days')
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE date_partitioned_table;

-- Show range values for timestamptz partitioned table
CREATE TABLE tstz_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 hour');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp with time zone as to_diff
    FROM get_missing_partition_ranges('tstz_partitioned_table', now() + INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 hour');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp with time zone as to_diff
    FROM get_missing_partition_ranges('tstz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '6 hours');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp with time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp with time zone as to_diff
    FROM get_missing_partition_ranges('tstz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tstz_partitioned_table', INTERVAL '1 day');
    SELECT 
        date_trunc('day', now()) - range_from_value::timestamp with time zone as from_diff,
        date_trunc('day', now()) - range_to_value::timestamp with time zone as to_diff
    FROM get_missing_partition_ranges('tstz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days')
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE tstz_partitioned_table;

-- Show range values for timestamp without time zone partitioned table
CREATE TABLE tswtz_partitioned_table(
    measureid integer,
    eventdatetime timestamp without time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 hour');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp without time zone as to_diff
    FROM get_missing_partition_ranges('tswtz_partitioned_table', now() + INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 hour');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp without time zone as to_diff
    FROM get_missing_partition_ranges('tswtz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '6 hours');
    SELECT 
        date_trunc('hour', now()) - range_from_value::timestamp without time zone as from_diff,
        date_trunc('hour', now()) - range_to_value::timestamp without time zone as to_diff
    FROM get_missing_partition_ranges('tswtz_partitioned_table', now() + INTERVAL '1 day', now() - INTERVAL '1 day')
    ORDER BY 1,2;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('tswtz_partitioned_table', INTERVAL '1 day');
    SELECT 
        date_trunc('day', now()) - range_from_value::timestamp without time zone as from_diff,
        date_trunc('day', now()) - range_to_value::timestamp without time zone as to_diff
    FROM get_missing_partition_ranges('tswtz_partitioned_table', now() + INTERVAL '5 days', now() - INTERVAL '5 days')
    ORDER BY 1,2;
ROLLBACK;

DROP TABLE tswtz_partitioned_table;
