-- Show that create_timeseries_table only works for empty range partitioned tables
-- 1) Unpartitioned tables are not supported
CREATE TABLE unpartitioned_table(
    measureid integer,
    eventdatetime date,
    measure_data integer);

SELECT create_timeseries_table('unpartitioned_table', INTERVAL '1 day');
DROP TABLE unpartitioned_table;

-- 2) Nonempty tables are not supported
CREATE TABLE nonempty_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

CREATE TABLE nonempty_partitioned_table_2021_2100 PARTITION OF nonempty_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2100-01-01');
INSERT INTO nonempty_partitioned_table VALUES (1, now(), 1);
SELECT create_timeseries_table('nonempty_partitioned_table', INTERVAL '1 year');
DROP TABLE nonempty_partitioned_table;

-- 3) Table must be partitioned on single column
CREATE TABLE multicolumn_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate, measureid);

SELECT create_timeseries_table('multicolumn_partitioned_table', INTERVAL '1 year');
DROP TABLE multicolumn_partitioned_table;

-- 4) Table must be partitioned by range
CREATE TABLE list_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY LIST(eventdate);

SELECT create_timeseries_table('list_partitioned_table', INTERVAL '1 year');
DROP TABLE list_partitioned_table;

CREATE TABLE hash_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY HASH(eventdate);

SELECT create_timeseries_table('hash_partitioned_table', INTERVAL '1 year');
DROP TABLE hash_partitioned_table;

-- Show that partition column type, partition interval and thresholds must align
-- 1) Partition interval must be multiple days for date partitioned tables
CREATE TABLE date_partitioned_table(
    measureid integer,
    eventdate date,
    measure_data integer) PARTITION BY RANGE(eventdate);

SELECT create_timeseries_table('date_partitioned_table', INTERVAL '15 minutes');
SELECT create_timeseries_table('date_partitioned_table', INTERVAL '3 hours');
SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 day 15 minutes');

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '2 days');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 week');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 month');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 month 15 weeks 3 days');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('date_partitioned_table', INTERVAL '1 year 1 month 15 weeks 3 days');
ROLLBACK;

DROP TABLE date_partitioned_table;

-- 2) retention threshold must be greater than compression threshold and
-- compression threshold must be greater than partition interval

-- With date partitioned table
CREATE TABLE ts_comp_date_partitioned_table(
    measureid integer,
    eventdatetime date,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

SELECT create_timeseries_table('ts_comp_date_partitioned_table', INTERVAL '1 week', compression_threshold => INTERVAL '5 days');
SELECT create_timeseries_table('ts_comp_date_partitioned_table', INTERVAL '1 week', compression_threshold => INTERVAL '10 days', retention_threshold => INTERVAL '9 days');
SELECT create_timeseries_table('ts_comp_date_partitioned_table', INTERVAL '1 week', retention_threshold => INTERVAL '5 days');

BEGIN;
    SELECT create_timeseries_table('ts_comp_date_partitioned_table', INTERVAL '1 week', compression_threshold => INTERVAL '10 days');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('ts_comp_date_partitioned_table', INTERVAL '1 week', compression_threshold => INTERVAL '10 days', retention_threshold => INTERVAL '15 days');
ROLLBACK;

DROP TABLE ts_comp_date_partitioned_table;

-- With timestamptz partitioned table
CREATE TABLE ts_comp_tstz_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

SELECT create_timeseries_table('ts_comp_tstz_partitioned_table', INTERVAL '2 hours', compression_threshold => INTERVAL '1 hour');
SELECT create_timeseries_table('ts_comp_tstz_partitioned_table', INTERVAL '2 hours', compression_threshold => INTERVAL '6 hours', retention_threshold => INTERVAL '5 hours');
SELECT create_timeseries_table('ts_comp_tstz_partitioned_table', INTERVAL '2 hours', retention_threshold => INTERVAL '1 hour');

BEGIN;
    SELECT create_timeseries_table('ts_comp_tstz_partitioned_table', INTERVAL '2 hours', compression_threshold => INTERVAL '6 hours');
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('ts_comp_tstz_partitioned_table', INTERVAL '90 minutes', compression_threshold => INTERVAL '180 minutes', retention_threshold => INTERVAL '360 minutes');
ROLLBACK;

DROP TABLE ts_comp_tstz_partitioned_table;

-- Show that create_timeseries_table can be called to provide either pre make interval or start from parameters
CREATE TABLE param_test_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

SELECT create_timeseries_table('param_test_partitioned_table', INTERVAL '90 minutes', premake_interval_count => 7, start_from => now());

BEGIN;
    SELECT create_timeseries_table('param_test_partitioned_table', INTERVAL '90 minutes', premake_interval_count => 7);
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('param_test_partitioned_table', INTERVAL '90 minutes', start_from => now());
ROLLBACK;

DROP TABLE param_test_partitioned_table;

-- Check pre make interval count and post make interval count
CREATE TABLE count_test_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('count_test_partitioned_table', INTERVAL '1 minute', postmake_interval_count => 0, premake_interval_count => 0);
    SELECT count(*)
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'count_test_partitioned_table'::regclass;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('count_test_partitioned_table', INTERVAL '1 hour', postmake_interval_count => 0, premake_interval_count => 5);
    SELECT count(*)
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'count_test_partitioned_table'::regclass;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('count_test_partitioned_table', INTERVAL '2 weeks', postmake_interval_count => 5, premake_interval_count => 0);
    SELECT count(*)
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'count_test_partitioned_table'::regclass;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('count_test_partitioned_table', INTERVAL '2 months', postmake_interval_count => 3, premake_interval_count => 4);
    SELECT count(*)
    FROM pg_catalog.time_partitions
    WHERE parent_table = 'count_test_partitioned_table'::regclass;
ROLLBACK;

DROP TABLE count_test_partitioned_table;

-- Check interval range values
CREATE TABLE range_check_test_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

BEGIN;
    SELECT create_timeseries_table('range_check_test_partitioned_table', INTERVAL '1 hour');
    SELECT partition,
        date_trunc('hour',now()) - from_value::timestamptz as from_diff,
        date_trunc('hour', now()) - to_value::timestamptz as to_diff
    FROM pg_catalog.time_partitions
    ORDER BY 1;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('range_check_test_partitioned_table', INTERVAL '1 day', postmake_interval_count => 5);
    SELECT partition,
        date_trunc('day',now()) - from_value::timestamptz as from_diff,
        date_trunc('day', now()) - to_value::timestamptz as to_diff
    FROM pg_catalog.time_partitions
    ORDER BY 1;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('range_check_test_partitioned_table', INTERVAL '1 week', premake_interval_count => 3);
    SELECT partition,
        date_trunc('week',now()) - from_value::timestamptz as from_diff,
        date_trunc('week', now()) - to_value::timestamptz as to_diff
    FROM pg_catalog.time_partitions
    ORDER BY 1;
ROLLBACK;

BEGIN;
    SELECT create_timeseries_table('range_check_test_partitioned_table', INTERVAL '1 week', start_from => now() - INTERVAL '4 weeks');
    SELECT partition,
        date_trunc('week',now()) - from_value::timestamptz as from_diff,
        date_trunc('week', now()) - to_value::timestamptz as to_diff
    FROM pg_catalog.time_partitions
    ORDER BY 1;
ROLLBACK;

-- Check drop table
CREATE TABLE drop_check_test_partitioned_table(
    measureid integer,
    eventdatetime timestamp with time zone,
    measure_data integer) PARTITION BY RANGE(eventdatetime);

SELECT create_timeseries_table('drop_check_test_partitioned_table', INTERVAL '2 hours');
SELECT * FROM timeseries.tables;
DROP TABLE drop_check_test_partitioned_table;
SELECT * FROM timeseries.tables;

BEGIN;
    CREATE TABLE drop_check_test_partitioned_table(
        measureid integer,
        eventdatetime timestamp with time zone,
        measure_data integer) PARTITION BY RANGE(eventdatetime);
    SELECT create_timeseries_table('drop_check_test_partitioned_table', INTERVAL '2 hours');
    SELECT * FROM timeseries.tables;
    DROP TABLE drop_check_test_partitioned_table;
    SELECT * FROM timeseries.tables;
COMMIT;
