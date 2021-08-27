SET search_path TO timeseries;

DROP FUNCTION pg_catalog.create_missing_partitions(regclass,timestamp with time zone,timestamp with time zone);
DROP FUNCTION pg_catalog.create_timeseries_table(regclass,interval,integer,integer,timestamp with time zone,interval,interval);
DROP FUNCTION pg_catalog.drop_timeseries_table(regclass);
DROP FUNCTION pg_catalog.get_missing_partition_ranges(regclass,timestamp with time zone,timestamp with time zone);

DROP TABLE tables;

RESET search_path;
DROP SCHEMA timeseries;
