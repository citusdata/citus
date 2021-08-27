SET search_path TO timeseries;

DROP FUNCTION pg_catalog.create_missing_partitions(regclass,timestamp with time zone,timestamp with time zone);
DROP FUNCTION pg_catalog.create_timeseries_table(regclass,interval,integer,integer,timestamp with time zone,interval,interval);
DROP FUNCTION pg_catalog.get_missing_partition_ranges(regclass,timestamp with time zone,timestamp with time zone);

-- In Citus 10.2, we added another internal udf (drop_timeseries_table_metadata)
-- to be called by citus_drop_trigger. Since this script is executed when
-- downgrading Citus, we don't have drop_timeseries_table_metadata in citus.so.
-- For this reason, we first need to downgrade citus_drop_trigger so it doesn't
-- call drop_timeseries_table_metadata.
#include "../../../distributed/sql/udfs/citus_drop_trigger/10.0-1.sql"

-- Now we can safely drop drop_timeseries_table_metadata as we downgraded citus_drop_trigger.
DROP FUNCTION pg_catalog.drop_timeseries_table_metadata(regclass);
DROP TABLE tables;

RESET search_path;
DROP SCHEMA timeseries;
