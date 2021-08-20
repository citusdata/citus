/* timeseries--10.1-1--10.2-1.sql */

CREATE SCHEMA citus_timeseries;
SET search_path TO citus_timeseries;

CREATE TABLE citus_timeseries_tables (
    logicalrelid regclass NOT NULL PRIMARY KEY,
    partitioninterval INTERVAL NOT NULL,
    premakeintervalcount INT NOT NULL,
    postmakeintervalcount INT NOT NULL,
    compressionthreshold INTERVAL,
    retentionthreshold INTERVAL
);

COMMENT ON TABLE citus_timeseries_tables IS 'Keeps interval and threshold informations for timeseries tables';

-- grant read access for timeseries metadata tables to unprivileged user
GRANT USAGE ON SCHEMA citus_timeseries TO PUBLIC;
GRANT SELECT ON ALL tables IN SCHEMA citus_timeseries TO PUBLIC;

RESET search_path;

-- Add trigger to delete from here
-- Add trigger to unschedule cron jobs in future
