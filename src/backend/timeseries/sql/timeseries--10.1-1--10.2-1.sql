/* timeseries--10.1-1--10.2-1.sql */

CREATE SCHEMA timeseries;
SET search_path TO timeseries;

CREATE TABLE tables (
    logicalrelid regclass NOT NULL PRIMARY KEY,
    partitioninterval INTERVAL NOT NULL,
    postmakeintervalcount INT NOT NULL,
    premakeintervalcount INT,
    startfrom timestamptz,
    compressionthreshold INTERVAL,
    retentionthreshold INTERVAL
);

COMMENT ON TABLE tables IS 'Keeps interval and threshold informations for timeseries tables';

-- grant read access for timeseries metadata tables to unprivileged user
GRANT USAGE ON SCHEMA timeseries TO PUBLIC;
GRANT SELECT ON ALL tables IN SCHEMA timeseries TO PUBLIC;

RESET search_path;
