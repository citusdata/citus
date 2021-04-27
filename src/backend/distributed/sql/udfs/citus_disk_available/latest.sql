CREATE OR REPLACE FUNCTION pg_catalog.citus_disk_available()
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_disk_available()
  IS 'a function that checks the amount of disk space that is available in bytes';
