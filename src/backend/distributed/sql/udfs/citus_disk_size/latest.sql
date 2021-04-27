CREATE OR REPLACE FUNCTION pg_catalog.citus_disk_size()
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_disk_size()
  IS 'a function that checks the size of the disk in bytes';
