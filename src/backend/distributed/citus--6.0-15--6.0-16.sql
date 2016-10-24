/* citus--6.0-9--6.0-10.sql */

CREATE FUNCTION citus_running_version()
  RETURNS text
  LANGUAGE C IMMUTABLE
  AS 'MODULE_PATHNAME', $$citus_running_version$$;
COMMENT ON FUNCTION citus_running_version()
  IS 'Get the version of the loaded citus module';
