CREATE OR REPLACE FUNCTION pg_catalog.citus_version_num()
    RETURNS integer
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_version_num$$;
COMMENT ON FUNCTION pg_catalog.citus_version_num()
    IS 'Citus version of the loaded library as a comparable integer (major*10000 + minor*100 + patch)';
