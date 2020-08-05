CREATE OR REPLACE FUNCTION pg_catalog.undistribute_table(
    table_name regclass)
    RETURNS VOID
    LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$undistribute_table$$;

COMMENT ON FUNCTION pg_catalog.undistribute_table(
    table_name regclass)
    IS 'undistributes a distributed table';
