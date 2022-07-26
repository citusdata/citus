CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_partition_metadata(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_partition_metadata(regclass) IS
    'Deletes a row from pg_dist_partition with table ownership checks';

