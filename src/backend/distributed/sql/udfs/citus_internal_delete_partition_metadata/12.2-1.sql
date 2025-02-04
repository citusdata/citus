CREATE OR REPLACE FUNCTION citus_internal.delete_partition_metadata(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_delete_partition_metadata$$;
COMMENT ON FUNCTION citus_internal.delete_partition_metadata(regclass) IS
    'Deletes a row from pg_dist_partition with table ownership checks';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_partition_metadata(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_partition_metadata(regclass) IS
    'Deletes a row from pg_dist_partition with table ownership checks';

