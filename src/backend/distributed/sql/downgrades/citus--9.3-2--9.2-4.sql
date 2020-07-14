-- citus--9.3-2--9.2-4
-- this is a downgrade path that will revert the changes made in citus--9.2-4--9.3-2.sql
--
-- 9.3-2 added citus extension owner as a distributed object, if not already in there.
-- However we can not really know if it was a distributed owner prior to 9.3-2.
-- That's why we leave the record in place.

-- Revert the return type to void
DROP FUNCTION pg_catalog.citus_extradata_container(INTERNAL);
CREATE FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_extradata_container$$;
COMMENT ON FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    IS 'placeholder function to store additional data in postgres node trees';

-- Remove newly introduced functions that are absent in earlier versions
DROP FUNCTION pg_catalog.update_distributed_table_colocation(regclass, text);
DROP FUNCTION pg_catalog.replicate_reference_tables();
DROP FUNCTION pg_catalog.citus_remote_connection_stats(
    OUT hostname text,
    OUT port int,
    OUT database_name text,
    OUT connection_count_to_node int);
DROP FUNCTION pg_catalog.worker_create_or_alter_role(
    role_name text,
    create_role_utility_query text,
    alter_role_utility_query text);
DROP FUNCTION pg_catalog.truncate_local_data_after_distributing_table(
    function_name regclass);
