CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_update_none_dist_table_metadata(
    relation_id oid,
    replication_model "char",
    colocation_id bigint,
    auto_converted boolean)
RETURNS void
LANGUAGE C
VOLATILE
AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_update_none_dist_table_metadata(oid, "char", bigint, boolean)
    IS 'Update pg_dist_node metadata for given none-distributed table, to convert it to another type of none-distributed table.';
