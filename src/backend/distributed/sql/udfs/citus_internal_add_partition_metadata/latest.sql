CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_partition_metadata(
							relation_id regclass, distribution_method "char",
							distribution_column text, colocation_id integer,
							replication_model "char")
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_add_partition_metadata(regclass, "char", text, integer, "char") IS
    'Inserts into pg_dist_partition with user checks';
