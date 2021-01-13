CREATE OR REPLACE FUNCTION pg_catalog.alter_distributed_table(
    table_name regclass, distribution_column text DEFAULT NULL, shard_count int DEFAULT NULL, colocate_with text DEFAULT NULL, cascade_to_colocated boolean DEFAULT NULL)
    RETURNS VOID
    LANGUAGE C
AS 'MODULE_PATHNAME', $$alter_distributed_table$$;

COMMENT ON FUNCTION pg_catalog.alter_distributed_table(
    table_name regclass, distribution_column text, shard_count int, colocate_with text, cascade_to_colocated boolean)
    IS 'alters a distributed table';
