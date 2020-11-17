CREATE OR REPLACE FUNCTION pg_catalog.alter_cstore_table_set(
    table_name regclass,
    block_row_count int DEFAULT NULL,
    stripe_row_count int DEFAULT NULL,
    compression name DEFAULT null)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_cstore_table_set';

COMMENT ON FUNCTION pg_catalog.alter_cstore_table_set(
    table_name regclass,
    block_row_count int,
    stripe_row_count int,
    compression name)
IS 'set one or more options on a cstore table, when set to NULL no change is made';
