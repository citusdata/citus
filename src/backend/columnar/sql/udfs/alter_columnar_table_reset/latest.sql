CREATE OR REPLACE FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_row_count bool DEFAULT false,
    stripe_row_count bool DEFAULT false,
    compression bool DEFAULT false,
    compression_level bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_columnar_table_reset';

COMMENT ON FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_row_count bool,
    stripe_row_count bool,
    compression bool,
    compression_level bool)
IS 'reset on or more options on a cstore table to the system defaults';
