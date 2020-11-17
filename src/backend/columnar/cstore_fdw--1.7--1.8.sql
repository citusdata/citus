/* cstore_fdw/cstore_fdw--1.7--1.8.sql */

DO $proc$
BEGIN

IF version() ~ '12' or version() ~ '13' THEN
  EXECUTE $$
    CREATE FUNCTION cstore_tableam_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
    AS 'MODULE_PATHNAME', 'cstore_tableam_handler';

    CREATE ACCESS METHOD cstore_tableam
    TYPE TABLE HANDLER cstore_tableam_handler;

    CREATE FUNCTION pg_catalog.alter_cstore_table_set(
        table_name regclass,
        block_row_count int DEFAULT NULL,
        stripe_row_count int DEFAULT NULL,
        compression name DEFAULT null)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', 'alter_cstore_table_set';

    CREATE FUNCTION pg_catalog.alter_cstore_table_reset(
        table_name regclass,
        block_row_count bool DEFAULT false,
        stripe_row_count bool DEFAULT false,
        compression bool DEFAULT false)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', 'alter_cstore_table_reset';
  $$;
END IF;
END$proc$;
