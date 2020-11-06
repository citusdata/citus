/* columnar--10.0-1--9.5-1.sql */

SET search_path TO cstore;

DO $proc$
BEGIN

IF version() ~ '12' or version() ~ '13' THEN
  EXECUTE $$
    DROP FUNCTION pg_catalog.alter_cstore_table_reset(
        table_name regclass,
        block_row_count bool,
        stripe_row_count bool,
        compression bool);

    DROP FUNCTION pg_catalog.alter_cstore_table_set(
        table_name regclass,
        block_row_count int,
        stripe_row_count int,
        compression name);

    DROP ACCESS METHOD cstore_tableam;

    DROP FUNCTION cstore_tableam_handler(internal);

  $$;
END IF;
END$proc$;

DROP VIEW cstore_options;
DROP TABLE cstore_skipnodes;
DROP TABLE cstore_stripes;
DROP TABLE cstore_data_files;

DROP FUNCTION pg_catalog.cstore_table_size(relation regclass);

DROP EVENT TRIGGER cstore_ddl_event_end;
DROP FUNCTION cstore_ddl_event_end_trigger();

DROP FOREIGN DATA WRAPPER cstore_fdw;
DROP FUNCTION cstore_fdw_validator(text[], oid);
DROP FUNCTION cstore_fdw_handler();

RESET search_path;
DROP SCHEMA cstore;
