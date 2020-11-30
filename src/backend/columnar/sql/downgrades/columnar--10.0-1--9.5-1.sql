/* columnar--10.0-1--9.5-1.sql */

SET search_path TO cstore;

DO $proc$
BEGIN

IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
  EXECUTE $$
    DROP FUNCTION pg_catalog.alter_columnar_table_reset(
        table_name regclass,
        block_row_count bool,
        stripe_row_count bool,
        compression bool);

    DROP FUNCTION pg_catalog.alter_columnar_table_set(
        table_name regclass,
        block_row_count int,
        stripe_row_count int,
        compression name);

    DROP ACCESS METHOD columnar;

    DROP FUNCTION columnar_handler(internal);

  $$;
END IF;
END$proc$;

DROP TABLE cstore_skipnodes;
DROP TABLE cstore_stripes;
DROP TABLE cstore_data_files;
DROP TABLE options;

DROP FUNCTION citus_internal.cstore_ensure_objects_exist();

RESET search_path;
DROP SCHEMA cstore;
