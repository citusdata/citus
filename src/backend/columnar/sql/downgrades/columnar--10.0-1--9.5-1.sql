/* columnar--10.0-1--9.5-1.sql */

SET search_path TO columnar;

DO $proc$
BEGIN

IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
  EXECUTE $$
    DROP FUNCTION pg_catalog.alter_columnar_table_reset(
        table_name regclass,
        chunk_row_count bool,
        stripe_row_count bool,
        compression bool,
        compression_level bool);

    DROP FUNCTION pg_catalog.alter_columnar_table_set(
        table_name regclass,
        chunk_row_count int,
        stripe_row_count int,
        compression name,
        compression_level int);

    DROP ACCESS METHOD columnar;

    DROP FUNCTION columnar_handler(internal);

  $$;
END IF;
END$proc$;

DROP TABLE columnar_skipnodes;
DROP TABLE columnar_stripes;
DROP TABLE options;
DROP SEQUENCE storageid_seq;

DROP FUNCTION citus_internal.columnar_ensure_objects_exist();

RESET search_path;
DROP SCHEMA columnar;
