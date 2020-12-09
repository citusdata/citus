/* columnar--9.5-1--10.0-1.sql */

CREATE SCHEMA columnar;
SET search_path TO columnar;

CREATE SEQUENCE storageid_seq MINVALUE 10000000000 NO CYCLE;

CREATE TABLE options (
    regclass regclass NOT NULL PRIMARY KEY,
    chunk_row_count int NOT NULL,
    stripe_row_count int NOT NULL,
    compression_level int NOT NULL,
    compression name NOT NULL
) WITH (user_catalog_table = true);

COMMENT ON TABLE options IS 'columnar table specific options, maintained by alter_columnar_table_set';

CREATE TABLE columnar_stripes (
    storageid bigint NOT NULL,
    stripe bigint NOT NULL,
    file_offset bigint NOT NULL,
    data_length bigint NOT NULL,
    column_count int NOT NULL,
    chunk_count int NOT NULL,
    chunk_row_count int NOT NULL,
    row_count bigint NOT NULL,
    PRIMARY KEY (storageid, stripe)
) WITH (user_catalog_table = true);

COMMENT ON TABLE columnar_stripes IS 'Columnar per stripe metadata';

CREATE TABLE columnar_skipnodes (
    storageid bigint NOT NULL,
    stripe bigint NOT NULL,
    attr int NOT NULL,
    chunk int NOT NULL,
    row_count bigint NOT NULL,
    minimum_value bytea,
    maximum_value bytea,
    value_stream_offset bigint NOT NULL,
    value_stream_length bigint NOT NULL,
    exists_stream_offset bigint NOT NULL,
    exists_stream_length bigint NOT NULL,
    value_compression_type int NOT NULL,
    value_compression_level int NOT NULL,
    value_decompressed_length bigint NOT NULL,
    PRIMARY KEY (storageid, stripe, attr, chunk),
    FOREIGN KEY (storageid, stripe) REFERENCES columnar_stripes(storageid, stripe) ON DELETE CASCADE
) WITH (user_catalog_table = true);

COMMENT ON TABLE columnar_skipnodes IS 'Columnar per chunk metadata';

DO $proc$
BEGIN

-- from version 12 and up we have support for tableam's if installed on pg11 we can't
-- create the objects here. Instead we rely on citus_finish_pg_upgrade to be called by the
-- user instead to add the missing objects
IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
  EXECUTE $$
#include "udfs/columnar_handler/10.0-1.sql"
#include "udfs/alter_columnar_table_set/10.0-1.sql"
#include "udfs/alter_columnar_table_reset/10.0-1.sql"
  $$;
END IF;
END$proc$;

#include "udfs/columnar_ensure_objects_exist/10.0-1.sql"

RESET search_path;
