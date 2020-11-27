/* columnar--9.5-1--10.0-1.sql */

CREATE SCHEMA cstore;
SET search_path TO cstore;

CREATE TABLE options (
    regclass regclass NOT NULL PRIMARY KEY,
    block_row_count int NOT NULL,
    stripe_row_count int NOT NULL,
    compression name NOT NULL
);

COMMENT ON TABLE options IS 'columnar table specific options, maintained by alter_columnar_table_set';

CREATE TABLE cstore_data_files (
    relfilenode oid NOT NULL,
    version_major bigint NOT NULL,
    version_minor bigint NOT NULL,
    PRIMARY KEY (relfilenode)
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_data_files IS 'CStore data file wide metadata';

CREATE TABLE cstore_stripes (
    relfilenode oid NOT NULL,
    stripe bigint NOT NULL,
    file_offset bigint NOT NULL,
    data_length bigint NOT NULL,
    column_count int NOT NULL,
    block_count int NOT NULL,
    block_row_count int NOT NULL,
    row_count bigint NOT NULL,
    PRIMARY KEY (relfilenode, stripe),
    FOREIGN KEY (relfilenode) REFERENCES cstore_data_files(relfilenode) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_stripes IS 'CStore per stripe metadata';

CREATE TABLE cstore_skipnodes (
    relfilenode oid NOT NULL,
    stripe bigint NOT NULL,
    attr int NOT NULL,
    block int NOT NULL,
    row_count bigint NOT NULL,
    minimum_value bytea,
    maximum_value bytea,
    value_stream_offset bigint NOT NULL,
    value_stream_length bigint NOT NULL,
    exists_stream_offset bigint NOT NULL,
    exists_stream_length bigint NOT NULL,
    value_compression_type int NOT NULL,
    PRIMARY KEY (relfilenode, stripe, attr, block),
    FOREIGN KEY (relfilenode, stripe) REFERENCES cstore_stripes(relfilenode, stripe) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_skipnodes IS 'CStore per block metadata';

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

#include "udfs/cstore_ensure_objects_exist/10.0-1.sql"

RESET search_path;
