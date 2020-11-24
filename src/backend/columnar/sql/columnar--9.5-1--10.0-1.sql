/* columnar--9.5-1--10.0-1.sql */

CREATE SCHEMA cstore;
SET search_path TO cstore;

CREATE SEQUENCE cstore_storageid_seq NO CYCLE;

CREATE TABLE cstore_data_files (
    storageid bigint NOT NULL,
    block_row_count int NOT NULL,
    stripe_row_count int NOT NULL,
    compression name NOT NULL,
    version_major bigint NOT NULL,
    version_minor bigint NOT NULL,
    PRIMARY KEY (storageid)
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_data_files IS 'CStore data file wide metadata';

CREATE TABLE cstore_stripes (
    storageid bigint NOT NULL,
    stripe bigint NOT NULL,
    file_offset bigint NOT NULL,
    data_length bigint NOT NULL,
    column_count int NOT NULL,
    block_count int NOT NULL,
    block_row_count int NOT NULL,
    row_count bigint NOT NULL,
    PRIMARY KEY (storageid, stripe),
    FOREIGN KEY (storageid) REFERENCES cstore_data_files(storageid) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_stripes IS 'CStore per stripe metadata';

CREATE TABLE cstore_skipnodes (
    storageid bigint NOT NULL,
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
    PRIMARY KEY (storageid, stripe, attr, block),
    FOREIGN KEY (storageid, stripe) REFERENCES cstore_stripes(storageid, stripe) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_skipnodes IS 'CStore per block metadata';

CREATE FUNCTION columnar_relation_storageid(relid oid) RETURNS bigint
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$columnar_relation_storageid$$;

CREATE VIEW columnar_options AS
SELECT c.oid::regclass regclass,
       d.block_row_count,
       d.stripe_row_count,
       d.compression
FROM pg_class c, cstore.cstore_data_files d
WHERE d.storageid=columnar_relation_storageid(c.oid);

COMMENT ON VIEW columnar_options IS 'CStore per table settings';

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
