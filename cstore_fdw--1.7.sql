/* cstore_fdw/cstore_fdw--1.7.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cstore_fdw" to load this file. \quit

CREATE FUNCTION cstore_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION cstore_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER cstore_fdw
HANDLER cstore_fdw_handler
VALIDATOR cstore_fdw_validator;

CREATE FUNCTION cstore_ddl_event_end_trigger()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE EVENT TRIGGER cstore_ddl_event_end
ON ddl_command_end
EXECUTE PROCEDURE cstore_ddl_event_end_trigger();

CREATE FUNCTION public.cstore_table_size(relation regclass)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE TABLE cstore_data_files (
    relfilenode oid NOT NULL,
    block_row_count int NOT NULL,
    stripe_row_count int NOT NULL,
    compression name NOT NULL,
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

CREATE VIEW cstore_options AS
SELECT c.oid::regclass regclass,
       d.block_row_count,
       d.stripe_row_count,
       d.compression
FROM pg_class c
JOIN cstore.cstore_data_files d USING(relfilenode);

COMMENT ON VIEW cstore_options IS 'CStore per table settings';
