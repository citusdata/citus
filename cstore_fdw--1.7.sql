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

CREATE OR REPLACE FUNCTION cstore_clean_table_resources(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION cstore_drop_trigger()
	RETURNS event_trigger
	LANGUAGE plpgsql
	AS $csdt$
DECLARE v_obj record;
BEGIN
	FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP

		IF v_obj.object_type NOT IN ('table', 'foreign table') THEN
			CONTINUE;
		END IF;

		PERFORM cstore.cstore_clean_table_resources(v_obj.objid);

	END LOOP;
END;
$csdt$;

CREATE EVENT TRIGGER cstore_drop_event
    ON SQL_DROP
    EXECUTE PROCEDURE cstore_drop_trigger();

CREATE TABLE cstore_tables (
    relid oid NOT NULL,
    block_row_count int NOT NULL,
    version_major bigint NOT NULL,
    version_minor bigint NOT NULL,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_tables IS 'CStore table wide metadata';

CREATE TABLE cstore_stripes (
    relid oid NOT NULL,
    stripe bigint NOT NULL,
    file_offset bigint NOT NULL,
    data_length bigint NOT NULL,
    column_count int NOT NULL,
    block_count int NOT NULL,
    block_row_count int NOT NULL,
    row_count bigint NOT NULL,
    PRIMARY KEY (relid, stripe),
    FOREIGN KEY (relid) REFERENCES cstore_tables(relid) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_tables IS 'CStore per stripe metadata';

CREATE TABLE cstore_skipnodes (
    relid oid NOT NULL,
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
    PRIMARY KEY (relid, stripe, attr, block),
    FOREIGN KEY (relid, stripe) REFERENCES cstore_stripes(relid, stripe) ON DELETE CASCADE INITIALLY DEFERRED
) WITH (user_catalog_table = true);

COMMENT ON TABLE cstore_tables IS 'CStore per block metadata';
