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

CREATE FUNCTION cstore_table_size(relation regclass)
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

		PERFORM public.cstore_clean_table_resources(v_obj.objid);

	END LOOP;
END;
$csdt$;

CREATE EVENT TRIGGER cstore_drop_event
    ON SQL_DROP
    EXECUTE PROCEDURE cstore_drop_trigger();

CREATE TABLE cstore_tables (
    relid oid,
    block_row_count int,
    version_major bigint,
    version_minor bigint,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

ALTER TABLE cstore_tables SET SCHEMA pg_catalog;

CREATE TABLE cstore_stripes (
    relid oid,
    stripe bigint,
    file_offset bigint,
    skiplist_length bigint,
    data_length bigint,
    PRIMARY KEY (relid, stripe),
    FOREIGN KEY (relid) REFERENCES cstore_tables(relid) ON DELETE CASCADE
) WITH (user_catalog_table = true);

ALTER TABLE cstore_stripes SET SCHEMA pg_catalog;

CREATE TABLE cstore_stripe_attr (
    relid oid,
    stripe bigint,
    attr int,
    exists_size bigint,
    value_size bigint,
    skiplist_size bigint,
    PRIMARY KEY (relid, stripe, attr),
    FOREIGN KEY (relid, stripe) REFERENCES cstore_stripes(relid, stripe) ON DELETE CASCADE
) WITH (user_catalog_table = true);

ALTER TABLE cstore_stripe_attr SET SCHEMA pg_catalog;
