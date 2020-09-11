/* cstore_fdw/cstore_fdw--1.7--1.8.sql */

CREATE FUNCTION cstore_tableam_handler(internal)
RETURNS table_am_handler
LANGUAGE C
AS 'MODULE_PATHNAME', 'cstore_tableam_handler';

CREATE ACCESS METHOD cstore_tableam
TYPE TABLE HANDLER cstore_tableam_handler;
