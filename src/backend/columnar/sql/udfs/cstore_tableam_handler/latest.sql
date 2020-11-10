CREATE OR REPLACE FUNCTION cstore.cstore_tableam_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', 'cstore_tableam_handler';

COMMENT ON FUNCTION cstore.cstore_tableam_handler(internal)
    IS 'internal function returning the handler for cstore tables';

CREATE ACCESS METHOD cstore_tableam TYPE TABLE HANDLER cstore.cstore_tableam_handler;
