CREATE OR REPLACE FUNCTION cstore.columnar_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', 'columnar_handler';

COMMENT ON FUNCTION cstore.columnar_handler(internal)
    IS 'internal function returning the handler for cstore tables';

CREATE ACCESS METHOD columnar TYPE TABLE HANDLER cstore.columnar_handler;
