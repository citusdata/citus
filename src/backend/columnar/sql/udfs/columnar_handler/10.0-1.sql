CREATE OR REPLACE FUNCTION columnar.columnar_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', 'columnar_handler';

COMMENT ON FUNCTION columnar.columnar_handler(internal)
    IS 'internal function returning the handler for columnar tables';

-- postgres 11.8 does not support the syntax for table am, also it is seemingly trying
-- to parse the upgrade file and erroring on unknown syntax.
-- normally this section would not execute on postgres 11 anyway. To trick it to pass on
-- 11.8 we wrap the statement in a plpgsql block together with an EXECUTE. This is valid
-- syntax on 11.8 and will execute correctly in 12
DO $create_table_am$
BEGIN
EXECUTE 'CREATE ACCESS METHOD columnar TYPE TABLE HANDLER columnar.columnar_handler';
END $create_table_am$;
