/* citus--7.1-3--7.1-4 */

CREATE OR REPLACE FUNCTION pg_catalog.read_records_file(filename text, format text default 'csv')
    RETURNS record
    LANGUAGE C STRICT IMMUTABLE
    AS 'MODULE_PATHNAME', $$read_records_file$$;
COMMENT ON FUNCTION pg_catalog.read_records_file(text,text)
    IS 'read a file and return it as a set of records';
