/* citus--7.2-3--7.2-4 */

CREATE OR REPLACE FUNCTION pg_catalog.read_intermediate_result(result_id text, format pg_catalog.citus_copy_format default 'csv')
    RETURNS SETOF record
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$read_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.read_intermediate_result(text,pg_catalog.citus_copy_format)
    IS 'read a file and return it as a set of records';
