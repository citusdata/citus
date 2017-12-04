/* citus--7.2-1--7.2-2 */

CREATE TYPE citus.copy_format AS ENUM ('csv', 'binary', 'text');

CREATE OR REPLACE FUNCTION pg_catalog.read_intermediate_result(result_id text, format citus.copy_format default 'csv')
    RETURNS record
    LANGUAGE C STRICT VOLATILE PARALLEL SAFE
    AS 'MODULE_PATHNAME', $$read_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.read_intermediate_result(text,citus.copy_format)
    IS 'read a file and return it as a set of records';

CREATE OR REPLACE FUNCTION pg_catalog.create_intermediate_result(result_id text, query text)
    RETURNS bigint
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$create_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.create_intermediate_result(text,text)
    IS 'execute a query and write its results to local result file';

CREATE OR REPLACE FUNCTION pg_catalog.broadcast_intermediate_result(result_id text, query text)
    RETURNS bigint
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$broadcast_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.broadcast_intermediate_result(text,text)
    IS 'execute a query and write its results to an result file on all workers';
