-- citus--9.5-1--10.0-1

-- bump version to 10.0-1

#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

CREATE OR REPLACE FUNCTION pg_catalog.read_distributed_intermediate_result(result_id text)
    RETURNS record
    LANGUAGE C STRICT VOLATILE PARALLEL SAFE
    AS 'MODULE_PATHNAME', $$read_distributed_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.read_distributed_intermediate_result(text)
    IS 'read a previously stored distributed intermediate result';

CREATE OR REPLACE FUNCTION pg_catalog.create_distributed_intermediate_result(result_id text, query text, distribution_column int, colocate_with text)
    RETURNS void
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$create_distributed_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.create_distributed_intermediate_result(text,text,int,text)
    IS 'execute a query and write its results to local result file';
