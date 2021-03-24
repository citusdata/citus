-- citus--10.0-2--10.0-3

#include "udfs/citus_update_table_statistics/10.0-3.sql"

CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_update_table_statistics$$;
COMMENT ON FUNCTION pg_catalog.master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table';

CREATE OR REPLACE FUNCTION pg_catalog.citus_get_active_worker_nodes(OUT node_name text, OUT node_port bigint)
    RETURNS SETOF record
    LANGUAGE C STRICT ROWS 100
    AS 'MODULE_PATHNAME', $$citus_get_active_worker_nodes$$;
COMMENT ON FUNCTION pg_catalog.citus_get_active_worker_nodes()
    IS 'fetch set of active worker nodes';

