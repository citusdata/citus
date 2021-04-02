-- citus--10.1-1--10.0-2

#include "../../../columnar/sql/downgrades/columnar--10.1-1--10.0-3.sql"

DROP FUNCTION pg_catalog.create_distributed_table(regclass, text, citus.distribution_type, text, int);
CREATE FUNCTION create_distributed_table(table_name regclass,
										 distribution_column text,
										 distribution_type citus.distribution_type DEFAULT 'hash',
										 colocate_with text DEFAULT 'default')
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$create_distributed_table$$;
COMMENT ON FUNCTION create_distributed_table(table_name regclass,
											 distribution_column text,
											 distribution_type citus.distribution_type,
											 colocate_with text)
    IS 'creates a distributed table';
