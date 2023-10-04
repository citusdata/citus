-- citus--12.1-1--12.2-1

--
-- citus_internal_database_command creates a database according to the given command.

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_database_command(command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$citus_internal_database_command$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_database_command(text) IS
 'run a database command without transaction block restrictions';

-- bump version to 12.2-1

#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"
