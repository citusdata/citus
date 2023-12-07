-- citus--12.2-1--12.1-1

DROP FUNCTION pg_catalog.citus_internal_database_command(text);
DROP FUNCTION pg_catalog.pg_database_size_local(name);
DROP FUNCTION pg_catalog.pg_database_size_local(oid);

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"
