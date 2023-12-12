-- citus--12.2-1--12.1-1

DROP FUNCTION pg_catalog.citus_internal_database_command(text);


#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"


DROP FUNCTION citus_internal.pg_database_size_local(name);
DROP FUNCTION citus_internal.pg_database_size_local(oid);

DROP FUNCTION pg_catalog.pg_dist_database_size(name);

drop table pg_catalog.pg_dist_database;
