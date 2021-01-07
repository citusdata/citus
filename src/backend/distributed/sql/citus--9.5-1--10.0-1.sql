-- citus--9.5-1--10.0-1

DROP FUNCTION pg_catalog.upgrade_to_reference_table(regclass);
DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);
DROP FUNCTION IF EXISTS pg_catalog.citus_add_node(text, integer, integer, noderole, name);

#include "udfs/citus_total_relation_size/10.0-1.sql"
#include "udfs/citus_tables/10.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"
#include "udfs/undistribute_table/10.0-1.sql"
#include "udfs/create_citus_local_table/10.0-1.sql"
#include "udfs/citus_set_coordinator_host/10.0-1.sql"
#include "udfs/citus_add_node/10.0-1.sql"
#include "udfs/citus_activate_node/10.0-1.sql"
#include "udfs/citus_add_inactive_node/10.0-1.sql"
#include "udfs/citus_add_secondary_node/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

#include "udfs/time_partition_range/10.0-1.sql"
#include "udfs/time_partitions/10.0-1.sql"
