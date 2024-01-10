-- citus--12.1-1--12.2-1
-- bump version to 12.2-1

#include "udfs/citus_internal_database_command/12.2-1.sql"
#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

#include "udfs/start_management_transaction/12.2-1.sql"
#include "udfs/execute_command_on_remote_nodes_as_user/12.2-1.sql"
#include "udfs/mark_object_distributed/12.2-1.sql"
#include "udfs/commit_management_command_2pc/12.2-1.sql"

ALTER TABLE pg_catalog.pg_dist_transaction ADD COLUMN outer_xid xid8;

#include "udfs/citus_internal_acquire_citus_advisory_object_class_lock/12.2-1.sql"
