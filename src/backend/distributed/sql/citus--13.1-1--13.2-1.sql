-- citus--13.1-1--13.2-1
-- bump version to 13.2-1
#include "udfs/worker_last_saved_explain_analyze/13.2-1.sql"

#include "cat_upgrades/add_clone_info_to_pg_dist_node.sql"
#include "udfs/citus_add_clone_node/13.2-1.sql"
#include "udfs/citus_remove_clone_node/13.2-1.sql"
#include "udfs/citus_promote_clone_and_rebalance/13.2-1.sql"
#include "udfs/get_snapshot_based_node_split_plan/13.2-1.sql"
