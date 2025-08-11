-- citus--13.2-1--13.1-1
-- downgrade version to 13.1-1

DROP FUNCTION IF EXISTS pg_catalog.worker_last_saved_explain_analyze();
#include "../udfs/worker_last_saved_explain_analyze/9.4-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.citus_add_clone_node(text, integer);
DROP FUNCTION IF EXISTS pg_catalog.citus_add_clone_node_with_nodeid(integer);
DROP FUNCTION IF EXISTS pg_catalog.citus_remove_clone_node(text, integer);
DROP FUNCTION IF EXISTS pg_catalog.citus_remove_clone_node_with_nodeid(integer);

DROP FUNCTION IF EXISTS pg_catalog.citus_promote_clone_and_rebalance(integer, name);
DROP FUNCTION IF EXISTS pg_catalog.get_snapshot_based_node_split_plan(text, int, text, int, name);

#include "../cat_upgrades/remove_clone_info_to_pg_dist_node.sql"
