-- citus--13.2-1--13.1-1
-- downgrade version to 13.1-1
DROP FUNCTION IF EXISTS citus_internal.citus_internal_copy_single_shard_placement(bigint, integer, integer, integer, citus.shard_transfer_mode);

DROP FUNCTION IF EXISTS pg_catalog.citus_rebalance_start(name, boolean, citus.shard_transfer_mode, boolean, boolean);
#include "../udfs/citus_rebalance_start/11.1-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.worker_last_saved_explain_analyze();
#include "../udfs/worker_last_saved_explain_analyze/9.4-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.citus_add_clone_node(text, integer, text, integer);
DROP FUNCTION IF EXISTS pg_catalog.citus_add_clone_node_with_nodeid(text, integer, integer);

DROP FUNCTION IF EXISTS pg_catalog.citus_remove_clone_node(text, integer);
DROP FUNCTION IF EXISTS pg_catalog.citus_remove_clone_node_with_nodeid(integer);

DROP FUNCTION IF EXISTS pg_catalog.citus_promote_clone_and_rebalance(integer, name, integer);
DROP FUNCTION IF EXISTS pg_catalog.get_snapshot_based_node_split_plan(text, integer, text, integer, name);

#include "../cat_upgrades/remove_clone_info_to_pg_dist_node.sql"
#include "../udfs/citus_finish_pg_upgrade/13.1-1.sql"

-- Note that we intentionally don't add the old columnar objects back to the "citus"
-- extension in this downgrade script, even if they were present in the older version.
--
-- If the user wants to create "citus_columnar" extension later, "citus_columnar"
-- will anyway properly create them at the scope of that extension.

DROP VIEW IF EXISTS pg_catalog.citus_stats;
