-- citus--11.2-1--11.3-1
#include "udfs/repl_origin_helper/11.3-1.sql"
ALTER TABLE pg_catalog.pg_dist_authinfo REPLICA IDENTITY USING INDEX pg_dist_authinfo_identification_index;
ALTER TABLE pg_catalog.pg_dist_partition REPLICA IDENTITY USING INDEX pg_dist_partition_logical_relid_index;
ALTER TABLE pg_catalog.pg_dist_placement REPLICA IDENTITY USING INDEX pg_dist_placement_placementid_index;
ALTER TABLE pg_catalog.pg_dist_rebalance_strategy REPLICA IDENTITY USING INDEX pg_dist_rebalance_strategy_name_key;
ALTER TABLE pg_catalog.pg_dist_shard REPLICA IDENTITY USING INDEX pg_dist_shard_shardid_index;
ALTER TABLE pg_catalog.pg_dist_transaction REPLICA IDENTITY USING INDEX pg_dist_transaction_unique_constraint;

#include "udfs/worker_drop_all_shell_tables/11.3-1.sql"
#include "udfs/citus_internal_mark_node_not_synced/11.3-1.sql"
#include "udfs/citus_stat_tenants_local/11.3-1.sql"
#include "udfs/citus_stat_tenants/11.3-1.sql"

#include "udfs/citus_stat_tenants_local_reset/11.3-1.sql"
#include "udfs/citus_stat_tenants_reset/11.3-1.sql"

-- we introduce nodes_involved, which will be used internally to
-- limit the number of parallel tasks running per node
ALTER TABLE pg_catalog.pg_dist_background_task ADD COLUMN nodes_involved int[] DEFAULT NULL;
