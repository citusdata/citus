-- citus--9.5-1--10.0-1

DROP FUNCTION pg_catalog.upgrade_to_reference_table(regclass);
DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);

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
#include "udfs/citus_disable_node/10.0-1.sql"
#include "udfs/citus_drain_node/10.0-1.sql"
#include "udfs/citus_remove_node/10.0-1.sql"
#include "udfs/citus_set_node_property/10.0-1.sql"
#include "udfs/citus_unmark_object_distributed/10.0-1.sql"
#include "udfs/citus_update_node/10.0-1.sql"
#include "udfs/citus_update_shard_statistics/10.0-1.sql"
#include "udfs/citus_update_table_statistics/10.0-1.sql"
#include "udfs/citus_copy_shard_placement/10.0-1.sql"
#include "udfs/citus_move_shard_placement/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

#include "udfs/time_partition_range/10.0-1.sql"
#include "udfs/time_partitions/10.0-1.sql"

ALTER FUNCTION master_conninfo_cache_invalidate()
RENAME TO citus_conninfo_cache_invalidate;
ALTER FUNCTION master_dist_local_group_cache_invalidate()
RENAME TO citus_dist_local_group_cache_invalidate;
ALTER FUNCTION master_dist_node_cache_invalidate()
RENAME TO citus_dist_node_cache_invalidate;
ALTER FUNCTION master_dist_object_cache_invalidate()
RENAME TO citus_dist_object_cache_invalidate;
ALTER FUNCTION master_dist_partition_cache_invalidate()
RENAME TO citus_dist_partition_cache_invalidate;
ALTER FUNCTION master_dist_placement_cache_invalidate()
RENAME TO citus_dist_placement_cache_invalidate;
ALTER FUNCTION master_dist_shard_cache_invalidate()
RENAME TO citus_dist_shard_cache_invalidate;

DROP FUNCTION pg_catalog.master_modify_multiple_shards(text);
