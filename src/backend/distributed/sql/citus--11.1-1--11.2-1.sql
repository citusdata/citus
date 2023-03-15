-- citus--11.1-1--11.2-1

DROP FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer);

#include "udfs/get_rebalance_progress/11.2-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.2-1.sql"
#include "datatypes/citus_cluster_clock/11.2-1.sql"
#include "udfs/citus_get_node_clock/11.2-1.sql"
#include "udfs/citus_get_transaction_clock/11.2-1.sql"
#include "udfs/citus_is_clock_after/11.2-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/11.2-1.sql"
#include "udfs/citus_job_list/11.2-1.sql"
#include "udfs/citus_job_status/11.2-1.sql"
#include "udfs/citus_rebalance_status/11.2-1.sql"
#include "udfs/worker_split_shard_replication_setup/11.2-1.sql"
#include "udfs/citus_task_wait/11.2-1.sql"
#include "udfs/citus_prepare_pg_upgrade/11.2-1.sql"
#include "udfs/citus_finish_pg_upgrade/11.2-1.sql"
#include "udfs/citus_copy_shard_placement/11.2-1.sql"
#include "udfs/citus_move_shard_placement/11.2-1.sql"
#include "udfs/citus_internal_add_placement_metadata/11.2-1.sql";

-- drop orphaned shards after inserting records for them into pg_dist_cleanup
INSERT INTO pg_dist_cleanup
    SELECT nextval('pg_dist_cleanup_recordid_seq'), 0, 1, shard_name(sh.logicalrelid, sh.shardid) AS object_name, plc.groupid AS node_group_id, 0
        FROM pg_dist_placement plc
        JOIN pg_dist_shard sh ON sh.shardid = plc.shardid
        WHERE plc.shardstate = 4;

DELETE FROM pg_dist_placement WHERE shardstate = 4;
