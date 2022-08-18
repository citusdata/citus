-- citus--10.2-5--11.0-1

-- bump version to 11.0-1
#include "udfs/citus_disable_node/11.0-1.sql"
#include "udfs/create_distributed_function/11.0-1.sql"

#include "udfs/citus_check_connection_to_node/11.0-1.sql"
#include "udfs/citus_check_cluster_node_health/11.0-1.sql"
#include "udfs/citus_shards_on_worker/11.0-1.sql"
#include "udfs/citus_shard_indexes_on_worker/11.0-1.sql"

#include "udfs/citus_internal_add_object_metadata/11.0-1.sql"
#include "udfs/citus_internal_add_colocation_metadata/11.0-1.sql"
#include "udfs/citus_internal_delete_colocation_metadata/11.0-1.sql"
#include "udfs/citus_run_local_command/11.0-1.sql"
#include "udfs/worker_drop_sequence_dependency/11.0-1.sql"
#include "udfs/worker_drop_shell_table/11.0-1.sql"

#include "udfs/get_all_active_transactions/11.0-1.sql"
#include "udfs/get_global_active_transactions/11.0-1.sql"

#include "udfs/citus_internal_local_blocked_processes/11.0-1.sql"
#include "udfs/citus_internal_global_blocked_processes/11.0-1.sql"

#include "udfs/run_command_on_all_nodes/11.0-1.sql"
#include "udfs/citus_stat_activity/11.0-1.sql"

#include "udfs/worker_create_or_replace_object/11.0-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.0-1.sql"
#include "udfs/citus_blocking_pids/11.0-1.sql"
#include "udfs/citus_calculate_gpid/11.0-1.sql"
#include "udfs/citus_backend_gpid/11.0-1.sql"

DROP VIEW IF EXISTS pg_catalog.citus_lock_waits;
DROP VIEW IF EXISTS pg_catalog.citus_dist_stat_activity;
DROP VIEW IF EXISTS pg_catalog.citus_worker_stat_activity;
DROP FUNCTION IF EXISTS pg_catalog.citus_dist_stat_activity();
DROP FUNCTION IF EXISTS pg_catalog.citus_worker_stat_activity();
#include "udfs/citus_dist_stat_activity/11.0-1.sql"

-- a very simple helper function defined for citus_lock_waits
CREATE OR REPLACE FUNCTION get_nodeid_for_groupid(groupIdInput int) RETURNS int AS $$
DECLARE
	returnNodeNodeId int := 0;
begin
	SELECT nodeId into returnNodeNodeId FROM pg_dist_node WHERE groupid = groupIdInput and nodecluster = current_setting('citus.cluster_name');
	RETURN returnNodeNodeId;
end
$$ LANGUAGE plpgsql;

#include "udfs/citus_lock_waits/11.0-1.sql"

#include "udfs/pg_cancel_backend/11.0-1.sql"
#include "udfs/pg_terminate_backend/11.0-1.sql"
#include "udfs/worker_partition_query_result/11.0-1.sql"

DROP FUNCTION pg_catalog.master_apply_delete_command(text);
DROP FUNCTION pg_catalog.master_get_table_metadata(text);
DROP FUNCTION pg_catalog.master_append_table_to_shard(bigint, text, text, integer);

-- all existing citus local tables are auto converted
-- none of the other tables can have auto-converted as true
ALTER TABLE pg_catalog.pg_dist_partition ADD COLUMN autoconverted boolean DEFAULT false;
ALTER TABLE citus.pg_dist_object ADD COLUMN force_delegation bool DEFAULT NULL;
UPDATE pg_catalog.pg_dist_partition SET autoconverted = TRUE WHERE partmethod = 'n' AND repmodel = 's';

REVOKE ALL ON FUNCTION start_metadata_sync_to_node(text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION stop_metadata_sync_to_node(text, integer,bool) FROM PUBLIC;

DO LANGUAGE plpgsql
$$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_dist_shard where shardstorage = 'c') THEN
	    RAISE EXCEPTION 'cstore_fdw tables are deprecated as of Citus 11.0'
        USING HINT = 'Install Citus 10.2 and convert your cstore_fdw tables to the columnar access method before upgrading further';
	END IF;
END;
$$;

-- Here we keep track of partitioned tables that exists before Citus 11
-- where we need to call fix_all_partition_shard_index_names() before
-- metadata is synced. Note that after citus-11, we automatically
-- adjust the indexes so we only need to fix existing indexes
DO LANGUAGE plpgsql
$$
DECLARE
  partitioned_table_exists bool :=false;
BEGIN
      SELECT count(*) > 0 INTO partitioned_table_exists FROM pg_dist_partition p JOIN pg_class c ON p.logicalrelid = c.oid WHERE c.relkind = 'p';
      UPDATE pg_dist_node_metadata SET metadata=jsonb_set(metadata, '{partitioned_citus_table_exists_pre_11}', to_jsonb(partitioned_table_exists), true);
END;
$$;

#include "udfs/citus_finalize_upgrade_to_citus11/11.0-1.sql"

ALTER TABLE citus.pg_dist_object SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_object TO public;
#include "udfs/citus_prepare_pg_upgrade/11.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/11.0-1.sql"

#include "udfs/citus_nodename_for_nodeid/11.0-1.sql"
#include "udfs/citus_nodeport_for_nodeid/11.0-1.sql"

#include "udfs/citus_nodeid_for_gpid/11.0-1.sql"
#include "udfs/citus_pid_for_gpid/11.0-1.sql"

#include "udfs/citus_coordinator_nodeid/11.0-1.sql"
