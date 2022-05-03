SET search_path = post_11_upgrade;

-- make sure that we always (re)sync the metadata
UPDATE pg_dist_node_metadata SET metadata=jsonb_set(metadata, '{partitioned_citus_table_exists_pre_11}', to_jsonb('true'::bool), true);
SELECT citus_finalize_upgrade_to_citus11(enforce_version_check:=false);

-- tables, views and their dependencies become objects with Citus 11+
SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM pg_catalog.pg_dist_object WHERE objid IN ('post_11_upgrade'::regnamespace, 'post_11_upgrade.part_table'::regclass, 'post_11_upgrade.sensors'::regclass, 'post_11_upgrade.func_in_transaction_def'::regproc, 'post_11_upgrade.partial_index_test_config'::regconfig, 'post_11_upgrade.my_type'::regtype, 'post_11_upgrade.employees'::regclass, 'post_11_upgrade.view_for_upgrade_test'::regclass, 'post_11_upgrade.my_type_for_view'::regtype, 'post_11_upgrade.view_for_upgrade_test_my_type'::regclass, 'post_11_upgrade.non_dist_table_for_view'::regclass, 'post_11_upgrade.non_dist_upgrade_test_view'::regclass, 'post_11_upgrade.non_dist_upgrade_test_view_local_join'::regclass, 'post_11_upgrade.non_dist_upgrade_multiple_dist_view'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view_2'::regclass, 'post_11_upgrade.reporting_line'::regclass, 'post_11_upgrade.v_test_1'::regclass, 'post_11_upgrade.v_test_2'::regclass) ORDER BY 1;

-- on all nodes
SELECT run_command_on_workers($$SELECT array_agg(pg_identify_object_as_address(classid, objid, objsubid)) FROM pg_catalog.pg_dist_object WHERE objid IN ('post_11_upgrade'::regnamespace, 'post_11_upgrade.part_table'::regclass, 'post_11_upgrade.sensors'::regclass, 'post_11_upgrade.func_in_transaction_def'::regproc, 'post_11_upgrade.partial_index_test_config'::regconfig, 'post_11_upgrade.my_type'::regtype, 'post_11_upgrade.view_for_upgrade_test'::regclass, 'post_11_upgrade.view_for_upgrade_test_my_type'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view_2'::regclass, 'post_11_upgrade.reporting_line'::regclass) ORDER BY 1;$$) ORDER BY 1;

-- Create the necessary test utility function
CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';

-- make sure that workers and the coordinator has the same datesyle
SET datestyle = "ISO, YMD";
SELECT 1 FROM run_command_on_workers($$ALTER SYSTEM SET datestyle = "ISO, YMD";$$);
SELECT 1 FROM run_command_on_workers($$SELECT pg_reload_conf()$$);

-- make sure that the metadata is consistent across all nodes
-- we exclude the distributed_object_data as they are
-- not sorted in the same order (as OIDs differ on the nodes)
SELECT count(*) = 0 AS same_metadata_in_workers FROM
(
	(
		SELECT unnest(activate_node_snapshot()) as command
			EXCEPT
		SELECT unnest(result::text[]) AS command
		FROM run_command_on_workers($$SELECT post_11_upgrade.activate_node_snapshot()$$)
	)
UNION
	(
		SELECT unnest(result::text[]) AS command
		FROM run_command_on_workers($$SELECT post_11_upgrade.activate_node_snapshot()$$)
			EXCEPT
		SELECT unnest(activate_node_snapshot()) as command
	)
) AS foo WHERE command NOT ILIKE '%distributed_object_data%';
