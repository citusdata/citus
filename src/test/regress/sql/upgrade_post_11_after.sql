-- run this test only when old citus version is earlier than 11.0
\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 11
AS upgrade_test_old_citus_version_lt_11_0;
\gset
\if :upgrade_test_old_citus_version_lt_11_0
\else
\q
\endif

SET search_path = post_11_upgrade;

-- tables, views and their dependencies become objects with Citus 11+
SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM pg_catalog.pg_dist_object WHERE objid IN ('post_11_upgrade'::regnamespace, 'post_11_upgrade.part_table'::regclass, 'post_11_upgrade.sensors'::regclass, 'post_11_upgrade.func_in_transaction_def'::regproc, 'post_11_upgrade.partial_index_test_config'::regconfig, 'post_11_upgrade.my_type'::regtype, 'post_11_upgrade.employees'::regclass, 'post_11_upgrade.view_for_upgrade_test'::regclass, 'post_11_upgrade.my_type_for_view'::regtype, 'post_11_upgrade.view_for_upgrade_test_my_type'::regclass, 'post_11_upgrade.non_dist_table_for_view'::regclass, 'post_11_upgrade.non_dist_upgrade_test_view'::regclass, 'post_11_upgrade.non_dist_upgrade_test_view_local_join'::regclass, 'post_11_upgrade.non_dist_upgrade_multiple_dist_view'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view_2'::regclass, 'post_11_upgrade.reporting_line'::regclass, 'post_11_upgrade.v_test_1'::regclass, 'post_11_upgrade.v_test_2'::regclass, 'post_11_upgrade.owned_by_extension_table'::regclass, 'post_11_upgrade.materialized_view'::regclass, 'post_11_upgrade.owned_by_extension_view'::regclass, 'post_11_upgrade.local_type'::regtype, 'post_11_upgrade.non_dist_dist_table_for_view'::regclass, 'post_11_upgrade.depends_on_nothing_1'::regclass, 'post_11_upgrade.depends_on_nothing_2'::regclass, 'post_11_upgrade.depends_on_pg'::regclass, 'post_11_upgrade.depends_on_citus'::regclass, 'post_11_upgrade.depends_on_seq'::regclass, 'post_11_upgrade.depends_on_seq_and_no_support'::regclass) ORDER BY 1;

-- on all nodes
SELECT run_command_on_workers($$SELECT array_agg(worker_object) FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) worker_object FROM pg_catalog.pg_dist_object WHERE objid IN ('post_11_upgrade'::regnamespace, 'post_11_upgrade.part_table'::regclass, 'post_11_upgrade.sensors'::regclass, 'post_11_upgrade.func_in_transaction_def'::regproc, 'post_11_upgrade.partial_index_test_config'::regconfig, 'post_11_upgrade.my_type'::regtype, 'post_11_upgrade.view_for_upgrade_test'::regclass, 'post_11_upgrade.view_for_upgrade_test_my_type'::regclass, 'post_11_upgrade.non_dist_upgrade_ref_view_2'::regclass, 'post_11_upgrade.reporting_line'::regclass) ORDER BY 1) worker_objects;$$) ORDER BY 1;

-- Create the necessary test utility function
CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';

-- make sure that workers and the coordinator has the same datesyle
SET datestyle = "ISO, YMD";
SELECT 1 FROM run_command_on_workers($$ALTER SYSTEM SET datestyle = "ISO, YMD";$$);
SELECT 1 FROM run_command_on_workers($$SELECT pg_reload_conf()$$);

-- In the version that we use for upgrade tests (v10.2.0), we propagate
-- "valid until" to the workers as "infinity" even if it's not set. And
-- given that "postgres" role is created in the older version, "valid until"
-- is set to "infinity" on the workers while this is not the case for
-- coordinator. See https://github.com/citusdata/citus/issues/7533.
--
-- We're fixing this for new versions of Citus and we'll probably backport
-- this to some older versions too. However, v10.2.0 won't ever have this
-- fix.
--
-- For this reason, here we set "valid until" to "infinity" for all the
-- nodes so that below query doesn't report any difference between the
-- metadata on coordinator and workers.
ALTER ROLE postgres WITH VALID UNTIL 'infinity';

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
) AS foo WHERE command NOT ILIKE '%distributed_object_data%' and
-- sequences differ per node, so exclude
command NOT ILIKE '%sequence%';
