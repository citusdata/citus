-- Due to a race condition that happens in TransferShards() when the same shard id
-- is used to create the same shard on a different worker node, need to call
-- citus_cleanup_orphaned_resources() to clean up any orphaned resources before
-- running the tests.
--
-- See https://github.com/citusdata/citus/pull/7180#issuecomment-1706786615.

SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
RESET client_min_messages;

CREATE SCHEMA isolate_placement;
SET search_path TO isolate_placement;

-- test null input
SELECT citus_internal.shard_property_set(NULL, false);

SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 2000000;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

-- test with user that doesn't have permission to execute the function
SELECT citus_internal.shard_property_set(shardid, true) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

DROP TABLE single_shard_1;

CREATE ROLE test_user_isolate_placement WITH LOGIN;
GRANT ALL ON SCHEMA isolate_placement TO test_user_isolate_placement;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'test_user_isolate_placement';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET ROLE test_user_isolate_placement;

-- test invalid shard id
SELECT citus_internal.shard_property_set(0, true);

-- test null needs_separate_node
SELECT citus_internal_add_shard_metadata(
    relation_id=>0,
    shard_id=>0,
    storage_type=>'0',
    shard_min_value=>'0',
    shard_max_value=>'0',
    needs_separate_node=>null);

RESET ROLE;
REVOKE ALL ON SCHEMA isolate_placement FROM test_user_isolate_placement;
DROP USER test_user_isolate_placement;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SET search_path TO isolate_placement;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 2001000;

CREATE USER mysuperuser superuser;
SET ROLE mysuperuser;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE USER regularuser;
GRANT USAGE ON SCHEMA isolate_placement TO regularuser;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'regularuser';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SET ROLE regularuser;

-- throws an error as the user is not the owner of the table
SELECT citus_shard_property_set(shardid) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;
SELECT citus_internal.shard_property_set(shardid, true) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

-- assign all tables to regularuser
RESET ROLE;
REASSIGN OWNED BY mysuperuser TO regularuser;

SET ROLE regularuser;

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.single_shard_1')
$$)
ORDER BY result;

SELECT citus_internal.shard_property_set(shardid, false) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.single_shard_1')
$$)
ORDER BY result;

SELECT citus_internal.shard_property_set(shardid, true) FROM pg_dist_shard WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

DROP TABLE single_shard_1;
RESET ROLE;
REVOKE USAGE ON SCHEMA isolate_placement FROM regularuser;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
DROP ROLE regularuser, mysuperuser;

SET search_path TO isolate_placement;

SET citus.next_shard_id TO 2002000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SET client_min_messages TO NOTICE;

SET citus.shard_replication_factor TO 2;

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);
CREATE TABLE dist_3(a int);
SELECT create_distributed_table('dist_1', 'a');
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');
SELECT create_distributed_table('dist_3', 'a', colocate_with=>'dist_1');

SET citus.shard_replication_factor TO 1;

-- none of the placements have been marked as needsseparatenode yet
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[2] AS shardgroup_5_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 5 \gset

-- no-op ..
SELECT citus_shard_property_set(:shardgroup_5_shardid, NULL);

CREATE ROLE test_user_isolate_placement WITH LOGIN;
GRANT ALL ON SCHEMA isolate_placement TO test_user_isolate_placement;
ALTER TABLE dist_1 OWNER TO test_user_isolate_placement;
ALTER TABLE dist_2 OWNER TO test_user_isolate_placement;
ALTER TABLE dist_3 OWNER TO test_user_isolate_placement;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'test_user_isolate_placement';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET ROLE test_user_isolate_placement;

-- no-op ..
SELECT citus_internal.shard_property_set(:shardgroup_5_shardid, NULL);

RESET ROLE;
ALTER TABLE dist_1 OWNER TO current_user;
ALTER TABLE dist_2 OWNER TO current_user;
ALTER TABLE dist_3 OWNER TO current_user;
REVOKE ALL ON SCHEMA isolate_placement FROM test_user_isolate_placement;
DROP USER test_user_isolate_placement;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- .. hence returns empty objects
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT citus_shard_property_set(:shardgroup_5_shardid, anti_affinity=>true);

SELECT shardids[3] AS shardgroup_10_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 10 \gset

SELECT citus_shard_property_set(:shardgroup_10_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_3_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 3 \gset

SELECT citus_shard_property_set(:shardgroup_3_shardid, anti_affinity=>false);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_10_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 10 \gset

SELECT citus_shard_property_set(:shardgroup_10_shardid, anti_affinity=>false);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_5_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 5 \gset

SELECT citus_shard_property_set(:shardgroup_5_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

-- test metadata sync

-- first, need to re-create them with shard_replication_factor = 1 because we will first remove worker_2

DROP TABLE dist_1, dist_2, dist_3;

SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);
CREATE TABLE dist_3(a int);
SELECT create_distributed_table('dist_1', 'a');
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');
SELECT create_distributed_table('dist_3', 'a', colocate_with=>'dist_1');

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT shardids[1] AS shardgroup_5_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 5 \gset

SELECT citus_shard_property_set(:shardgroup_5_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

CREATE TABLE dist_4(a int);
SELECT create_distributed_table('dist_4', 'a', colocate_with=>'dist_1');

CREATE TABLE dist_4_concurrently(a int);
SELECT create_distributed_table_concurrently('dist_4_concurrently', 'a', colocate_with=>'dist_1');

-- Placements of a new distributed table created within the same colocated
-- group inherit needsseparatenode from the colocated placements too.
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

DROP TABLE dist_4, dist_4_concurrently;

-- Returns source and target node ids that can be used to perform a
-- shard transfer for one of the placements of given shard.
CREATE OR REPLACE FUNCTION get_candidate_node_for_shard_transfer(
    p_shardid bigint)
RETURNS TABLE (source_nodeid integer, target_nodeid integer)
SET search_path TO 'pg_catalog, public'
AS $func$
DECLARE
    v_source_nodeids integer[];
    v_target_nodeid integer;
BEGIN
    SELECT array_agg(nodeid) INTO v_source_nodeids
    FROM pg_dist_shard
    JOIN pg_dist_placement USING (shardid)
    JOIN pg_dist_node USING (groupid)
    WHERE noderole = 'primary' AND shardid = p_shardid;

    IF v_source_nodeids IS NULL
    THEN
        RAISE EXCEPTION 'could not determine the source node of shard %', p_shardid;
    END IF;

    SELECT nodeid INTO v_target_nodeid
    FROM pg_dist_node
    WHERE isactive AND shouldhaveshards AND noderole='primary' AND
          nodeid NOT IN (SELECT unnest(v_source_nodeids))
    LIMIT 1;

    IF v_target_nodeid IS NULL
    THEN
        RAISE EXCEPTION 'could not determine a node to transfer the placement to';
    END IF;

    RETURN QUERY SELECT v_source_nodeids[1], v_target_nodeid;
END;
$func$ LANGUAGE plpgsql;

SELECT shardids[1] AS shardgroup_15_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 15 \gset

SELECT citus_move_shard_placement(:shardgroup_5_shardid, source_nodeid, target_nodeid, 'block_writes')
FROM get_candidate_node_for_shard_transfer(:shardgroup_5_shardid);

SELECT citus_move_shard_placement(:shardgroup_15_shardid, source_nodeid, target_nodeid, 'block_writes')
FROM get_candidate_node_for_shard_transfer(:shardgroup_15_shardid);

-- so that citus_copy_shard_placement works
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'isolate_placement.dist_1'::regclass;

SELECT citus_copy_shard_placement(:shardgroup_5_shardid, source_nodeid, target_nodeid, 'block_writes')
FROM get_candidate_node_for_shard_transfer(:shardgroup_5_shardid);

SELECT citus_copy_shard_placement(:shardgroup_15_shardid, source_nodeid, target_nodeid, 'block_writes')
FROM get_candidate_node_for_shard_transfer(:shardgroup_15_shardid);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

DROP TABLE dist_1, dist_2, dist_3;

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>3);
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');

SELECT shardids[1] AS shardgroup_3_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 3 \gset

SELECT citus_shard_property_set(:shardgroup_3_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

-- so that replicate_table_shards works
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'isolate_placement.dist_1'::regclass;

SET client_min_messages TO WARNING;
SELECT replicate_table_shards('isolate_placement.dist_1', shard_replication_factor=>2, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

DROP TABLE dist_1, dist_2;

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);
SELECT create_distributed_table('dist_1', 'a');
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');

SELECT shardids[1] AS shardgroup_9_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 9 \gset

SELECT citus_shard_property_set(:shardgroup_9_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

SELECT pg_catalog.citus_split_shard_by_split_points(
    :shardgroup_9_shardid,
    ARRAY[((shardminvalue::bigint + shardmaxvalue::bigint) / 2)::text],
    ARRAY[:worker_1_node, :worker_2_node],
    'block_writes')
FROM pg_dist_shard
WHERE shardid = :shardgroup_9_shardid;

-- We shouldn't see shard group 9 because shard-split operation doesn't
-- preserve needsseparatenode flag when splitting the shard.
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_12_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 12 \gset

SELECT citus_shard_property_set(:shardgroup_12_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_10_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 10 \gset

SELECT pg_catalog.citus_split_shard_by_split_points(
    :shardgroup_10_shardid,
    ARRAY[((shardminvalue::bigint + shardmaxvalue::bigint) / 2)::text],
    ARRAY[:worker_1_node, :worker_2_node],
    'block_writes')
FROM pg_dist_shard
WHERE shardid = :shardgroup_10_shardid;

-- We should see old shard group 12 (now as 13 due to split
-- of a prior shard) because it's not the one we splitted.
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

CREATE TABLE dist_3(a int);
SELECT create_distributed_table('dist_3', 'a', colocate_with=>'none');

SELECT shardids[1] AS shardgroup_17_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_3')
WHERE shardgroupindex = 17 \gset

SELECT citus_shard_property_set(:shardgroup_17_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_3')
$$)
ORDER BY result;

-- verify that shard key value 100 is stored on shard group 17
select get_shard_id_for_distribution_column('dist_3', 100) = :shardgroup_17_shardid;

SELECT 1 FROM isolate_tenant_to_new_shard('dist_3', 100, shard_transfer_mode => 'block_writes');

-- We shouldn't see shard group 17 because isolate_tenant_to_new_shard doesn't
-- preserve needsseparatenode flag when splitting the shard.
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_3')
$$)
ORDER BY result;

SELECT shardids[1] AS shardgroup_18_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_3')
WHERE shardgroupindex = 18 \gset

SELECT citus_shard_property_set(:shardgroup_18_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_3')
$$)
ORDER BY result;

-- verify that shard key value 1000 is _not_ stored on shard group 18
SELECT get_shard_id_for_distribution_column('dist_3', 1000) != :shardgroup_18_shardid;

SELECT 1 FROM isolate_tenant_to_new_shard('dist_3', 1000, shard_transfer_mode => 'block_writes');

-- We should see shard group 18 (now as 20 due to split of a prior shard)
-- because it's not the one we splitted.
SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_3')
$$)
ORDER BY result;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

SELECT shardids[1] AS shardgroup_1_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.single_shard_1')
WHERE shardgroupindex = 1 \gset

SELECT citus_shard_property_set(:shardgroup_1_shardid, anti_affinity=>true);

-- noop
SELECT citus_shard_property_set(:shardgroup_1_shardid, NULL);
SELECT citus_shard_property_set(:shardgroup_1_shardid);

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'single_shard_1');

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.single_shard_1')
$$)
ORDER BY result;

-- test invalid input
SELECT citus_shard_property_set(NULL, anti_affinity=>true);
SELECT citus_shard_property_set(0, anti_affinity=>true);
SELECT citus_shard_property_set(NULL, anti_affinity=>false);
SELECT citus_shard_property_set(0, anti_affinity=>false);

-- we verify whether shard exists even if anti_affinity is not provided
SELECT citus_shard_property_set(0, anti_affinity=>NULL);

CREATE TABLE append_table (a int, b int);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT 1 FROM master_create_empty_shard('append_table');

CREATE TYPE composite_key_type AS (f1 int, f2 text);
CREATE TABLE range_table(key composite_key_type, value int);
SELECT create_distributed_table('range_table', 'key', 'range');
CALL public.create_range_partitioned_shards('range_table', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

CREATE TABLE local_table(a int);
SELECT citus_add_local_table_to_metadata('local_table');

-- all should fail
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'append_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'range_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'ref_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid = 'local_table'::regclass LIMIT 1;

SELECT citus_shard_property_set(shardid, anti_affinity=>false) FROM pg_dist_shard WHERE logicalrelid = 'append_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>false) FROM pg_dist_shard WHERE logicalrelid = 'range_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>false) FROM pg_dist_shard WHERE logicalrelid = 'ref_table'::regclass LIMIT 1;
SELECT citus_shard_property_set(shardid, anti_affinity=>false) FROM pg_dist_shard WHERE logicalrelid = 'local_table'::regclass LIMIT 1;

DROP TABLE range_table;
DROP TYPE composite_key_type;

SET client_min_messages TO WARNING;
DROP SCHEMA isolate_placement CASCADE;

CREATE SCHEMA isolate_placement;
SET search_path TO isolate_placement;

SET client_min_messages TO NOTICE;

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>4);
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');

CREATE TABLE dist_non_colocated(a int);
SELECT create_distributed_table('dist_non_colocated', 'a', shard_count=>4, colocate_with=>'none');

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

CREATE TABLE append_table (a int, b int);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT 1 FROM master_create_empty_shard('append_table');

CREATE TABLE range_table(a int, b int);
SELECT create_distributed_table('range_table', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table', '{"0","25"}','{"26","50"}');

CREATE TABLE reference_table_1(a int);
SELECT create_reference_table('reference_table_1');

CREATE TABLE local_table_1(a int);
SELECT citus_add_local_table_to_metadata('local_table_1');

SELECT shardids[1] AS shardgroup_1_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 1 \gset

SELECT citus_shard_property_set(:shardgroup_1_shardid, anti_affinity=>true);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

CREATE TABLE reference_table_2(a int);
SELECT create_reference_table('reference_table_2');

CREATE TABLE local_table_2(a int);
SELECT citus_add_local_table_to_metadata('local_table_2');

-- make sure that we still have placements for both reference tables on all nodes
SELECT COUNT(DISTINCT(groupid))=3 FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'reference_table_1'::regclass;
SELECT COUNT(DISTINCT(groupid))=3 FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'reference_table_2'::regclass;

-- sanity check for local tables
SELECT groupid = 0 FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'local_table_1'::regclass;
SELECT groupid = 0 FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'local_table_2'::regclass;

CREATE TABLE dist_post_non_colocated(a int);
SELECT create_distributed_table('dist_post_non_colocated', 'a', shard_count=>4, colocate_with=>'none');

CREATE TABLE dist_post_concurrently_non_colocated(a int);
SELECT create_distributed_table_concurrently('dist_post_concurrently_non_colocated', 'a', shard_count=>4, colocate_with=>'none');

CREATE TABLE dist_post_colocated(a int);
SELECT create_distributed_table('dist_post_colocated', 'a', colocate_with=>'dist_1');

CREATE TABLE dist_post_concurrently_colocated(a int);
SELECT create_distributed_table_concurrently('dist_post_concurrently_colocated', 'a', colocate_with=>'dist_1');

CREATE TABLE single_shard_post(a int);
SELECT create_distributed_table('single_shard_post', null, colocate_with=>'none');

CREATE TABLE append_table_post(a int, b int);
SELECT create_distributed_table('append_table_post', 'a', 'append');
SELECT 1 FROM master_create_empty_shard('append_table_post');

CREATE TABLE range_table_post(a int, b int);
SELECT create_distributed_table('range_table_post', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table_post', '{"0","25"}','{"26","50"}');

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('isolate_placement.dist_1')
$$)
ORDER BY result;

-- Make sure that the node that contains shard-group 1 of isolate_placement.dist_1
-- doesn't have any other placements.
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1);

SET client_min_messages TO ERROR;
SELECT citus_drain_node('localhost', :worker_1_port, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT citus_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);

-- drain node should have failed and the node should still have the same set of placements
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1);

SET client_min_messages TO ERROR;
SELECT citus_drain_node('localhost', :worker_2_port, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

-- drain node should have failed and the node should still have the same set of placements
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1);

CREATE TABLE dist_3(a int);
SELECT create_distributed_table('dist_3', 'a', colocate_with=>'dist_1');

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1);

SET citus.shard_replication_factor TO 2;

CREATE TABLE dist_replicated(a int);

-- fails as we only have one node that's not used to isolate a shard placement group
SELECT create_distributed_table('dist_replicated', 'a', shard_count=>4, colocate_with=>'none');

SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_to_be_replicated(a int);
SELECT create_distributed_table('dist_to_be_replicated', 'a', shard_count=>4, colocate_with=>'none');

UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'isolate_placement.dist_to_be_replicated'::regclass;

SET client_min_messages TO WARNING;
-- fails as we only have one node that's not used to isolate a shard placement group
SELECT replicate_table_shards('isolate_placement.dist_to_be_replicated', shard_replication_factor=>2, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

SET client_min_messages TO WARNING;
-- succeeds as now we have two nodes that are not used to isolate a shard placement group
SELECT replicate_table_shards('isolate_placement.dist_to_be_replicated', shard_replication_factor=>2, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1);

SELECT DISTINCT(table_name::regclass::text)
FROM citus_shards
JOIN pg_class ON (oid = table_name)
WHERE relnamespace = 'isolate_placement'::regnamespace AND has_separate_node
ORDER BY 1;

SELECT bool_or(has_separate_node) = false
FROM citus_shards
JOIN (
    SELECT unnest(shardids) shardid
    FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
    WHERE shardgroupindex != 1
) shards_except_group_1 USING (shardid);

DROP TABLE dist_to_be_replicated;

SELECT citus_drain_node('localhost', :master_port, shard_transfer_mode=>'block_writes');

DROP TABLE dist_replicated;

SET client_min_messages TO WARNING;
DROP SCHEMA isolate_placement CASCADE;

CREATE SCHEMA isolate_placement;
SET search_path TO isolate_placement;

SET client_min_messages TO NOTICE;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass, 'single_shard_2'::regclass);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- fails

CREATE TABLE dist_1(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>4);

CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

CREATE TABLE append_table (a int, b int);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT 1 FROM master_create_empty_shard('append_table');

CREATE TABLE range_table(a int, b int);
SELECT create_distributed_table('range_table', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table', '{"0","25"}','{"26","50"}');

-- succeeds

CREATE TABLE reference_table_1(a int);
SELECT create_reference_table('reference_table_1');

CREATE TABLE local_table_1(a int);
SELECT citus_add_local_table_to_metadata('local_table_1');

CREATE TABLE single_shard_4(a int);
SELECT create_distributed_table('single_shard_4', null, colocate_with=>'single_shard_1');

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1);
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_2', 1);

SET client_min_messages TO WARNING;
DROP SCHEMA isolate_placement CASCADE;

CREATE SCHEMA isolate_placement;
SET search_path TO isolate_placement;

SET client_min_messages TO NOTICE;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

-- Make sure that we don't assume that a node is used to isolate a shard placement
-- group just because it contains a single shard placement group.
CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

SET client_min_messages TO WARNING;
DROP SCHEMA isolate_placement CASCADE;

CREATE SCHEMA isolate_placement;
SET search_path TO isolate_placement;

SET client_min_messages TO NOTICE;

SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int); -- will replicate this
CREATE TABLE dist_3(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>1);
SELECT create_distributed_table('dist_2', 'a', shard_count=>1, colocate_with=>'none');
SELECT create_distributed_table('dist_3', 'a', shard_count=>1, colocate_with=>'none');

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('dist_1'::regclass, 'dist_2'::regclass);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- so that replicate_table_shards works
UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid = 'isolate_placement.dist_2'::regclass;

SET client_min_messages TO WARNING;
-- succeeds but breaks the isolation requirement for either of dist_1 or dist_2 ..
SELECT replicate_table_shards('isolate_placement.dist_2', shard_replication_factor=>2, shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- .. so check the xor of the isolation requirements for dist_1 and dist_2
SELECT (public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) OR public.verify_placements_in_shard_group_isolated('isolate_placement.dist_2', 1)) = true AND
       (public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) AND public.verify_placements_in_shard_group_isolated('isolate_placement.dist_2', 1)) = false;

DROP TABLE dist_1, dist_2, dist_3;

SELECT citus_drain_node('localhost', :master_port, shard_transfer_mode=>'block_writes');

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

CREATE TABLE dist_1(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>4);

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);

SELECT groupid AS single_shard_1_group_id FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass \gset

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1);

-- show that we try to isolate placements where they were staying at the time rebalancer is invoked
SELECT groupid = :single_shard_1_group_id FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass;

DROP TABLE dist_1, single_shard_1, single_shard_2;

SET citus.shard_replication_factor TO 2;

CREATE TABLE dist_1(a int);
CREATE TABLE dist_2(a int);

SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

SELECT create_distributed_table('dist_1', 'a', shard_count=>1);
SELECT create_distributed_table('dist_2', 'a', colocate_with=>'dist_1');

SET citus.shard_replication_factor TO 1;

SELECT shardids[1] AS shardgroup_1_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_2')
WHERE shardgroupindex = 1 \gset

SELECT citus_shard_property_set(:shardgroup_1_shardid, anti_affinity=>true);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) = true;

DROP TABLE dist_1, dist_2;

SELECT citus_drain_node('localhost', :master_port, shard_transfer_mode=>'block_writes');

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

CREATE TABLE single_shard_4(a int);
SELECT create_distributed_table('single_shard_4', null, colocate_with=>'none');

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1);

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_2'::regclass);
SELECT citus_shard_property_set(shardid, anti_affinity=>false) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_2', 1);
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1) = false;

DROP TABLE single_shard_1, single_shard_2, single_shard_3, single_shard_4;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

-- this would be placed on the same node as single_shard_1
CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

DROP TABLE single_shard_2;

SELECT shardid, nodeid INTO single_shard_3_shardid_nodeid
FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) JOIN pg_dist_node USING (groupid)
WHERE logicalrelid = 'isolate_placement.single_shard_3'::regclass AND noderole = 'primary';

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_3'::regclass);

-- tell rebalancer that single_shard_3 cannot be placed on the node where it is currently placed
CREATE OR REPLACE FUNCTION test_shard_allowed_on_node(p_shardid bigint, p_nodeid int)
    RETURNS boolean AS
$$
    SELECT
        CASE
            WHEN (p_shardid = shardid and p_nodeid = nodeid) THEN false
            ELSE true
        END
    FROM single_shard_3_shardid_nodeid;
$$ LANGUAGE sql;

INSERT INTO pg_catalog.pg_dist_rebalance_strategy(
    name,
    default_strategy,
    shard_cost_function,
    node_capacity_function,
    shard_allowed_on_node_function,
    default_threshold,
    minimum_threshold,
    improvement_threshold
)
VALUES (
    'test_isolate_placement',
    false,
    'citus_shard_cost_1',
    'citus_node_capacity_1',
    'isolate_placement.test_shard_allowed_on_node',
    0,
    0,
    0
);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(rebalance_strategy := 'test_isolate_placement', shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- test_shard_allowed_on_node() didn't cause rebalance_table_shards() to fail.
--
-- Right now single_shard_1 & single_shard_3 are placed on the same node. And
-- due to order we follow when assigning nodes to placement groups that need an
-- isolated node, we will try placing single_shard_1 to the node where it is
-- currently placed, and then we will try placing single_shard_3 to some other
-- node (as its current node is already assigned to single_shard_1), not to the
-- one we disallowed in test_shard_allowed_on_node().
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1);
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_3', 1);

DROP TABLE single_shard_3_shardid_nodeid;
DELETE FROM pg_catalog.pg_dist_rebalance_strategy WHERE name='test_isolate_placement';

DROP TABLE single_shard_1, single_shard_3;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

-- this would be placed on the same node as single_shard_1
CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

DROP TABLE single_shard_2;

SELECT shardid, nodeid INTO single_shard_3_shardid_nodeid
FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) JOIN pg_dist_node USING (groupid)
WHERE logicalrelid = 'isolate_placement.single_shard_3'::regclass AND noderole = 'primary';

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_3'::regclass);

-- Same test above but this time we tell rebalancer that single_shard_3 cannot be placed
-- on any node except the one where it is currently placed.
CREATE OR REPLACE FUNCTION test_shard_allowed_on_node(p_shardid bigint, p_nodeid int)
    RETURNS boolean AS
$$
    SELECT
        CASE
            WHEN (p_shardid = shardid and p_nodeid != nodeid) THEN false
            ELSE true
        END
    FROM single_shard_3_shardid_nodeid;
$$ LANGUAGE sql;

INSERT INTO pg_catalog.pg_dist_rebalance_strategy(
    name,
    default_strategy,
    shard_cost_function,
    node_capacity_function,
    shard_allowed_on_node_function,
    default_threshold,
    minimum_threshold,
    improvement_threshold
)
VALUES (
    'test_isolate_placement',
    false,
    'citus_shard_cost_1',
    'citus_node_capacity_1',
    'isolate_placement.test_shard_allowed_on_node',
    0,
    0,
    0
);

SET client_min_messages TO ERROR;
SELECT rebalance_table_shards(rebalance_strategy := 'test_isolate_placement', shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- This time, test_shard_allowed_on_node() caused rebalance_table_shards() to
-- fail.
--
-- Right now single_shard_1 & single_shard_3 are placed on the same node. And
-- due to order we follow when assigning nodes to placement groups that need an
-- isolated node, we will try placing single_shard_1 to the node where it is
-- currently placed, and then we will try placing single_shard_3 to some other
-- node (as its current node is already assigned to single_shard_1). However,
-- test_shard_allowed_on_node() doesn't allow that.
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1) = false;
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_3', 1) = false;

DROP TABLE single_shard_3_shardid_nodeid;
DELETE FROM pg_catalog.pg_dist_rebalance_strategy WHERE name='test_isolate_placement';

DROP TABLE single_shard_1, single_shard_3;

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

CREATE TABLE single_shard_2(a int);
SELECT create_distributed_table('single_shard_2', null, colocate_with=>'none');

-- this would be placed on the same node as single_shard_1
CREATE TABLE single_shard_3(a int);
SELECT create_distributed_table('single_shard_3', null, colocate_with=>'none');

DROP TABLE single_shard_2;

SELECT shardid, nodeid INTO single_shard_1_shardid_nodeid
FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) JOIN pg_dist_node USING (groupid)
WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass AND noderole = 'primary';

SELECT shardid, nodeid INTO single_shard_3_shardid_nodeid
FROM pg_dist_shard JOIN pg_dist_placement USING (shardid) JOIN pg_dist_node USING (groupid)
WHERE logicalrelid = 'isolate_placement.single_shard_3'::regclass AND noderole = 'primary';

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);
SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_3'::regclass);

-- Tell rebalancer that single_shard_1 cannot be placed on the node where it is currently placed
-- and that single_shard_3 cannot be placed on any node except the one where it is currently placed.
CREATE OR REPLACE FUNCTION test_shard_allowed_on_node(p_shardid bigint, p_nodeid int)
    RETURNS boolean AS
$$
    SELECT
    (
        SELECT
            CASE
                WHEN (p_shardid = shardid and p_nodeid = nodeid) THEN false
                ELSE true
            END
        FROM single_shard_1_shardid_nodeid
    ) AND
    (
        SELECT
            CASE
                WHEN (p_shardid = shardid and p_nodeid != nodeid) THEN false
                ELSE true
            END
        FROM single_shard_3_shardid_nodeid
    )
$$ LANGUAGE sql;

INSERT INTO pg_catalog.pg_dist_rebalance_strategy(
    name,
    default_strategy,
    shard_cost_function,
    node_capacity_function,
    shard_allowed_on_node_function,
    default_threshold,
    minimum_threshold,
    improvement_threshold
)
VALUES (
    'test_isolate_placement',
    false,
    'citus_shard_cost_1',
    'citus_node_capacity_1',
    'isolate_placement.test_shard_allowed_on_node',
    0,
    0,
    0
);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(rebalance_strategy := 'test_isolate_placement', shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- This time, test_shard_allowed_on_node() didn't cause rebalance_table_shards() to
-- fail.
--
-- Right now single_shard_1 & single_shard_3 are placed on the same node. And
-- due to order we follow when assigning nodes to placement groups that need an
-- isolated node, we will try placing single_shard_1 to the node where it is
-- currently placed but this is not possible due to test_shard_allowed_on_node().
-- But this is not a problem because we will take the specified rebalancer strategy
-- into the account when assigning nodes to placements that need separate nodes and
-- will try to place it to a different node. Then we will try placing single_shard_3
-- to the node where it is currently placed, and this is ok.
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1) = true;
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_3', 1) = true;

DROP TABLE single_shard_1_shardid_nodeid, single_shard_3_shardid_nodeid;
DELETE FROM pg_catalog.pg_dist_rebalance_strategy WHERE name='test_isolate_placement';

DROP TABLE single_shard_1, single_shard_3;

SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE dist_1(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>4);

SELECT shardids[1] AS shardgroup_1_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 1 \gset

SELECT citus_shard_property_set(:shardgroup_1_shardid, anti_affinity=>true);

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

SELECT citus_shard_property_set(shardid, anti_affinity=>true) FROM pg_dist_shard WHERE logicalrelid IN ('single_shard_1'::regclass);

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards(shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1) = true;
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) = true;

SET client_min_messages TO WARNING;
SELECT rebalance_table_shards('isolate_placement.dist_1', shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- Make sure that calling the rebalancer specifically for dist_1 doesn't
-- break the placement separation rules.
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.single_shard_1', 1) = true;
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) = true;

DROP TABLE dist_1, single_shard_1;
SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', false);

CREATE TABLE dist_1(a int);
SELECT create_distributed_table('dist_1', 'a', shard_count=>4);

SELECT shardids[1] AS shardgroup_1_shardid
FROM public.get_enumerated_shard_groups('isolate_placement.dist_1')
WHERE shardgroupindex = 1 \gset

CREATE TABLE single_shard_1(a int);
SELECT create_distributed_table('single_shard_1', null, colocate_with=>'none');

-- idempotantly move shard of single_shard_1 to the node that contains shard-group 1 of dist_1
WITH
    shardid_and_source_node AS (
        SELECT shardid, nodeid
        FROM pg_dist_shard
        JOIN pg_dist_placement USING (shardid)
        JOIN pg_dist_node USING (groupid)
        WHERE logicalrelid = 'isolate_placement.single_shard_1'::regclass
    ),
    target_node AS (
        SELECT nodeid
        FROM pg_dist_shard
        JOIN pg_dist_placement USING (shardid)
        JOIN pg_dist_node USING (groupid)
        WHERE shardid = :shardgroup_1_shardid
    )
SELECT COUNT(
    citus_move_shard_placement(
        shardid_and_source_node.shardid,
        shardid_and_source_node.nodeid,
        target_node.nodeid,
        'block_writes'
    )
) >= 0
FROM shardid_and_source_node, target_node
WHERE shardid_and_source_node.nodeid != target_node.nodeid;

SELECT citus_shard_property_set(:shardgroup_1_shardid, anti_affinity=>true);

SET client_min_messages TO ERROR;
SELECT rebalance_table_shards('isolate_placement.dist_1', shard_transfer_mode=>'block_writes');
SET client_min_messages TO NOTICE;

-- Make sure that calling the rebalancer specifically for dist_1 enforces
-- placement separation rules too.
SELECT public.verify_placements_in_shard_group_isolated('isolate_placement.dist_1', 1) = true;

DROP TABLE dist_1, single_shard_1;

SET client_min_messages TO WARNING;
DROP SCHEMA isolate_placement CASCADE;

SELECT citus_remove_node('localhost', :master_port);
