SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS upgrade_isolate_placement_before CASCADE;

CREATE SCHEMA upgrade_isolate_placement_before;
SET search_path TO upgrade_isolate_placement_before;

SET client_min_messages TO NOTICE;

CREATE TABLE table_with_isolated_placements (a int, b int);
SELECT create_distributed_table('table_with_isolated_placements', 'a', colocate_with=>'none');

SELECT shardids[1] AS shardgroup_5_shardid
FROM public.get_enumerated_shard_groups('upgrade_isolate_placement_before.table_with_isolated_placements')
WHERE shardgroupindex = 5 \gset

SELECT citus_shard_property_set(:shardgroup_5_shardid, anti_affinity=>true);

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('upgrade_isolate_placement_before.table_with_isolated_placements')
$$)
ORDER BY nodeid;
