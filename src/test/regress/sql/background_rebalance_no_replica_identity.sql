--
-- Exercises the force_logical_auto_identity shard transfer mode through the
-- background rebalancer (citus_rebalance_start). This runs against a clean
-- two-worker cluster and creates/drops its own tables, so a whole-cluster
-- rebalance only ever has to move the tables created here.
--
CREATE SCHEMA background_rebalance_no_ri;
SET search_path TO background_rebalance_no_ri;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 12345000;

-- make the background task queue monitor pick up scheduled jobs quickly
ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

--
-- 1) A table force_logical_auto_identity cannot rescue: it has no replica identity
--    (so it would be set to REPLICA IDENTITY FULL) but it has a json column, which
--    has no equality operator. citus_rebalance_start must reject it up front, before
--    it schedules any background job, just like it rejects a no-replica-identity
--    table under the default mode.
--
CREATE TABLE reject_json (a int, payload json);
SELECT create_distributed_table('reject_json', 'a', shard_count => 4, colocate_with => 'none');
INSERT INTO reject_json SELECT g, json_build_object('v', g) FROM generate_series(1, 40) g;
-- imbalance: move every shard on worker_2 onto worker_1 so a rebalance has work to do
SELECT citus_move_shard_placement(s.shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, shard_transfer_mode => 'block_writes')
FROM pg_dist_shard s JOIN pg_dist_shard_placement p USING (shardid)
WHERE s.logicalrelid='reject_json'::regclass AND p.nodeport = :worker_2_port
ORDER BY s.shardid;
SELECT public.wait_for_resource_cleanup();
-- rejected synchronously; no background job is scheduled
SELECT citus_rebalance_start(shard_transfer_mode => 'force_logical_auto_identity');
DROP TABLE reject_json;

--
-- 2) A table force_logical_auto_identity can move: no replica identity, but every
--    column is comparable. The background rebalance succeeds, the data is preserved,
--    and the source replica identity is restored to 'd'.
--
CREATE TABLE move_ok (a int, b text);
SELECT create_distributed_table('move_ok', 'a', shard_count => 4, colocate_with => 'none');
INSERT INTO move_ok SELECT g, 'v'||g FROM generate_series(1, 400) g;
-- imbalance: move every shard on worker_2 onto worker_1
SELECT citus_move_shard_placement(s.shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, shard_transfer_mode => 'block_writes')
FROM pg_dist_shard s JOIN pg_dist_shard_placement p USING (shardid)
WHERE s.logicalrelid='move_ok'::regclass AND p.nodeport = :worker_2_port
ORDER BY s.shardid;
SELECT public.wait_for_resource_cleanup();
-- all four shards now live on a single worker
SELECT count(DISTINCT nodeport) AS distinct_nodes_before
FROM pg_dist_shard s JOIN pg_dist_shard_placement p USING (shardid)
WHERE s.logicalrelid='move_ok'::regclass;

SELECT citus_rebalance_start(shard_transfer_mode => 'force_logical_auto_identity') > 0 AS started;
SELECT citus_rebalance_wait();
SELECT public.wait_for_resource_cleanup();
-- rebalanced back across both workers (2 shards each)
SELECT count(*) AS shards_per_node, (count(*) = 2) AS balanced
FROM pg_dist_shard s JOIN pg_dist_shard_placement p USING (shardid)
WHERE s.logicalrelid='move_ok'::regclass
GROUP BY p.nodeport ORDER BY 1;
SELECT count(*) AS rows FROM move_ok;
-- the source replica identity was restored to the original 'd' on every placement
SELECT DISTINCT result FROM run_command_on_placements('move_ok','SELECT relreplident FROM pg_class WHERE oid=''%s''::regclass');
DROP TABLE move_ok;

SET client_min_messages TO WARNING;
DROP SCHEMA background_rebalance_no_ri CASCADE;
