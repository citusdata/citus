CREATE SCHEMA background_rebalance_unlogged;
SET search_path TO background_rebalance_unlogged;
SET citus.shard_replication_factor TO 1;

-- Start with all four colocated shard groups on the first worker.
SELECT citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
CREATE TABLE logged_table (id int PRIMARY KEY);
SELECT create_distributed_table('logged_table', 'id', shard_count => 4, colocate_with => 'none');
CREATE UNLOGGED TABLE unlogged_table (id int PRIMARY KEY);
SELECT create_distributed_table('unlogged_table', 'id', colocate_with => 'logged_table');
INSERT INTO logged_table SELECT generate_series(1, 20);
INSERT INTO unlogged_table SELECT generate_series(1, 20);
SELECT citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

-- The preview shows moves for both tables. The transfer mode is chosen at start.
SELECT table_name, count(*) AS planned_moves
FROM get_rebalance_table_shards_plan(rebalance_strategy => 'by_shard_count')
GROUP BY table_name ORDER BY table_name::text;

-- A primary key passes replica identity validation, but the unlogged table
-- still prevents a logical move. Report that before creating a background job.
SELECT count(*) AS jobs_before FROM pg_dist_background_job \gset
SELECT citus_rebalance_start(rebalance_strategy => 'by_shard_count', shard_transfer_mode => 'auto');
SELECT count(*) = :jobs_before AS no_job_created FROM pg_dist_background_job;

-- The same shard groups can move when writes are blocked during the copy.
SELECT citus_rebalance_start(rebalance_strategy => 'by_shard_count', shard_transfer_mode => 'block_writes') AS blocking_job_id \gset
SELECT citus_job_wait(:blocking_job_id);
SELECT state FROM pg_dist_background_job WHERE job_id = :blocking_job_id;
SELECT table_name, nodeport = :worker_2_port AS second_worker, count(*) AS shards
FROM citus_shards
WHERE table_name IN ('logged_table'::regclass, 'unlogged_table'::regclass)
  AND nodeport IN (:worker_1_port, :worker_2_port)
GROUP BY table_name, second_worker ORDER BY table_name::text, second_worker;
SELECT count(*) AS rows FROM logged_table JOIN unlogged_table USING (id);

SELECT public.wait_for_resource_cleanup();
DROP TABLE unlogged_table, logged_table;
DROP SCHEMA background_rebalance_unlogged;
