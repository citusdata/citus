CREATE SCHEMA background_rebalance;
SET search_path TO background_rebalance;
SET citus.next_shard_id TO 85675000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;

ALTER SEQUENCE pg_dist_background_job_job_id_seq RESTART 100;
ALTER SEQUENCE pg_dist_background_task_task_id_seq RESTART 1000;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

alter system set citus.max_parallel_tasks_per_node to 2;
select pg_reload_conf();

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
select citus_remove_node('localhost', :worker_2_port);
CREATE TABLE t1 (a int PRIMARY KEY);
SELECT create_distributed_table('t1', 'a', shard_count => 4, colocate_with => 'none');
CREATE TABLE t2 (a int PRIMARY KEY);
SELECT create_distributed_table('t2', 'a', shard_count => 4, colocate_with => 'none');
CREATE TABLE t3 (a int PRIMARY KEY);
SELECT create_distributed_table('t3', 'a', shard_count => 4, colocate_with => 'none');
select citus_add_node('localhost', :worker_2_port);

SELECT * FROM get_rebalance_table_shards_plan() ORDER BY shardid;

SELECT citus_rebalance_start AS job_id from citus_rebalance_start() \gset

-- show that exactly two are running
SELECT citus_task_wait(1000, desired_status => 'running');

SELECT job_id, task_id, status, nodes_involved
FROM pg_dist_background_task WHERE job_id in (:job_id) ORDER BY task_id;

SELECT citus_task_wait(1000, desired_status => 'done');

SELECT job_id, task_id, status, nodes_involved
FROM pg_dist_background_task WHERE job_id in (:job_id) ORDER BY task_id;

DROP SCHEMA background_rebalance CASCADE;
TRUNCATE pg_dist_background_job CASCADE;
SELECT public.wait_for_resource_cleanup();
