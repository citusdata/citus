CREATE SCHEMA background_rebalance_parallel;
SET search_path TO background_rebalance_parallel;
SET citus.next_shard_id TO 85674000;
SET citus.shard_replication_factor TO 1;

ALTER SEQUENCE pg_dist_background_job_job_id_seq RESTART 17777;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

SELECT citus_disable_node('localhost', :worker_2_port, synchronous:=true); 

/* Create two tables table1_coloc1, table2_coloc1 and in a colocation group  */

CREATE TABLE table1_colg1 (a int PRIMARY KEY);
SELECT create_distributed_table('table1_colg1', 'a', shard_count => 8, colocate_with => 'none');

CREATE TABLE table2_colg1 (b int PRIMARY KEY); 

SELECT create_distributed_table('table2_colg1', 'b' , colocate_with => 'table1_colg1'); 

CREATE TABLE table1_colg2 (a int PRIMARY KEY); 

SELECT create_distributed_table('table1_colg2 ', 'a', shard_count => 8, colocate_with => 'none'); 

CREATE TABLE  table2_colg2 (b int primary key); 

SELECT create_distributed_table('table2_colg2', 'b' , colocate_with => 'table1_colg2'); 


/* Activate a node so that we can rebalance */
SELECT * FROM citus_activate_node('localhost', :worker_2_port); 

SELECT * FROM citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true); 

SELECT * FROM citus_rebalance_start();

SELECT citus_rebalance_wait();

/* Check that all the move tasks for shards of table1_colg1 and table2_colg1 sequentially take a dependency on each other.
 * The all the move tasks of table1_colg2 and table2_colg2 sequentially take a dependency on each other *
   The first move task scheduled in each group has no dependency.
 */
SELECT * FROM pg_dist_background_task_depend WHERE job_id = 17777 ORDER BY task_id ASC;

/* Check that if there is a reference table that needs to be synched to a node, the first move scheduled in a colocation group takes a dependency on the 
   replicate_reference_tables task */

SELECT citus_drain_node('localhost',:worker_2_port);
SELECT citus_disable_node('localhost', :worker_2_port, synchronous:=true);

CREATE TABLE ref_table(a int PRIMARY KEY); 

SELECT create_reference_table('ref_table'); 

/* Activate a node so that we can rebalance */
SELECT * FROM citus_activate_node('localhost', :worker_2_port);

SELECT * FROM citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

SELECT * FROM citus_rebalance_start();

SELECT citus_rebalance_wait();

SELECT * FROM pg_dist_background_task_depend WHERE job_id = 17778 ORDER BY task_id ASC;

DROP SCHEMA background_rebalance_parallel CASCADE;
