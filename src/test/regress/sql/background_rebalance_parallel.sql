/*
  Test to check if the background tasks scheduled by the background rebalancer
  has the correct dependencies.
*/
CREATE SCHEMA background_rebalance_parallel;
SET search_path TO background_rebalance_parallel;
SET citus.next_shard_id TO 85674000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;

ALTER SEQUENCE pg_dist_background_job_job_id_seq RESTART 17777;
ALTER SEQUENCE pg_dist_background_task_task_id_seq RESTART 1000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 50050;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

/* Colocation group 1: create two tables table1_colg1, table2_colg1 and in a colocation group  */
CREATE TABLE table1_colg1 (a int PRIMARY KEY);
SELECT create_distributed_table('table1_colg1', 'a', shard_count => 4 , colocate_with => 'none');

CREATE TABLE table2_colg1 (b int PRIMARY KEY);

SELECT create_distributed_table('table2_colg1', 'b' , colocate_with => 'table1_colg1');

/* Colocation group 2: create two tables table1_colg2, table2_colg2 and in a colocation group  */
CREATE TABLE table1_colg2 (a int PRIMARY KEY);

SELECT create_distributed_table('table1_colg2 ', 'a', shard_count => 4, colocate_with => 'none');

CREATE TABLE  table2_colg2 (b int primary key);

SELECT create_distributed_table('table2_colg2', 'b' , colocate_with => 'table1_colg2');

/* Colocation group 3: create two tables table1_colg3, table2_colg3 and in a colocation group  */
CREATE TABLE table1_colg3 (a int PRIMARY KEY);

SELECT create_distributed_table('table1_colg3 ', 'a', shard_count => 4, colocate_with => 'none');

CREATE TABLE  table2_colg3 (b int primary key);

SELECT create_distributed_table('table2_colg3', 'b' , colocate_with => 'table1_colg3');


/* Add two new node so that we can rebalance */
SELECT 1 FROM citus_add_node('localhost', :worker_3_port);
SELECT 1 FROM citus_add_node('localhost', :worker_4_port);

SELECT * FROM citus_rebalance_start();

SELECT citus_rebalance_wait();

/*Check that a move is dependent on
	1. any other move scheduled earlier in its colocation group.
	2. any other move scheduled earlier whose source node or target
	   node overlaps with the current moves nodes. */
SELECT S.shardid, P.colocationid
FROM pg_dist_shard S, pg_dist_partition P
WHERE S.logicalrelid = P.logicalrelid ORDER BY S.shardid ASC;

SELECT D.task_id,
       (SELECT T.command FROM pg_dist_background_task T WHERE T.task_id = D.task_id),
       D.depends_on,
       (SELECT T.command FROM pg_dist_background_task T WHERE T.task_id = D.depends_on)
FROM pg_dist_background_task_depend D  WHERE job_id = 17777 ORDER BY D.task_id, D.depends_on ASC;


/* Check that if there is a reference table that needs to be synched to a node,
   any move without a dependency must depend on the move task for reference table. */
SELECT 1 FROM citus_drain_node('localhost',:worker_4_port);
SELECT 1 FROM citus_disable_node('localhost', :worker_4_port, synchronous:=true);

/* Drain worker_3 so that we can move only one colocation group to worker_3
   to create an unbalance that would cause parallel rebalancing. */
SELECT 1 FROM citus_drain_node('localhost',:worker_3_port);
SELECT citus_set_node_property('localhost', :worker_3_port, 'shouldhaveshards', true);

CALL citus_cleanup_orphaned_resources();

CREATE TABLE ref_table(a int PRIMARY KEY);

SELECT create_reference_table('ref_table');

/* Move all the shards of Colocation group 3 to worker_3.*/
SELECT
master_move_shard_placement(shardid, 'localhost', nodeport, 'localhost', :worker_3_port, 'block_writes')
FROM
        pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
        logicalrelid = 'table1_colg3'::regclass AND nodeport <> :worker_3_port
ORDER BY
      shardid;

CALL citus_cleanup_orphaned_resources();

/* Activate and new  nodes so that we can rebalance. */
SELECT 1 FROM citus_activate_node('localhost', :worker_4_port);
SELECT citus_set_node_property('localhost', :worker_4_port, 'shouldhaveshards', true);

SELECT 1 FROM citus_add_node('localhost', :worker_5_port);
SELECT 1 FROM citus_add_node('localhost', :worker_6_port);

SELECT * FROM citus_rebalance_start();

SELECT citus_rebalance_wait();

SELECT S.shardid, P.colocationid
FROM pg_dist_shard S, pg_dist_partition P
WHERE S.logicalrelid = P.logicalrelid ORDER BY S.shardid ASC;

SELECT D.task_id,
       (SELECT T.command FROM pg_dist_background_task T WHERE T.task_id = D.task_id),
       D.depends_on,
       (SELECT T.command FROM pg_dist_background_task T WHERE T.task_id = D.depends_on)
FROM pg_dist_background_task_depend D  WHERE job_id = 17778 ORDER BY D.task_id, D.depends_on ASC;

DROP SCHEMA background_rebalance_parallel CASCADE;
TRUNCATE pg_dist_background_job CASCADE;
select citus_remove_node('localhost', :worker_3_port);
select citus_remove_node('localhost', :worker_4_port);
select citus_remove_node('localhost', :worker_5_port);
select citus_remove_node('localhost', :worker_6_port);
