--
-- Test for negative scenarios in clone promotion functionality
--

--try to add follower_worker_1 as a clone of worker_1 to the cluster
-- this should fail as previous test has already promoted worker_1 to a primary node
SELECT citus_add_clone_node('localhost', :follower_worker_1_port, 'localhost', :worker_1_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

--try to add worker_node2 as a clone of worker_node1
-- this should fail as it is not a valid replica of worker_1
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_1_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

--try add
-- create a distributed table and load data
CREATE TABLE backup_test(id int, value text);
SELECT create_distributed_table('backup_test', 'id', 'hash');
INSERT INTO backup_test SELECT g, 'test' || g FROM generate_series(1, 10) g;

-- Create reference table
CREATE TABLE ref_table(id int PRIMARY KEY);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table SELECT i FROM generate_series(1, 5) i;

SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

-- verify initial shard placement
SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

-- Try to add replica of worker_node2 as a clone of worker_node1
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_1_port) AS clone_node_id \gset

-- Test 1: Try to promote a non-existent clone node
SELECT citus_promote_clone_and_rebalance(clone_nodeid =>99999);

-- Test 2: Try to promote a regular worker node (not a clone)
SELECT citus_promote_clone_and_rebalance(clone_nodeid => 1);

-- Test 3: Try to promote with invalid timeout (negative)
SELECT citus_promote_clone_and_rebalance(clone_nodeid => 1,
         catchup_timeout_seconds => -100);

-- register the new node as a clone, This should pass
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_2_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

SELECT :clone_node_id;

-- Test 4: Try to promote clone with invalid strategy name
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id, rebalance_strategy => 'invalid_strategy');

SELECT * from pg_dist_node ORDER by nodeid;

-- Test 9: Rollback the citus_promote_clone_and_rebalance transaction
BEGIN;
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id);
ROLLBACK;

-- Verify no data is lost after rooling back the transaction
SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

SELECT * from pg_dist_node ORDER by nodeid;

-- Test 5: Try to add and promote a proper replica after rollback
SELECT master_add_node('localhost', :worker_3_port) AS nodeid_3 \gset
SELECT citus_add_clone_node('localhost', :follower_worker_3_port, 'localhost', :worker_3_port) AS clone_node_id_3 \gset

set citus.shard_count = 100;
CREATE TABLE backup_test2(id int, value text);
SELECT create_distributed_table('backup_test2', 'id', 'hash');
INSERT INTO backup_test2 SELECT g, 'test' || g FROM generate_series(1, 10) g;

-- Create reference table
CREATE TABLE ref_table2(id int PRIMARY KEY);
SELECT create_reference_table('ref_table2');
INSERT INTO ref_table2 SELECT i FROM generate_series(1, 5) i;

SELECT * from get_snapshot_based_node_split_plan('localhost', :worker_3_port, 'localhost', :follower_worker_3_port);

SET client_min_messages to 'LOG';
SELECT citus_promote_clone_and_rebalance(clone_nodeid => :clone_node_id_3);
SET client_min_messages to DEFAULT;

SELECT COUNT(*) from backup_test;
SELECT COUNT(*) from ref_table;

SELECT * from pg_dist_node ORDER by nodeid;

-- check the shard placement

SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

set citus.shard_count to default;

-- cleanup
DROP TABLE backup_test;
DROP TABLE ref_table;
DROP TABLE backup_test2;
DROP TABLE ref_table2;
