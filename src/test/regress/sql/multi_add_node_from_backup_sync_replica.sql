--
-- Test for negative scenarios in clone promotion functionality
-- We do not allow synchronous replicas to be added as clones
-- this test is to ensure that we do not allow this
--

SELECT * from pg_dist_node ORDER by nodeid;

SELECT master_remove_node('localhost', :follower_worker_1_port);
SELECT master_remove_node('localhost', :follower_worker_2_port);

-- this should fail as the replica is a synchronous replica that is not allowed
SELECT citus_add_clone_node('localhost', :follower_worker_1_port, 'localhost', :worker_1_port) AS clone_node_id \gset

-- this should fail as the replica is a synchronous replica that is not allowed
SELECT citus_add_clone_node('localhost', :follower_worker_2_port, 'localhost', :worker_2_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;
