ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1220000;

-- Tests functions related to cluster membership

-- add the nodes to the cluster
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- try to add the node again when it is activated
SELECT master_add_node('localhost', :worker_1_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- try to remove a node (with no placements)
SELECT master_remove_node('localhost', :worker_2_port); 

-- verify that the node has been deleted
SELECT master_get_active_worker_nodes();

-- add some shard placements to the cluster
SELECT master_add_node('localhost', :worker_2_port);
CREATE TABLE cluster_management_test (col_1 text, col_2 int);
SELECT master_create_distributed_table('cluster_management_test', 'col_1', 'hash');
SELECT master_create_worker_shards('cluster_management_test', 16, 1);

-- see that there are some active placements in the candidate node
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE nodeport=:worker_2_port;

-- try to remove a node with active placements and see that node removal is failed
SELECT master_remove_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- mark all placements in the candidate node as inactive
UPDATE pg_dist_shard_placement SET shardstate=3 WHERE nodeport=:worker_2_port;
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE nodeport=:worker_2_port;

-- try to remove a node with only inactive placements and see that node is removed
SELECT master_remove_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- clean-up
SELECT master_add_node('localhost', :worker_2_port);
UPDATE pg_dist_shard_placement SET shardstate=1 WHERE nodeport=:worker_2_port;
DROP TABLE cluster_management_test;
