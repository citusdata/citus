ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;

-- Tests functions related to cluster membership

-- before starting the test, lets try to create reference table and see a 
-- meaningful error
CREATE TABLE test_reference_table (y int primary key, name text);
SELECT create_reference_table('test_reference_table');

-- add the nodes to the cluster
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- try to add a node that is already in the cluster
SELECT * FROM master_add_node('localhost', :worker_1_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- try to remove a node (with no placements)
SELECT master_remove_node('localhost', :worker_2_port); 

-- verify that the node has been deleted
SELECT master_get_active_worker_nodes();

-- try to disable a node with no placements see that node is removed
SELECT master_add_node('localhost', :worker_2_port);
SELECT master_disable_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- add some shard placements to the cluster
SELECT master_activate_node('localhost', :worker_2_port);
CREATE TABLE cluster_management_test (col_1 text, col_2 int);
SELECT master_create_distributed_table('cluster_management_test', 'col_1', 'hash');
SELECT master_create_worker_shards('cluster_management_test', 16, 1);

-- see that there are some active placements in the candidate node
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE nodeport=:worker_2_port;

-- try to remove a node with active placements and see that node removal is failed
SELECT master_remove_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- insert a row so that master_disable_node() exercises closing connections
INSERT INTO test_reference_table VALUES (1, '1');

-- try to disable a node with active placements see that node is removed
-- observe that a notification is displayed
SELECT master_disable_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- restore the node for next tests
SELECT master_add_node('localhost', :worker_2_port);

-- try to remove a node with active placements and see that node removal is failed
SELECT master_remove_node('localhost', :worker_2_port); 

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

-- check that adding/removing nodes are propagated to nodes with hasmetadata=true
SELECT master_remove_node('localhost', :worker_2_port);
UPDATE pg_dist_node SET hasmetadata=true WHERE nodeport=:worker_1_port;
SELECT master_add_node('localhost', :worker_2_port);
\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port
SELECT master_remove_node('localhost', :worker_2_port);
\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port

-- check that added nodes are not propagated to nodes with hasmetadata=false
UPDATE pg_dist_node SET hasmetadata=false WHERE nodeport=:worker_1_port;
SELECT master_add_node('localhost', :worker_2_port);
\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port

-- check that removing two nodes in the same transaction works
SELECT 
	master_remove_node('localhost', :worker_1_port), 
	master_remove_node('localhost', :worker_2_port);
SELECT * FROM pg_dist_node ORDER BY nodeid;

-- check that adding two nodes in the same transaction works
SELECT
	master_add_node('localhost', :worker_1_port),
	master_add_node('localhost', :worker_2_port);
SELECT * FROM pg_dist_node ORDER BY nodeid;

-- check that mixed add/remove node commands work fine inside transaction
BEGIN;
SELECT master_remove_node('localhost', :worker_2_port);
SELECT master_add_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_2_port);
COMMIT;

SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;

UPDATE pg_dist_node SET hasmetadata=true WHERE nodeport=:worker_1_port;
BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_2_port);
SELECT master_add_node('localhost', :worker_2_port);
COMMIT;

SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;

\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port

SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_port);

-- check that a distributed table can be created after adding a node in a transaction

SELECT master_remove_node('localhost', :worker_2_port);
BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
CREATE TABLE temp(col1 text, col2 int);
SELECT create_distributed_table('temp', 'col1');
INSERT INTO temp VALUES ('row1', 1);
INSERT INTO temp VALUES ('row2', 2);
COMMIT;

SELECT col1, col2 FROM temp ORDER BY col1;

SELECT 
	count(*) 
FROM 
	pg_dist_shard_placement, pg_dist_shard 
WHERE 
	pg_dist_shard_placement.shardid = pg_dist_shard.shardid
	AND pg_dist_shard.logicalrelid = 'temp'::regclass
	AND pg_dist_shard_placement.nodeport = :worker_2_port;
	
DROP TABLE temp;

\c - - - :worker_1_port
DELETE FROM pg_dist_partition;
DELETE FROM pg_dist_shard;
DELETE FROM pg_dist_shard_placement;
DELETE FROM pg_dist_node;
\c - - - :master_port
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
