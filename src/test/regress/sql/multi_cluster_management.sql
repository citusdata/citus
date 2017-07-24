ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;

-- Tests functions related to cluster membership

-- before starting the test, lets try to create reference table and see a 
-- meaningful error
CREATE TABLE test_reference_table (y int primary key, name text);
SELECT create_reference_table('test_reference_table');

-- add the nodes to the cluster
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

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
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
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
SELECT master_activate_node('localhost', :worker_2_port);

-- try to remove a node with active placements and see that node removal is failed
SELECT master_remove_node('localhost', :worker_2_port); 

-- mark all placements in the candidate node as inactive
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
UPDATE pg_dist_placement SET shardstate=3 WHERE groupid=:worker_2_group;
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE nodeport=:worker_2_port;

-- try to remove a node with only inactive placements and see that removal still fails
SELECT master_remove_node('localhost', :worker_2_port); 
SELECT master_get_active_worker_nodes();

-- clean-up
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
UPDATE pg_dist_placement SET shardstate=1 WHERE groupid=:worker_2_group;

-- when there is no primary we should get a pretty error
UPDATE pg_dist_node SET noderole = 'secondary' WHERE nodeport=:worker_2_port;
SELECT * FROM cluster_management_test;

-- when there is no node at all in the group we should get a different error
DELETE FROM pg_dist_node WHERE nodeport=:worker_2_port;
SELECT * FROM cluster_management_test;

-- clean-up
SELECT groupid as new_group FROM master_add_node('localhost', :worker_2_port) \gset
UPDATE pg_dist_placement SET groupid = :new_group WHERE groupid = :worker_2_group;

-- test that you are allowed to remove secondary nodes even if there are placements
SELECT master_add_node('localhost', 9990, groupid => :new_group, noderole => 'secondary');
SELECT master_remove_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', 9990);

-- clean-up
DROP TABLE cluster_management_test;

-- check that adding/removing nodes are propagated to nodes with hasmetadata=true
SELECT master_remove_node('localhost', :worker_2_port);
UPDATE pg_dist_node SET hasmetadata=true WHERE nodeport=:worker_1_port;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port
SELECT master_remove_node('localhost', :worker_2_port);
\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port

-- check that added nodes are not propagated to nodes with hasmetadata=false
UPDATE pg_dist_node SET hasmetadata=false WHERE nodeport=:worker_1_port;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
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
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_2_port);
COMMIT;

SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;

UPDATE pg_dist_node SET hasmetadata=true WHERE nodeport=:worker_1_port;
BEGIN;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_2_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
COMMIT;

SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;

\c - - - :worker_1_port
SELECT nodename, nodeport FROM pg_dist_node WHERE nodename='localhost' AND nodeport=:worker_2_port;
\c - - - :master_port

SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- check that a distributed table can be created after adding a node in a transaction

SELECT master_remove_node('localhost', :worker_2_port);
BEGIN;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
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
DELETE FROM pg_dist_placement;
DELETE FROM pg_dist_node;
\c - - - :master_port
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

-- check that you can't add more than one primary to a group
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
SELECT master_add_node('localhost', 9999, groupid => :worker_1_group, noderole => 'primary');

-- check that you can add secondaries and unavailable nodes to a group
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
SELECT master_add_node('localhost', 9998, groupid => :worker_1_group, noderole => 'secondary');
SELECT master_add_node('localhost', 9997, groupid => :worker_1_group, noderole => 'unavailable');
-- add_inactive_node also works with secondaries
SELECT master_add_inactive_node('localhost', 9996, groupid => :worker_2_group, noderole => 'secondary');

-- check that you can't manually add two primaries to a group
INSERT INTO pg_dist_node (nodename, nodeport, groupid, noderole)
  VALUES ('localhost', 5000, :worker_1_group, 'primary');
UPDATE pg_dist_node SET noderole = 'primary'
  WHERE groupid = :worker_1_group AND nodeport = 9998;

-- don't remove the secondary and unavailable nodes, check that no commands are sent to
-- them in any of the remaining tests
