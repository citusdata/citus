SET citus.next_shard_id TO 1220000;

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
SELECT nodeid, groupid FROM master_add_node('localhost', :worker_1_port);

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
SET citus.shard_count TO 16;
SET citus.shard_replication_factor TO 1;

SELECT isactive FROM master_activate_node('localhost', :worker_2_port);
CREATE TABLE cluster_management_test (col_1 text, col_2 int);
SELECT create_distributed_table('cluster_management_test', 'col_1', 'hash');

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

-- try to disable a node which does not exist and see that an error is thrown
SELECT master_disable_node('localhost.noexist', 2345);

CREATE USER non_super_user;
CREATE USER node_metadata_user;
GRANT EXECUTE ON FUNCTION master_activate_node(text,int) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_add_inactive_node(text,int,int,noderole,name) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_add_node(text,int,int,noderole,name) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_add_secondary_node(text,int,text,int,name) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_disable_node(text,int) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_remove_node(text,int) TO node_metadata_user;
GRANT EXECUTE ON FUNCTION master_update_node(int,text,int,bool,int) TO node_metadata_user;

-- try to manipulate node metadata via non-super user
SET ROLE non_super_user;
SELECT 1 FROM master_initialize_node_metadata();
SELECT 1 FROM master_add_inactive_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_activate_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_disable_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_remove_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_add_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_add_secondary_node('localhost', :worker_2_port + 2, 'localhost', :worker_2_port);
SELECT master_update_node(nodeid, 'localhost', :worker_2_port + 3) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- try to manipulate node metadata via privileged user
SET ROLE node_metadata_user;
BEGIN;
SELECT 1 FROM master_add_inactive_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_activate_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_disable_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_remove_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_add_node('localhost', :worker_2_port + 1);
SELECT 1 FROM master_add_secondary_node('localhost', :worker_2_port + 2, 'localhost', :worker_2_port);
SELECT master_update_node(nodeid, 'localhost', :worker_2_port + 3) FROM pg_dist_node WHERE nodeport = :worker_2_port;
SELECT nodename, nodeport, noderole FROM pg_dist_node ORDER BY nodeport;
ABORT;

\c - postgres - :master_port
SELECT master_get_active_worker_nodes();

-- restore the node for next tests
SELECT isactive FROM master_activate_node('localhost', :worker_2_port);

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
SELECT 1 FROM master_add_node('localhost', 9990, groupid => :new_group, noderole => 'secondary');
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
SELECT count(1) FROM pg_dist_node;

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
SET citus.shard_count TO 4;

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

-- check that you can't add a primary to a non-default cluster
SELECT master_add_node('localhost', 9999, nodecluster => 'olap');

-- check that you can't add more than one primary to a group
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
SELECT master_add_node('localhost', 9999, groupid => :worker_1_group, noderole => 'primary');

-- check that you can add secondaries and unavailable nodes to a group
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
SELECT 1 FROM master_add_node('localhost', 9998, groupid => :worker_1_group, noderole => 'secondary');
SELECT 1 FROM master_add_node('localhost', 9997, groupid => :worker_1_group, noderole => 'unavailable');
-- add_inactive_node also works with secondaries
SELECT 1 FROM master_add_inactive_node('localhost', 9996, groupid => :worker_2_group, noderole => 'secondary');

-- check that you can add a seconary to a non-default cluster, and activate it, and remove it
SELECT master_add_inactive_node('localhost', 9999, groupid => :worker_2_group, nodecluster => 'olap', noderole => 'secondary');
SELECT master_activate_node('localhost', 9999);
SELECT master_disable_node('localhost', 9999);
SELECT master_remove_node('localhost', 9999);

-- check that you can't manually add two primaries to a group
INSERT INTO pg_dist_node (nodename, nodeport, groupid, noderole)
  VALUES ('localhost', 5000, :worker_1_group, 'primary');
UPDATE pg_dist_node SET noderole = 'primary'
  WHERE groupid = :worker_1_group AND nodeport = 9998;

-- check that you can't manually add a primary to a non-default cluster
INSERT INTO pg_dist_node (nodename, nodeport, groupid, noderole, nodecluster)
  VALUES ('localhost', 5000, 1000, 'primary', 'olap');
UPDATE pg_dist_node SET nodecluster = 'olap'
  WHERE nodeport = :worker_1_port;

-- check that you /can/ add a secondary node to a non-default cluster
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport = :worker_2_port \gset
SELECT master_add_node('localhost', 8888, groupid => :worker_1_group, noderole => 'secondary', nodecluster=> 'olap');

-- check that super-long cluster names are truncated
SELECT master_add_node('localhost', 8887, groupid => :worker_1_group, noderole => 'secondary', nodecluster=>
	'thisisasixtyfourcharacterstringrepeatedfourtimestomake256chars.'
	'thisisasixtyfourcharacterstringrepeatedfourtimestomake256chars.'
	'thisisasixtyfourcharacterstringrepeatedfourtimestomake256chars.'
	'thisisasixtyfourcharacterstringrepeatedfourtimestomake256chars.'
	'overflow'
);
SELECT * FROM pg_dist_node WHERE nodeport=8887;

-- don't remove the secondary and unavailable nodes, check that no commands are sent to
-- them in any of the remaining tests

-- master_add_secondary_node lets you skip looking up the groupid
SELECT master_add_secondary_node('localhost', 9995, 'localhost', :worker_1_port);
SELECT master_add_secondary_node('localhost', 9994, primaryname => 'localhost', primaryport => :worker_2_port);
SELECT master_add_secondary_node('localhost', 9993, 'localhost', 2000);
SELECT master_add_secondary_node('localhost', 9992, 'localhost', :worker_1_port, nodecluster => 'second-cluster');

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset

-- master_update_node checks node exists
SELECT master_update_node(100, 'localhost', 8000);
-- master_update_node disallows aliasing existing node
SELECT master_update_node(:worker_1_node, 'localhost', :worker_2_port);

-- master_update_node moves a node
SELECT master_update_node(:worker_1_node, 'somehost', 9000);

SELECT * FROM pg_dist_node WHERE nodeid = :worker_1_node;

-- cleanup
SELECT master_update_node(:worker_1_node, 'localhost', :worker_1_port);
SELECT * FROM pg_dist_node WHERE nodeid = :worker_1_node;
