--
-- ADD_COORDINATOR
--

-- The colocation group for reference tables should have replication factor 2
SELECT replicationfactor = 2 FROM pg_dist_colocation WHERE distributioncolumntype=0;

SELECT master_add_node('localhost', :master_port, groupid => 0) AS master_nodeid \gset

-- adding the same node again should return the existing nodeid
SELECT master_add_node('localhost', :master_port, groupid => 0) = :master_nodeid;

-- adding another node with groupid=0 should error out
SELECT master_add_node('localhost', 12345, groupid => 0) = :master_nodeid;

-- The colocation group for reference tables should have replication factor 3
SELECT replicationfactor = 3 FROM pg_dist_colocation WHERE distributioncolumntype=0;

-- start_metadata_sync_to_node() for coordinator should fail
SELECT start_metadata_sync_to_node('localhost', :master_port);
