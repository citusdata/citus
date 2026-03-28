\c - - - :master_port
-- prepare testing
SELECT citus_set_coordinator_host('localhost', :master_port);
SET citus.metadata_sync_mode TO 'transactional';

-- add inactive secondary node
SELECT 1 FROM citus_add_secondary_node('localhost', :follower_worker_2_port, 'localhost', :worker_2_port);
SELECT 1 FROM citus_disable_node('localhost', :follower_worker_2_port);

-- check inactive node
SELECT count(*) FROM pg_dist_node WHERE nodeport=:follower_worker_2_port AND nodename='localhost' AND NOT isactive;
SELECT start_metadata_sync_to_all_nodes();


\c - - - :worker_2_port
-- check inactive node on worker
SELECT count(*) FROM pg_dist_node WHERE nodeport=:follower_worker_2_port AND nodename='localhost' AND NOT isactive;

\c - - - :master_port

-- main test: activate secondary node
SELECT 1 FROM citus_activate_node('localhost', :follower_worker_2_port);


-- check is active node
SELECT count(*) FROM pg_dist_node WHERE nodeport=:follower_worker_2_port AND nodename='localhost' AND isactive;

\c - - - :worker_2_port
-- check is active node
SELECT count(*) FROM pg_dist_node WHERE nodeport=:follower_worker_2_port AND nodename='localhost' AND isactive;

\c - - - :worker_1_port
-- check active node
SELECT count(*) FROM pg_dist_node WHERE nodeport=:follower_worker_2_port AND nodename='localhost' AND isactive;

\c - - - :master_port
SET citus.metadata_sync_mode TO 'nontransactional';

-- error this operation cannot be completed in nontransactional metadata sync mode if the GUC citus.metadata_sync_mode set to 'nontransactional'
SELECT 1 FROM citus_activate_node('localhost', :follower_worker_2_port);

SET citus.metadata_sync_mode TO 'transactional';
-- error because the node does not exist 
SELECT 1 FROM citus_activate_node('localhost', 7777);
