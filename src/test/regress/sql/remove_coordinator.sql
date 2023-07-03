-- removing coordinator from pg_dist_node should update pg_dist_colocation
SELECT master_remove_node('localhost', :master_port);

-- restore coordinator for the rest of the tests
SELECT citus_set_coordinator_host('localhost', :master_port);
