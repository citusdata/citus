-- removing coordinator from pg_dist_node should update pg_dist_colocation
SELECT master_remove_node('localhost', :master_port);
