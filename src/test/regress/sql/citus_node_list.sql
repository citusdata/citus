SET client_min_messages TO ERROR;

SELECT * FROM citus_set_coordinator_host('localhost', :master_port);
SELECT * FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

SELECT * from citus_disable_node('localhost', :worker_2_port);

SELECT * from citus_node_list();

SELECT * from citus_node_list(active := NULL);

SELECT * from citus_node_list(active := TRUE);

SELECT * from citus_node_list(active := FALSE);

SELECT * from citus_node_list(role := 'worker');

SELECT * from citus_node_list(role := 'coordinator');

SELECT * from citus_node_list(role := NULL);

SELECT * from citus_node_list(role := 'foo');

SELECT * from citus_node_list(active := FALSE, role := 'coordinator');

SELECT * from citus_node_list(active := FALSE, role := 'worker');

SET client_min_messages TO WARNING;
