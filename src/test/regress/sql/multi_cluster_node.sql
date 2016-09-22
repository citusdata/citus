-- Tests functions related to cluster membership

-- add the nodes to the cluster
SELECT cluster_add_node('localhost', :worker_1_port);
SELECT cluster_add_node('localhost', :worker_2_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- de-activate a node
SELECT cluster_deactivate_node('localhost', :worker_1_port);

-- try to add the node again when it is de-activated
SELECT cluster_add_node('localhost', :worker_1_port);

-- get the active nodes again to see deactivation
SELECT master_get_active_worker_nodes();

-- we cannot de-activate a node that is already de-activated
SELECT cluster_deactivate_node('localhost', :worker_1_port);

-- activate it again
SELECT cluster_activate_node('localhost', :worker_1_port);

-- try to add the node again when it is activated
SELECT cluster_add_node('localhost', :worker_1_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();