-- Tests functions related to cluster membership

-- add the nodes to the cluster
SELECT cluster_add_node('localhost', :worker_1_port);
SELECT cluster_add_node('localhost', :worker_2_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();

-- try to add the node again when it is activated
SELECT cluster_add_node('localhost', :worker_1_port);

-- get the active nodes
SELECT master_get_active_worker_nodes();
