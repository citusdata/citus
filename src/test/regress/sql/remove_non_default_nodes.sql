-- The default nodes for the citus test suite are coordinator and 2 worker nodes
-- Which we identify with master_port, worker_1_port, worker_2_port.
-- When needed in some tests, GetLocalNodeId() does not behave correctly,
-- So we remove the non default nodes. This tests expects the non default nodes
-- to not have any active placements.
SELECT any_value(citus_remove_node('localhost', nodeport))
FROM pg_dist_node
WHERE nodeport NOT IN (:master_port, :worker_1_port, :worker_2_port);
