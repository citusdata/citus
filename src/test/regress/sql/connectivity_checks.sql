SELECT bool_and(coalesce(result, false)) FROM citus_check_cluster_node_health();
