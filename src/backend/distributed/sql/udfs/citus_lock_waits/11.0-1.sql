SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS
WITH
citus_dist_stat_activity AS
(
    SELECT * FROM citus_dist_stat_activity
),
unique_global_wait_edges AS
(
    SELECT DISTINCT ON(waiting_global_pid, blocking_global_pid) * FROM citus_internal_global_blocked_processes()
),
citus_dist_stat_activity_with_node_id AS
(
    SELECT
    citus_dist_stat_activity.*, (CASE citus_dist_stat_activity.distributed_query_host_name WHEN 'coordinator_host' THEN 0 ELSE pg_dist_node.nodeid END) as initiator_node_id
    FROM
    citus_dist_stat_activity LEFT JOIN pg_dist_node
    ON
    citus_dist_stat_activity.distributed_query_host_name = pg_dist_node.nodename AND
    citus_dist_stat_activity.distributed_query_host_port = pg_dist_node.nodeport
)
SELECT
 waiting.global_pid as waiting_gpid,
 blocking.global_pid as blocking_gpid,
 waiting.pid AS waiting_pid,
 blocking.pid AS blocking_pid,
 waiting.query AS blocked_statement,
 blocking.query AS current_statement_in_blocking_process,
 waiting.initiator_node_id AS waiting_node_id,
 blocking.initiator_node_id AS blocking_node_id,
 waiting.distributed_query_host_name AS waiting_node_name,
 blocking.distributed_query_host_name AS blocking_node_name,
 waiting.distributed_query_host_port AS waiting_node_port,
 blocking.distributed_query_host_port AS blocking_node_port
FROM
 unique_global_wait_edges
JOIN
 citus_dist_stat_activity_with_node_id waiting ON (unique_global_wait_edges.waiting_global_pid = waiting.global_pid)
JOIN
 citus_dist_stat_activity_with_node_id blocking ON (unique_global_wait_edges.blocking_global_pid = blocking.global_pid);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
