SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS
WITH
unique_global_wait_edges_with_calculated_gpids AS (
SELECT
			-- if global_pid is NULL, it is most likely that a backend is blocked on a DDL
			-- also for legacy reasons citus_internal_global_blocked_processes() returns groupId, we replace that with nodeIds
			case WHEN waiting_global_pid  !=0 THEN waiting_global_pid   ELSE citus_calculate_gpid(get_nodeid_for_groupid(waiting_node_id),  waiting_pid)  END waiting_global_pid,
			case WHEN blocking_global_pid !=0 THEN blocking_global_pid  ELSE citus_calculate_gpid(get_nodeid_for_groupid(blocking_node_id), blocking_pid) END blocking_global_pid,

			-- citus_internal_global_blocked_processes returns groupId, we replace it here with actual
			-- nodeId to be consisten with the other views
			get_nodeid_for_groupid(blocking_node_id) as blocking_node_id,
			get_nodeid_for_groupid(waiting_node_id) as waiting_node_id,

			blocking_transaction_waiting

			FROM citus_internal_global_blocked_processes()
),
unique_global_wait_edges AS
(
	SELECT DISTINCT ON(waiting_global_pid, blocking_global_pid) * FROM unique_global_wait_edges_with_calculated_gpids
),
citus_dist_stat_activity_with_calculated_gpids AS
(
	-- if global_pid is NULL, it is most likely that a backend is blocked on a DDL
	SELECT CASE WHEN global_pid != 0 THEN global_pid ELSE citus_calculate_gpid(nodeid, pid) END global_pid, nodeid, pid, query FROM citus_dist_stat_activity
)
SELECT
	waiting.global_pid as waiting_gpid,
	blocking.global_pid as blocking_gpid,
	waiting.query AS blocked_statement,
	blocking.query AS current_statement_in_blocking_process,
	waiting.nodeid AS waiting_nodeid,
	blocking.nodeid AS blocking_nodeid
FROM
	unique_global_wait_edges
		JOIN
	citus_dist_stat_activity_with_calculated_gpids waiting ON (unique_global_wait_edges.waiting_global_pid = waiting.global_pid)
		JOIN
	citus_dist_stat_activity_with_calculated_gpids blocking ON (unique_global_wait_edges.blocking_global_pid = blocking.global_pid);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
