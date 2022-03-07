SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS
WITH
unique_global_wait_edges AS
(
    SELECT DISTINCT ON(waiting_global_pid, blocking_global_pid) * FROM citus_internal_global_blocked_processes()
)
SELECT
 waiting.global_pid as waiting_gpid,
 blocking.global_pid as blocking_gpid,
 waiting.pid AS waiting_pid,
 blocking.pid AS blocking_pid,
 waiting.query AS blocked_statement,
 blocking.query AS current_statement_in_blocking_process,
 waiting.nodeid AS waiting_nodeid,
 blocking.nodeid AS blocking_nodeid
FROM
 unique_global_wait_edges
JOIN
 citus_dist_stat_activity waiting ON (unique_global_wait_edges.waiting_global_pid = waiting.global_pid)
JOIN
 citus_dist_stat_activity blocking ON (unique_global_wait_edges.blocking_global_pid = blocking.global_pid);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
