-- add coordinator in idempotent way, this is needed so we get a nodeid
SET client_min_messages TO ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

-- Kill maintenance daemon so it gets restarted and gets a gpid containing our
-- nodeid
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE application_name = 'Citus Maintenance Daemon' \gset

-- reconnect to make sure we get a session with the gpid containing our nodeid
\c - - - -
CREATE SCHEMA global_cancel;
SET search_path TO global_cancel;
SET citus.next_shard_id TO 56789000;
SET citus.grep_remote_commands TO '%pg_cancel_backend%';

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a', shard_count:=4);
INSERT INTO dist_table VALUES (1, 1);

SELECT global_pid AS coordinator_gpid FROM get_all_active_transactions() WHERE process_id = pg_backend_pid() \gset
SELECT pg_typeof(:coordinator_gpid);

SELECT pg_cancel_backend(:coordinator_gpid);

SET citus.log_remote_commands TO ON;
SELECT pg_cancel_backend(:coordinator_gpid) FROM dist_table WHERE a = 1;

BEGIN;
SELECT pg_cancel_backend(:coordinator_gpid) FROM dist_table WHERE a = 1;
END;

SET citus.log_remote_commands TO OFF;

SELECT global_pid AS maintenance_daemon_gpid
FROM pg_stat_activity psa JOIN get_all_active_transactions() gaat ON psa.pid = gaat.process_id
WHERE application_name = 'Citus Maintenance Daemon' \gset

SET client_min_messages TO ERROR;
CREATE USER global_cancel_user;
SELECT 1 FROM run_command_on_workers('CREATE USER global_cancel_user');
RESET client_min_messages;

\c - global_cancel_user - :master_port

SELECT pg_typeof(:maintenance_daemon_gpid);

\set VERBOSITY terse

SELECT pg_cancel_backend(:maintenance_daemon_gpid);
SELECT pg_terminate_backend(:maintenance_daemon_gpid);

\set VERBOSITY default
-- we can cancel our own backend
SELECT pg_cancel_backend(citus_backend_gpid());

\c - postgres - :master_port

SET client_min_messages TO DEBUG;

-- 10000000000 is the node id multiplier for global pid
SELECT pg_cancel_backend(10000000000 * citus_coordinator_nodeid() + 0);
SELECT pg_terminate_backend(10000000000 * citus_coordinator_nodeid() + 0);

RESET client_min_messages;

SELECT citus_backend_gpid() = citus_calculate_gpid(citus_coordinator_nodeid(), pg_backend_pid());

SELECT nodename = citus_nodename_for_nodeid(nodeid) AND nodeport = citus_nodeport_for_nodeid(nodeid)
FROM pg_dist_node
WHERE isactive = true AND noderole = 'primary';

SELECT citus_nodeid_for_gpid(10000000000 * 2 + 3);
SELECT citus_pid_for_gpid(10000000000 * 2 + 3);

DROP SCHEMA global_cancel CASCADE;
