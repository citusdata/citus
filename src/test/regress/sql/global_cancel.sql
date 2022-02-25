CREATE SCHEMA global_cancel;
SET search_path TO global_cancel;
SET citus.next_shard_id TO 56789000;

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

SET ROLE global_cancel_user;

SELECT pg_typeof(:maintenance_daemon_gpid);

SELECT pg_cancel_backend(:maintenance_daemon_gpid);
SELECT pg_terminate_backend(:maintenance_daemon_gpid);

RESET ROLE;

SELECT nodeid AS coordinator_node_id FROM pg_dist_node WHERE nodeport = :master_port \gset

SET client_min_messages TO DEBUG;

-- 10000000000 is the node id multiplier for global pid
SELECT pg_cancel_backend(10000000000 * :coordinator_node_id + 0);
SELECT pg_terminate_backend(10000000000 * :coordinator_node_id + 0);

RESET client_min_messages;

DROP SCHEMA global_cancel CASCADE;
