CREATE SCHEMA run_command_on_all_nodes;
SET search_path TO run_command_on_all_nodes;

-- check coordinator isn't in metadata
SELECT count(*) != 0 AS "Coordinator is in Metadata"
FROM pg_dist_node
WHERE groupid IN (
    SELECT groupid FROM pg_dist_local_group
);

-- run a simple select query and check it also runs in coordinator
SELECT nodeid NOT IN (SELECT nodeid FROM pg_dist_node) AS "Is Coordinator", success, result FROM run_command_on_all_nodes('SELECT 1') ORDER BY 1;

-- check that when coordinator is not in metadata and run_command_on_all_nodes is called from
-- a worker node, command is not run on the coordinator
\c - - - :worker_1_port
SELECT nodeid NOT IN (SELECT nodeid FROM pg_dist_node) AS "Is Coordinator", success, result FROM run_command_on_all_nodes('SELECT 1') ORDER BY 1;

\c - - - :master_port

-- create a table
SELECT result FROM run_command_on_all_nodes('CREATE TABLE run_command_on_all_nodes.tbl (a INT)');

SELECT tablename FROM pg_tables WHERE schemaname = 'run_command_on_all_nodes';

\c - - - :worker_1_port
SELECT tablename FROM pg_tables WHERE schemaname = 'run_command_on_all_nodes';

\c - - - :master_port
SELECT result FROM run_command_on_all_nodes('SELECT tablename FROM pg_tables WHERE schemaname = ''run_command_on_all_nodes'';');

-- break a node and check messages
BEGIN;
SELECT nodeid AS worker_1_nodeid FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
UPDATE pg_dist_node SET nodeport = 0 WHERE nodeid = :worker_1_nodeid;

SELECT nodeid = :worker_1_nodeid AS "Is Worker 1", success, result FROM run_command_on_all_nodes('SELECT 1') ORDER BY 1;
SELECT nodeid = :worker_1_nodeid AS "Is Worker 1", success, result FROM run_command_on_all_nodes('SELECT 1', give_warning_for_connection_errors:=true) ORDER BY 1;

ROLLBACK;

-- break connection to localhost
BEGIN;
UPDATE pg_dist_node SET nodeport = 0 WHERE groupid = 0;

SELECT success, result
FROM run_command_on_coordinator('SELECT inet_server_port()') ORDER BY 1;

SELECT success, result
FROM run_command_on_coordinator('SELECT inet_server_port()', give_warning_for_connection_errors:=true) ORDER BY 1;

ROLLBACK;

-- we cannot use run_command_on_coordinator from workers if coordinator is not in the metadata
SELECT success, result FROM run_command_on_all_nodes($$select result from run_command_on_coordinator('select inet_server_port()')$$);

-- we can use run_command_on_coordinator from any node if the coordinator is in the metadata
SELECT citus_set_coordinator_host('localhost');
SELECT success, result FROM run_command_on_all_nodes($$select result from run_command_on_coordinator('select inet_server_port()')$$);
SELECT citus_remove_node(nodename, nodeport) FROM pg_dist_node WHERE groupid = 0;

-- check that we fail when pg_dist_node is empty
BEGIN;
DELETE FROM pg_dist_node;
SELECT success, result FROM run_command_on_coordinator('select inet_server_port()');
ROLLBACK;

DROP SCHEMA run_command_on_all_nodes CASCADE;
