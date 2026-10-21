-- A removed worker must not recover transactions belonging to its old coordinator.
SELECT current_database() AS original_database \gset
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

SET citus.enable_create_database_propagation TO off;
CREATE DATABASE node_removal_recovery;
\c - - - :worker_1_port
SET citus.enable_create_database_propagation TO off;
CREATE DATABASE node_removal_recovery;
\c - - - :worker_2_port
SET citus.enable_create_database_propagation TO off;
CREATE DATABASE node_removal_recovery;

\c node_removal_recovery - - :worker_1_port
CREATE EXTENSION citus;
\c node_removal_recovery - - :worker_2_port
CREATE EXTENSION citus;
\c node_removal_recovery - - :master_port
CREATE EXTENSION citus;
SELECT citus_set_coordinator_host('localhost', :master_port);
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);
SELECT groupid AS worker_1_group_id FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
SELECT groupid AS worker_2_group_id FROM pg_dist_node WHERE nodeport = :worker_2_port \gset

\c - - - :worker_1_port
BEGIN;
CREATE TABLE should_commit_after_node_removal(value int);
INSERT INTO should_commit_after_node_removal VALUES (42);
PREPARE TRANSACTION 'citus_0_node_removal';

\c - - - :worker_2_port
CREATE TABLE retained_local_data(value int);
INSERT INTO retained_local_data VALUES (42);

\c - - - :master_port
INSERT INTO pg_dist_transaction(groupid, gid)
VALUES (:worker_1_group_id, 'citus_0_node_removal');
SELECT citus_remove_node('localhost', :worker_2_port);

\c - - - :worker_2_port
SELECT groupid FROM pg_dist_local_group;
SELECT count(*) FROM pg_dist_node;
SELECT recover_prepared_transactions();
SELECT * FROM retained_local_data;

\c - - - :worker_1_port
SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'citus_0_node_removal';

\c - - - :master_port
SELECT count(*) FROM pg_dist_transaction WHERE gid = 'citus_0_node_removal';
SELECT recover_prepared_transactions();

\c - - - :worker_1_port
SELECT * FROM should_commit_after_node_removal;

\c - - - :master_port
SELECT 1 FROM citus_add_node('localhost', :worker_2_port, groupid => :worker_2_group_id);
\c - - - :worker_2_port
SELECT groupid = :worker_2_group_id AS identity_restored FROM pg_dist_local_group;
SELECT count(*) = 3 AS routes_restored FROM pg_dist_node;
SELECT * FROM retained_local_data;

-- Removing the coordinator's placement entry must preserve its worker routes.
\c - - - :master_port
SELECT citus_remove_node('localhost', :master_port);
SELECT count(*) = 2 AS worker_routes_preserved FROM pg_dist_node;

-- An entry pointing at the coordinator with a non-zero group must also keep its worker list.
SELECT 1 FROM citus_add_inactive_node('localhost', :master_port);
SELECT citus_remove_node('localhost', :master_port);
SELECT count(*) = 2 AS worker_routes_preserved FROM pg_dist_node;

\c :original_database - - :worker_1_port
SET citus.enable_create_database_propagation TO off;
DROP DATABASE node_removal_recovery WITH (FORCE);
\c :original_database - - :worker_2_port
SET citus.enable_create_database_propagation TO off;
DROP DATABASE node_removal_recovery WITH (FORCE);
\c :original_database - - :master_port
SET citus.enable_create_database_propagation TO off;
DROP DATABASE node_removal_recovery WITH (FORCE);
ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();
