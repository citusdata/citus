-- Test creation of mx tables and metadata syncing

SELECT nextval('pg_catalog.pg_dist_placement_placementid_seq') AS last_placement_id
\gset
SELECT nextval('pg_catalog.pg_dist_groupid_seq') AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') AS last_node_id \gset
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
SELECT nextval('pg_catalog.pg_dist_shardid_seq') AS last_shard_id \gset


SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 1;

\set VERBOSITY terse

-- Simulates a readonly node by setting default_transaction_read_only.
CREATE FUNCTION mark_node_readonly(hostname TEXT, port INTEGER, isreadonly BOOLEAN)
    RETURNS TEXT
    LANGUAGE sql
    AS $$
    SELECT master_run_on_worker(ARRAY[hostname], ARRAY[port],
           ARRAY['ALTER SYSTEM SET default_transaction_read_only TO ' || isreadonly::TEXT], false);
    SELECT result FROM
        master_run_on_worker(ARRAY[hostname], ARRAY[port],
                             ARRAY['SELECT pg_reload_conf()'], false);
$$;

CREATE OR REPLACE FUNCTION trigger_metadata_sync()
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

CREATE OR REPLACE FUNCTION raise_error_in_metadata_sync()
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

CREATE PROCEDURE wait_until_process_count(appname text, target_count int) AS $$
declare
   counter integer := -1;
begin
 while counter != target_count loop
  -- pg_stat_activity is cached at xact level and there is no easy way to clear it.
  -- Look it up in a new connection to get latest updates.
  SELECT result::int into counter FROM
   master_run_on_worker(ARRAY['localhost'], ARRAY[57636], ARRAY[
      'SELECT count(*) FROM pg_stat_activity WHERE application_name = ' || quote_literal(appname) || ';'], false);
  PERFORM pg_sleep(0.1);
 end loop;
end$$ LANGUAGE plpgsql;

-- add a node to the cluster
SELECT master_add_node('localhost', :worker_1_port) As nodeid_1 \gset
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- create couple of tables
CREATE TABLE ref_table(a int primary key);
SELECT create_reference_table('ref_table');

CREATE TABLE dist_table_1(a int primary key, b int references ref_table(a));
SELECT create_distributed_table('dist_table_1', 'a');

CREATE SEQUENCE sequence;
CREATE TABLE reference_table (a int default nextval('sequence'));
SELECT create_reference_table('reference_table');

-- update the node
SELECT 1 FROM master_update_node((SELECT nodeid FROM pg_dist_node),
                                 'localhost', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- start syncing metadata to the node
SELECT 1 FROM start_metadata_sync_to_node('localhost', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test that maintenance daemon syncs after master_update_node
--------------------------------------------------------------------------

-- Update the node again. We do this as epeatable read, so we just see the
-- changes by master_update_node(). This is to avoid inconsistent results
-- if the maintenance daemon does the metadata sync too fast.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- wait until maintenance daemon does the next metadata sync, and then
-- check if metadata is synced again
SELECT wait_until_metadata_sync(30000);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

SELECT verify_metadata('localhost', :worker_1_port);

-- Update the node to a non-existent node. This is to simulate updating to
-- a unwriteable node.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- maintenace daemon metadata sync should fail, because node is still unwriteable.
SELECT wait_until_metadata_sync(30000);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

-- verify that metadata sync daemon has started
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Metadata Sync Daemon';

--
-- terminate maintenance daemon, and verify that we don't spawn multiple
-- metadata sync daemons
--
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';
CALL wait_until_process_count('Citus Maintenance Daemon', 1);
select trigger_metadata_sync();
select wait_until_metadata_sync();
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Metadata Sync Daemon';

--
-- cancel metadata sync daemon, and verify that it exits and restarts.
--
select pid as pid_before_cancel from pg_stat_activity where application_name like 'Citus Met%' \gset
select pg_cancel_backend(pid) from pg_stat_activity where application_name = 'Citus Metadata Sync Daemon';
select wait_until_metadata_sync();
select pid as pid_after_cancel from pg_stat_activity where application_name like 'Citus Met%' \gset
select :pid_before_cancel != :pid_after_cancel AS metadata_sync_restarted;

--
-- cancel metadata sync daemon so it exits and restarts, but at the
-- same time tell maintenanced to trigger a new metadata sync. One
-- of these should exit to avoid multiple metadata syncs.
--
select pg_cancel_backend(pid) from pg_stat_activity where application_name = 'Citus Metadata Sync Daemon';
select trigger_metadata_sync();
select wait_until_metadata_sync();
-- we assume citus.metadata_sync_retry_interval is 500ms. Change amount we sleep to ceiling + 0.2 if it changes.
select pg_sleep(1.2);
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Metadata Sync Daemon';

--
-- error in metadata sync daemon, and verify it exits and restarts.
--
select pid as pid_before_error from pg_stat_activity where application_name like 'Citus Met%' \gset
select raise_error_in_metadata_sync();
select wait_until_metadata_sync(30000);
select pid as pid_after_error from pg_stat_activity where application_name like 'Citus Met%' \gset
select :pid_before_error != :pid_after_error AS metadata_sync_restarted;


SELECT trigger_metadata_sync();
SELECT wait_until_metadata_sync(30000);
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Metadata Sync Daemon';

-- update it back to :worker_1_port, now metadata should be synced
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT wait_until_metadata_sync(30000);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test updating a node when another node is in readonly-mode
--------------------------------------------------------------------------

-- first, add node and sync metadata in the same transaction
CREATE TYPE some_type AS (a int, b int);
CREATE TABLE some_ref_table (a int, b some_type);
SELECT create_reference_table('some_ref_table');
INSERT INTO some_ref_table (a) SELECT i FROM generate_series(0,10)i;

BEGIN;
	SELECT master_add_node('localhost', :worker_2_port) AS nodeid_2 \gset

  -- and modifications can be read from any worker in the same transaction
  INSERT INTO some_ref_table (a) SELECT i FROM generate_series(0,10)i;
  SET LOCAL citus.task_assignment_policy TO "round-robin";
  SELECT count(*) FROM some_ref_table;
  SELECT count(*) FROM some_ref_table;
COMMIT;

DROP TABLE some_ref_table;
DROP TYPE some_type;

-- Create a table with shards on both nodes
CREATE TABLE dist_table_2(a int);
SELECT create_distributed_table('dist_table_2', 'a');
INSERT INTO dist_table_2 SELECT i FROM generate_series(1, 100) i;

SELECT mark_node_readonly('localhost', :worker_2_port, TRUE);

-- Now updating the other node will mark worker 2 as not synced.
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;
COMMIT;

-- worker_2 is out of sync, so further updates aren't sent to it and
-- we shouldn't see the warnings.
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 23456);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

-- Make the node writeable.
SELECT mark_node_readonly('localhost', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync(30000);

-- Mark the node readonly again, so the following master_update_node warns
SELECT mark_node_readonly('localhost', :worker_2_port, TRUE);

-- Revert the nodeport of worker 1.
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT count(*) FROM dist_table_2;
END;

SELECT wait_until_metadata_sync(30000);

-- Make the node writeable.
SELECT mark_node_readonly('localhost', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync(30000);

SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

--------------------------------------------------------------------------
-- Test that master_update_node rolls back properly
--------------------------------------------------------------------------
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
ROLLBACK;

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

--------------------------------------------------------------------------
-- Test that master_update_node invalidates the plan cache
--------------------------------------------------------------------------

PREPARE foo AS SELECT COUNT(*) FROM dist_table_1 WHERE a = 1;

SET citus.log_remote_commands = ON;
-- trigger caching for prepared statements
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;

SELECT master_update_node(:nodeid_1, '127.0.0.1', :worker_1_port);
SELECT wait_until_metadata_sync(30000);

-- make sure the nodename changed.
EXECUTE foo;

SET citus.log_remote_commands TO OFF;

--------------------------------------------------------------------------
-- Test that master_update_node can appear in a prepared transaction.
--------------------------------------------------------------------------
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
PREPARE TRANSACTION 'tx01';
COMMIT PREPARED 'tx01';

SELECT wait_until_metadata_sync(30000);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
PREPARE TRANSACTION 'tx01';
COMMIT PREPARED 'tx01';

SELECT wait_until_metadata_sync(30000);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

--------------------------------------------------------------------------
-- Test that changes in isactive is propagated to the metadata nodes
--------------------------------------------------------------------------
-- Don't drop the reference table so it has shards on the nodes being disabled
DROP TABLE dist_table_1, dist_table_2;

SELECT pg_catalog.citus_disable_node('localhost', :worker_2_port);
SELECT wait_until_metadata_sync(30000);

-- show that node metadata is the same
-- note that we cannot use verify_metadata here
-- because there are several shards/placements
-- in the metadata that are manually modified on the coordinator
-- not on the worker, and pg_catalog.citus_disable_node does
-- not sync the metadata
SELECT result FROM run_command_on_workers($$SELECT jsonb_agg(row_to_json(row(pg_dist_node.*))) FROM pg_dist_node$$) WHERE nodeport=:worker_1_port
EXCEPT
SELECT jsonb_agg(row_to_json(row(pg_dist_node.*)))::text FROM pg_dist_node;

SELECT 1 FROM master_activate_node('localhost', :worker_2_port);
SELECT verify_metadata('localhost', :worker_1_port);

------------------------------------------------------------------------------------
-- Test citus_disable_node_and_wait() when the node that is being disabled is actually down
------------------------------------------------------------------------------------
SELECT master_update_node(:nodeid_2, 'localhost', 1);
SELECT wait_until_metadata_sync(30000);

-- set metadatasynced so we try porpagating metadata changes
UPDATE pg_dist_node SET metadatasynced = TRUE WHERE nodeid IN (:nodeid_1, :nodeid_2);

-- should not error out, citus_disable_node is tolerant for node failures
-- but we should not wait metadata syncing to finish as this node is down
SELECT 1 FROM citus_disable_node('localhost', 1, true);

SELECT verify_metadata('localhost', :worker_1_port);

SELECT master_update_node(:nodeid_2, 'localhost', :worker_2_port);
SELECT wait_until_metadata_sync(30000);

SELECT 1 FROM master_activate_node('localhost', :worker_2_port);
SELECT verify_metadata('localhost', :worker_1_port);

------------------------------------------------------------------------------------
-- Test citus_disable_node_and_wait() when the other node is down
------------------------------------------------------------------------------------
-- node 1 is down.
SELECT master_update_node(:nodeid_1, 'localhost', 1);
SELECT wait_until_metadata_sync(30000);

-- set metadatasynced so we try porpagating metadata changes
UPDATE pg_dist_node SET metadatasynced = TRUE WHERE nodeid IN (:nodeid_1, :nodeid_2);

-- should not error out, citus_disable_node is tolerant for node failures
-- but we should not wait metadata syncing to finish as this node is down
SELECT 1 FROM citus_disable_node('localhost', :worker_2_port);

-- try again after stopping metadata sync
SELECT stop_metadata_sync_to_node('localhost', 1);
SELECT 1 FROM citus_disable_node_and_wait('localhost', :worker_2_port);

-- bring up node 1
SELECT master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT wait_until_metadata_sync(30000);

SELECT 1 FROM master_activate_node('localhost', :worker_2_port);

SELECT verify_metadata('localhost', :worker_1_port);

-- verify that metadata sync daemon exits
call wait_until_process_count('Citus Metadata Sync Daemon', 0);

-- verify that DROP DATABASE terminates metadata sync
SELECT current_database() datname \gset
CREATE DATABASE db_to_drop;
SELECT run_command_on_workers('CREATE DATABASE db_to_drop');

\c db_to_drop - - :worker_1_port
CREATE EXTENSION citus;

\c db_to_drop - - :master_port
CREATE EXTENSION citus;
SELECT master_add_node('localhost', :worker_1_port);
UPDATE pg_dist_node SET hasmetadata = true;

SELECT master_update_node(nodeid, 'localhost', 12345) FROM pg_dist_node;

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION trigger_metadata_sync()
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';
RESET citus.enable_metadata_sync;
SELECT trigger_metadata_sync();

\c :datname - - :master_port

SELECT datname FROM pg_stat_activity WHERE application_name LIKE 'Citus Met%';

DROP DATABASE db_to_drop;

SELECT datname FROM pg_stat_activity WHERE application_name LIKE 'Citus Met%';

-- cleanup
DROP SEQUENCE sequence CASCADE;
DROP TABLE ref_table;
DROP TABLE reference_table;
TRUNCATE pg_dist_colocation;
SELECT run_command_on_workers('TRUNCATE pg_dist_colocation');
SELECT count(*) FROM (SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node) t;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART :last_placement_id;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART :last_shard_id;

RESET citus.shard_count;
RESET citus.shard_replication_factor;
