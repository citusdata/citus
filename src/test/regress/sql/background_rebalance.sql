CREATE SCHEMA background_rebalance;
SET search_path TO background_rebalance;
SET citus.next_shard_id TO 85674000;
SET citus.shard_replication_factor TO 1;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

CREATE TABLE t1 (a int PRIMARY KEY);
SELECT create_distributed_table('t1', 'a', shard_count => 4, colocate_with => 'none');

-- verify the rebalance works - no-op - when the shards are balanced. Noop is shown by wait complaining there is nothing
-- to wait on.
SELECT 1 FROM citus_rebalance_start();
SELECT citus_rebalance_wait();

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

-- rebalance a table in the background
SELECT 1 FROM citus_rebalance_start();
SELECT citus_rebalance_wait();

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

CREATE TABLE t2 (a int);
SELECT create_distributed_table('t2', 'a' , colocate_with => 't1');

-- show that we get an error when a table in the colocation group can't be moved non-blocking
SELECT 1 FROM citus_rebalance_start();
SELECT 1 FROM citus_rebalance_start(shard_transfer_mode => 'block_writes');
SELECT citus_rebalance_wait();

DROP TABLE t2;

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

-- show we can stop a rebalance, the stop causes the move to not have happened, eg, our move back below fails.
SELECT 1 FROM citus_rebalance_start();
SELECT citus_rebalance_stop();
-- waiting on this rebalance is racy, as it sometimes sees no rebalance is ongoing while other times it actually sees it ongoing
-- we simply sleep a bit here
SELECT pg_sleep(1);

-- failing move due to a stopped rebalance, first clean orphans to make the error stable
SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
RESET client_min_messages;
SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');


-- show we can't start the rebalancer twice
SELECT 1 FROM citus_rebalance_start();
SELECT 1 FROM citus_rebalance_start();
SELECT citus_rebalance_wait();

-- show that the old rebalancer cannot be started with a background rebalance in progress
SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');
SELECT 1 FROM citus_rebalance_start();
SELECT rebalance_table_shards();
SELECT citus_rebalance_wait();

DROP TABLE t1;

-- make sure a non-super user can stop rebalancing
CREATE USER non_super_user_rebalance WITH LOGIN;
GRANT ALL ON SCHEMA background_rebalance TO non_super_user_rebalance;

SET ROLE non_super_user_rebalance;

CREATE TABLE non_super_user_t1 (a int PRIMARY KEY);
SELECT create_distributed_table('non_super_user_t1', 'a', shard_count => 4, colocate_with => 'none');
SELECT citus_move_shard_placement(85674008, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

SELECT 1 FROM citus_rebalance_start();
SELECT citus_rebalance_stop();

RESET ROLE;

CREATE TABLE ref_no_pk(a int);
SELECT create_reference_table('ref_no_pk');
CREATE TABLE ref_with_pk(a int primary key);
SELECT create_reference_table('ref_with_pk');
-- Add coordinator so there's a node which doesn't have the reference tables
SELECT 1 FROM citus_add_node('localhost', :master_port, groupId=>0);

-- fails
BEGIN;
SELECT 1 FROM citus_rebalance_start();
ROLLBACK;
-- success
BEGIN;
SELECT 1 FROM citus_rebalance_start(shard_transfer_mode := 'force_logical');
ROLLBACK;
-- success
BEGIN;
SELECT 1 FROM citus_rebalance_start(shard_transfer_mode := 'block_writes');
ROLLBACK;

-- fails
SELECT 1 FROM citus_rebalance_start();
-- succeeds
SELECT 1 FROM citus_rebalance_start(shard_transfer_mode := 'force_logical');
-- wait for success
SELECT citus_rebalance_wait();
SELECT state, details from citus_rebalance_status();

SELECT public.wait_for_resource_cleanup();

-- Remove coordinator again to allow rerunning of this test
SELECT 1 FROM citus_remove_node('localhost', :master_port);
SELECT public.wait_until_metadata_sync(30000);

-- make sure a non-super user can rebalance when there are reference tables to replicate
CREATE TABLE ref_table(a int primary key);
SELECT create_reference_table('ref_table');

-- add a new node to trigger replicate_reference_tables task
SELECT 1 FROM citus_add_node('localhost', :worker_3_port);

SET ROLE non_super_user_rebalance;
SELECT 1 FROM citus_rebalance_start(shard_transfer_mode := 'force_logical');

-- wait for success
SELECT citus_rebalance_wait();
SELECT state, details from citus_rebalance_status();

RESET ROLE;

-- reset the the number of nodes by removing the previously added node
SELECT 1 FROM citus_drain_node('localhost', :worker_3_port);
CALL citus_cleanup_orphaned_resources();
SELECT 1 FROM citus_remove_node('localhost', :worker_3_port);

SET client_min_messages TO WARNING;
DROP SCHEMA background_rebalance CASCADE;
DROP USER non_super_user_rebalance;
