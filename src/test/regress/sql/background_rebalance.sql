CREATE SCHEMA background_rebalance;
SET search_path TO background_rebalance;
SET citus.next_shard_id TO 85674000;
SET citus.shard_replication_factor TO 1;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

CREATE TABLE t1 (a int PRIMARY KEY);
SELECT create_distributed_table('t1', 'a', shard_count => 4, colocate_with => 'none');

-- verify the rebalance works - no-op - when the shards aer balanced. Noop is shown by wait complaining there is nothing
-- to wait on.
SELECT citus_rebalance_start();
SELECT citus_rebalance_wait();

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

-- rebalance a table in the background
SELECT citus_rebalance_start();
SELECT citus_rebalance_wait();

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

CREATE TABLE t2 (a int);
SELECT create_distributed_table('t2', 'a' , colocate_with => 't1');

-- show that we get an error when a table in the colocation group can't be moved non-blocking
SELECT citus_rebalance_start();
SELECT citus_rebalance_start(shard_transfer_mode => 'block_writes');
SELECT citus_rebalance_wait();

DROP TABLE t2;

SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');

-- show we can stop a rebalance, the stop causes the move to not have happened, eg, our move back below fails.
SELECT citus_rebalance_start();
SELECT citus_rebalance_stop();
-- waiting on this rebalance is racy, as it sometimes sees no rebalance is ongoing while other times it actually sees it ongoing
-- we simply sleep a bit here
SELECT pg_sleep(1);

-- failing move due to a stopped rebalance, first clean orphans to make the error stable
SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_shards();
RESET client_min_messages;
SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');


-- show we can't start the rebalancer twice
SELECT citus_rebalance_start();
SELECT citus_rebalance_start();
SELECT citus_rebalance_wait();

-- show that the old rebalancer cannot be started with a background rebalance in progress
SELECT citus_move_shard_placement(85674000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode => 'block_writes');
SELECT citus_rebalance_start();
SELECT rebalance_table_shards();
SELECT citus_rebalance_wait();


SET client_min_messages TO WARNING;
DROP SCHEMA background_rebalance CASCADE;