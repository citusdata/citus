--
-- SHARD_MOVE_DEFERRED_DELETE
--

SET citus.next_shard_id TO 20000000;

SET citus.shard_count TO 6;
SET citus.shard_replication_factor TO 1;
SET citus.defer_drop_after_shard_move TO on;

CREATE SCHEMA shard_move_deferred_delete;
SET search_path TO shard_move_deferred_delete;

CREATE TABLE t1 ( id int PRIMARY KEY);
SELECT create_distributed_table('t1', 'id');

-- by counting how ofter we see the specific shard on all workers we can verify is the shard is there
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000001';
$cmd$);

-- move shard
SELECT master_move_shard_placement(20000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- we expect the shard to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- execute delayed removal
SELECT public.master_defer_delete_shards();

-- we expect the shard to be on only the second worker
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SELECT master_move_shard_placement(20000000, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- we expect the shard to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- enable auto delete
ALTER SYSTEM SET citus.defer_shard_delete_interval TO 10;
SELECT pg_reload_conf();

-- Sleep 1 second to give Valgrind enough time to clear transactions
SELECT pg_sleep(1);

-- we expect the shard to be on only the first worker
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- reset test suite
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

-- move shard
SELECT master_move_shard_placement(20000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- we expect shard 0 to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SET citus.force_disk_available = 20;

SELECT citus_shard_cost_by_disk_size(20000001);

-- When there's not enough space the move should fail
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);


BEGIN;
-- when we disable the setting, the move should not give "not enough space" error
set citus.check_available_space_before_move to false;
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);
ROLLBACK;


-- we expect shard 0 to be on only the second worker now, since
-- master_move_shard_placement will try to drop marked shards
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SET citus.force_disk_available = 8300;
SET citus.force_disk_size = 8500;

-- When there would not be enough free space left after the move, the move should fail
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);


DROP SCHEMA shard_move_deferred_delete CASCADE;
