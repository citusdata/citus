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

DROP SCHEMA shard_move_deferred_delete CASCADE;
