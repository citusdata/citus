CREATE SCHEMA logical_rep_consistency;
SET search_path TO logical_rep_consistency;
SET citus.next_shard_id TO 1990000;

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

-- make sure that we are on the default state
SELECT run_command_on_workers($$alter system SET citus.show_shards_for_app_name_prefixes TO '';$$);
SELECT run_command_on_workers($$SELECT pg_reload_conf();$$);

CREATE TABLE test(a int primary key);
SELECT create_distributed_table('test', 'a', colocate_with:='none');

INSERT INTO test SELECT i FROM generate_Series(0,100)i;
SELECT count(*) FROM test;

-- use both APIs and shard_transfer_modes
SELECT citus_move_shard_placement(1990000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
SELECT rebalance_table_shards('test', shard_transfer_mode:='force_logical');
SELECT citus_move_shard_placement(1990000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='block_writes');

SELECT count(*) FROM test;

SET client_min_messages TO ERROR;
DROP SCHEMA logical_rep_consistency CASCADE;
