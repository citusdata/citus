setup
{
	-- We disable deffered drop, so we can easily trigger blocking the shard
	-- move at the end of the move. This is done in a separate setup step,
	-- because this cannot run in a transaction.
	ALTER SYSTEM SET citus.defer_drop_after_shard_move TO OFF;
}
setup
{
	SELECT pg_reload_conf();
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1500001;
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT master_set_node_property('localhost', 57638, 'shouldhaveshards', false);
	CREATE TABLE colocated1 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated1', 'test_id', 'hash', 'none');
	CREATE TABLE colocated2 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated2', 'test_id', 'hash', 'colocated1');
	CREATE TABLE separate (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('separate', 'test_id', 'hash', 'none');
	-- 1 and 3 are chosen so they go to shard 1 and 2
	INSERT INTO colocated1(test_id) SELECT 1 from generate_series(0, 1000) i;
	INSERT INTO colocated2(test_id) SELECT 1 from generate_series(0, 10000) i;
	INSERT INTO colocated1(test_id) SELECT 3 from generate_series(0, 5000) i;
	INSERT INTO separate(test_id) SELECT 1 from generate_series(0, 3000) i;
	select * from pg_dist_placement;
	SELECT master_set_node_property('localhost', 57638, 'shouldhaveshards', true);
}

teardown
{
	SELECT pg_reload_conf();
	DROP TABLE colocated2;
	DROP TABLE colocated1;
	DROP TABLE separate;
}

session "s1"

step "s1-rebalance-c1-block-writes"
{
	BEGIN;
	SELECT * FROM get_rebalance_table_shards_plan('colocated1');
	SELECT rebalance_table_shards('colocated1', shard_transfer_mode:='block_writes');
}

step "s1-rebalance-c1-online"
{
	BEGIN;
	SELECT * FROM get_rebalance_table_shards_plan('colocated1');
	SELECT rebalance_table_shards('colocated1', shard_transfer_mode:='force_logical');
}

step "s1-shard-move-c1-block-writes"
{
	BEGIN;
	SELECT citus_move_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes');
}

step "s1-shard-move-c1-online"
{
	BEGIN;
	SELECT citus_move_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='force_logical');
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-lock-1-start"
{
	BEGIN;
	DELETE FROM colocated1 WHERE test_id = 1;
	DELETE FROM separate WHERE test_id = 1;
}

step "s2-unlock-1-start"
{
	ROLLBACK;
}

session "s3"

step "s3-lock-2-start"
{
	BEGIN;
	DELETE FROM colocated1 WHERE test_id = 3;
}

step "s3-unlock-2-start"
{
	ROLLBACK;
}

session "s4"

step "s4-shard-move-sep-block-writes"
{
	BEGIN;
	SELECT citus_move_shard_placement(1500009, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes');
}

step "s4-commit"
{
	COMMIT;
}

session "s6"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s6-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s6-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}


session "s7"

// get_rebalance_progress internally calls pg_total_relation_size on all the
// shards. This means that it takes AccessShareLock on those shards. Because we
// run with deferred drop that means that get_rebalance_progress actually waits
// for the shard move to complete the drop. But we want to get the progress
// before the shards are dropped. So we grab the locks first with a simple
// query that reads from all shards. We force using a single connection because
// get_rebalance_progress isn't smart enough to reuse the right connection for
// the right shards and will simply use a single one for all of them.
step "s7-grab-lock"
{
	BEGIN;
	SET LOCAL citus.max_adaptive_executor_pool_size = 1;
	SELECT 1 FROM colocated1 LIMIT 1;
	SELECT 1 FROM separate LIMIT 1;
}

step "s7-get-progress"
{
	set LOCAL client_min_messages=NOTICE;
	SELECT
		table_name,
		shardid,
		shard_size,
		sourcename,
		sourceport,
		source_shard_size,
		targetname,
		targetport,
		target_shard_size,
		progress
	FROM get_rebalance_progress();
}

step "s7-release-lock"
{
	COMMIT;
}

session "s8"

// After running these tests we want to enable deferred-drop again. Sadly
// the isolation tester framework does not support multiple teardown steps
// and this cannot be run in a transaction. So we need to do it manually at
// the end of the last test.
step "enable-deferred-drop"
{
	ALTER SYSTEM RESET citus.defer_drop_after_shard_move;
}
// blocking rebalancer does what it should
permutation "s2-lock-1-start" "s1-rebalance-c1-block-writes" "s7-get-progress" "s2-unlock-1-start" "s1-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s3-lock-2-start" "s1-rebalance-c1-block-writes" "s7-get-progress" "s3-unlock-2-start" "s1-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s7-grab-lock" "s1-rebalance-c1-block-writes" "s7-get-progress" "s7-release-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"

// online rebalancer
permutation "s6-acquire-advisory-lock" "s1-rebalance-c1-online" "s7-get-progress" "s6-release-advisory-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s7-grab-lock" "s1-shard-move-c1-online" "s7-get-progress" "s7-release-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"

// blocking shard move
permutation "s2-lock-1-start" "s1-shard-move-c1-block-writes" "s7-get-progress" "s2-unlock-1-start" "s1-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s7-grab-lock" "s1-shard-move-c1-block-writes" "s7-get-progress" "s7-release-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"

// online shard move
permutation "s6-acquire-advisory-lock" "s1-shard-move-c1-online" "s7-get-progress" "s6-release-advisory-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s7-grab-lock" "s1-shard-move-c1-online" "s7-get-progress" "s7-release-lock" "s1-commit" "s7-get-progress" "enable-deferred-drop"


// parallel blocking shard move
permutation "s2-lock-1-start" "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes" "s7-get-progress" "s2-unlock-1-start" "s1-commit" "s4-commit" "s7-get-progress" "enable-deferred-drop"
permutation "s7-grab-lock" "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes" "s7-get-progress" "s7-release-lock" "s1-commit" "s4-commit" "s7-get-progress" "enable-deferred-drop"
