setup
{
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1500001;
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
	SELECT 1 FROM master_add_node('localhost', 57637);

	CREATE OR REPLACE PROCEDURE isolation_cleanup_orphaned_resources()
		LANGUAGE C
		AS 'citus', $$isolation_cleanup_orphaned_resources$$;
	COMMENT ON PROCEDURE isolation_cleanup_orphaned_resources()
		IS 'cleanup orphaned shards';
		RESET citus.enable_metadata_sync;
	CALL isolation_cleanup_orphaned_resources();

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
	DROP TABLE colocated2;
	DROP TABLE colocated1;
	DROP TABLE separate;
}

session "s1"

step "s1-rebalance-c1-block-writes"
{
	SELECT rebalance_table_shards('colocated1', shard_transfer_mode:='block_writes');
}

step "s1-rebalance-c1-online"
{
	SELECT rebalance_table_shards('colocated1', shard_transfer_mode:='force_logical');
}

step "s1-shard-move-c1-block-writes"
{
	SELECT citus_move_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes');
}

step "s1-shard-copy-c1-block-writes"
{
	UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid IN ('colocated1', 'colocated2');
	SELECT citus_copy_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, transfer_mode:='block_writes');
}

step "s1-shard-move-c1-online"
{
	SELECT citus_move_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='force_logical');
}

step "s1-shard-copy-c1-online"
{
	UPDATE pg_dist_partition SET repmodel = 'c' WHERE logicalrelid IN ('colocated1', 'colocated2');
	SELECT citus_copy_shard_placement(1500001, 'localhost', 57637, 'localhost', 57638, transfer_mode:='force_logical');
}

step "s1-wait" {}

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
	SELECT citus_move_shard_placement(1500009, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes');
}

step "s4-wait" {}

session "s5"

// this advisory lock with (almost) random values are only used
// is taken before any data is copied. For details check the source
// code of ConflictWithIsolationTestingBeforeCopy
step "s5-acquire-advisory-lock-before-copy"
{
    SELECT pg_advisory_lock(55152, 44000);
}

step "s5-release-advisory-lock"
{
    SELECT pg_advisory_unlock(55152, 44000);
}


session "s6"

// this advisory lock with (almost) random values are only used
// is taken after all data is copied. For details check the source
// code of ConflictWithIsolationTestingAfterCopy
step "s6-acquire-advisory-lock-after-copy"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s6-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}


session "s7"

step "s7-get-progress"
{
	set LOCAL client_min_messages=NOTICE;
	WITH possible_sizes(size) as (VALUES (0), (8000), (40000), (200000), (480000))
	SELECT
		table_name,
		shardid,
		( SELECT size FROM possible_sizes WHERE ABS(size - shard_size) = (SELECT MIN(ABS(size - shard_size)) FROM possible_sizes )) shard_size,
		sourcename,
		sourceport,
		( SELECT size FROM possible_sizes WHERE ABS(size - source_shard_size) = (SELECT MIN(ABS(size - source_shard_size)) FROM possible_sizes )) source_shard_size,
		targetname,
		targetport,
		( SELECT size FROM possible_sizes WHERE ABS(size - target_shard_size) = (SELECT MIN(ABS(size - target_shard_size)) FROM possible_sizes )) target_shard_size,
		progress,
		operation_type,
		target_lsn IS NULL OR source_lsn >= target_lsn AS lsn_sanity_check,
		source_lsn IS NOT NULL AS source_lsn_available,
		target_lsn IS NOT NULL AS target_lsn_available,
		status
	FROM get_rebalance_progress();
}

// When getting progress from multiple monitors at the same time it can result
// in random order of the tuples, because there's no defined order of the
// monitors. So in those cases we need to order the output for consistent results.
step "s7-get-progress-ordered"
{
	set LOCAL client_min_messages=NOTICE;
	WITH possible_sizes(size) as (VALUES (0), (8000), (40000), (200000), (480000))
	SELECT
		table_name,
		shardid,
		( SELECT size FROM possible_sizes WHERE ABS(size - shard_size) = (SELECT MIN(ABS(size - shard_size)) FROM possible_sizes )) shard_size,
		sourcename,
		sourceport,
		( SELECT size FROM possible_sizes WHERE ABS(size - source_shard_size) = (SELECT MIN(ABS(size - source_shard_size)) FROM possible_sizes )) source_shard_size,
		targetname,
		targetport,
		( SELECT size FROM possible_sizes WHERE ABS(size - target_shard_size) = (SELECT MIN(ABS(size - target_shard_size)) FROM possible_sizes )) target_shard_size,
		progress,
		operation_type,
		target_lsn IS NULL OR source_lsn >= target_lsn AS lsn_sanity_check,
		source_lsn IS NOT NULL AS source_lsn_available,
		target_lsn IS NOT NULL AS target_lsn_available
	FROM get_rebalance_progress()
	ORDER BY 1, 2, 3, 4, 5;
}

// blocking rebalancer does what it should
permutation "s5-acquire-advisory-lock-before-copy" "s1-rebalance-c1-block-writes" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s3-lock-2-start" "s1-rebalance-c1-block-writes" "s7-get-progress" "s3-unlock-2-start" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-rebalance-c1-block-writes" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// online rebalancer
permutation "s5-acquire-advisory-lock-before-copy" "s1-rebalance-c1-online" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-rebalance-c1-online" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// blocking shard move
permutation "s5-acquire-advisory-lock-before-copy" "s1-shard-move-c1-block-writes" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-shard-move-c1-block-writes" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// blocking shard copy
permutation "s5-acquire-advisory-lock-before-copy" "s1-shard-copy-c1-block-writes" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-shard-copy-c1-block-writes" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// online shard move
permutation "s5-acquire-advisory-lock-before-copy" "s1-shard-move-c1-online" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-shard-move-c1-online" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// online shard copy
permutation "s5-acquire-advisory-lock-before-copy" "s1-shard-copy-c1-online" "s7-get-progress" "s5-release-advisory-lock" "s1-wait" "s7-get-progress"
permutation "s6-acquire-advisory-lock-after-copy" "s1-shard-copy-c1-online" "s7-get-progress" "s6-release-advisory-lock" "s1-wait" "s7-get-progress"

// parallel blocking shard move
permutation "s5-acquire-advisory-lock-before-copy" "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes"("s1-shard-move-c1-block-writes") "s7-get-progress-ordered" "s5-release-advisory-lock" "s1-wait" "s4-wait" "s7-get-progress-ordered"
permutation "s6-acquire-advisory-lock-after-copy" "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes"("s1-shard-move-c1-block-writes") "s7-get-progress-ordered" "s6-release-advisory-lock"  "s1-wait" "s4-wait" "s7-get-progress-ordered"
