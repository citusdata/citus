// This file is adapted from isolation_logical_replication_single_shard_commands.spec
// in order to separate testing of partitinoned and non-partitioned
// tables. We do that to enabled testing of logical replication
// on older postgres versions that do not support partitioning yet.
// When we drop support for PG 96 we should consider merging this
// file with isolation_logical_replication_single_shard_commands.spec
// We used 5 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{
	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;

	CREATE TABLE logical_replicate_partitioned(x int, y int, PRIMARY KEY (x,y) ) PARTITION BY RANGE(y);
	SELECT create_distributed_table('logical_replicate_partitioned', 'x');
	CREATE TABLE logical_replicate_partitioned_1 PARTITION OF logical_replicate_partitioned
		FOR VALUES FROM (0) TO (100);
	CREATE TABLE logical_replicate_partitioned_2 PARTITION OF logical_replicate_partitioned
		FOR VALUES FROM (100) TO (200);

	SELECT get_shard_id_for_distribution_column('logical_replicate_partitioned', 5) INTO selected_partitioned_shard;
	SELECT get_shard_id_for_distribution_column('logical_replicate_partitioned_1', 5) INTO selected_single_partition_shard;
}

teardown
{
	DROP TABLE selected_partitioned_shard;
	DROP TABLE selected_single_partition_shard;
	DROP TABLE logical_replicate_partitioned;
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-move-placement-partitioned"
{
    	SELECT master_move_shard_placement((SELECT * FROM selected_partitioned_shard), 'localhost', 57638, 'localhost', 57637);
}

step "s1-move-placement-single-partition"
{
    	SELECT master_move_shard_placement((SELECT * FROM selected_single_partition_shard), 'localhost', 57638, 'localhost', 57637);
}

step "s1-end"
{
	COMMIT;
}

session "s2"

step "s2-insert-partitioned"
{
    INSERT INTO logical_replicate_partitioned VALUES (5, 15);
}

step "s2-delete-partitioned"
{
    DELETE FROM logical_replicate_partitioned WHERE x = 5;
}

step "s2-update-partitioned"
{
    UPDATE logical_replicate_partitioned SET y = y + 1 WHERE x = 5;
}

step "s2-upsert-partitioned"
{
    INSERT INTO logical_replicate_partitioned VALUES (5, 15);

    INSERT INTO logical_replicate_partitioned VALUES (5, 15) ON CONFLICT (x, y) DO UPDATE SET y = logical_replicate_partitioned.y + 1;
}

step "s2-copy-partitioned"
{
	COPY logical_replicate_partitioned FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
}

step "s2-truncate-partitioned"
{
	TRUNCATE logical_replicate_partitioned;
}

step "s2-alter-table-partitioned"
{
	ALTER TABLE logical_replicate_partitioned ADD COLUMN z INT;
}


session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}


// the following tests show the non-blocking modifications while shard is being moved
// in fact, the shard move blocks the writes for a very short duration of time
// by using an advisory and allowing the other commands continue to run, we prevent
// the modifications to block on that blocking duration
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-partitioned" "s2-insert-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-partitioned" "s2-upsert-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-partitioned" "s2-update-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-partitioned" "s2-delete-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-partitioned" "s2-copy-partitioned" "s3-release-advisory-lock" "s1-end"


permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-single-partition" "s2-insert-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-single-partition" "s2-upsert-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-single-partition" "s2-update-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-single-partition" "s2-delete-partitioned" "s3-release-advisory-lock" "s1-end"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement-single-partition" "s2-copy-partitioned" "s3-release-advisory-lock" "s1-end"


// now show that DDLs and truncate are blocked by move placement
permutation "s1-begin" "s1-move-placement-partitioned" "s2-truncate-partitioned" "s1-end"
permutation "s1-begin" "s1-move-placement-partitioned" "s2-alter-table-partitioned" "s1-end"
permutation "s1-begin" "s2-truncate-partitioned" "s1-move-placement-partitioned"  "s1-end"
permutation "s1-begin" "s2-alter-table-partitioned" "s1-move-placement-partitioned"  "s1-end"

permutation "s1-begin" "s1-move-placement-single-partition" "s2-truncate-partitioned" "s1-end"
permutation "s1-begin" "s1-move-placement-single-partition" "s2-alter-table-partitioned" "s1-end"
permutation "s1-begin" "s2-truncate-partitioned" "s1-move-placement-single-partition"  "s1-end"
permutation "s1-begin" "s2-alter-table-partitioned" "s1-move-placement-single-partition"  "s1-end"


