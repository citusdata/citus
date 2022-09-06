// we use 5 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{
	SET citus.shard_count TO 2;
	SET citus.shard_replication_factor TO 2;
	CREATE TABLE test_hash_table (x int, y int);
	SELECT create_distributed_table('test_hash_table', 'x');

	SELECT get_shard_id_for_distribution_column('test_hash_table', 5) INTO selected_shard_for_test_table;
}

teardown
{
	DROP TABLE test_hash_table;
	DROP TABLE selected_shard_for_test_table;
}

session "s1"

// since test_hash_table has rep > 1 simple select query doesn't hit all placements
// hence not all placements are cached
// but with copy all placements are cached
step "s1-load-cache"
{
	COPY test_hash_table FROM PROGRAM 'echo 1,1 && echo 2,2 && echo 3,3 && echo 4,4 && echo 5,5' WITH CSV;
}

step "s1-repair-placement"
{
	SELECT citus_copy_shard_placement((SELECT * FROM selected_shard_for_test_table), 'localhost', 57637, 'localhost', 57638);
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-delete-inactive"
{
	DELETE FROM pg_dist_shard_placement WHERE shardid IN (SELECT * FROM selected_shard_for_test_table) AND nodeport = 57638;
}

step "s2-repair-placement"
{
	SELECT citus_copy_shard_placement((SELECT * FROM selected_shard_for_test_table), 'localhost', 57637, 'localhost', 57638, transfer_mode := 'block_writes');
}

// since test_hash_table has rep > 1 simple select query doesn't hit all placements
// hence not all placements are cached
// but with copy all placements are cached
step "s2-load-cache"
{
	COPY test_hash_table FROM PROGRAM 'echo 1,1 && echo 2,2 && echo 3,3 && echo 4,4 && echo 5,5' WITH CSV;
}

step "s2-commit"
{
	COMMIT;
}

// two concurrent shard repairs on the same shard
// note that "s1-repair-placement" errors out but that is expected
// given that "s2-repair-placement" succeeds and the placement is
// already repaired
permutation "s1-load-cache" "s2-load-cache" "s2-delete-inactive" "s2-begin" "s2-repair-placement" "s1-repair-placement" "s2-commit"

// the same test without the load caches
permutation "s2-delete-inactive" "s2-begin" "s2-repair-placement" "s1-repair-placement" "s2-commit"
