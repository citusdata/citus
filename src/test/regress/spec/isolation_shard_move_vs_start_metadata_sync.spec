setup
{
	SET citus.shard_replication_factor TO 1;
	SET citus.shard_count TO 4;

	CREATE TABLE dist_table (id int, value int);
	SELECT create_distributed_table('dist_table', 'id');

	SELECT shardid INTO selected_shard FROM pg_dist_shard WHERE logicalrelid='dist_table'::regclass LIMIT 1;

}

teardown
{
	DROP TABLE dist_table;
	DROP TABLE selected_shard;
}

session "s1"

step "s1-count-shards-by-worker"
{
	SELECT COUNT(*) FROM pg_dist_placement WHERE groupid=1 AND shardstate != 4;
	SELECT * FROM run_command_on_workers('SELECT COUNT(*) FROM pg_dist_placement WHERE groupid=1') ORDER BY 1, 2, 3, 4;
	SELECT COUNT(*) FROM pg_dist_placement WHERE groupid=2 AND shardstate != 4;
	SELECT * FROM run_command_on_workers('SELECT COUNT(*) FROM pg_dist_placement WHERE groupid=2') ORDER BY 1, 2, 3, 4;
}

step "s1-move-shard-force-logical"
{
	SELECT * FROM master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638, 'force_logical');
}

step "s1-move-shard-block-writes"
{
	SELECT * FROM master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638, 'block_writes');
}

step "s1-begin"
{
	BEGIN;
}

step "s1-commit"
{
	COMMIT;
}


session "s2"

step "s2-start-metadata-sync"
{
	SELECT * FROM start_metadata_sync_to_node('localhost', 57637);
}

// It is expected to observe different number of placements on coordinator and worker
// since we call that spec while mx is off.
permutation "s1-begin" "s1-move-shard-force-logical" "s2-start-metadata-sync" "s1-commit" "s1-count-shards-by-worker"
permutation "s1-begin" "s1-move-shard-block-writes" "s2-start-metadata-sync" "s1-commit" "s1-count-shards-by-worker"
