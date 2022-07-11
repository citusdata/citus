setup
{
	select setval('pg_dist_shardid_seq', GREATEST(1500000, nextval('pg_dist_shardid_seq')));
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT master_set_node_property('localhost', 57638, 'shouldhaveshards', false);
	CREATE TABLE colocated1 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated1', 'test_id', 'hash', 'none');
	CREATE TABLE colocated2 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated2', 'test_id', 'hash', 'colocated1');
	-- 1 and 3 are chosen so they go to shard 1 and 2
	INSERT INTO colocated1(test_id) SELECT 1 from generate_series(0, 1000) i;
	INSERT INTO colocated2(test_id) SELECT 1 from generate_series(0, 10000) i;
	INSERT INTO colocated1(test_id) SELECT 3 from generate_series(0, 5000) i;
	select * from pg_dist_placement;
	SELECT master_set_node_property('localhost', 57638, 'shouldhaveshards', true);
}

teardown
{
	DROP TABLE colocated2;
	DROP TABLE colocated1;
}

session "s1"

step "s1-rebalance-c1"
{
	BEGIN;
	SELECT * FROM get_rebalance_table_shards_plan('colocated1');
	SELECT rebalance_table_shards('colocated1', shard_transfer_mode:='block_writes');
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-lock-1"
{
	SELECT pg_advisory_lock(29279, 1);
}

step "s2-lock-2"
{
	SELECT pg_advisory_lock(29279, 2);
}

step "s2-unlock-1"
{
	SELECT pg_advisory_unlock(29279, 1);
}

step "s2-unlock-2"
{
	SELECT pg_advisory_unlock(29279, 2);
}

session "s3"

step "s3-progress"
{
	set client_min_messages=NOTICE;
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

permutation "s2-lock-1" "s2-lock-2" "s1-rebalance-c1" "s3-progress" "s2-unlock-1" "s3-progress" "s2-unlock-2" "s3-progress" "s1-commit" "s3-progress"
