setup
{
  	CREATE TABLE concurrent_table_1(id int PRIMARY KEY);
  	CREATE TABLE concurrent_table_2(id int PRIMARY KEY);
	CREATE TABLE concurrent_table_3(id int PRIMARY KEY);

	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('concurrent_table_1', 'id', colocate_with := 'none');
}

teardown
{
	DROP TABLE concurrent_table_1, concurrent_table_2, concurrent_table_3 CASCADE;
}

session "s1"


step "s1-move-shard-logical"
{
	WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'concurrent_table_1'::regclass ORDER BY shardid LIMIT 1)
	SELECT citus_move_Shard_placement(shardid.shardid, 'localhost', 57637, 'localhost', 57638) FROM shardid;
}

step "s1-move-shard-block"
{
	WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'concurrent_table_1'::regclass ORDER BY shardid LIMIT 1)
	SELECT citus_move_Shard_placement(shardid.shardid, 'localhost', 57637, 'localhost', 57638, 'block_writes') FROM shardid;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-create_distributed_table"
{
	SELECT create_distributed_table('concurrent_table_2', 'id', colocate_with := 'concurrent_table_1');
}

step "s2-commit"
{
	COMMIT;
}

session "s3"

step "s3-create_distributed_table"
{
	SELECT create_distributed_table('concurrent_table_3', 'id', colocate_with := 'concurrent_table_1');
}

step "s3-sanity-check"
{
	SELECT count(*) FROM pg_dist_shard LEFT JOIN pg_dist_shard_placement USING(shardid) WHERE nodename IS NULL;
}

step "s3-sanity-check-2"
{
	SELECT count(*) FROM concurrent_table_1 JOIN concurrent_table_2 USING (id);
}

//concurrent create_distributed_table with the same colocation should not block each other
permutation  "s2-begin" "s2-create_distributed_table" "s3-create_distributed_table" "s2-commit"

// concurrent create colocated table and shard move properly block each other, and cluster is healthy
permutation  "s2-begin" "s2-create_distributed_table" "s1-move-shard-logical" "s2-commit" "s3-sanity-check" "s3-sanity-check-2"
permutation  "s2-begin" "s2-create_distributed_table" "s1-move-shard-block" "s2-commit" "s3-sanity-check" "s3-sanity-check-2"

