setup
{
  	CREATE TABLE concurrent_table_1(id int PRIMARY KEY);
  	CREATE TABLE concurrent_table_2(id int PRIMARY KEY);
	CREATE TABLE concurrent_table_3(id int PRIMARY KEY);
	CREATE TABLE concurrent_table_4(id int PRIMARY KEY);
	CREATE TABLE concurrent_table_5(id int PRIMARY KEY);

	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('concurrent_table_1', 'id', colocate_with := 'none');
	SELECT create_distributed_table('concurrent_table_4', 'id');

	SELECT nodeid INTO first_node_id FROM pg_dist_node WHERE nodeport = 57637;
}

teardown
{
	DROP TABLE concurrent_table_1, concurrent_table_2, concurrent_table_3, concurrent_table_4, concurrent_table_5, first_node_id CASCADE;
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

step "s1-split-block"
{
	WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'concurrent_table_1'::regclass ORDER BY shardid LIMIT 1)
	SELECT citus_split_shard_by_split_points(
	shardid.shardid, ARRAY['2113265921'], ARRAY[(SELECT * FROM first_node_id), (SELECT * FROM first_node_id)], 'block_writes') FROM shardid;
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

step "s3-sanity-check-3"
{
	SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('concurrent_table_4', 'concurrent_table_5');
}

step "s3-sanity-check-4"
{
	SELECT count(*) FROM concurrent_table_4 JOIN concurrent_table_5 USING (id);
}


session "s4"

step "s4-begin"
{
	BEGIN;
}

step "s4-commit"
{
	commit;
}

step "s4-move-shard-logical"
{
	WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'concurrent_table_4'::regclass ORDER BY shardid LIMIT 1)
	SELECT citus_move_Shard_placement(shardid.shardid, 'localhost', 57637, 'localhost', 57638) FROM shardid;
}

step "s4-move-shard-block"
{
	WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'concurrent_table_4'::regclass ORDER BY shardid LIMIT 1)
	SELECT citus_move_Shard_placement(shardid.shardid, 'localhost', 57637, 'localhost', 57638, 'block_writes') FROM shardid;
}

session "s5"

step "s5-setup-rep-factor"
{
	SET citus.shard_replication_factor TO 1;
}

step "s5-create_implicit_colocated_distributed_table"
{
	SELECT create_distributed_table('concurrent_table_5', 'id');
}


//concurrent create_distributed_table with the same colocation should not block each other
permutation  "s2-begin" "s2-create_distributed_table" "s3-create_distributed_table" "s2-commit"

// concurrent create colocated table and shard move properly block each other, and cluster is healthy
permutation  "s2-begin" "s2-create_distributed_table" "s1-move-shard-logical" "s2-commit" "s3-sanity-check" "s3-sanity-check-2"
permutation  "s2-begin" "s2-create_distributed_table" "s1-move-shard-block" "s2-commit" "s3-sanity-check" "s3-sanity-check-2"
permutation  "s2-begin" "s2-create_distributed_table" "s1-split-block" "s2-commit" "s3-sanity-check" "s3-sanity-check-2"

// same test above, but this time implicitly colocated tables
permutation  "s4-begin" "s4-move-shard-logical" "s5-setup-rep-factor" "s5-create_implicit_colocated_distributed_table" "s4-commit" "s3-sanity-check" "s3-sanity-check-3" "s3-sanity-check-4"
permutation  "s4-begin" "s4-move-shard-block" "s5-setup-rep-factor" "s5-create_implicit_colocated_distributed_table" "s4-commit" "s3-sanity-check" "s3-sanity-check-3" "s3-sanity-check-4"

