setup
{
	-- make sure coordinator is in metadata
	SELECT citus_set_coordinator_host('localhost', 57636);
  	CREATE TABLE table_to_distribute(id int PRIMARY KEY);
  	CREATE TABLE table_to_distribute2(id smallint PRIMARY KEY);
}

teardown
{
	DROP TABLE table_to_distribute CASCADE;
	DROP TABLE table_to_distribute2 CASCADE;
	SELECT citus_remove_node('localhost', 57636);
}

session "s1"

step "s1-create_distributed_table_concurrently"
{
	SELECT create_distributed_table_concurrently('table_to_distribute', 'id');
}

step "s1-settings"
{
	-- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
}

step "s1-truncate"
{
	TRUNCATE table_to_distribute;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-settings"
{
	-- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
}

step "s2-insert"
{
	INSERT INTO table_to_distribute SELECT s FROM generate_series(1,20) s;
}

step "s2-update"
{
	UPDATE table_to_distribute SET id = 21 WHERE id = 20;
}

step "s2-delete"
{
	DELETE FROM table_to_distribute WHERE id = 11;
}

step "s2-copy"
{
	COPY table_to_distribute FROM PROGRAM 'echo 30 && echo 31 && echo 32 && echo 33 && echo 34 && echo 35 && echo 36 && echo 37 && echo 38';
}

step "s2-reindex"
{
	REINDEX TABLE table_to_distribute;
}

step "s2-reindex-concurrently"
{
	REINDEX TABLE CONCURRENTLY table_to_distribute;
}

step "s2-create_distributed_table_concurrently-same"
{
	SELECT create_distributed_table_concurrently('table_to_distribute', 'id');
}

step "s2-create_distributed_table-same"
{
	SELECT create_distributed_table('table_to_distribute', 'id');
}

step "s2-create_distributed_table_concurrently-no-same"
{
	SELECT create_distributed_table_concurrently('table_to_distribute2', 'id');
}

step "s2-create_distributed_table-no-same"
{
	SELECT create_distributed_table('table_to_distribute2', 'id');
}

step "s2-print-status"
{
	-- sanity check on partitions
	SELECT * FROM pg_dist_shard
		WHERE logicalrelid = 'table_to_distribute'::regclass OR logicalrelid = 'table_to_distribute2'::regclass
		ORDER BY shardminvalue::BIGINT, logicalrelid;

	-- sanity check on total elements in the table
	SELECT COUNT(*) FROM table_to_distribute;
}

step "s2-commit"
{
	COMMIT;
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

session "s4"

step "s4-print-waiting-locks"
{
	SELECT mode, relation::regclass, granted FROM pg_locks
		WHERE relation = 'table_to_distribute'::regclass OR relation = 'table_to_distribute2'::regclass
		ORDER BY mode, relation, granted;
}

// show concurrent insert is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-advisory-lock" "s1-settings" "s2-settings" "s1-create_distributed_table_concurrently" "s2-begin" "s2-insert" "s2-commit" "s3-release-advisory-lock" "s2-print-status"

// show concurrent update is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-begin" "s2-insert" "s2-update" "s2-commit" "s3-release-advisory-lock" "s2-print-status"

// show concurrent delete is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-begin" "s2-insert" "s2-delete" "s2-commit" "s3-release-advisory-lock" "s2-print-status"

// show concurrent copy is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-begin" "s2-insert" "s2-copy" "s2-commit" "s3-release-advisory-lock" "s2-print-status"

// show concurrent reindex concurrently is blocked by create_distributed_table_concurrently
// both tries to acquire SHARE UPDATE EXCLUSIVE on the table
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-insert" "s2-reindex-concurrently" "s4-print-waiting-locks" "s3-release-advisory-lock"

// show concurrent reindex is blocked by create_distributed_table_concurrently
// reindex tries to acquire ACCESS EXCLUSIVE lock while create_distributed_table_concurrently tries to acquire SHARE UPDATE EXCLUSIVE on the table
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-insert" "s2-reindex" "s4-print-waiting-locks" "s3-release-advisory-lock"

// show create_distributed_table_concurrently operation inside a transaction are NOT allowed
permutation "s2-begin" "s2-create_distributed_table_concurrently-same" "s2-commit"

// show concurrent create_distributed_table_concurrently operations with the same table are NOT allowed
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-create_distributed_table_concurrently-same" "s3-release-advisory-lock"

// show concurrent create_distributed_table_concurrently operations with different tables are NOT allowed
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-create_distributed_table_concurrently-no-same" "s3-release-advisory-lock"

// show concurrent create_distributed_table_concurrently and create_distribute_table operations with the same table are NOT allowed
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-create_distributed_table-same" "s3-release-advisory-lock"

// show concurrent create_distributed_table_concurrently and create_distribute_table operations with different tables are allowed
permutation "s3-acquire-advisory-lock" "s1-create_distributed_table_concurrently" "s2-create_distributed_table-no-same" "s3-release-advisory-lock"
