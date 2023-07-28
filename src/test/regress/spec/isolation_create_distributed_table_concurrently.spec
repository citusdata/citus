setup
{
	select setval('pg_dist_shardid_seq', GREATEST(1400292, nextval('pg_dist_shardid_seq')-1));
	-- make sure coordinator is in metadata
	SELECT citus_set_coordinator_host('localhost', 57636);
  	CREATE TABLE table_1(id int PRIMARY KEY);
  	CREATE TABLE table_2(id smallint PRIMARY KEY);
  	CREATE TABLE table_default_colocated(id int PRIMARY KEY);
  	CREATE TABLE table_none_colocated(id int PRIMARY KEY);
}

teardown
{
	DROP TABLE table_1 CASCADE;
	DROP TABLE table_2 CASCADE;
	DROP TABLE table_default_colocated CASCADE;
	DROP TABLE table_none_colocated CASCADE;
	SELECT citus_remove_node('localhost', 57636);
}

session "s1"

step "s1-create-concurrently-table_1"
{
	SELECT create_distributed_table_concurrently('table_1', 'id');
}

step "s1-create-concurrently-table_2"
{
	SELECT create_distributed_table_concurrently('table_2', 'id');
}

step "s1-create-concurrently-table_default_colocated"
{
	SELECT create_distributed_table_concurrently('table_default_colocated', 'id');
}

step "s1-create-concurrently-table_none_colocated"
{
	SELECT create_distributed_table_concurrently('table_none_colocated', 'id', colocate_with => 'none');
}

step "s1-settings"
{
	-- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
}

step "s1-truncate"
{
	TRUNCATE table_1;
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
	INSERT INTO table_1 SELECT s FROM generate_series(1,20) s;
}

step "s2-update"
{
	UPDATE table_1 SET id = 21 WHERE id = 20;
}

step "s2-delete"
{
	DELETE FROM table_1 WHERE id = 11;
}

step "s2-copy"
{
	COPY table_1 FROM PROGRAM 'echo 30 && echo 31 && echo 32 && echo 33 && echo 34 && echo 35 && echo 36 && echo 37 && echo 38';
}

step "s2-reindex"
{
	REINDEX TABLE table_1;
}

step "s2-reindex-concurrently"
{
	REINDEX TABLE CONCURRENTLY table_1;
}

step "s2-create-concurrently-table_1"
{
	SELECT create_distributed_table_concurrently('table_1', 'id');
}

step "s2-create-table_1"
{
	SELECT create_distributed_table('table_1', 'id');
}

step "s2-create-concurrently-table_2"
{
	SELECT create_distributed_table_concurrently('table_2', 'id');
}

step "s2-create-table_2"
{
	SELECT create_distributed_table('table_2', 'id');
}

step "s2-create-table_2-none"
{
	SELECT create_distributed_table('table_2', 'id', colocate_with => 'none');
}

step "s2-print-status"
{
	-- sanity check on partitions
	SELECT * FROM pg_dist_shard
		WHERE logicalrelid = 'table_1'::regclass OR logicalrelid = 'table_2'::regclass
		ORDER BY shardminvalue::BIGINT, logicalrelid;

	-- sanity check on total elements in the table
	SELECT COUNT(*) FROM table_1;
}

step "s2-commit"
{
	COMMIT;
}

session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-split-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-split-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

session "s4"

step "s4-print-waiting-locks"
{
	SELECT mode, relation::regclass, granted FROM pg_locks
		WHERE relation = 'table_1'::regclass OR relation = 'table_2'::regclass
		ORDER BY mode, relation, granted;
}

step "s4-print-waiting-advisory-locks"
{
	SELECT mode, classid, objid, objsubid, granted FROM pg_locks
		WHERE locktype = 'advisory' AND classid = 0 AND objid = 3 AND objsubid = 9
		ORDER BY granted;
}

step "s4-print-colocations"
{
	SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation ORDER BY colocationid;
}

// show concurrent insert is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-split-advisory-lock" "s1-settings" "s2-settings" "s1-create-concurrently-table_1" "s2-begin" "s2-insert" "s2-commit" "s3-release-split-advisory-lock" "s2-print-status"

// show concurrent update is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-begin" "s2-insert" "s2-update" "s2-commit" "s3-release-split-advisory-lock" "s2-print-status"

// show concurrent delete is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-begin" "s2-insert" "s2-delete" "s2-commit" "s3-release-split-advisory-lock" "s2-print-status"

// show concurrent copy is NOT blocked by create_distributed_table_concurrently
permutation "s1-truncate" "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-begin" "s2-insert" "s2-copy" "s2-commit" "s3-release-split-advisory-lock" "s2-print-status"

// show concurrent reindex concurrently is blocked by create_distributed_table_concurrently
// both tries to acquire SHARE UPDATE EXCLUSIVE on the table
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-insert" "s2-reindex-concurrently" "s4-print-waiting-locks" "s3-release-split-advisory-lock"

// show concurrent reindex is blocked by create_distributed_table_concurrently
// reindex tries to acquire ACCESS EXCLUSIVE lock while create-concurrently tries to acquire SHARE UPDATE EXCLUSIVE on the table
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-insert" "s2-reindex" "s4-print-waiting-locks" "s3-release-split-advisory-lock"

// show create_distributed_table_concurrently operation inside a transaction are NOT allowed
permutation "s2-begin" "s2-create-concurrently-table_1" "s2-commit"

// show concurrent create_distributed_table_concurrently operations with the same table are NOT allowed
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-create-concurrently-table_1" "s3-release-split-advisory-lock"

// show concurrent create_distributed_table_concurrently operations with different tables are NOT allowed
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-create-concurrently-table_2" "s3-release-split-advisory-lock"

// show concurrent create_distributed_table_concurrently and create_distribute_table operations with the same table are NOT allowed
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-create-table_1" "s3-release-split-advisory-lock"

// show concurrent create_distributed_table_concurrently and create_distribute_table operations with different tables are allowed
permutation "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-create-table_2" "s3-release-split-advisory-lock"

// tests with colocated_with combinations
// show concurrent colocate_with => 'default' and colocate_with => 'default' are NOT allowed if there is no default colocation entry yet.
permutation "s2-begin" "s2-create-table_2" "s1-create-concurrently-table_default_colocated" "s4-print-waiting-advisory-locks" "s2-commit" "s4-print-colocations"

// show concurrent colocate_with => 'default' and colocate_with => 'default' are allowed if there is already a default colocation entry.
permutation "s1-create-concurrently-table_default_colocated" "s3-acquire-split-advisory-lock" "s1-create-concurrently-table_1" "s2-create-table_2" "s4-print-waiting-advisory-locks" "s3-release-split-advisory-lock" "s4-print-colocations"

// show concurrent colocate_with => 'default' and colocate_with => 'none' are allowed.
permutation "s2-begin" "s2-create-table_2" "s1-create-concurrently-table_none_colocated" "s4-print-waiting-advisory-locks" "s2-commit" "s4-print-colocations"

// show concurrent colocate_with => 'none' and colocate_with => 'none' are allowed.
permutation "s2-begin" "s2-create-table_2-none" "s1-create-concurrently-table_none_colocated" "s4-print-waiting-advisory-locks" "s2-commit" "s4-print-colocations"
