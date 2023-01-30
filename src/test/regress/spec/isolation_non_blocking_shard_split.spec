#include "isolation_mx_common.include.spec"

// Test scenario for nonblocking split and concurrent INSERT/UPDATE/DELETE
//  session s1 - Executes non-blocking shard split
//  session s2 - Does concurrent writes
//  session s3 - Holds advisory locks
//  session s4 - Tries to insert when the shards are Blocked for write
//
setup
{
	SET citus.shard_count to 1;
	SET citus.shard_replication_factor to 1;
    SELECT setval('pg_dist_shardid_seq', 1500000);

	-- Cleanup any orphan shards that might be left over from a previous run.
	CREATE OR REPLACE FUNCTION run_try_drop_marked_resources()
	RETURNS VOID
	AS 'citus'
	LANGUAGE C STRICT VOLATILE;

	CREATE TABLE to_split_table (id int PRIMARY KEY, value int);
	SELECT create_distributed_table('to_split_table', 'id');
}

teardown
{
	SELECT run_try_drop_marked_resources();

    DROP TABLE to_split_table;
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

// cache all placements
step "s1-load-cache"
{
	-- Indirect way to load cache.
	TRUNCATE to_split_table;
}

step "s1-lock-to-split-shard"
{
  SELECT run_commands_on_session_level_connection_to_node('BEGIN; LOCK TABLE to_split_table_1500001 IN ACCESS SHARE MODE;');
}

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s1-acquire-split-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s1-release-split-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

step "s1-run-cleaner"
{
	SELECT run_try_drop_marked_resources();
}

step "s1-start-connection"
{
  SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-stop-connection"
{
    SELECT stop_session_level_connection_to_node();
}

step "s1-show-pg_dist_cleanup"
{
	SELECT object_name, object_type, policy_type FROM pg_dist_cleanup;
}

step "s1-non-blocking-shard-split"
{
    	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500001,
		ARRAY['-1073741824'],
		ARRAY[2, 2],
		'force_logical');
}

step "s1-end"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-insert"
{
	SELECT get_shard_id_for_distribution_column('to_split_table', 123456789);
	INSERT INTO to_split_table VALUES (123456789, 1);
}

step "s2-update"
{
	UPDATE to_split_table SET value = 111 WHERE id = 123456789;
}

step "s2-delete"
{
	DELETE FROM to_split_table WHERE id = 123456789;
}

step "s2-select"
{
	SELECT count(*) FROM to_split_table WHERE id = 123456789;
}

step "s2-end"
{
	  COMMIT;
}

step "s2-non-blocking-shard-split"
{
	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500001,
		ARRAY['-1073741824'],
		ARRAY[1, 2],
		'force_logical');
}

step "s2-print-locks"
{
    SELECT * FROM master_run_on_worker(
		ARRAY['localhost']::text[],
		ARRAY[57637]::int[],
		ARRAY[
			'SELECT CONCAT(relation::regclass, ''-'', locktype, ''-'', mode) AS LockInfo FROM pg_locks
				WHERE relation::regclass::text = ''to_split_table_1500001'';'
			 ]::text[],
		false);
}

step "s2-show-pg_dist_cleanup"
{
	SELECT object_name, object_type, policy_type FROM pg_dist_cleanup;
}

step "s2-show-pg_dist_cleanup-shards"
{
	SELECT object_name, object_type, policy_type FROM pg_dist_cleanup
	WHERE object_type = 1;
}

step "s2-print-cluster"
{
	-- row count per shard
	SELECT
		nodeport, shardid, success, result
	FROM
		run_command_on_placements('to_split_table', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;
	-- rows
	SELECT id, value FROM to_split_table ORDER BY id, value;
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

step "s4-begin"
{
    BEGIN;
}

step "s4-insert"
{
	INSERT INTO to_split_table VALUES (900, 1);
}

step "s4-end"
{
	  COMMIT;
}


// Concurrent Insert:
// s3 holds advisory lock -> s1 starts non-blocking shard split and waits for advisory lock ->
// s2 inserts a row successfully demonstrating nonblocking split -> s3 releases the advisory lock
// -> s1 completes split -> result is reflected in new shards
permutation "s1-load-cache" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s2-begin" "s1-non-blocking-shard-split" "s2-insert" "s2-end" "s2-print-cluster" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"

// Concurrent Update:
// s2 inserts a row to be updated later ->s3 holds advisory lock -> s1 starts non-blocking shard split and waits for advisory lock ->
// s2 udpates the row -> s3 releases the advisory lock
// -> s1 completes split -> result is reflected in new shards
permutation "s1-load-cache" "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-update" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"

// Concurrent Delete:
// s2 inserts a row to be deleted later ->s3 holds advisory lock -> s1 starts non-blocking shard split and waits for advisory lock ->
// s2 deletes the row -> s3 releases the advisory lock
// -> s1 completes split -> result is reflected in new shards
permutation "s1-load-cache" "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-delete" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"

// Demonstrating blocking Insert when the writes are blocked by nonblocking split workflow
// s3 holds advisory lock -> s1 starts non-blocking shard split and waits for advisory lock ->
// s2 inserts the row successfully-> s4 begins-> s3 releases the advisory lock thus s2 moves ahead to block writes
// -> s4 inserts(waiting as the writes are blocked) -> s1 commits -> s4 fails as shard meta data gets update
permutation "s1-load-cache" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s2-begin" "s1-non-blocking-shard-split" "s2-insert" "s2-end" "s2-print-cluster" "s4-begin" "s3-release-advisory-lock" "s4-insert" "s1-end" "s4-end" "s2-print-cluster"

// Same flow without loading cache
permutation "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s2-begin" "s1-non-blocking-shard-split" "s2-insert" "s2-end" "s2-print-cluster" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-update" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"
permutation "s2-insert" "s2-print-cluster" "s3-acquire-advisory-lock" "s1-begin" "s1-non-blocking-shard-split" "s2-delete" "s3-release-advisory-lock" "s1-end" "s2-print-cluster"


// With Deferred drop, AccessShareLock (acquired by SELECTS) do not block split from completion.
permutation "s1-load-cache" "s1-start-connection"  "s1-lock-to-split-shard" "s2-print-locks" "s2-non-blocking-shard-split" "s2-print-locks" "s2-show-pg_dist_cleanup-shards" "s1-stop-connection"
// The same test above without loading the cache at first
permutation "s1-start-connection"  "s1-lock-to-split-shard" "s2-print-locks" "s2-non-blocking-shard-split" "s2-print-cluster" "s2-show-pg_dist_cleanup-shards" "s1-stop-connection"

// When a split operation is running, cleaner cannot clean its resources.
permutation "s1-load-cache" "s1-acquire-split-advisory-lock" "s2-non-blocking-shard-split" "s1-run-cleaner" "s1-show-pg_dist_cleanup" "s1-release-split-advisory-lock" "s1-run-cleaner" "s2-show-pg_dist_cleanup"
// The same test above without loading the cache at first
permutation "s1-acquire-split-advisory-lock" "s2-non-blocking-shard-split" "s1-run-cleaner" "s1-show-pg_dist_cleanup" "s1-release-split-advisory-lock" "s1-run-cleaner" "s2-show-pg_dist_cleanup"
