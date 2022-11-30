#include "isolation_mx_common.include.spec"

setup
{
	SET citus.shard_count to 2;
	SET citus.shard_replication_factor to 1;
    SELECT setval('pg_dist_shardid_seq', 1500000);

	-- Cleanup any orphan shards that might be left over from a previous run.
	CREATE OR REPLACE FUNCTION run_try_drop_marked_resources()
	RETURNS VOID
	AS 'citus'
	LANGUAGE C STRICT VOLATILE;

	CREATE TABLE to_split_table (id int, value int);
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

    -- the tests are written with the logic where single shard SELECTs
    -- do not to open transaction blocks
    SET citus.select_opens_transaction_block TO false;
}

// cache all placements
step "s1-load-cache"
{
	-- Indirect way to load cache.
	TRUNCATE to_split_table;
}

step "s1-insert"
{
	-- Id '123456789' maps to shard 1500002.
	SELECT get_shard_id_for_distribution_column('to_split_table', 123456789);

	INSERT INTO to_split_table VALUES (123456789, 1);
}

step "s1-update"
{
	UPDATE to_split_table SET value = 111 WHERE id = 123456789;
}

step "s1-delete"
{
	DELETE FROM to_split_table WHERE id = 123456789;
}

step "s1-select"
{
	SELECT count(*) FROM to_split_table WHERE id = 123456789;
}

step "s1-ddl"
{
	CREATE INDEX test_table_index ON to_split_table(id);
}

step "s1-copy"
{
	COPY to_split_table FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
}

step "s1-lock-to-split-shard"
{
  SELECT run_commands_on_session_level_connection_to_node('BEGIN; LOCK TABLE to_split_table_1500002 IN ACCESS SHARE MODE;');
}

step "s1-start-connection"
{
  SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s1-stop-connection"
{
    SELECT stop_session_level_connection_to_node();
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

step "s1-show-pg_dist_cleanup"
{
	SELECT object_name, object_type, policy_type FROM pg_dist_cleanup;
}

step "s1-blocking-shard-split"
{
	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500001,
		ARRAY['-1073741824'],
		ARRAY[1, 2],
		'block_writes');
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-print-locks"
{
    SELECT * FROM master_run_on_worker(
		ARRAY['localhost']::text[],
		ARRAY[57638]::int[],
		ARRAY[
			'SELECT CONCAT(relation::regclass, ''-'', locktype, ''-'', mode) AS LockInfo FROM pg_locks
				WHERE relation::regclass::text = ''to_split_table_1500002'';'
			 ]::text[],
		false);
}

step "s2-show-pg_dist_cleanup"
{
	SELECT object_name, object_type, policy_type FROM pg_dist_cleanup;
}

step "s2-blocking-shard-split"
{
	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500002,
		ARRAY['1073741824'],
		ARRAY[1, 2],
		'block_writes');
}

step "s2-commit"
{
	COMMIT;
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

step "s2-print-index-count"
{
	SELECT
		nodeport, success, result
	FROM
		run_command_on_placements('to_split_table', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

// Run shard split while concurrently performing DML and index creation
// We expect DML,Copy to fail because the shard they are waiting for is destroyed.
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"
// The same tests without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"

// Concurrent shard split blocks on different shards of the same table (or any colocated table)
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-blocking-shard-split" "s2-blocking-shard-split" "s1-commit" "s2-print-cluster"
// The same test above without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-blocking-shard-split" "s2-blocking-shard-split" "s1-commit" "s2-print-cluster"

// Concurrent DDL blocks on different shards of the same table (or any colocated table)
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster" "s2-print-index-count"
// The same tests without loading the cache at first
permutation "s1-begin" "s1-select" "s2-begin" "s2-blocking-shard-split" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster" "s2-print-index-count"

// With Deferred drop, AccessShareLock (acquired by SELECTS) do not block split from completion.
permutation "s1-load-cache" "s1-start-connection"  "s1-lock-to-split-shard" "s2-print-locks" "s2-blocking-shard-split" "s2-print-locks" "s2-show-pg_dist_cleanup" "s1-stop-connection"
// The same test above without loading the cache at first
permutation "s1-start-connection"  "s1-lock-to-split-shard" "s2-print-locks" "s2-blocking-shard-split" "s2-print-cluster" "s2-show-pg_dist_cleanup" "s1-stop-connection"

// When a split operation is running, cleaner cannot clean its resources.
permutation "s1-load-cache" "s1-acquire-split-advisory-lock" "s2-blocking-shard-split" "s1-run-cleaner" "s1-show-pg_dist_cleanup" "s1-release-split-advisory-lock" "s1-run-cleaner" "s2-show-pg_dist_cleanup"
// The same test above without loading the cache at first
permutation "s1-acquire-split-advisory-lock" "s2-blocking-shard-split" "s1-run-cleaner" "s1-show-pg_dist_cleanup" "s1-release-split-advisory-lock" "s1-run-cleaner" "s2-show-pg_dist_cleanup"
