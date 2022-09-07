setup
{
	SET citus.shard_count to 2;
	SET citus.shard_replication_factor to 1;
	SELECT setval('pg_dist_shardid_seq',
		CASE WHEN nextval('pg_dist_shardid_seq') > 1599999 OR nextval('pg_dist_shardid_seq') < 1500072
			THEN 1500072
			ELSE nextval('pg_dist_shardid_seq')-2
		END);

	CREATE TABLE isolation_table (id int PRIMARY KEY, value int);
	SELECT create_distributed_table('isolation_table', 'id');

	-- different colocation id
	CREATE TABLE isolation_table2 (id smallint PRIMARY KEY, value int);
	SELECT create_distributed_table('isolation_table2', 'id');
}

teardown
{
	DROP TABLE isolation_table;
	DROP TABLE isolation_table2;
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
	TRUNCATE isolation_table;
	TRUNCATE isolation_table2;
}

step "s1-insert"
{
	INSERT INTO isolation_table VALUES (5, 10);
	INSERT INTO isolation_table2 VALUES (5, 10);
}

step "s1-update"
{
	UPDATE isolation_table SET value = 5 WHERE id = 5;
}

step "s1-update-complex"
{
	UPDATE isolation_table SET value = 5 WHERE id IN (
        SELECT max(id) FROM isolation_table
    );
}

step "s1-delete"
{
	DELETE FROM isolation_table WHERE id = 5;
}

step "s1-select"
{
	SELECT count(*) FROM isolation_table WHERE id = 5;
}

step "s1-copy"
{
	COPY isolation_table FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
}

step "s1-isolate-tenant-same-coloc"
{
	SELECT isolate_tenant_to_new_shard('isolation_table', 2, shard_transfer_mode => 'force_logical');
}

step "s1-isolate-tenant-same-coloc-blocking"
{
	SELECT isolate_tenant_to_new_shard('isolation_table', 2, shard_transfer_mode => 'block_writes');
}

step "s1-isolate-tenant-no-same-coloc"
{
	SELECT isolate_tenant_to_new_shard('isolation_table2', 2, shard_transfer_mode => 'force_logical');
}

step "s1-isolate-tenant-no-same-coloc-blocking"
{
	SELECT isolate_tenant_to_new_shard('isolation_table2', 2, shard_transfer_mode => 'block_writes');
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

step "s2-isolate-tenant"
{
	SELECT isolate_tenant_to_new_shard('isolation_table', 5, shard_transfer_mode => 'force_logical');
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
		run_command_on_placements('isolation_table', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;

	-- rows
	SELECT id, value FROM isolation_table ORDER BY id, value;
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

// Even if we do not block shard split copy operations, we eventually have to block concurrent sessions during metadata operations (tiny amount of time)
// To prevent concurrent DML from being blocked by metadata operations, we take the advisory lock from another dummy session, and let the concurrent DML
// executes concurrently with shard split copy. Just before committing, we release the advisory lock from the dummy session to allow all the sessions to finish.
// s3 takes an advisory lock before s2 takes it (we take the same lock for shard movement only during isolation tests) =>
// s1 can execute its DML command concurrently with s2 shard isolation =>
// s3 releases the advisory lock so that s2 can finish the transaction

// run tenant isolation while concurrently performing an DML
// we expect DML queries of s2 to succeed without being blocked.
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-delete" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update-complex" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-load-cache" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-insert" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-load-cache" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-copy" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"

// the same tests without loading the cache at first
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-delete" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update-complex" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-insert" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-copy" "s1-commit" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"

// concurrent nonblocking tenant isolations with the same colocation id are not allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-isolate-tenant" "s1-isolate-tenant-same-coloc" "s3-release-advisory-lock" "s2-print-cluster"

// concurrent blocking and nonblocking tenant isolations with the same colocation id are not allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-isolate-tenant" "s1-isolate-tenant-same-coloc-blocking" "s3-release-advisory-lock" "s2-print-cluster"

// concurrent nonblocking tenant isolations in different transactions are not allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-isolate-tenant" "s1-isolate-tenant-no-same-coloc" "s3-release-advisory-lock" "s2-print-cluster"

// concurrent nonblocking tenant isolations in the same transaction are not allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-begin" "s2-isolate-tenant" "s1-isolate-tenant-no-same-coloc" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"

// concurrent blocking and nonblocking tenant isolations with different colocation ids in different transactions are allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-isolate-tenant" "s1-isolate-tenant-no-same-coloc-blocking" "s3-release-advisory-lock" "s2-print-cluster"

// concurrent blocking and nonblocking tenant isolations with different colocation ids in the same transaction are allowed
permutation "s1-load-cache" "s1-insert" "s3-acquire-advisory-lock" "s2-isolate-tenant" "s1-isolate-tenant-no-same-coloc-blocking" "s3-release-advisory-lock" "s2-print-cluster"
