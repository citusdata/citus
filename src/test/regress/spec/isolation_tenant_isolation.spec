setup
{
	SET citus.shard_count to 2;
	SET citus.shard_replication_factor to 1;
	SELECT setval('pg_dist_shardid_seq',
		CASE WHEN nextval('pg_dist_shardid_seq') > 1599999 OR nextval('pg_dist_shardid_seq') < 1500000
			THEN 1500000
			ELSE nextval('pg_dist_shardid_seq')-2
		END);

	CREATE TABLE isolation_table (id int, value int);
	SELECT create_distributed_table('isolation_table', 'id');
}

teardown
{
	DROP TABLE isolation_table;
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
}

step "s1-insert"
{
	INSERT INTO isolation_table VALUES (5, 10);
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

step "s1-ddl"
{
	CREATE INDEX test_table_index ON isolation_table(id);
}

step "s1-copy"
{
	COPY isolation_table FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
}

step "s1-isolate-tenant"
{
	SELECT isolate_tenant_to_new_shard('isolation_table', 2, shard_transfer_mode => 'block_writes');
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
	SELECT isolate_tenant_to_new_shard('isolation_table', 5, shard_transfer_mode => 'block_writes');
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

step "s2-print-index-count"
{
	SELECT
		nodeport, success, result
	FROM
		run_command_on_placements('isolation_table', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

// run tenant isolation while concurrently performing an DML and index creation
// we expect DML/DDL queries to fail because the shard they are waiting for is destroyed
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update-complex" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster" "s2-print-index-count"


// the same tests without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-insert" "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-update-complex" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s1-begin" "s1-select" "s2-begin" "s2-isolate-tenant" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster" "s2-print-index-count"


// concurrent tenant isolation blocks on different shards of the same table (or any colocated table)
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-isolate-tenant" "s2-isolate-tenant" "s1-commit" "s2-print-cluster"

// the same test above without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-isolate-tenant" "s2-isolate-tenant" "s1-commit" "s2-print-cluster"
