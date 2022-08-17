setup
{
  -- set it to a predefined value as we check shard id's as well in output of this test
	SELECT setval('pg_dist_shardid_seq',
		CASE WHEN nextval('pg_dist_shardid_seq') > 1399999 OR nextval('pg_dist_shardid_seq') < 1300000
			THEN 1300000
			ELSE nextval('pg_dist_shardid_seq')-2
		END);

  SET citus.shard_count to 2;
	SET citus.shard_replication_factor to 1;

	CREATE TABLE reference_table (id int PRIMARY KEY, value int);
	SELECT create_reference_table('reference_table');

	CREATE TABLE isolation_table (id int, value int);
	SELECT create_distributed_table('isolation_table', 'id');
}

teardown
{
	DROP TABLE isolation_table CASCADE;
	DROP TABLE reference_table CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
	INSERT INTO reference_table VALUES (5, 10);
}

step "s1-update"
{
	UPDATE reference_table SET value = 5 WHERE id = 5;
}

step "s1-delete"
{
	DELETE FROM reference_table WHERE id = 5;
}

step "s1-ddl"
{
	CREATE INDEX reference_table_index ON reference_table(id);
}

step "s1-copy"
{
	COPY reference_table FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
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

step "s2-add-fkey"
{
	ALTER TABLE isolation_table ADD CONSTRAINT fkey_const FOREIGN KEY(value) REFERENCES reference_table(id);
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

// run tenant isolation while concurrently performing an DML and index creation on the
// reference table which the distributed table have a foreign key to
// all modifications should block tenant isolation
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-isolate-tenant" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-isolate-tenant" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-isolate-tenant" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-isolate-tenant" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-isolate-tenant" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster"
