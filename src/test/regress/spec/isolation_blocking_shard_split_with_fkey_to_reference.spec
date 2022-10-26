setup
{
    SELECT setval('pg_dist_shardid_seq', 1500000);
    SET citus.shard_count to 2;
	SET citus.shard_replication_factor to 1;

	CREATE TABLE reference_table (id int PRIMARY KEY, value int);
	SELECT create_reference_table('reference_table');

	CREATE TABLE table_to_split (id int, value int);
	SELECT create_distributed_table('table_to_split', 'id');
}

teardown
{
	-- Cleanup any orphan shards that might be left over from a previous run.
	CREATE OR REPLACE FUNCTION run_try_drop_marked_resources()
	RETURNS VOID
	AS 'citus'
	LANGUAGE C STRICT VOLATILE;
	SELECT run_try_drop_marked_resources();

	DROP TABLE table_to_split CASCADE;
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

step "s2-blocking-shard-split"
{
	SELECT pg_catalog.citus_split_shard_by_split_points(
		1500002,
		ARRAY['-1073741824'],
		ARRAY[1, 2],
		'block_writes');
}

step "s2-add-fkey"
{
	ALTER TABLE table_to_split ADD CONSTRAINT fkey_const FOREIGN KEY(value) REFERENCES reference_table(id);
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
		run_command_on_placements('table_to_split', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;

	-- rows
	SELECT id, value FROM table_to_split ORDER BY id, value;
}

// Run shard split while concurrently performing an DML and index creation on the
// reference table which the distributed table have a foreign key to.
// All modifications should block on shard split.
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-blocking-shard-split" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-blocking-shard-split" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-blocking-shard-split" "s1-insert" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-blocking-shard-split" "s1-copy" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-begin" "s2-begin" "s2-blocking-shard-split" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster"
