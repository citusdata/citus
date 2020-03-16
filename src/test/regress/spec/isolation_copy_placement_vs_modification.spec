// we use 5 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{
	SET citus.shard_count TO 2;
	SET citus.shard_replication_factor TO 2;
	CREATE TABLE test_repair_placement_vs_modification (x int, y int);
	SELECT create_distributed_table('test_repair_placement_vs_modification', 'x');

	SELECT get_shard_id_for_distribution_column('test_repair_placement_vs_modification', 5) INTO selected_shard;

	SET citus.shard_replication_factor TO 1;
	CREATE TABLE test_copy_placement_vs_modification (x int, y int);
	SELECT create_distributed_table('test_copy_placement_vs_modification', 'x');
}

teardown
{
	DROP TABLE test_repair_placement_vs_modification;
	DROP TABLE selected_shard;
	DROP TABLE test_copy_placement_vs_modification;
}

session "s1"

step "s1-begin"
{
    BEGIN;
	SET LOCAL citus.select_opens_transaction_block TO off;
}

// since test_repair_placement_vs_modification has rep > 1 simple select query doesn't hit all placements
// hence not all placements are cached
step "s1-load-cache"
{
	TRUNCATE test_repair_placement_vs_modification;
}

step "s1-insert"
{
	INSERT INTO test_repair_placement_vs_modification VALUES (5, 10);
}

step "s1-update"
{
	UPDATE test_repair_placement_vs_modification SET y = 5 WHERE x = 5;
}

step "s1-delete"
{
	DELETE FROM test_repair_placement_vs_modification WHERE x = 5;
}

step "s1-select"
{
	SELECT count(*) FROM test_repair_placement_vs_modification WHERE x = 5;
}

step "s1-ddl"
{
	CREATE INDEX test_repair_placement_vs_modification_index ON test_repair_placement_vs_modification(x);
}

step "s1-copy"
{
	COPY test_repair_placement_vs_modification FROM PROGRAM 'echo 1,1 && echo 2,2 && echo 3,3 && echo 4,4 && echo 5,5' WITH CSV;
}

step "s1-insert-copy-table"
{
	INSERT INTO test_copy_placement_vs_modification VALUES (5, 10);
}

step "s1-update-copy-table"
{
	UPDATE test_copy_placement_vs_modification SET y = 5 WHERE x = 5;
}

step "s1-delete-copy-table"
{
	DELETE FROM test_copy_placement_vs_modification WHERE x = 5;
}

step "s1-select-copy-table"
{
	SELECT count(*) FROM test_copy_placement_vs_modification WHERE x = 5;
}

step "s1-ddl-copy-table"
{
	CREATE INDEX test_copy_placement_vs_modification_index ON test_copy_placement_vs_modification(x);
}

step "s1-copy-copy-table"
{
	COPY test_copy_placement_vs_modification FROM PROGRAM 'echo 1,1 && echo 2,2 && echo 3,3 && echo 4,4 && echo 5,5' WITH CSV;
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

step "s2-set-placement-inactive"
{
	UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid IN (SELECT * FROM selected_shard) AND nodeport = 57638;
}

step "s2-repair-placement"
{
	SELECT master_copy_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638);
}

step "s2-copy-placement"
{
	SELECT master_copy_shard_placement((SELECT get_shard_id_for_distribution_column('test_copy_placement_vs_modification', 5)),
									   'localhost', 57637, 'localhost', 57638,
									   do_repair := false, transfer_mode := 'block_writes');
}

step "s2-commit"
{
	COMMIT;
}

step "s2-print-content"
{
	SELECT
		nodeport, success, result
	FROM
		run_command_on_placements('test_repair_placement_vs_modification', 'select y from %s WHERE x = 5')
	WHERE
		shardid IN (SELECT * FROM selected_shard)
	ORDER BY
		nodeport;
}

step "s2-print-index-count"
{
	SELECT
		nodeport, success, result
	FROM
		run_command_on_placements('test_repair_placement_vs_modification', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

// repair a placement while concurrently performing an update/delete/insert/copy
// note that at some points we use "s1-select" just after "s1-begin" given that BEGIN
// may invalidate cache at certain cases
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-update" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-delete" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-insert" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-copy" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-ddl" "s2-commit" "s1-commit" "s2-print-index-count"


// the same tests without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-update" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-delete" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-insert" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-copy" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-ddl" "s2-commit" "s1-commit" "s2-print-index-count"

// verify that copy placement (do_repair := false) blocks other operations, except SELECT
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-update-copy-table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-delete-copy-table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-insert-copy-table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-copy-copy-table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-ddl-copy-table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-copy-placement" "s1-select-copy-table" "s2-commit" "s1-commit"

// verify that copy placement (do_repair := false) is blocked by other operations, except SELECT
permutation "s1-begin" "s2-begin" "s1-update-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-delete-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-insert-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-copy-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-ddl-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-select-copy-table" "s2-copy-placement" "s1-commit" "s2-commit"
