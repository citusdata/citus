# we use 5 as the partition key value through out the test
# so setting the corresponding shard here is useful
setup
{	
	SET citus.shard_count TO 2;
	SET citus.shard_replication_factor TO 2;
	CREATE TABLE test_table (x int, y int);
	SELECT create_distributed_table('test_table', 'x');
	
	SELECT get_shard_id_for_distribution_column('test_table', 5) INTO selected_shard;
}

teardown
{
	DROP TABLE test_table;
	DROP TABLE selected_shard;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

# since test_table has rep > 1 simple select query doesn't hit all placements
# hence not all placements are cached 
step "s1-load-cache"
{
	TRUNCATE test_table;
}

step "s1-insert"
{
	INSERT INTO test_table VALUES (5, 10);
}

step "s1-update"
{
	UPDATE test_table SET y = 5 WHERE x = 5;
}

step "s1-delete"
{
	DELETE FROM test_table WHERE x = 5;
}

step "s1-select"
{
	SELECT count(*) FROM test_table WHERE x = 5;
}

step "s1-ddl"
{
	CREATE INDEX test_table_index ON test_table(x);
}

step "s1-copy"
{
	COPY test_table FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5"' WITH CSV;
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

step "s2-commit"
{
	COMMIT;
}

step "s2-print-content"
{
	SELECT 
		nodeport, success, result 
	FROM 
		run_command_on_placements('test_table', 'select y from %s WHERE x = 5')
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
		run_command_on_placements('test_table', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

# repair a placement while concurrently performing an update/delete/insert/copy
# note that at some points we use "s1-select" just after "s1-begin" given that BEGIN
# may invalidate cache at certain cases
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-update" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-delete" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-insert" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-copy" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-load-cache" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-ddl" "s2-commit" "s1-commit" "s2-print-index-count"


# the same tests without loading the cache at first
permutation "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-update" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-insert" "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-delete" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-insert" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-copy" "s2-commit" "s1-commit" "s2-print-content"
permutation "s1-begin" "s1-select" "s2-set-placement-inactive" "s2-begin" "s2-repair-placement" "s1-ddl" "s2-commit" "s1-commit" "s2-print-index-count"
