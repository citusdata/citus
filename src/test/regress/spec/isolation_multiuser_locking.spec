setup
{
	SET citus.shard_replication_factor TO 1;

	CREATE USER test_user_1;
	SELECT run_command_on_workers('CREATE USER test_user_1');

	CREATE USER test_user_2;
	SELECT run_command_on_workers('CREATE USER test_user_2');

	SET ROLE test_user_1;
	CREATE TABLE test_table(column1 int, column2 int);
	SELECT create_distributed_table('test_table', 'column1');
	RESET ROLE;
}

teardown
{
	BEGIN;
	DROP TABLE IF EXISTS test_table;
	DROP USER test_user_1, test_user_2;
	SELECT run_command_on_workers('DROP USER test_user_1, test_user_2');
	COMMIT;
}

session "s1"

// run_command_on_placements is done in a separate step because the setup is executed as a single transaction
step "s1-grant"
{
	SET ROLE test_user_1;
	SELECT bool_and(success) FROM run_command_on_placements('test_table', 'GRANT ALL ON TABLE %s TO test_user_2');
	GRANT ALL ON test_table TO test_user_2;
}

step "s1-begin"
{
	BEGIN;
	SET ROLE test_user_1;
}

step "s1-index"
{
	CREATE INDEX test_index ON test_table(column1);
}

step "s1-reindex"
{
	REINDEX TABLE test_table;
}

step "s1-drop-index"
{
	DROP INDEX IF EXISTS test_index;
}

step "s1-insert"
{
	UPDATE test_table SET column2 = 1;
}

step "s1-truncate"
{
	TRUNCATE test_table;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
	SET ROLE test_user_2;
}

step "s2-index"
{
	CREATE INDEX test_index ON test_table(column1);
}

step "s2-reindex"
{
	REINDEX TABLE test_table;
}

step "s2-drop-index"
{
	DROP INDEX IF EXISTS test_index;
}

step "s2-insert"
{
	UPDATE test_table SET column2 = 2;
}

step "s2-truncate"
{
	TRUNCATE test_table;
}

step "s2-commit"
{
	COMMIT;
}

// REINDEX
permutation "s1-begin" "s2-begin" "s2-reindex" "s1-insert" "s2-commit" "s1-commit"
permutation "s1-grant" "s1-begin" "s2-begin" "s2-reindex" "s1-insert" "s2-insert" "s2-commit" "s1-commit"
permutation "s1-grant" "s1-begin" "s2-begin" "s1-reindex" "s2-insert" "s1-insert" "s1-commit" "s2-commit"

// CREATE INDEX
permutation "s1-begin" "s2-begin" "s2-index" "s1-insert" "s2-commit" "s1-commit" "s2-drop-index"
permutation "s1-grant" "s1-begin" "s2-begin" "s2-insert" "s1-index" "s2-insert" "s2-commit" "s1-commit" "s1-drop-index"
permutation "s1-grant" "s1-begin" "s2-begin" "s1-index" "s2-index" "s1-insert" "s1-commit" "s2-commit" "s1-drop-index" "s2-drop-index"

// TRUNCATE
permutation "s1-begin" "s2-begin" "s2-truncate" "s1-insert" "s2-commit" "s1-commit"
permutation "s1-grant" "s1-begin" "s2-begin" "s1-truncate" "s2-insert" "s1-insert" "s1-commit" "s2-commit"
permutation "s1-grant" "s1-begin" "s2-begin" "s1-truncate" "s2-truncate" "s1-commit" "s2-commit"

