setup
{
	CREATE TABLE migration_table (test_id integer NOT NULL, data text);
}

teardown
{
	DROP TABLE migration_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-begin-serializable"
{
	BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	SELECT 1;
}

step "s1-create_distributed_table"
{
	SELECT create_distributed_table('migration_table', 'test_id');
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

step "s2-copy"
{
	COPY migration_table FROM PROGRAM 'echo 1,hello' WITH CSV;
}

step "s2-insert"
{
	INSERT INTO migration_table VALUES (1, 'hello');
}

step "s2-commit"
{
	COMMIT;
}

step "s2-select"
{
	SELECT * FROM migration_table ORDER BY test_id;
}

// verify that local COPY is picked up by create_distributed_table once it commits
permutation "s2-begin" "s2-copy" "s1-create_distributed_table" "s2-commit" "s2-select"
// verify that COPY is distributed once create_distributed_table commits
permutation "s1-begin" "s1-create_distributed_table" "s2-copy" "s1-commit" "s2-select"

// verify that local INSERT is picked up by create_distributed_table once it commits
permutation "s2-begin" "s2-insert" "s1-create_distributed_table" "s2-commit" "s2-select"
// verify that INSERT is distributed once create_distributed_table commits
permutation "s1-begin" "s1-create_distributed_table" "s2-insert" "s1-commit" "s2-select"

// verify that changes are picked up even in serializable mode
permutation "s1-begin-serializable" "s2-copy" "s1-create_distributed_table" "s1-commit" "s2-select"
permutation "s1-begin-serializable" "s2-insert" "s1-create_distributed_table" "s1-commit" "s2-select"
