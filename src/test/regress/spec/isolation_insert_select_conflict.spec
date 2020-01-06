setup
{
	CREATE TABLE target_table(col_1 int primary key, col_2 int);
	SELECT create_distributed_table('target_table','col_1');
	INSERT INTO target_table VALUES(1,2),(2,3),(3,4),(4,5),(5,6);

	CREATE TABLE source_table(col_1 int, col_2 int, col_3 int);
	SELECT create_distributed_table('source_table','col_1');
	INSERT INTO source_table VALUES(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);

	SET citus.shard_replication_factor to 2;
	CREATE TABLE target_table_2(col_1 int primary key, col_2 int, col_3 int);
	SELECT create_distributed_table('target_table_2', 'col_1');
}

teardown
{
	DROP TABLE target_table, target_table_2, source_table;
}

session "s1"

step "s1-begin"
{
	SET citus.shard_replication_factor to 1;
	BEGIN;
}

step "s1-begin-replication-factor-2"
{
	SET citus.shard_replication_factor to 2;
	BEGIN;
}

step "s1-insert-into-select-conflict-update"
{
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

step "s1-insert-into-select-conflict-do-nothing"
{
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT DO NOTHING;
}

step "s1-commit"
{
	COMMIT;
}

step "s1-insert-into-select-conflict-update-replication-factor-2"
{
	INSERT INTO target_table_2
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-begin-replication-factor-2"
{
	SET citus.shard_replication_factor to 2;
	BEGIN;
}

step "s2-insert-into-select-conflict-update"
{
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

step "s2-insert-into-select-conflict-update-replication-factor-2"
{
	INSERT INTO target_table_2
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

step "s2-insert-into-select-conflict-do-nothing"
{
	INSERT INTO target_table
	SELECT
		col_1, col_2
	FROM (
		SELECT
			col_1, col_2, col_3
		FROM
			source_table
		LIMIT 5
	) as foo
	ON CONFLICT DO NOTHING;
}

step "s2-update"
{
	UPDATE target_table SET col_2 = 5;
}

step "s2-delete"
{
	DELETE FROM target_table;
}

step "s2-commit"
{
	COMMIT;
}

permutation "s1-begin" "s1-insert-into-select-conflict-update" "s2-begin" "s2-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-insert-into-select-conflict-do-nothing" "s2-begin" "s2-delete" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-insert-into-select-conflict-do-nothing" "s2-begin" "s2-insert-into-select-conflict-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-insert-into-select-conflict-update" "s2-begin" "s2-insert-into-select-conflict-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-insert-into-select-conflict-update" "s2-begin" "s2-insert-into-select-conflict-do-nothing" "s1-commit" "s2-commit"
permutation "s1-begin-replication-factor-2" "s1-insert-into-select-conflict-update-replication-factor-2" "s2-begin-replication-factor-2" "s2-insert-into-select-conflict-update-replication-factor-2" "s1-commit" "s2-commit"
