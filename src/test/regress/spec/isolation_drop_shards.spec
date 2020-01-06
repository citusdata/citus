setup
{
	CREATE TABLE append_table (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('append_table', 'test_id', 'append');

	SELECT 1 FROM (
		SELECT min(master_create_empty_shard('append_table')) FROM generate_series(1,16)
	) a;
}

teardown
{
	DROP TABLE append_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-truncate"
{
	TRUNCATE append_table;
}

step "s1-apply-delete-command"
{
	SELECT master_apply_delete_command('DELETE FROM append_table');
}

step "s1-drop-all-shards"
{
	SELECT master_drop_all_shards('append_table', 'public', 'append_table');
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-truncate"
{
	TRUNCATE append_table;
}

step "s2-apply-delete-command"
{
	SELECT master_apply_delete_command('DELETE FROM append_table');
}

step "s2-drop-all-shards"
{
	SELECT master_drop_all_shards('append_table', 'public', 'append_table');
}

step "s2-select"
{
	SELECT * FROM append_table;
}

permutation "s1-begin" "s1-drop-all-shards" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-drop-all-shards" "s2-apply-delete-command" "s1-commit"
permutation "s1-begin" "s1-drop-all-shards" "s2-drop-all-shards" "s1-commit"
permutation "s1-begin" "s1-drop-all-shards" "s2-select" "s1-commit"

// We can't verify master_apply_delete_command + SELECT since it blocks on the
// the workers, but this is not visible on the master, meaning the isolation
// test cannot proceed.
permutation "s1-begin" "s1-apply-delete-command" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-apply-delete-command" "s2-apply-delete-command" "s1-commit"
permutation "s1-begin" "s1-apply-delete-command" "s2-drop-all-shards" "s1-commit"

permutation "s1-begin" "s1-truncate" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-truncate" "s2-apply-delete-command" "s1-commit"
permutation "s1-begin" "s1-truncate" "s2-drop-all-shards" "s1-commit"
permutation "s1-begin" "s1-truncate" "s2-select" "s1-commit"
