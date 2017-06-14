setup
{
	CREATE TABLE restore_table (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('restore_table', 'test_id');
}

teardown
{
	DROP TABLE IF EXISTS restore_table, test_create_distributed_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-create-distributed"
{
	CREATE TABLE test_create_distributed_table (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('test_create_distributed_table', 'test_id');
}

step "s1-insert"
{
	INSERT INTO restore_table VALUES (1,'hello');
}

step "s1-modify-multiple"
{
	SELECT master_modify_multiple_shards($$UPDATE restore_table SET data = 'world'$$);
}

step "s1-ddl"
{
	ALTER TABLE restore_table ADD COLUMN x int;
}

step "s1-copy"
{
	COPY restore_table FROM PROGRAM 'echo 1,hello' WITH CSV;
}

step "s1-drop"
{
	DROP TABLE restore_table;
}

step "s1-add-node"
{
	SELECT 1 FROM master_add_inactive_node('localhost', 9999);
}

step "s1-remove-node"
{
	SELECT master_remove_node('localhost', 9999);
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-create-restore"
{
	SELECT 1 FROM citus_create_restore_point('citus-test');
}

# verify that citus_create_restore_point is blocked by concurrent create_distributed_table
permutation "s1-begin" "s1-create-distributed" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent INSERT
permutation "s1-begin" "s1-insert" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent master_modify_multiple_shards
permutation "s1-begin" "s1-modify-multiple" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent DDL
permutation "s1-begin" "s1-ddl" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent COPY
permutation "s1-begin" "s1-copy" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent DROP TABLE
permutation "s1-begin" "s1-drop" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent master_add_node
permutation "s1-begin" "s1-add-node" "s2-create-restore" "s1-commit"

# verify that citus_create_restore_point is blocked by concurrent master_remove_node
permutation "s1-begin" "s1-remove-node" "s2-create-restore" "s1-commit"
