setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE restore_table (test_id integer NOT NULL, data text);
	CREATE TABLE restore_ref_table (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('restore_table', 'test_id');
	SELECT create_reference_table('restore_ref_table');
}

teardown
{
	DROP TABLE IF EXISTS restore_table, restore_ref_table, test_create_distributed_table, test_create_reference_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-create-reference"
{
	CREATE TABLE test_create_reference_table (test_id integer NOT NULL, data text);
	SELECT create_reference_table('test_create_reference_table');
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

step "s1-insert-ref"
{
	INSERT INTO restore_ref_table VALUES (1,'hello');
}

step "s1-modify-multiple"
{
	UPDATE restore_table SET data = 'world';
}

step "s1-modify-multiple-ref"
{
	UPDATE restore_ref_table SET data = 'world';
}

step "s1-multi-statement-ref"
{
	BEGIN;
	INSERT INTO restore_ref_table VALUES (1,'hello');
	INSERT INTO restore_ref_table VALUES (2,'hello');
	COMMIT;
}

step "s1-multi-statement"
{
	BEGIN;
	INSERT INTO restore_table VALUES (1,'hello');
	INSERT INTO restore_table VALUES (2,'hello');
	COMMIT;
}

step "s1-ddl-ref"
{
	ALTER TABLE restore_ref_table ADD COLUMN x int;
}

step "s1-ddl"
{
	ALTER TABLE restore_table ADD COLUMN x int;
}

step "s1-copy-ref"
{
	COPY restore_ref_table FROM PROGRAM 'echo 1,hello' WITH CSV;
}

step "s1-copy"
{
	COPY restore_table FROM PROGRAM 'echo 1,hello' WITH CSV;
}

step "s1-recover"
{
	SELECT recover_prepared_transactions();
}

step "s1-create-restore"
{
	SELECT 1 FROM citus_create_restore_point('citus-test-2');
}

step "s1-drop"
{
	DROP TABLE restore_table;
}

step "s1-drop-ref"
{
	DROP TABLE restore_ref_table;
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

step "s2-begin"
{
	BEGIN;
}

step "s2-create-restore"
{
	SELECT 1 FROM citus_create_restore_point('citus-test');
}

step "s2-commit"
{
	COMMIT;
}

// verify that citus_create_restore_point is blocked by concurrent create_distributed_table
permutation "s1-begin" "s1-create-distributed" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent INSERT (only commit)
permutation "s1-begin" "s1-insert" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent multi-shard UPDATE (only commit)
permutation "s1-begin" "s1-modify-multiple" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent DDL (only commit)
permutation "s1-begin" "s1-ddl" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent COPY (only commit)
permutation "s1-begin" "s1-copy" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent recover_prepared_transactions
permutation "s1-begin" "s1-recover" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent DROP TABLE
permutation "s1-begin" "s1-drop" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent master_add_node
permutation "s1-begin" "s1-add-node" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent master_remove_node
permutation "s1-begin" "s1-remove-node" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent citus_create_restore_point
permutation "s1-begin" "s1-create-restore" "s2-create-restore" "s1-commit"

// verify that multi-shard UPDATE is blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-modify-multiple" "s2-commit"

// verify that DDL is blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-ddl" "s2-commit"

// verify that multi-statement transactions are blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-multi-statement" "s2-commit"

// verify that citus_create_restore_point is blocked by concurrent create_reference_table
permutation "s1-begin" "s1-create-reference" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent reference table INSERT (only commit)
permutation "s1-begin" "s1-insert-ref" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent reference table UPDATE (only commit)
permutation "s1-begin" "s1-modify-multiple-ref" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent refence table DDL (only commit)
permutation "s1-begin" "s1-ddl-ref" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is not blocked by concurrent COPY to reference table (only commit)
permutation "s1-begin" "s1-copy-ref" "s2-create-restore" "s1-commit"

// verify that citus_create_restore_point is blocked by concurrent DROP TABLE when table is a reference table
permutation "s1-begin" "s1-drop-ref" "s2-create-restore" "s1-commit"

// verify that reference table UPDATE is blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-modify-multiple-ref" "s2-commit"

// verify that reference table DDL is blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-ddl-ref" "s2-commit"

// verify that multi-statement transactions with reference tables are blocked by concurrent citus_create_restore_point
permutation "s2-begin" "s2-create-restore" "s1-multi-statement-ref" "s2-commit"
