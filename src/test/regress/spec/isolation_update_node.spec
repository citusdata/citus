setup
{
    -- revert back to pg_isolation_test_session_is_blocked until the tests are fixed
    SELECT citus_internal.restore_isolation_tester_func();

    ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 22;

    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);

    SELECT nodeid, nodename, nodeport from pg_dist_node ORDER BY 1 DESC;
}

teardown
{
    -- replace pg_isolation_test_session_is_blocked so that next tests are run with Citus implementation
    SELECT citus_internal.replace_isolation_tester_func();

    DROP TABLE IF EXISTS test;
    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
    SELECT nodeid, nodename, nodeport from pg_dist_node ORDER BY 1 DESC;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-prepare-transaction" {
    PREPARE transaction 'label';
}

step "s1-commit-prepared" {
    COMMIT prepared 'label';
}

step "s1-update-node-1"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'localhost',
        58637);
}

step "s1-update-node-nonexistent" {
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'non-existent',
        57637);
}

step "s1-update-node-existent" {
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'localhost',
        57637);
}

step "s1-commit"
{
	COMMIT;
}

step "s1-show-nodes"
{
    SELECT nodeid, nodename, nodeport, isactive
      FROM pg_dist_node
  ORDER BY nodename, nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-update-node-1"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'localhost',
        58637);
}

step "s2-update-node-2"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57638),
        'localhost',
        58638);
}

step "s2-create-table" {
    CREATE TABLE test (a int);
    SELECT create_distributed_table('test','a');
}

step "s2-cache-prepared-statement" {
    PREPARE foo AS SELECT COUNT(*) FROM test WHERE a = 3;
    EXECUTE foo;
    EXECUTE foo;
    EXECUTE foo;
    EXECUTE foo;
    EXECUTE foo;
    EXECUTE foo;
}

step "s2-execute-prepared" {
    EXECUTE foo;
}

step "s2-abort"
{
	ABORT;
}

session "s3"

step "s3-update-node-1-back"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 58637),
        'localhost',
        57637);
}

step "s3-update-node-2-back"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 58638),
        'localhost',
        57638);
}


// since we update the nodes to unexistent nodes we break metadata, so here we fix it manually
step "s3-manually-fix-metadata"
{
    UPDATE pg_dist_node SET metadatasynced = 't' WHERE nodeport = 57637;
    UPDATE pg_dist_node SET metadatasynced = 't' WHERE nodeport = 57638;
    SELECT start_metadata_sync_to_node('localhost', 57637);
    SELECT start_metadata_sync_to_node('localhost', 57638);
}


// session 1 updates node 1, session 2 updates node 2, should be ok
permutation "s1-begin" "s1-update-node-1" "s2-update-node-2" "s1-commit" "s1-show-nodes" "s3-update-node-1-back" "s3-update-node-2-back" "s3-manually-fix-metadata"

// sessions 1 updates node 1, session 2 tries to do the same
permutation "s1-begin" "s1-update-node-1" "s2-begin" "s2-update-node-1" "s1-commit" "s2-abort" "s1-show-nodes" "s3-update-node-1-back" "s3-manually-fix-metadata"

// make sure we have entries in prepared statement cache
// then make sure that after we update pg_dist_node, the changes are visible to
// the prepared statement
permutation "s2-create-table" "s1-begin" "s1-update-node-nonexistent" "s1-prepare-transaction" "s2-cache-prepared-statement" "s1-commit-prepared" "s2-execute-prepared" "s1-update-node-existent" "s3-manually-fix-metadata"
