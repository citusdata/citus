#include "isolation_mx_common.include.spec"

setup
{
	CREATE TABLE ref_table(id integer, value integer);
	SELECT create_reference_table('ref_table');
}

// Create and use UDF to close the connection opened in the setup step. Also return the cluster
// back to the initial state.
teardown
{
        DROP TABLE IF EXISTS ref_table CASCADE;
        SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-add-primary-key"
{
	ALTER TABLE ref_table ADD CONSTRAINT pri_key PRIMARY KEY (id);
}

// We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s1-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s1-update"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table SET value=12 WHERE id=1 OR id=2');
}

step "s1-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table WHERE id=1 OR id=2');
}

step "s1-upsert"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table VALUES (1, 3), (2, 3) ON CONFLICT (id) DO UPDATE SET value=3');
}

step "s1-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}


session "s2"

// We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-select"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM ref_table WHERE id=1 OR id=2');
}

step "s2-insert-select-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table SELECT * FROM ref_table');
}

step "s2-drop"
{
	DROP TABLE ref_table;
}

step "s2-truncate"
{
	SELECT run_commands_on_session_level_connection_to_node('TRUNCATE ref_table');
}

step "s2-coordinator-create-index-concurrently"
{
	CREATE INDEX CONCURRENTLY ref_table_index ON ref_table(id);
}

step "s2-commit-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s2-stop-connection"
{
        SELECT stop_session_level_connection_to_node();
}


session "s3"

step "s3-select-count"
{
	SELECT COUNT(*) FROM ref_table;
}



permutation "s1-add-primary-key""s1-start-session-level-connection" "s1-begin-on-worker" "s1-upsert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-select-ref-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-add-primary-key" "s1-start-session-level-connection" "s1-begin-on-worker" "s1-upsert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-drop" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
//Not able to test the next permutation, until issue with CREATE INDEX CONCURRENTLY's locks is resolved. Issue #2966
//permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s3-select-count" "s1-stop-connection"
