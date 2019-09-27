setup 
{
	CREATE TABLE truncate_table(id integer, value integer);
	SELECT create_distributed_table('truncate_table', 'id');
	COPY truncate_table FROM PROGRAM 'echo 1, 10 && echo 2, 20 && echo 3, 30 && echo 4, 40 && echo 5, 50' WITH CSV;
}

# Create and use UDF to close the connection opened in the setup step. Also return the cluster
# back to the initial state.
teardown
{
        DROP TABLE IF EXISTS truncate_table CASCADE;
        SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s1-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s1-truncate"
{
	SELECT run_commands_on_session_level_connection_to_node('TRUNCATE truncate_table');
}

step "s1-select"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM truncate_table WHERE id = 6');
}

step "s1-insert-select"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO truncate_table SELECT * FROM truncate_table');
}

step "s1-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM truncate_table WHERE id IN (5, 6, 7)');
}

step "s1-copy"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY truncate_table FROM PROGRAM ''echo 5, 50 && echo 9, 90 && echo 10, 100''WITH CSV');
}

step "s1-alter"
{
	ALTER TABLE truncate_table DROP value;
}

step "s1-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM truncate_table WHERE id=5 FOR UPDATE');
}

step "s1-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s1-commit"
{
	COMMIT;
}


session "s2"

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-truncate"
{
	SELECT run_commands_on_session_level_connection_to_node('TRUNCATE truncate_table');
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
	SELECT COUNT(*) FROM truncate_table;
}


permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-truncate" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-begin" "s1-alter" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit" "s2-commit-worker" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select-for-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
