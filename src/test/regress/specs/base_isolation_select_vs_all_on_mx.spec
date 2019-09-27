setup 
{
	CREATE TABLE select_table(id integer, value integer);
	SELECT create_distributed_table('select_table', 'id');
	COPY select_table FROM PROGRAM 'echo 1, 10 && echo 2, 20 && echo 3, 30 && echo 4, 40 && echo 5, 50' WITH CSV;
}

# Create and use UDF to close the connection opened in the setup step. Also return the cluster
# back to the initial state.
teardown
{
        DROP TABLE IF EXISTS select_table CASCADE;
        SELECT citus_internal.restore_isolation_tester_func();
}	

session "s1"

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s1-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s1-select"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM select_table');
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

step "s2-begin"
{
	BEGIN;
}

# We do not need to begin a transaction on coordinator, since it will be open on workers.

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
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM select_table');
}

step "s2-insert"
{
        SELECT run_commands_on_session_level_connection_to_node('INSERT INTO select_table VALUES(6, 60)');
}

step "s2-insert-select"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO select_table SELECT * FROM select_table');
}

step "s2-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM select_table WHERE id = 1');
}

step "s2-copy"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY select_table FROM PROGRAM ''echo 9, 90 && echo 10, 100''WITH CSV');
}

step "s2-index"
{
	CREATE INDEX select_index ON select_table(id);
}

step "s2-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM select_table WHERE id = 6 FOR UPDATE');
}

step "s2-coordinator-create-index-concurrently"
{
	CREATE INDEX CONCURRENTLY select_table_index ON select_table(id);
}

step "s2-commit-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s2-stop-connection"
{
        SELECT stop_session_level_connection_to_node();
}

step "s2-commit"
{
	COMMIT;
}


session "s3"

step "s3-select-count"
{
	SELECT COUNT(*) FROM select_table;
}



permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-select" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-begin" "s2-index" "s1-commit-worker" "s2-commit" "s1-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-update" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s1-stop-connection"
