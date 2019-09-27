setup 
{
	CREATE TABLE insert_table(id integer, value integer);
	SELECT create_distributed_table('insert_table', 'id');
	COPY insert_table FROM PROGRAM 'echo 1, 10 && echo 2, 20 && echo 3, 30 && echo 4, 40 && echo 5, 50' WITH CSV;
}

# Create and use UDF to close the connection opened in the setup step. Also return the cluster
# back to the initial state.
teardown
{
        DROP TABLE IF EXISTS insert_table CASCADE;
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

step "s1-insert"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO insert_table VALUES(6, 60)');
}

step "s1-insert-multi-row"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO insert_table VALUES(6, 60), (7, 70), (8, 80)');
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

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-insert"
{
        SELECT run_commands_on_session_level_connection_to_node('INSERT INTO insert_table VALUES(6, 60)');
}

step "s2-insert-multi-row"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO insert_table VALUES(6, 60), (7, 70), (8, 80)');
}

step "s2-select"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM insert_table WHERE id = 6');
}

step "s2-insert-select"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO insert_table SELECT * FROM insert_table');
}

step "s2-update"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE insert_table SET value = 65 WHERE id = 6');
}

step "s2-update-multi-row"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE insert_table SET value = 67 WHERE id IN (6, 7)');
}

step "s2-copy"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY insert_table FROM PROGRAM ''echo 9, 90 && echo 10, 100''WITH CSV');
}

step "s2-truncate"
{
	SELECT run_commands_on_session_level_connection_to_node('TRUNCATE insert_table');
}

step "s2-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM insert_table WHERE id = 6 FOR UPDATE');
}

step "s2-coordinator-create-index-concurrently"
{
	CREATE INDEX CONCURRENTLY insert_table_index ON insert_table(id);
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
	SELECT COUNT(*) FROM insert_table;
}



permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s1-commit-worker" "s2-commit-worker" "s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-multi-row" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s1-commit-worker" "s2-commit-worker" "s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-multi-row" "s1-commit-worker" "s2-commit-worker" "s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-multi-row" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-multi-row" "s1-commit-worker" "s2-commit-worker" "s3-select-count" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-select" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-multi-row" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-multi-row" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-update" "s1-commit-worker" "s2-commit-worker""s3-select-count" "s1-stop-connection" "s2-stop-connection"
#Not able to test the next permutation, until issue with CREATE INDEX CONCURRENTLY's locks is resolved. Issue #2966 
#permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s3-select-count" "s1-stop-connection"
