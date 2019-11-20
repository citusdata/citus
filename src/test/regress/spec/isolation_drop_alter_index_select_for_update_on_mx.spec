#include "isolation_mx_common.spec"

setup 
{
	CREATE TABLE dist_table(id integer, value integer);
	SELECT create_distributed_table('dist_table', 'id');
	COPY dist_table FROM PROGRAM 'echo 1, 10 && echo 2, 20 && echo 3, 30 && echo 4, 40 && echo 5, 50' WITH CSV;
}

// Create and use UDF to close the connection opened in the setup step. Also return the cluster
// back to the initial state.
teardown
{
        DROP TABLE IF EXISTS dist_table CASCADE;
        SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
	BEGIN;
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

step "s1-insert"
{
        SELECT run_commands_on_session_level_connection_to_node('INSERT INTO dist_table VALUES(5, 55)');
}

step "s1-index"
{
	CREATE INDEX dist_table_index ON dist_table (id);
}

step "s1-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table WHERE id = 5 FOR UPDATE');
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

// We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-alter"
{
	ALTER TABLE dist_table DROP value;
}

step "s2-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table WHERE id = 5 FOR UPDATE');
}

step "s2-coordinator-create-index-concurrently"
{
	CREATE INDEX CONCURRENTLY dist_table_index_conc ON dist_table(id);
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
	SELECT COUNT(*) FROM dist_table;
}



permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-alter" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-begin" "s1-index" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-update" "s1-commit" "s2-commit-worker" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select-for-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-update" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select-for-update" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s1-stop-connection"
