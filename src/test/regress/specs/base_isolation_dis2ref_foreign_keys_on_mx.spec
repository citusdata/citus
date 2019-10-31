#include "isolation_mx_common.spec"

setup
{	
	CREATE TABLE ref_table(id int PRIMARY KEY, value int);
	SELECT create_reference_table('ref_table');

	CREATE TABLE dist_table(id int, value int REFERENCES ref_table(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_distributed_table('dist_table', 'id');

	INSERT INTO ref_table VALUES (1, 10), (2, 20);
	INSERT INTO dist_table VALUES (1, 1), (2, 2);
}

teardown
{
	DROP TABLE ref_table, dist_table;
	SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN'); 
}

step "s1-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table WHERE id=1');
}

step "s1-update"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table SET id=id+2 WHERE id=1');
}

step "s1-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-rollback-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "s1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

session "s2"

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
        SELECT run_commands_on_session_level_connection_to_node('INSERT INTO dist_table VALUES (1, 1)');
}

step "s2-select"
{
        SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table WHERE id=1');
}

step "s2-insert-select"
{
        SELECT run_commands_on_session_level_connection_to_node('INSERT INTO dist_table SELECT * FROM dist_table');
}

step "s2-update"
{
        SELECT run_commands_on_session_level_connection_to_node('UPDATE dist_table SET value=2 WHERE id=1');
}

step "s2-copy"
{
        SELECT run_commands_on_session_level_connection_to_node('COPY dist_table FROM PROGRAM ''echo 1, 1''WITH CSV');
}

step "s2-truncate"
{
        SELECT run_commands_on_session_level_connection_to_node('TRUNCATE dist_table');
}

step "s2-select-for-udpate"
{
        SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table WHERE id=1 FOR UPDATE');
}

step "s2-coordinator-create-index-concurrently"
{
        CREATE INDEX CONCURRENTLY dist_table_index ON dist_table(id);
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

step "s3-display"
{
	SELECT * FROM ref_table ORDER BY id, value;
	SELECT * FROM dist_table ORDER BY id, value;
}


permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s1-rollback-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-select" "s1-rollback-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy" "s1-rollback-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-udpate" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-display"
//permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s1-stop-connection" "s3-display"
