#include "isolation_mx_common.spec"

setup {
	CREATE TABLE ref_table(user_id int, value_1 int);
	SELECT create_reference_table('ref_table');
	INSERT INTO ref_table VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);

	CREATE TABLE tt1(user_id int, value_1 int);
	SELECT create_distributed_table('tt1', 'user_id');
	INSERT INTO tt1 VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);
}

// Create and use UDF to close the connection opened in the setup step. Also return the cluster
// back to the initial state.
teardown
{
	DROP TABLE ref_table;
	DROP TABLE tt1;
	SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-update-ref-table-from-coordinator"
{
	UPDATE ref_table SET value_1 = 15;
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

step "s1-update-dist-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE tt1 SET value_1 = 4');
}

step "s1-update-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table SET value_1 = 12 WHERE user_id = 1');
}

step "s1-delete-from-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table WHERE user_id = 1');
}

step "s1-insert-into-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table VALUES(8,81),(9,91)');
}

step "s1-copy-to-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY ref_table FROM PROGRAM ''echo 10, 101 && echo 11, 111'' WITH CSV');
}

step "s1-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM ref_table FOR UPDATE');
}

step "s1-commit-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-alter-table"
{
	ALTER TABLE ref_table ADD CONSTRAINT rf_p_key PRIMARY KEY(user_id);
}

step "s1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s1-update-on-the-coordinator"
{
	UPDATE tt1 SET value_1 = 4;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
	COMMIT;
}

step "s2-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-update-dist-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE tt1 SET value_1 = 5');
}

step "s2-update-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table SET value_1 = 12 WHERE user_id = 1');
}

step "s2-select-from-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT count(*) FROM ref_table');
}

step "s2-delete-from-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table WHERE user_id = 2');
}

step "s2-insert-into-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table VALUES(8,81),(9,91)');
}

step "s2-copy-to-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY ref_table FROM PROGRAM ''echo 10, 101 && echo 11, 111'' WITH CSV');
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s2-update-on-the-coordinator"
{
	UPDATE tt1 SET value_1 = 4;
}

step "s2-commit-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

session "s3"

step "s3-select-distributed-waiting-queries"
{
	SELECT blocked_statement, current_statement_in_blocking_process, waiting_node_name, blocking_node_name, waiting_node_port, blocking_node_port FROM citus_lock_waits;
}

// session s1 and s4 executes the commands on the same worker node
session "s4"

step "s4-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s4-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s4-update-dist-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE tt1 SET value_1 = 5');
}

step "s4-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}
step "s4-commit-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

permutation "s1-begin" "s1-update-ref-table-from-coordinator" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit" "s2-commit-worker" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-dist-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-dist-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete-from-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-to-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-to-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-to-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy-to-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-select-for-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-ref-table" "s1-begin" "s1-alter-table" "s3-select-distributed-waiting-queries" "s2-commit-worker" "s1-commit" "s2-stop-connection"

// make sure that multi-shard modification queries
// show up in the waiting processes even if they are
// blocked on the same node
permutation "s1-begin" "s1-update-on-the-coordinator" "s2-update-on-the-coordinator" "s3-select-distributed-waiting-queries" "s1-commit"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-dist-table" "s4-start-session-level-connection" "s4-begin-on-worker" "s4-update-dist-table" "s3-select-distributed-waiting-queries" "s1-commit-worker" "s4-commit-worker" "s1-stop-connection" "s4-stop-connection"
