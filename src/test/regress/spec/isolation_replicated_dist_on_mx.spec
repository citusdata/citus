#include "isolation_mx_common.include.spec"

setup
{
	CREATE TABLE replicated_table(user_id int, value_1 int);
	SET citus.shard_replication_factor TO 2;
	SELECT create_distributed_table('replicated_table', 'user_id', shard_count:=4);
	INSERT INTO replicated_table VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);

	CREATE TABLE replicated_table_2(user_id int, value_1 int);
	SET citus.shard_replication_factor TO 2;
	SELECT create_distributed_table('replicated_table_2', 'user_id', shard_count:=4);
	INSERT INTO replicated_table_2 VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);

	CREATE TABLE single_replicated_table(user_id int, value_1 int);
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('single_replicated_table', 'user_id', shard_count:=4);
	INSERT INTO single_replicated_table VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);


}

// Create and use UDF to close the connection opened in the setup step. Also return the cluster
// back to the initial state.
teardown
{
	DROP TABLE replicated_table, replicated_table_2, single_replicated_table;
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

step "s1-update-1-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE replicated_table SET value_1 = 12 WHERE user_id = 1');
}

step "s1-update-all-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE replicated_table SET value_1 = 12');
}

step "s1-delete-1-from-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM replicated_table WHERE user_id = 1');
}

step "s1-delete-all-from-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM replicated_table');
}

step "s1-insert-into-1-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table VALUES(1,81)');
}

step "s1-insert-into-all-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table VALUES(8,81),(9,91),(10,91),(11,91),(12,91), (13,91), (14,91), (15,91), (16,91), (17,91), (18,91), (19,91), (20,91)');
}

step "s1-insert-into-select"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table SELECT * FROM replicated_table_2');
}

step "s1-insert-into-select-from-single-rep"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table SELECT * FROM single_replicated_table LIMIT 10');
}

step "s1-copy-all-to-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY replicated_table FROM PROGRAM ''echo 10, 101 && echo 11, 111 && echo 11, 111 && echo 12, 111 && echo 13, 111 && echo 14, 111 && echo 15, 111 && echo 16, 111 && echo 17, 111 && echo 18, 111 && echo 19, 111 && echo 20, 111 && echo 21, 111 && echo 22, 111 && echo 23, 111'' WITH CSV');
}

step "s1-copy-1-to-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY replicated_table FROM PROGRAM ''echo 1, 101 && echo 1, 111 && echo 1,1111'' WITH CSV');
}

step "s1-commit-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-alter-table"
{
	ALTER TABLE replicated_table ADD COLUMN x INT;
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

step "s2-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-update-1-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE replicated_table SET value_1 = 12 WHERE user_id = 1');
}

step "s2-update-all-single-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE single_replicated_table SET value_1 = 12');
}

step "s2-update-all-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE replicated_table SET value_1 = 12');
}

step "s2-select-from-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT count(*) FROM replicated_table');
}

step "s2-insert-into-1-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table VALUES(1,81)');
}

step "s2-insert-into-all-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('INSERT INTO replicated_table VALUES(8,81),(9,91),(10,91),(11,91),(12,91), (13,91), (14,91), (15,91), (16,91), (17,91), (18,91), (19,91), (20,91)');
}

step "s2-copy-all-to-rep-table"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY replicated_table FROM PROGRAM ''echo 10, 101 && echo 11, 111 && echo 11, 111 && echo 12, 111 && echo 13, 111 && echo 14, 111 && echo 15, 111 && echo 16, 111 && echo 17, 111 && echo 18, 111 && echo 19, 111 && echo 20, 111 && echo 21, 111 && echo 22, 111 && echo 23, 111'' WITH CSV');
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s2-commit-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete-1-from-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete-all-from-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete-1-from-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete-all-from-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-1-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-all-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-1-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-1-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-all-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-all-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-all-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-1-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-all-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy-all-to-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-copy-all-to-rep-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-from-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-begin" "s1-alter-table" "s2-commit-worker" "s1-commit" "s2-stop-connection"
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-from-rep-table" "s1-begin" "s1-alter-table" "s2-commit-worker" "s1-commit" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-select" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-into-1-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-insert-into-select-from-single-rep" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-all-single-rep-table" "s2-update-all-rep-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
