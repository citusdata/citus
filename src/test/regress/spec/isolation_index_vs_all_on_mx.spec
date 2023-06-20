#include "isolation_mx_common.include.spec"

setup
{
	CREATE TABLE dist_table(id int, data int);
	SELECT create_distributed_table('dist_table', 'id');
	COPY dist_table FROM PROGRAM 'echo 1, 10 && echo 2, 20 && echo 3, 30 && echo 4, 40 && echo 5, 50' WITH CSV;
	SELECT citus_set_coordinator_host('localhost', 57636);
}

teardown
{
	DROP TABLE IF EXISTS dist_table CASCADE;
	SELECT citus_remove_node('localhost', 57636);
}

session "w1"

step "w1-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "w1-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "w1-create-named-index"
{
	SELECT run_commands_on_session_level_connection_to_node('CREATE INDEX dist_table_index ON dist_table(id)');
}

step "w1-create-unnamed-index"
{
	SELECT run_commands_on_session_level_connection_to_node('CREATE INDEX ON dist_table(id,data)');
}

step "w1-reindex"
{
	SELECT run_commands_on_session_level_connection_to_node('REINDEX INDEX dist_table_index');
}

step "w1-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM dist_table');
}

step "w1-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "w1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}


session "w2"

step "w2-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "w2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "w2-create-named-index"
{
	SELECT run_commands_on_session_level_connection_to_node('CREATE INDEX dist_table_index_2 ON dist_table(id)');
}

step "w2-create-unnamed-index"
{
	SELECT run_commands_on_session_level_connection_to_node('CREATE INDEX ON dist_table(id,data)');
}

step "w2-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "w2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

session "coord"

step "coord-begin"
{
	BEGIN;
}

step "coord-rollback"
{
	ROLLBACK;
}

step "coord-print-index-count"
{
	SELECT
		result
	FROM
		run_command_on_placements('dist_table', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

step "coord-take-lock"
{
	LOCK TABLE dist_table IN ACCESS EXCLUSIVE MODE;
}

step "coord-short-statement-timeout"
{
	SET statement_timeout = 100;
}

session "deadlock-checker"

step "deadlock-checker-call"
{
  SELECT check_distributed_deadlocks();
}

permutation "w1-start-session-level-connection" "w1-begin-on-worker" // open transaction on worker 1
			"w2-start-session-level-connection" "w2-begin-on-worker" // open transaction on worker 2
			"w1-create-named-index" "w2-create-named-index"			 // create indexes with unique names on workers
			"w1-commit-worker" "w2-commit-worker"					 // commit transactions on workers
			"w1-stop-connection" "w2-stop-connection"				 // close connections to workers
			"coord-print-index-count"					 			 // show indexes on coordinator

permutation "w1-start-session-level-connection" "w1-begin-on-worker" // open transaction on worker 1
			"w2-start-session-level-connection" "w2-begin-on-worker" // open transaction on worker 2
			"w1-create-unnamed-index" "w2-create-unnamed-index"		 // create unnamed indexes on workers
			"w1-commit-worker"										 // commit transactions on worker 1 and see error on worker 2 due to name clash
			"w1-stop-connection" "w2-stop-connection"				 // close connections to workers
			"coord-print-index-count"					 			 // show indexes on coordinator

// the following permutation is expected to fail with a distributed deadlock. However, we do not detect the deadlock, and get blocked until statement_timeout.
permutation "w1-start-session-level-connection"				 		 // start session on worker 1 only
			"w1-create-named-index"									 // create index on worker 1
			"w1-begin-on-worker"									 // open transaction block on worker 1
			"w1-delete"												 // delete from table on worker 1
			"coord-begin"											 // open transaction on coordinator to test distributed deadlock
			"coord-short-statement-timeout"							 // set statement timeout on coordinator to early abort deadlock check
			"coord-take-lock"										 // take ACCESS EXCLUSIVE lock on table on coordinator, get blocked on worker 1
			"w1-reindex"											 // reindex on worker that will acquire ACCESS EXCLUSIVE lock on table, create distributed deadlock
			"deadlock-checker-call"									 // check that distributed deadlock is detected properly
			"coord-rollback"										 // rollback transaction on coordinator to unblock
			"w1-commit-worker"										 // commit transaction on worker 1
			"w1-stop-connection"									 // close connection to worker 1
