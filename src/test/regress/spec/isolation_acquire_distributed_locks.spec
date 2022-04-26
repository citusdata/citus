#include "isolation_mx_common.include.spec"

setup
{
	CREATE TABLE dist_table(a int);
	CREATE TABLE local_table(a int);
	CREATE TABLE ref_table(a int);

	SELECT create_distributed_table('dist_table', 'a');
	SELECT create_reference_table('ref_table');

	CREATE VIEW sub_view(a) AS
		SELECT 2 * a AS a
		FROM ref_table;

	CREATE VIEW main_view AS
		SELECT t1.a a1, t2.a a2, t3.a a3
		FROM dist_table t1
			JOIN local_table t2 ON t1.a = t2.a
			JOIN sub_view t3 ON t2.a = t3.a;

	INSERT INTO dist_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO local_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO ref_table SELECT n FROM generate_series(1, 5) n;
}

teardown
{
	DROP VIEW main_view;
	DROP VIEW sub_view;
	DROP TABLE dist_table;
	DROP TABLE local_table;
	DROP TABLE ref_table;

	SELECT citus_internal.restore_isolation_tester_func();
}

// coordinator session
session "s1"

step "s1-add-coordinator-to-metadata"
{
	SELECT citus_set_coordinator_host('localhost', 57636);
}

step "s1-remove-coordinator-from-metadata"
{
	SELECT citus_remove_node('localhost', 57636);
}

step "s1-begin"
{
	BEGIN;
}

step "s1-acquire-aggresive-lock-on-dist-table"
{
	LOCK dist_table IN ACCESS EXCLUSIVE MODE;
}

step "s1-acquire-aggresive-lock-on-dist-table-nowait"
{
	LOCK dist_table IN ACCESS EXCLUSIVE MODE NOWAIT;
}

step "s1-acquire-weak-lock-on-dist-table"
{
	LOCK dist_table IN ACCESS SHARE MODE;
}

step "s1-acquire-aggresive-lock-on-view"
{
	LOCK main_view IN ACCESS EXCLUSIVE MODE;
}

step "s1-acquire-aggresive-lock-on-view-nowait"
{
	LOCK main_view IN ACCESS EXCLUSIVE MODE NOWAIT;
}

step "s1-lock-all"
{
	LOCK dist_table, local_table, ref_table, main_view, sub_view IN ACCESS EXCLUSIVE MODE;
}

step "s1-read-dist-table"
{
	SELECT * FROM dist_table;
}

step "s1-read-ref-table"
{
	SELECT * FROM ref_table;
}

step "s1-rollback"
{
	ROLLBACK;
}

// worker 1 xact session
session "s2"

step "s2-start-session-level-connection"
{
    SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s2-begin"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-read-dist-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table');
}

step "s2-read-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM ref_table');
}

step "s2-acquire-aggressive-lock-dist-table" {
	SELECT run_commands_on_session_level_connection_to_node('LOCK dist_table IN ACCESS EXCLUSIVE MODE');
}

step "s2-lock-reference-table"
{
	SELECT run_commands_on_session_level_connection_to_node('LOCK ref_table IN ACCESS EXCLUSIVE MODE');
}

step "s2-rollback"
{
	SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

// worker 2 xact session
session "s3"

step "s3-start-session-level-connection"
{
    SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s3-begin"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s3-acquire-aggressive-lock-dist-table" {
	SELECT run_commands_on_session_level_connection_to_node('LOCK dist_table IN ACCESS EXCLUSIVE MODE');
}

step "s3-rollback"
{
	SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "s3-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

permutation "s1-begin" "s1-acquire-aggresive-lock-on-dist-table" "s2-start-session-level-connection" "s2-begin" "s2-read-dist-table" "s1-rollback" "s2-rollback" "s2-stop-connection"
permutation "s1-add-coordinator-to-metadata" "s1-begin" "s1-acquire-aggresive-lock-on-dist-table" "s2-start-session-level-connection" "s2-begin" "s2-acquire-aggressive-lock-dist-table" "s1-rollback" "s1-read-dist-table" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-add-coordinator-to-metadata" "s2-start-session-level-connection" "s2-begin" "s2-acquire-aggressive-lock-dist-table" "s1-begin" "s1-acquire-aggresive-lock-on-dist-table-nowait" "s1-rollback" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-add-coordinator-to-metadata" "s2-start-session-level-connection" "s2-begin" "s3-start-session-level-connection" "s3-begin" "s2-acquire-aggressive-lock-dist-table" "s3-acquire-aggressive-lock-dist-table" "s2-rollback" "s2-read-dist-table" "s3-rollback" "s2-stop-connection" "s3-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-add-coordinator-to-metadata" "s1-begin" "s1-acquire-weak-lock-on-dist-table" "s2-start-session-level-connection" "s2-begin" "s2-read-dist-table" "s2-acquire-aggressive-lock-dist-table" "s1-rollback" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-add-coordinator-to-metadata" "s2-start-session-level-connection" "s2-begin" "s2-lock-reference-table" "s1-begin" "s1-read-ref-table" "s2-rollback" "s1-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-begin" "s1-acquire-aggresive-lock-on-view" "s2-start-session-level-connection" "s2-begin" "s2-read-dist-table" "s1-rollback" "s2-rollback" "s2-stop-connection"
permutation "s1-add-coordinator-to-metadata" "s1-begin" "s1-acquire-aggresive-lock-on-view" "s2-start-session-level-connection" "s2-begin" "s2-acquire-aggressive-lock-dist-table" "s1-rollback" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-begin" "s1-acquire-aggresive-lock-on-view" "s2-start-session-level-connection" "s2-begin" "s2-read-ref-table" "s1-rollback" "s2-rollback" "s2-stop-connection"
permutation "s1-add-coordinator-to-metadata" "s2-start-session-level-connection" "s2-begin" "s2-acquire-aggressive-lock-dist-table" "s1-begin" "s1-acquire-aggresive-lock-on-view-nowait" "s1-rollback" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
permutation "s1-add-coordinator-to-metadata" "s1-begin" "s1-lock-all" "s2-start-session-level-connection" "s2-begin" "s2-read-ref-table" "s1-rollback" "s2-rollback" "s2-stop-connection" "s1-remove-coordinator-from-metadata"
