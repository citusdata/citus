#include "isolation_mx_common.include.spec"

setup
{
	SELECT citus_set_coordinator_host('localhost', 57636);

	CREATE TABLE dist_table(a int);
	CREATE TABLE citus_local_table(a int);
	CREATE TABLE local_table(a int);
	CREATE TABLE ref_table(a int);

	CREATE TABLE partitioned_table(a int)
	PARTITION BY RANGE(a);

	CREATE TABLE partition_1 PARTITION OF partitioned_table
	FOR VALUES FROM (1) TO (11);

	CREATE TABLE partition_2 PARTITION OF partitioned_table
	FOR VALUES FROM (11) TO (21);

	SELECT create_distributed_table('dist_table', 'a');
	SELECT create_reference_table('ref_table');
	SELECT citus_add_local_table_to_metadata('citus_local_table');
	SELECT create_distributed_table('partitioned_table', 'a');

	CREATE VIEW sub_view(a) AS
		SELECT 2 * a AS a
		FROM ref_table;

	CREATE VIEW main_view AS
		SELECT t1.a a1, t2.a a2, t3.a a3
		FROM dist_table t1
			JOIN citus_local_table t2 ON t1.a = t2.a
			JOIN sub_view t3 ON t2.a = t3.a;

	INSERT INTO dist_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO citus_local_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO local_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO ref_table SELECT n FROM generate_series(1, 5) n;
	INSERT INTO partitioned_table SELECT n FROM generate_series(8, 12) n;
}

teardown
{
	DROP VIEW main_view;
	DROP VIEW sub_view;
	DROP TABLE dist_table;
	DROP TABLE citus_local_table;
	DROP TABLE local_table;
	DROP TABLE ref_table;
	DROP TABLE partitioned_table;

	SELECT citus_remove_node('localhost', 57636);
}

// coordinator session
session "coor"

step "coor-begin"
{
	BEGIN;
}

step "coor-acquire-aggresive-lock-on-dist-table"
{
	LOCK dist_table IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-dist-table-nowait"
{
	LOCK dist_table IN ACCESS EXCLUSIVE MODE NOWAIT;
}

step "coor-acquire-weak-lock-on-dist-table"
{
	LOCK dist_table IN ACCESS SHARE MODE;
}

step "coor-acquire-aggresive-lock-on-view"
{
	LOCK main_view IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-only-view"
{
	LOCK ONLY main_view IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-view-nowait"
{
	LOCK main_view IN ACCESS EXCLUSIVE MODE NOWAIT;
}

step "coor-lock-all"
{
	LOCK dist_table, citus_local_table, ref_table, main_view, sub_view, local_table IN ACCESS EXCLUSIVE MODE;
}

step "coor-read-dist-table"
{
	SELECT COUNT(*) FROM dist_table;
}

step "coor-read-ref-table"
{
	SELECT COUNT(*) FROM ref_table;
}

step "coor-acquire-aggresive-lock-on-partitioned-table"
{
	LOCK partitioned_table IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-partitioned-table-with-*-syntax"
{
	LOCK partitioned_table * IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-only-partitioned-table"
{
	LOCK ONLY partitioned_table IN ACCESS EXCLUSIVE MODE;
}

step "coor-acquire-aggresive-lock-on-ref-table"
{
	LOCK ref_table IN ACCESS EXCLUSIVE MODE;
}

step "coor-rollback"
{
	ROLLBACK;
}

// worker 1 xact session
session "w1"

step "w1-start-session-level-connection"
{
    SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "w1-begin"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "w1-read-dist-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM dist_table');
}

step "w1-read-ref-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM ref_table');
}

step "w1-read-citus-local-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM citus_local_table');
}

step "w1-acquire-aggressive-lock-dist-table" {
	SELECT run_commands_on_session_level_connection_to_node('LOCK dist_table IN ACCESS EXCLUSIVE MODE');
}

step "w1-lock-reference-table"
{
	SELECT run_commands_on_session_level_connection_to_node('LOCK ref_table IN ACCESS EXCLUSIVE MODE');
}

step "w1-read-partitioned-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM partitioned_table');
}

step "w1-read-partition-of-partitioned-table"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM partition_1');
}

step "w1-read-main-view"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT COUNT(*) FROM main_view');
}

step "w1-rollback"
{
	SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "w1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

// worker 2 xact session
session "w2"

step "w2-start-session-level-connection"
{
    SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "w2-begin"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "w2-acquire-aggressive-lock-dist-table" {
	SELECT run_commands_on_session_level_connection_to_node('LOCK dist_table IN ACCESS EXCLUSIVE MODE');
}

step "w2-rollback"
{
	SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "w2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

permutation "coor-begin" "coor-acquire-aggresive-lock-on-dist-table" "w1-start-session-level-connection" "w1-begin" "w1-read-dist-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-dist-table" "w1-start-session-level-connection" "w1-begin" "w1-acquire-aggressive-lock-dist-table" "coor-rollback" "coor-read-dist-table" "w1-rollback" "w1-stop-connection"
permutation "w1-start-session-level-connection" "w1-begin" "w1-acquire-aggressive-lock-dist-table" "coor-begin" "coor-acquire-aggresive-lock-on-dist-table-nowait" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "w1-start-session-level-connection" "w1-begin" "w2-start-session-level-connection" "w2-begin" "w1-acquire-aggressive-lock-dist-table" "w2-acquire-aggressive-lock-dist-table" "w1-rollback" "w1-read-dist-table" "w2-rollback" "w1-stop-connection" "w2-stop-connection"
permutation "coor-begin" "coor-acquire-weak-lock-on-dist-table" "w1-start-session-level-connection" "w1-begin" "w1-read-dist-table" "w1-acquire-aggressive-lock-dist-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "w1-start-session-level-connection" "w1-begin" "w1-lock-reference-table" "coor-begin" "coor-read-ref-table" "w1-rollback" "coor-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-view" "w1-start-session-level-connection" "w1-begin" "w1-read-dist-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-view" "w1-start-session-level-connection" "w1-begin" "w1-acquire-aggressive-lock-dist-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-view" "w1-start-session-level-connection" "w1-begin" "w1-read-ref-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-only-view" "w1-start-session-level-connection" "w1-begin" "w1-read-ref-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "w1-start-session-level-connection" "w1-begin" "w1-acquire-aggressive-lock-dist-table" "coor-begin" "coor-acquire-aggresive-lock-on-view-nowait" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-lock-all" "w1-start-session-level-connection" "w1-begin" "w1-read-citus-local-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-partitioned-table" "w1-start-session-level-connection" "w1-begin" "w1-read-partitioned-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-partitioned-table" "w1-start-session-level-connection" "w1-begin" "w1-read-partition-of-partitioned-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-partitioned-table-with-*-syntax" "w1-start-session-level-connection" "w1-begin" "w1-read-partition-of-partitioned-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-only-partitioned-table" "w1-start-session-level-connection" "w1-begin" "w1-read-partitioned-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-only-partitioned-table" "w1-start-session-level-connection" "w1-begin" "w1-read-partition-of-partitioned-table" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-acquire-aggresive-lock-on-ref-table" "w1-start-session-level-connection" "w1-begin" "w1-read-main-view" "coor-rollback" "w1-rollback" "w1-stop-connection"
permutation "coor-begin" "coor-read-dist-table" "w2-start-session-level-connection" "w2-begin" "w1-start-session-level-connection" "w1-begin" "w2-acquire-aggressive-lock-dist-table" "w1-acquire-aggressive-lock-dist-table" "coor-rollback" "w2-rollback" "w1-rollback" "w1-stop-connection" "w2-stop-connection"
