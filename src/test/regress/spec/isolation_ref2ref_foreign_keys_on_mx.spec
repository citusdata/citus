#include "isolation_mx_common.spec"

setup 
{
    CREATE TABLE ref_table_1(id int PRIMARY KEY, value int);
	SELECT create_reference_table('ref_table_1');

    CREATE TABLE ref_table_2(id int PRIMARY KEY, value int REFERENCES ref_table_1(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_reference_table('ref_table_2');

    CREATE TABLE ref_table_3(id int PRIMARY KEY, value int REFERENCES ref_table_2(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_reference_table('ref_table_3');

    INSERT INTO ref_table_1 VALUES (1, 1), (3, 3), (5, 5);
    INSERT INTO ref_table_2 SELECT * FROM ref_table_1;
    INSERT INTO ref_table_3 SELECT * FROM ref_table_2;
}

teardown
{
	DROP TABLE ref_table_1, ref_table_2, ref_table_3;
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

step "s1-view-locks"
{
    SELECT * FROM master_run_on_worker(
		ARRAY['localhost']::text[],
		ARRAY[57637]::int[],
		ARRAY[$$
          SELECT array_agg(ROW(t.mode, t.count) ORDER BY t.mode) FROM
          (SELECT mode, count(*) count FROM pg_locks 
           WHERE locktype='advisory' GROUP BY mode) t$$]::text[],
		false);
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
	SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN'); 
}

step "s2-insert-table-1"
{
    SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table_1 VALUES (7, 7)');
}

step "s2-update-table-1"
{
    SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table_1 SET id = 2 WHERE id = 1');
}

step "s2-delete-table-1"
{
    SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table_1 WHERE id = 1');
}

step "s2-insert-table-2"
{
    SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table_2 VALUES (7, 5)');
}

step "s2-update-table-2"
{
    SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table_2 SET id = 2 WHERE id = 1');
}

step "s2-delete-table-2"
{
    SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table_2 WHERE id = 1');
}

step "s2-insert-table-3"
{
    SELECT run_commands_on_session_level_connection_to_node('INSERT INTO ref_table_3 VALUES (7, 5)');
}

step "s2-update-table-3"
{
    SELECT run_commands_on_session_level_connection_to_node('UPDATE ref_table_3 SET id = 2 WHERE id = 1');
}

step "s2-delete-table-3"
{
    SELECT run_commands_on_session_level_connection_to_node('DELETE FROM ref_table_3 WHERE id = 1');
}

step "s2-rollback-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('ROLLBACK');
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

// Case 1. UPDATE/DELETE ref_table_1 should only lock its own shard in Exclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-table-1" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete-table-1" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
// Case 2. Modifying ref_table_2 should also lock ref_table_1 shard in Exclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-table-2" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete-table-2" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
// Case 3. Modifying ref_table_3 should also lock ref_table_1 and ref_table_2 shards in Exclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-table-3" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete-table-3" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
// Case 4. Inserting into ref_table_1 should only lock its own shard in RowExclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-table-1" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
// Case 5. Modifying ref_table_2 should also lock ref_table_1 in RowExclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-table-2" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
// Case 6. Modifying ref_table_2 should also lock ref_table_1 in RowExclusive mode.
permutation "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert-table-3" "s1-start-session-level-connection"  "s1-view-locks" "s2-rollback-worker" "s1-view-locks" "s1-stop-connection" "s2-stop-connection"
