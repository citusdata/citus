// we use 15 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{
    SET citus.enable_metadata_sync TO off;
    CREATE OR REPLACE FUNCTION run_try_drop_marked_resources()
    RETURNS VOID
    AS 'citus'
    LANGUAGE C STRICT VOLATILE;

    CREATE OR REPLACE FUNCTION start_session_level_connection_to_node(text, integer)
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$start_session_level_connection_to_node$$;

    CREATE OR REPLACE FUNCTION run_commands_on_session_level_connection_to_node(text)
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$run_commands_on_session_level_connection_to_node$$;

    CREATE OR REPLACE FUNCTION stop_session_level_connection_to_node()
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$stop_session_level_connection_to_node$$;

CREATE OR REPLACE PROCEDURE isolation_cleanup_orphaned_shards()
    LANGUAGE C
    AS 'citus', $$isolation_cleanup_orphaned_shards$$;
COMMENT ON PROCEDURE isolation_cleanup_orphaned_shards()
    IS 'cleanup orphaned shards';
    RESET citus.enable_metadata_sync;

    SET citus.next_shard_id to 120000;
	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE t1 (x int PRIMARY KEY, y int);
	SELECT create_distributed_table('t1', 'x');

	SELECT get_shard_id_for_distribution_column('t1', 15) INTO selected_shard;
}

teardown
{
  DROP TABLE selected_shard;
  DROP TABLE t1;
}


session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-move-placement"
{
    SELECT master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638);
}

step "s1-move-placement-back"
{
    SET client_min_messages to NOTICE;
    SHOW log_error_verbosity;
    SELECT master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57638, 'localhost', 57637);
}

step "s1-drop-marked-shards"
{
    SET client_min_messages to NOTICE;
    CALL isolation_cleanup_orphaned_shards();
}

step "s1-lock-pg-dist-placement" {
    LOCK TABLE pg_dist_placement IN SHARE ROW EXCLUSIVE MODE;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-drop-old-shards" {
    SELECT run_try_drop_marked_resources();
}

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s2-lock-table-on-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('BEGIN;');
    SELECT run_commands_on_session_level_connection_to_node('LOCK TABLE t1_120000');
}

step "s2-drop-marked-shards"
{
    SET client_min_messages to DEBUG1;
    CALL isolation_cleanup_orphaned_shards();
}


permutation "s1-begin" "s1-move-placement" "s1-drop-marked-shards" "s2-drop-marked-shards" "s1-commit"
permutation "s1-begin" "s1-move-placement" "s2-drop-marked-shards" "s1-drop-marked-shards" "s1-commit"
permutation "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-lock-table-on-worker" "s1-drop-marked-shards" "s1-commit" "s2-stop-connection"
// make sure we give a clear error when we try to replace an orphaned shard that is still in use
permutation "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-lock-table-on-worker" "s1-commit" "s1-begin" "s1-move-placement-back" "s1-commit" "s2-stop-connection"
// make sure we error if we cannot get the lock on pg_dist_placement
permutation "s1-begin" "s1-lock-pg-dist-placement" "s2-drop-old-shards" "s1-commit"
