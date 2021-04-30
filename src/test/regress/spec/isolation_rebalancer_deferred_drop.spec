// we use 15 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{

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

  SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();

CREATE OR REPLACE FUNCTION master_defer_delete_shards()
    RETURNS int
    LANGUAGE C STRICT
    AS 'citus', $$master_defer_delete_shards$$;
COMMENT ON FUNCTION master_defer_delete_shards()
    IS 'remove orphaned shards';

    SET citus.next_shard_id to 120000;
	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;
    SET citus.defer_drop_after_shard_move TO ON;
	CREATE TABLE t1 (x int PRIMARY KEY, y int);
	SELECT create_distributed_table('t1', 'x');

	SELECT get_shard_id_for_distribution_column('t1', 15) INTO selected_shard;
}

teardown
{
  SELECT citus_internal.restore_isolation_tester_func();

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
        SET citus.defer_drop_after_shard_move TO ON;
    	SELECT master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638);
}

step "s1-drop-marked-shards"
{
    SELECT public.master_defer_delete_shards();
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

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
    SELECT public.master_defer_delete_shards();
}

permutation "s1-begin" "s1-move-placement" "s1-drop-marked-shards" "s2-drop-marked-shards" "s1-commit"
permutation "s1-begin" "s1-move-placement" "s2-drop-marked-shards" "s1-drop-marked-shards" "s1-commit"
permutation "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-lock-table-on-worker" "s1-drop-marked-shards" "s1-commit" "s2-stop-connection"
