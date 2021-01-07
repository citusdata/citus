// we use 15 as partition key values through out the test
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

  -- start_metadata_sync_to_node can not be run inside a transaction block
  -- following is a workaround to overcome that
  -- port numbers are hard coded at the moment
  SELECT master_run_on_worker(
          ARRAY['localhost']::text[],
          ARRAY[57636]::int[],
          ARRAY[format('SELECT start_metadata_sync_to_node(''%s'', %s)', nodename, nodeport)]::text[],
          false)
  FROM pg_dist_node;

  SET citus.replication_model to streaming;
  SET citus.shard_replication_factor TO 1;

	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE logical_replicate_placement (x int PRIMARY KEY, y int);
	SELECT create_distributed_table('logical_replicate_placement', 'x');

	SELECT get_shard_id_for_distribution_column('logical_replicate_placement', 15) INTO selected_shard;
}

teardown
{
	DROP TABLE selected_shard;
	DROP TABLE logical_replicate_placement;

  SELECT citus_internal.restore_isolation_tester_func();
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-move-placement"
{
    	SELECT master_move_shard_placement(get_shard_id_for_distribution_column, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes') FROM selected_shard;
}

step "s1-commit"
{
	COMMIT;
}

step "s1-select"
{
  SELECT * FROM logical_replicate_placement order by y;
}

step "s1-insert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172);
}

step "s1-get-shard-distribution"
{
    select nodeport from pg_dist_placement inner join pg_dist_node on(pg_dist_placement.groupid = pg_dist_node.groupid) where shardid in (SELECT * FROM selected_shard) order by nodeport;
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

step "s2-select"
{
    SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM logical_replicate_placement ORDER BY y');
}

step "s2-insert"
{
    SELECT run_commands_on_session_level_connection_to_node('INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172)');
}

step "s2-delete"
{
    SELECT run_commands_on_session_level_connection_to_node('DELETE FROM logical_replicate_placement');
}

step "s2-update"
{
    SELECT run_commands_on_session_level_connection_to_node('UPDATE logical_replicate_placement SET y = y + 1');
}

step "s2-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s2-stop-connection"
{
  SELECT stop_session_level_connection_to_node();
}

permutation "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s1-move-placement"  "s2-commit-worker"  "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-get-shard-distribution" "s2-stop-connection"

