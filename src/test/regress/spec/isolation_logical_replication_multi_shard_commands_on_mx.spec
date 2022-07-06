#include "isolation_mx_common.include.spec"

// we use 15 as partition key values through out the test
// so setting the corresponding shard here is useful

setup
{
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
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-move-placement"
{
    	SELECT master_move_shard_placement(get_shard_id_for_distribution_column, 'localhost', 57637, 'localhost', 57638) FROM selected_shard;
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
    select nodeport from pg_dist_placement inner join pg_dist_node on(pg_dist_placement.groupid = pg_dist_node.groupid) where shardstate != 4 AND shardid in (SELECT * FROM selected_shard) order by nodeport;
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

session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

##// nonblocking tests lie below ###

// move placement first
// the following tests show the non-blocking modifications while shard is being moved
// in fact, the shard move blocks the writes for a very short duration of time
// by using an advisory and allowing the other commands continue to run, we prevent
// the modifications to block on that blocking duration

permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s2-commit-worker" "s3-release-advisory-lock" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update" "s2-commit-worker" "s3-release-advisory-lock" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete" "s2-commit-worker" "s3-release-advisory-lock" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s2-commit-worker" "s3-release-advisory-lock" "s1-commit" "s1-get-shard-distribution" "s2-stop-connection"

// move placement second
// force shard-move to be a blocking call intentionally
permutation "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-insert" "s1-move-placement"  "s2-commit-worker"  "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-select" "s1-get-shard-distribution" "s2-stop-connection"
permutation "s1-insert" "s1-begin" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select" "s1-move-placement" "s2-commit-worker" "s1-commit" "s1-get-shard-distribution" "s2-stop-connection"

