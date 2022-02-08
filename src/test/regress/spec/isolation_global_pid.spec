#include "isolation_mx_common.include.spec"

setup
{
    SET citus.next_shard_id TO 12345000;
    CREATE TABLE dist_table (a INT, b INT);
    SELECT create_distributed_table('dist_table', 'a', shard_count:=4);
}

teardown
{
    DROP TABLE dist_table;
    SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-coordinator-begin"
{
    BEGIN;
}

step "s1-coordinator-select"
{
    SET citus.enable_local_execution TO off;
    SET citus.force_max_query_parallelization TO ON;
    SELECT * FROM dist_table;
}

step "s1-coordinator-commit"
{
    COMMIT;
}

step "s1-start-session-level-connection"
{

    SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-worker-begin"
{
    SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s1-worker-select"
{
    SELECT run_commands_on_session_level_connection_to_node('SET citus.enable_local_execution TO off; SET citus.force_max_query_parallelization TO ON; SELECT * FROM dist_table');
}

step "s1-worker-commit"
{
    SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-stop-session-level-connection"
{
    SELECT stop_session_level_connection_to_node();
}

session "s2"

step "s2-coordinator-citus_dist_stat_activity"
{
    SELECT global_pid != 0 FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%';
}

step "s2-coordinator-citus_worker_stat_activity"
{
    SELECT query FROM citus_worker_stat_activity() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    )
    ORDER BY 1;
}

step "s2-coordinator-get_all_active_transactions"
{
    SELECT count(*) FROM get_all_active_transactions() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    );
}

step "s2-coordinator-get_global_active_transactions"
{
    SELECT count(*) FROM get_global_active_transactions() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    )
    AND transaction_number != 0;
}


// worker - coordinator
permutation "s1-start-session-level-connection" "s1-worker-begin" "s1-worker-select" "s2-coordinator-citus_dist_stat_activity" "s2-coordinator-citus_worker_stat_activity" "s1-worker-commit" "s1-stop-session-level-connection"

// coordinator - coordinator
permutation "s1-coordinator-begin" "s1-coordinator-select" "s2-coordinator-citus_dist_stat_activity" "s2-coordinator-citus_worker_stat_activity" "s2-coordinator-get_all_active_transactions" "s2-coordinator-get_global_active_transactions" "s1-coordinator-commit"
