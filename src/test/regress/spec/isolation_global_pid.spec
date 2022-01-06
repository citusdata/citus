setup
{
    CREATE TABLE dist_table (a INT, b INT);
    SELECT create_distributed_table('dist_table', 'a');
    SET citus.force_max_query_parallelization TO ON;
}

teardown
{
    DROP TABLE dist_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-select"
{
    SELECT * FROM dist_table;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-citus_dist_stat_activity"
{
    SELECT global_pid != 0 FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%';
}

step "s2-citus_worker_stat_activity"
{
    SELECT count(*) FROM citus_worker_stat_activity() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    );
}

step "s2-get_all_active_transactions"
{
    SELECT count(*) FROM get_all_active_transactions() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    );
}

step "s2-get_global_active_transactions"
{
    SELECT count(*) FROM get_global_active_transactions() WHERE global_pid IN (
        SELECT global_pid FROM citus_dist_stat_activity() WHERE query LIKE '%SELECT * FROM dist\_table%'
    );
}


permutation "s1-begin" "s1-select" "s2-citus_dist_stat_activity" "s1-commit"
permutation "s1-begin" "s1-select" "s2-citus_worker_stat_activity" "s1-commit"
permutation "s1-begin" "s1-select" "s2-get_all_active_transactions" "s1-commit"
permutation "s1-begin" "s1-select" "s2-get_global_active_transactions" "s1-commit"
