setup
{
    CREATE TABLE dist_table(a INT, b INT);
    SELECT create_distributed_table('dist_table', 'a');
}

teardown
{
    DROP TABLE IF EXISTS dist_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-drop-table"
{
    DROP TABLE dist_table;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-citus-update-table-statistics"
{
    SET client_min_messages TO NOTICE;
    SELECT citus_update_table_statistics(logicalrelid) FROM pg_dist_partition;
}

step "s2-citus-shards"
{
    SELECT 1 AS result FROM citus_shards GROUP BY result;
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-drop-table" "s2-citus-update-table-statistics" "s1-commit"
permutation "s1-begin" "s1-drop-table" "s2-citus-shards" "s1-commit"
permutation "s2-begin" "s2-citus-shards" "s1-drop-table" "s2-commit"

// ERROR:  tuple concurrently deleted -- is expected in the following permutation
// Check the explanation at PR #5155 in the following comment
// https://github.com/citusdata/citus/pull/5155#issuecomment-897028194
permutation "s2-begin" "s2-citus-update-table-statistics" "s1-drop-table" "s2-commit"
