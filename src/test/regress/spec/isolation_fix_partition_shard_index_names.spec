setup
{
    CREATE TABLE dist_partitioned_table(a INT, created_at timestamptz) PARTITION BY RANGE (created_at);
    SELECT create_distributed_table('dist_partitioned_table', 'a');
}

teardown
{
    DROP TABLE IF EXISTS dist_partitioned_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-drop-table"
{
    DROP TABLE dist_partitioned_table;
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

step "s2-fix-partition-shard-index-names"
{
    SET client_min_messages TO NOTICE;
    SELECT fix_partition_shard_index_names('dist_partitioned_table'::regclass);
}

step "s2-fix-all-partition-shard-index-names"
{
    SET client_min_messages TO NOTICE;
    SELECT fix_all_partition_shard_index_names();
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-drop-table" "s2-fix-partition-shard-index-names" "s1-commit"
permutation "s1-begin" "s1-drop-table" "s2-fix-all-partition-shard-index-names" "s1-commit"
permutation "s2-begin" "s2-fix-partition-shard-index-names" "s1-drop-table" "s2-commit"
permutation "s2-begin" "s2-fix-all-partition-shard-index-names" "s1-drop-table" "s2-commit"
