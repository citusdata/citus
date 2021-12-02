setup
{
    CREATE TABLE dist_partitioned_table(a INT, created_at timestamptz) PARTITION BY RANGE (created_at);
    CREATE TABLE p PARTITION OF dist_partitioned_table FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
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

step "s1-select-from-table"
{
    SELECT * FROM dist_partitioned_table;
}

step "s1-insert-into-table"
{
    INSERT INTO dist_partitioned_table VALUES (0, '2019-01-01');
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

step "s2-create-index"
{
    CREATE INDEX ON dist_partitioned_table USING btree(a);
}

step "s2-create-index-concurrently"
{
    CREATE INDEX CONCURRENTLY ON dist_partitioned_table USING btree(a);
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-drop-table" "s2-fix-partition-shard-index-names" "s1-commit"
permutation "s2-begin" "s2-fix-partition-shard-index-names" "s1-drop-table" "s2-commit"

// CREATE INDEX should not block concurrent reads but should block concurrent writes
permutation "s2-begin" "s2-create-index" "s1-select-from-table" "s2-commit"
permutation "s2-begin" "s2-create-index" "s1-insert-into-table" "s2-commit"

// CREATE INDEX CONCURRENTLY is currently not supported for partitioned tables in PG
// when it's supported, we would want to not block any concurrent reads or writes
permutation "s1-begin" "s1-select-from-table" "s2-create-index-concurrently" "s1-commit"
permutation "s1-begin" "s1-insert-into-table" "s2-create-index-concurrently" "s1-commit"

// running the following just for consistency
permutation "s1-begin" "s1-select-from-table" "s2-create-index" "s1-commit"
permutation "s1-begin" "s1-insert-into-table" "s2-create-index" "s1-commit"
permutation "s2-begin" "s2-create-index-concurrently" "s1-select-from-table" "s2-commit"
permutation "s2-begin" "s2-create-index-concurrently" "s1-insert-into-table" "s2-commit"
