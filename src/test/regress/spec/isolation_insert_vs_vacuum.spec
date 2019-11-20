setup
{
    SET citus.shard_replication_factor TO 1;
    CREATE TABLE test_insert_vacuum(column1 int, column2 int);
    SELECT create_distributed_table('test_insert_vacuum', 'column1');
}

teardown
{
    DROP TABLE test_insert_vacuum;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_insert_vacuum VALUES(1, 1);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-vacuum-analyze"
{
    VACUUM ANALYZE test_insert_vacuum;
}

step "s2-vacuum-full"
{
    VACUUM FULL test_insert_vacuum;
}

// INSERT and VACUUM ANALYZE should not block each other.
permutation "s1-begin" "s1-insert" "s2-vacuum-analyze" "s1-commit"

// INSERT and VACUUM FULL should block each other.
permutation "s1-begin" "s1-insert" "s2-vacuum-full" "s1-commit"

