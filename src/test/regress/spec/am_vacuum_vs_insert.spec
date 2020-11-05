setup
{
    CREATE TABLE test_vacuum_vs_insert (a int, b int) USING cstore_tableam;
}

teardown
{
    DROP TABLE IF EXISTS test_vacuum_vs_insert CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_vacuum_vs_insert SELECT i, 2 * i FROM generate_series(1, 3) i;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-vacuum"
{
    VACUUM VERBOSE test_vacuum_vs_insert;
}

step "s2-vacuum-full"
{
    VACUUM FULL VERBOSE test_vacuum_vs_insert;
}

step "s2-select"
{
    SELECT * FROM test_vacuum_vs_insert;
}

permutation "s1-insert" "s1-begin" "s1-insert" "s2-vacuum" "s1-commit" "s2-select"
permutation "s1-insert" "s1-begin" "s1-insert" "s2-vacuum-full" "s1-commit" "s2-select"
