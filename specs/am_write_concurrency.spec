setup
{
    CREATE TABLE test_insert_concurrency (a int, b int) USING cstore_tableam;
}

teardown
{
    DROP TABLE IF EXISTS test_insert_concurrency CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_insert_concurrency SELECT i, 2 * i FROM generate_series(1, 3) i;
}

step "s1-copy"
{
    COPY test_insert_concurrency(a) FROM PROGRAM 'seq 11 13';
}

step "s1-select"
{
    SELECT * FROM test_insert_concurrency ORDER BY a;
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

step "s2-insert"
{
    INSERT INTO test_insert_concurrency SELECT i, 2 * i FROM generate_series(4, 6) i;
}

step "s2-select"
{
    SELECT * FROM test_insert_concurrency ORDER BY a;
}

step "s2-commit"
{
    COMMIT;
}

# writes shouldn't block writes or reads
permutation "s1-begin" "s2-begin" "s1-insert" "s2-insert" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"

# copy vs insert
permutation "s1-begin" "s2-begin" "s1-copy" "s2-insert" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"

# insert vs copy
permutation "s1-begin" "s2-begin" "s2-insert" "s1-copy" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"
