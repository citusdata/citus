setup
{
    CREATE TABLE test_insert_concurrency (a int, b int) USING columnar;

    CREATE OR REPLACE FUNCTION columnar_relation_storageid(relid oid) RETURNS bigint
        LANGUAGE C STABLE STRICT
        AS 'citus', $$columnar_relation_storageid$$;
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

step "s1-insert-10000-rows"
{
    INSERT INTO test_insert_concurrency SELECT i, 2 * i FROM generate_series(1, 10000) i;
}

step "s1-copy"
{
    COPY test_insert_concurrency(a) FROM PROGRAM 'seq 11 13';
}

step "s1-select"
{
    SELECT * FROM test_insert_concurrency ORDER BY a;
}

step "s1-truncate"
{
    TRUNCATE test_insert_concurrency;
}

step "s1-verify-metadata"
{
    WITH test_insert_concurrency_stripes AS (
      SELECT first_row_number, stripe_num, row_count
      FROM columnar.stripe a, pg_class b
      WHERE columnar_relation_storageid(b.oid)=a.storage_id AND
            relname = 'test_insert_concurrency'
    )
    SELECT
      -- verify that table has two stripes ..
      count(*) = 2 AND
      -- .. and those stripes look like:
      sum(case when stripe_num = 1 AND first_row_number = 150001 AND row_count = 3 then 1 end) = 1 AND
      sum(case when stripe_num = 2 AND first_row_number = 1 AND row_count = 10000 then 1 end) = 1
      AS stripe_metadata_for_test_insert_concurrency_ok
    FROM test_insert_concurrency_stripes;
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

step "s2-begin-repeatable"
{
    BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
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

// writes shouldn't block writes or reads
permutation "s1-begin" "s2-begin" "s1-insert" "s2-insert" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"

// copy vs insert
permutation "s1-begin" "s2-begin" "s1-copy" "s2-insert" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"

// insert vs copy
permutation "s1-begin" "s2-begin" "s2-insert" "s1-copy" "s1-select" "s2-select" "s1-commit" "s2-commit" "s1-select"

# insert vs insert
# Start inserting rows in session 1, reserve first_row_number to be 1 for session 1 but commit session 2 before session 1.
# Then verify that while the stripe written by session 2 has the greater first_row_number, stripe written by session 1 has
# the greater stripe_num. This is because, we reserve stripe_num and first_row_number at different times.
permutation "s1-truncate" "s1-begin" "s1-insert-10000-rows" "s2-begin" "s2-insert" "s2-commit" "s1-commit" "s1-verify-metadata"

permutation "s1-begin" "s2-begin-repeatable" "s1-insert" "s2-insert" "s2-select" "s1-commit" "s2-select" "s2-commit"
