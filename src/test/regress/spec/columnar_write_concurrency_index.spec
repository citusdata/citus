setup
{
    CREATE TABLE write_concurrency_index (a text, b int unique,
    EXCLUDE USING hash (a WITH =)) USING columnar;
}

teardown
{
    DROP TABLE IF EXISTS write_concurrency_index CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-commit"
{
    COMMIT;
}

step "s1-rollback"
{
    ROLLBACK;
}

step "s1-insert-1"
{
    INSERT INTO write_concurrency_index SELECT (3*s)::text, s FROM generate_series(1,2) s;
}

step "s1-insert-2"
{
    INSERT INTO write_concurrency_index SELECT s::text, 3*s FROM generate_series(1,2) s;
}

step "s1-copy-1"
{
    COPY write_concurrency_index(b) FROM PROGRAM 'seq 1 2';
}

step "s1-select-all"
{
    SELECT * FROM write_concurrency_index ORDER BY a,b;
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

step "s2-commit"
{
    COMMIT;
}

step "s2-rollback"
{
    ROLLBACK;
}

step "s2-insert-1"
{
    INSERT INTO write_concurrency_index SELECT (2*s)::text, s FROM generate_series(1,4) s;
}

step "s2-insert-2"
{
    INSERT INTO write_concurrency_index SELECT (5*s)::text, s FROM generate_series(1,2) s;
}

step "s2-insert-3"
{
    INSERT INTO write_concurrency_index SELECT (5*s)::text, s FROM generate_series(3,4) s;
}

step "s2-insert-4"
{
    INSERT INTO write_concurrency_index SELECT s::text, 2*s FROM generate_series(1,4) s;
}

step "s2-insert-5"
{
    INSERT INTO write_concurrency_index SELECT s::text, 5*s FROM generate_series(1,2) s;
}

step "s2-insert-6"
{
    INSERT INTO write_concurrency_index SELECT s::text, 5*s FROM generate_series(3,4) s;
}

step "s2-copy-1"
{
    COPY write_concurrency_index(b) FROM PROGRAM 'seq 1 4';
}

step "s2-copy-2"
{
    COPY write_concurrency_index(b) FROM PROGRAM 'seq 3 4';
}

step "s2-index-select-all-b"
{
    SET enable_seqscan TO OFF;
    SET columnar.enable_custom_scan TO OFF;
    SELECT b FROM write_concurrency_index ORDER BY 1;
}

session "s3"

step "s3-insert-1"
{
    INSERT INTO write_concurrency_index SELECT (7*s)::text, s FROM generate_series(3,4) s;
}

step "s3-insert-2"
{
    INSERT INTO write_concurrency_index SELECT (7*s)::text, s FROM generate_series(2,3) s;
}

step "s3-insert-3"
{
    INSERT INTO write_concurrency_index SELECT s::text, 7*s FROM generate_series(3,4) s;
}

step "s3-insert-4"
{
    INSERT INTO write_concurrency_index SELECT s::text, 7*s FROM generate_series(2,3) s;
}

step "s3-index-select-all-b"
{
    SET enable_seqscan TO OFF;
    SET columnar.enable_custom_scan TO OFF;
    SELECT b FROM write_concurrency_index ORDER BY 1;
}

// unique (btree) on int column
permutation "s1-begin" "s1-insert-1" "s2-copy-1" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-copy-1" "s2-insert-1" "s1-rollback" "s1-select-all"
permutation "s1-begin" "s1-copy-1" "s2-insert-2" "s3-insert-1" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-insert-1" "s2-insert-2" "s3-insert-1" "s1-rollback" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-insert-1" "s2-insert-3" "s3-insert-2" "s1-commit" "s2-rollback" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-copy-1" "s2-copy-2" "s3-insert-2" "s1-rollback" "s2-commit" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-insert-1" "s2-copy-2" "s3-insert-2" "s1-rollback" "s2-rollback" "s1-select-all"

// exclusion (hash) on text column that checks against duplicate values
permutation "s1-begin" "s1-insert-2" "s2-insert-4" "s1-rollback" "s1-select-all"
permutation "s1-begin" "s1-insert-2" "s2-insert-5" "s3-insert-3" "s1-commit" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-insert-2" "s2-insert-6" "s3-insert-4" "s1-commit" "s2-rollback" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-insert-2" "s2-insert-6" "s3-insert-4" "s1-rollback" "s2-rollback" "s1-select-all"

// make sure that pending writes are not visible to other backends
permutation "s1-begin" "s1-insert-1" "s2-index-select-all-b" "s1-rollback"
permutation "s1-begin" "s2-begin" "s1-insert-1" "s2-copy-2" "s2-index-select-all-b" "s3-index-select-all-b" "s1-commit" "s2-index-select-all-b" "s2-rollback"

// force flushing write state of s1 before inserting some more data via other sessions
permutation "s1-begin" "s2-begin" "s1-insert-1" "s1-select-all" "s2-insert-1" "s1-commit" "s2-rollback"
permutation "s1-begin" "s2-begin" "s1-insert-1" "s1-select-all" "s2-insert-1" "s1-rollback" "s2-rollback"
permutation "s1-begin" "s1-copy-1" "s1-select-all" "s2-insert-2" "s3-insert-1" "s1-rollback" "s1-select-all"
permutation "s1-begin" "s1-insert-2" "s1-select-all" "s2-insert-5" "s3-insert-3" "s1-commit" "s1-select-all"
permutation "s1-begin" "s2-begin" "s1-insert-2" "s1-select-all" "s2-insert-6" "s3-insert-4" "s1-rollback" "s2-rollback" "s1-select-all"

// test with repeatable read isolation mode
permutation "s1-begin" "s2-begin-repeatable" "s1-insert-1" "s2-insert-1" "s1-commit" "s2-rollback"
