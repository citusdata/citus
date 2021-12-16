setup
{
    CREATE TABLE columnar_table (a INT, b INT) USING columnar;
}

teardown
{
    DROP TABLE IF EXISTS columnar_table CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO columnar_table SELECT i, 1000*i FROM generate_series(1, 10) i;
}

step "s1-commit"
{
    COMMIT;
}

step "s1-rollback"
{
    ROLLBACK;
}

step "s1-reset-table"
{
    DROP INDEX IF EXISTS idx_s2, conc_s2_idx, conc_unique_s2_idx, unique_idx_s2, conc_partial_s2_idx;
    TRUNCATE columnar_table;
}

step "s1-force-index-scan"
{
    SET enable_seqscan TO OFF;
    SET columnar.enable_custom_scan TO OFF;
    SET enable_indexscan TO ON;
}

step "s1-check-test-1-2"
{
    SELECT SUM(a)=30 FROM columnar_table WHERE a=5 OR a=25;
}

step "s1-check-test-3"
{
    SELECT COUNT(a)=0 FROM columnar_table WHERE a=25;
    SELECT SUM(a)=55 FROM columnar_table WHERE a=5 OR a=15 OR a=35;
}

step "s1-check-test-4"
{
    SELECT COUNT(a)=0 FROM columnar_table WHERE a=5 OR a=15;
    SELECT SUM(a)=225 FROM columnar_table WHERE a=25 OR a=35 OR a=45 OR a=55 OR a=65;
}

session "s2"

step "s2-create-index"
{
    CREATE INDEX idx_s2 ON columnar_table (a);
}

step "s2-create-unique-index"
{
    CREATE UNIQUE INDEX unique_idx_s2 ON columnar_table (a);
}

step "s2-create-index-concurrently"
{
    CREATE INDEX CONCURRENTLY conc_s2_idx ON columnar_table(a);
}

step "s2-create-unique-index-concurrently"
{
    CREATE UNIQUE INDEX CONCURRENTLY conc_unique_s2_idx ON columnar_table(a);
}

step "s2-create-partial-concurrently"
{
    CREATE INDEX CONCURRENTLY conc_partial_s2_idx ON columnar_table(a)
    WHERE a > 50 AND a <= 80;
}

step "s2-reindex-unique-concurrently"
{
    REINDEX INDEX CONCURRENTLY unique_idx_s2;
}

step "s2-reindex-concurrently"
{
    REINDEX INDEX CONCURRENTLY idx_s2;
}

session "s3"

step "s3-begin"
{
    BEGIN;
}

step "s3-rollback"
{
    ROLLBACK;
}

step "s3-insert"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(21, 30) i;
}

step "s3-commit"
{
    COMMIT;
}

session "s4"

step "s4-insert-1"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(31, 40) i;
}

step "s4-insert-2"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(41, 50) i;
}

step "s4-insert-3"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(51, 60) i;
}

step "s4-insert-4"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(61, 70) i;
}

step "s4-insert-5"
{
    -- Insert values conflicting with "s1-insert" so that concurrent index
    -- build leaves an invalid index behind.
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(1, 10) i;
}

session "s5"

step "s5-insert"
{
    INSERT INTO columnar_table SELECT i, i*1000 FROM generate_series(11, 20) i;
}

step "s5-begin"
{
    BEGIN;
}

step "s5-commit"
{
    COMMIT;
}

step "s5-rollback"
{
    ROLLBACK;
}

// CREATE INDEX (without CONCURRENTLY)
permutation "s1-begin" "s1-insert" "s2-create-index" "s3-insert" "s1-commit" "s1-force-index-scan" "s1-check-test-1-2" "s1-reset-table"

// Start a session that executes INSERT in a transaction block before
// CREATE INDEX / REINDEX CONCURRENTLY so that the latter one blocks.

// CREATE INDEX CONCURRENTLY
permutation "s1-begin" "s1-insert" "s2-create-index-concurrently" "s3-insert" "s1-commit" "s1-force-index-scan" "s1-check-test-1-2" "s1-reset-table"
permutation "s1-begin" "s1-insert" "s2-create-index-concurrently" "s5-begin" "s5-insert" "s3-begin" "s3-insert" "s1-commit" "s4-insert-1" "s5-commit" "s3-rollback" "s1-force-index-scan" "s1-check-test-3" "s1-reset-table"
permutation "s4-insert-4" "s1-begin" "s1-insert" "s4-insert-2" "s2-create-index-concurrently" "s4-insert-3" "s5-begin" "s5-insert" "s3-begin" "s3-insert" "s1-rollback" "s4-insert-1" "s5-rollback" "s3-commit" "s1-force-index-scan" "s1-check-test-4" "s1-reset-table"
permutation "s4-insert-4" "s1-begin" "s1-insert" "s2-create-partial-concurrently" "s4-insert-1" "s1-rollback" "s1-reset-table"

// similar tests with REINDEX INDEX CONCURRENTLY
permutation "s2-create-index" "s1-begin" "s1-insert" "s2-reindex-concurrently" "s3-insert" "s1-commit" "s1-force-index-scan" "s1-check-test-1-2" "s1-reset-table"
permutation "s2-create-index" "s1-begin" "s1-insert" "s2-reindex-concurrently" "s5-begin" "s5-insert" "s3-begin" "s3-insert" "s1-commit" "s4-insert-1" "s5-commit" "s3-rollback" "s1-force-index-scan" "s1-check-test-3" "s1-reset-table"
permutation "s2-create-index" "s4-insert-4" "s1-begin" "s1-insert" "s4-insert-2" "s2-reindex-concurrently" "s4-insert-3" "s5-begin" "s5-insert" "s3-begin" "s3-insert" "s1-rollback" "s4-insert-1" "s5-rollback" "s3-commit" "s1-force-index-scan" "s1-check-test-4" "s1-reset-table"

// CREATE INDEX / REINDEX CONCURRENTLY fails due to duplicate values
permutation "s4-insert-5" "s1-begin" "s1-insert" "s2-create-unique-index-concurrently" "s1-commit" "s1-reset-table"
permutation "s2-create-unique-index" "s4-insert-5" "s1-begin" "s1-insert" "s2-reindex-unique-concurrently" "s1-commit" "s1-reset-table"
