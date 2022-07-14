setup
{
    SET citus.max_cached_conns_per_worker to 0;
    SET citus.shard_replication_factor TO 1;

    CREATE USER test_user_1;

    CREATE USER test_user_2;

    CREATE TABLE test_table(column1 int, column2 int);
    ALTER TABLE test_table OWNER TO test_user_1;
    SELECT create_distributed_table('test_table', 'column1');
}

teardown
{
    BEGIN;
    DROP TABLE IF EXISTS test_table;
    DROP USER test_user_1, test_user_2;
    COMMIT;
}

session "s1"

// due to bug #3785 a second permutation of the isolation test would reuse a cached
// connection bound to the deleted user. This causes the tests to fail with unexplainable
// permission denied errors.
// By setting the cached connections to zero we prevent the use of cached conncetions.
// These steps can be removed once the root cause is solved
step "s1-no-connection-cache" {
    SET citus.max_cached_conns_per_worker to 0;
}

step "s1-grant"
{
    SET ROLE test_user_1;
    GRANT ALL ON test_table TO test_user_2;
}

step "s1-begin"
{
    BEGIN;
    SET ROLE test_user_1;
}

step "s1-index"
{
    CREATE INDEX test_index ON test_table(column1);
}

step "s1-reindex"
{
    REINDEX TABLE test_table;
}

step "s1-drop-index"
{
    DROP INDEX IF EXISTS test_index;
}

step "s1-insert"
{
    UPDATE test_table SET column2 = 1;
}

step "s1-truncate"
{
    TRUNCATE test_table;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

// due to bug #3785 a second permutation of the isolation test would reuse a cached
// connection bound to the deleted user. This causes the tests to fail with unexplainable
// permission denied errors.
// By setting the cached connections to zero we prevent the use of cached conncetions.
// These steps can be removed once the root cause is solved
step "s2-no-connection-cache" {
    SET citus.max_cached_conns_per_worker to 0;
}

step "s2-begin"
{
    BEGIN;
    SET ROLE test_user_2;
}

step "s2-index"
{
    CREATE INDEX test_index ON test_table(column1);
}

step "s2-reindex"
{
    REINDEX TABLE test_table;
}

step "s2-drop-index"
{
    DROP INDEX IF EXISTS test_index;
}

step "s2-insert"
{
    UPDATE test_table SET column2 = 2;
}

step "s2-truncate"
{
    TRUNCATE test_table;
}

step "s2-commit"
{
    COMMIT;
}

// REINDEX
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-begin" "s2-begin" "s2-reindex" "s1-insert" "s2-commit" "s1-commit"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s2-reindex" "s1-insert" "s2-insert" "s2-commit" "s1-commit"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s1-reindex" "s2-insert" "s1-insert" "s1-commit" "s2-commit"

// CREATE INDEX
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-begin" "s2-begin" "s2-index" "s1-insert" "s2-commit" "s1-commit" "s2-drop-index"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s2-insert" "s1-index" "s2-insert" "s2-commit" "s1-commit" "s1-drop-index"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s1-index" "s2-index" "s1-insert" "s1-commit" "s2-commit" "s1-drop-index" "s2-drop-index"

// TRUNCATE
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-begin" "s2-begin" "s2-truncate" "s1-insert" "s2-commit" "s1-commit"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s1-truncate" "s2-insert" "s1-insert" "s1-commit" "s2-commit"
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s1-grant" "s1-begin" "s2-begin" "s1-truncate" "s2-truncate" "s1-commit" "s2-commit"
