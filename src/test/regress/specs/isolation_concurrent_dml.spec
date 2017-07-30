setup
{
    CREATE TABLE test_concurrent_dml (test_id integer NOT NULL, data text);
    SELECT master_create_distributed_table('test_concurrent_dml', 'test_id', 'hash');
    SELECT master_create_worker_shards('test_concurrent_dml', 4, 2);
}

teardown
{
    DROP TABLE IF EXISTS test_concurrent_dml CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert"
{
    INSERT INTO test_concurrent_dml VALUES(1);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-update"
{
    UPDATE test_concurrent_dml SET data = 'blarg' WHERE test_id = 1;
}

# verify that an in-progress insert blocks concurrent updates
permutation "s1-begin" "s1-insert" "s2-update" "s1-commit"
# but an insert without xact will not block
permutation "s1-insert" "s2-update"
