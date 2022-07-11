setup
{
    CREATE TABLE test_concurrent_dml (test_id integer NOT NULL, data text);
	SET citus.shard_replication_factor TO 2;
    SELECT create_distributed_table('test_concurrent_dml', 'test_id', 'hash', shard_count:=4);
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

step "s1-multi-insert"
{
    INSERT INTO test_concurrent_dml VALUES (1), (2);
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

step "s2-update"
{
    UPDATE test_concurrent_dml SET data = 'blarg' WHERE test_id = 1;
}

step "s2-multi-insert-overlap"
{
    INSERT INTO test_concurrent_dml VALUES (1), (4);
}

step "s2-multi-insert"
{
    INSERT INTO test_concurrent_dml VALUES (3), (4);
}

step "s2-commit"
{
    COMMIT;
}

// verify that an in-progress insert blocks concurrent updates
permutation "s1-begin" "s1-insert" "s2-update" "s1-commit"

// but an insert without xact will not block
permutation "s1-insert" "s2-update"

// verify that an in-progress multi-row insert blocks concurrent updates
permutation "s1-begin" "s1-multi-insert" "s2-update" "s1-commit"

// two multi-row inserts that hit same shards will block
permutation "s1-begin" "s1-multi-insert" "s2-multi-insert-overlap" "s1-commit"

// but concurrent multi-row inserts don't block unless shards overlap
permutation "s1-begin" "s2-begin" "s1-multi-insert" "s2-multi-insert" "s1-commit" "s2-commit"
