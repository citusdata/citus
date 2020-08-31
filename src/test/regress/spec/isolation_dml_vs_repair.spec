setup
{
    CREATE TABLE test_dml_vs_repair (test_id integer NOT NULL, data int);
    SELECT master_create_distributed_table('test_dml_vs_repair', 'test_id', 'hash');
    SELECT master_create_worker_shards('test_dml_vs_repair', 1, 2);
}

teardown
{
    DROP TABLE IF EXISTS test_dml_vs_repair CASCADE;
}

session "s1"

setup
{
    DEALLOCATE all;
    TRUNCATE test_dml_vs_repair;
    PREPARE insertone AS INSERT INTO test_dml_vs_repair VALUES(1, 1);
    PREPARE insertall AS INSERT INTO test_dml_vs_repair SELECT test_id, data+1 FROM test_dml_vs_repair;
}

step "s1-begin"
{
    BEGIN;
}

step "s1-insertone"
{
    INSERT INTO test_dml_vs_repair VALUES(1, 1);
}

step "s1-prepared-insertone"
{
    EXECUTE insertone;
}

step "s1-insertall"
{
    INSERT INTO test_dml_vs_repair SELECT test_id, data+1 FROM test_dml_vs_repair;
}

step "s1-prepared-insertall"
{
    EXECUTE insertall;
}

step "s1-display"
{
    SELECT * FROM test_dml_vs_repair WHERE test_id = 1 ORDER BY test_id;
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

step "s2-invalidate-57637"
{
    UPDATE pg_dist_shard_placement SET shardstate = '3' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_dml_vs_repair'::regclass) AND nodeport = 57637;
}

step "s2-invalidate-57638"
{
    UPDATE pg_dist_shard_placement SET shardstate = '3' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_dml_vs_repair'::regclass) AND nodeport = 57638;
}

step "s2-revalidate-57638"
{
    UPDATE pg_dist_shard_placement SET shardstate = '1' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_dml_vs_repair'::regclass) AND nodeport = 57638;
}

step "s2-repair"
{
    SELECT master_copy_shard_placement((SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_dml_vs_repair'::regclass), 'localhost', 57638, 'localhost', 57637);
}

step "s2-commit"
{
    COMMIT;
}

// verify that repair is blocked by ongoing modifying simple transaction
permutation "s2-invalidate-57637" "s1-begin" "s1-insertone" "s2-repair" "s1-commit"

// verify that repair is blocked by ongoing modifying insert...select transaction
permutation "s1-insertone" "s2-invalidate-57637" "s1-begin" "s1-insertall" "s2-repair" "s1-commit"

// verify that modifications wait for shard repair
permutation "s2-invalidate-57637" "s2-begin" "s2-repair" "s1-insertone" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"

// verify that prepared plain modifications wait for shard repair
permutation "s2-invalidate-57637" "s1-prepared-insertone" "s2-begin" "s2-repair" "s1-prepared-insertone" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"

// verify that prepared INSERT ... SELECT waits for shard repair
permutation "s2-invalidate-57637" "s1-insertone" "s1-prepared-insertall" "s2-begin" "s2-repair" "s1-prepared-insertall" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"
