setup
{
    CREATE TABLE test_table (test_id integer NOT NULL, data int);
    SELECT master_create_distributed_table('test_table', 'test_id', 'hash');
    SELECT master_create_worker_shards('test_table', 1, 2);
}

teardown
{
    DROP TABLE IF EXISTS test_table CASCADE;
}

session "s1"

setup
{
    DEALLOCATE all;
    TRUNCATE test_table;
    PREPARE insertone AS INSERT INTO test_table VALUES(1, 1);
    PREPARE insertall AS INSERT INTO test_table SELECT test_id, data+1 FROM test_table;
}

step "s1-begin"
{
    BEGIN;
}

step "s1-insertone"
{
    INSERT INTO test_table VALUES(1, 1);
}

step "s1-prepared-insertone"
{
    EXECUTE insertone;
}

step "s1-insertall"
{
    INSERT INTO test_table SELECT test_id, data+1 FROM test_table;
}

step "s1-prepared-insertall"
{
    EXECUTE insertall;
}

step "s1-display"
{
    SELECT * FROM test_table WHERE test_id = 1;
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
    UPDATE pg_dist_shard_placement SET shardstate = '3' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_table'::regclass) AND nodeport = 57637;
}

step "s2-revalidate-57637"
{
    UPDATE pg_dist_shard_placement SET shardstate = '1' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_table'::regclass) AND nodeport = 57637;
}

step "s2-invalidate-57638"
{
    UPDATE pg_dist_shard_placement SET shardstate = '3' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_table'::regclass) AND nodeport = 57638;
}

step "s2-revalidate-57638"
{
    UPDATE pg_dist_shard_placement SET shardstate = '1' WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_table'::regclass) AND nodeport = 57638;
}

step "s2-repair"
{
    SELECT master_copy_shard_placement((SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_table'::regclass), 'localhost', 57638, 'localhost', 57637);
}

step "s2-commit"
{
    COMMIT;
}

# verify that repair is blocked by ongoing modifying simple transaction
permutation "s2-invalidate-57637" "s1-begin" "s1-insertone" "s2-repair" "s1-commit"

# verify that repair is blocked by ongoing modifying insert...select transaction
permutation "s1-insertone" "s2-invalidate-57637" "s1-begin" "s1-insertall" "s2-repair" "s1-commit"

# verify that modifications wait for shard repair
permutation "s2-invalidate-57637" "s2-begin" "s2-repair" "s1-insertone" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"

# verify that prepared plain modifications wait for shard repair (and then fail to avoid race)
permutation "s2-invalidate-57637" "s1-prepared-insertone" "s2-begin" "s2-repair" "s1-prepared-insertone" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"

# verify that prepared INSERT ... SELECT waits for shard repair  (and then fail to avoid race)
permutation "s2-invalidate-57637" "s1-insertone" "s1-prepared-insertall" "s2-begin" "s2-repair" "s1-prepared-insertall" "s2-commit" "s2-invalidate-57638" "s1-display" "s2-invalidate-57637" "s2-revalidate-57638" "s1-display"
