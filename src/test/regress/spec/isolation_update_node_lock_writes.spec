setup
{
    SELECT 1 FROM master_add_node('localhost', 57637);

    SET citus.shard_replication_factor TO 1;
    CREATE TABLE update_node(id integer primary key, f1 text);
    SELECT create_distributed_table('update_node', 'id');
}

// we sleep 2 seconds to let isolation test sync metadata
// which is longer than citus.metadata_sync_interval, 1 second
teardown
{
    SELECT pg_sleep(2);

    RESET citus.shard_replication_factor;
    DROP TABLE update_node CASCADE;

    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
    SELECT nodeid, nodename, nodeport from pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-update-node-1"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'localhost',
        57638);
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
    INSERT INTO update_node(id, f1)
         SELECT id, md5(id::text)
           FROM generate_series(1, 10) as t(id);
}

step "s2-abort"
{
	ABORT;
}

step "s2-commit"
{
	COMMIT;
}

// session 1 updates node 1, session 2 writes should be blocked
permutation "s1-begin" "s1-update-node-1" "s2-begin" "s2-insert" "s1-commit" "s2-abort"
permutation "s2-begin" "s2-insert" "s1-update-node-1" "s2-commit"

