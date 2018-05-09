setup
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);

    SELECT nodeid, nodename, nodeport from pg_dist_node;
}

teardown
{
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
        58637);
}

step "s1-update-node-2"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57638),
        'localhost',
        58638);
}

step "s1-commit"
{
	COMMIT;
}

step "s1-show-nodes"
{
    SELECT nodeid, nodename, nodeport, isactive
      FROM pg_dist_node
  ORDER BY nodename, nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-update-node-1"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57637),
        'localhost',
        58637);
}

step "s2-update-node-2"
{
    SELECT 1 FROM master_update_node(
        (select nodeid from pg_dist_node where nodeport = 57638),
        'localhost',
        58638);
}

step "s2-abort"
{
	ABORT;
}

step "s2-commit"
{
	COMMIT;
}

# session 1 updates node 1, session 2 updates node 2, should be ok
permutation "s1-begin" "s1-update-node-1" "s2-update-node-2" "s1-commit" "s1-show-nodes"

# sessions 1 updates node 1, session 2 tries to do the same
permutation "s1-begin" "s1-update-node-1" "s2-begin" "s2-update-node-1" "s1-commit" "s2-abort" "s1-show-nodes"
