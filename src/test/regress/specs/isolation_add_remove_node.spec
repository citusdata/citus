setup
{
	SELECT 1;
}

teardown
{
	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-add-node-1"
{
	SELECT 1 FROM master_add_node('localhost', 57637);
}

step "s1-add-node-2"
{
	SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s1-add-inactive-1"
{
	SELECT 1 FROM master_add_inactive_node('localhost', 57637);
}

step "s1-activate-node-1"
{
	SELECT 1 FROM master_activate_node('localhost', 57637);
}

step "s1-disable-node-1"
{
	SELECT 1 FROM master_disable_node('localhost', 57637);
}

step "s1-remove-node-1"
{
	SELECT * FROM master_remove_node('localhost', 57637);
}

step "s1-remove-node-2"
{
	SELECT * FROM master_remove_node('localhost', 57638);
}

step "s1-commit"
{
	COMMIT;
}

step "s1-show-nodes"
{
	SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodename, nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-add-node-1"
{
	SELECT 1 FROM master_add_node('localhost', 57637);
}

step "s2-add-node-2"
{
	SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s2-activate-node-1"
{
	SELECT 1 FROM master_activate_node('localhost', 57637);
}

step "s2-disable-node-1"
{
	SELECT 1 FROM master_disable_node('localhost', 57637);
}

step "s2-remove-node-1"
{
	SELECT * FROM master_remove_node('localhost', 57637);
}

step "s2-remove-node-2"
{
	SELECT * FROM master_remove_node('localhost', 57638);
}

step "s2-commit"
{
	COMMIT;
}

# session 1 adds a node, session 2 removes it, should be ok
permutation "s1-begin" "s1-add-node-1" "s2-remove-node-1" "s1-commit" "s1-show-nodes"
# add a different node from 2 sessions, should be ok
permutation "s1-begin" "s1-add-node-1" "s2-add-node-1" "s1-commit" "s1-show-nodes"
# add the same node from 2 sessions, should be ok (idempotent)
permutation "s1-begin" "s1-add-node-1" "s2-add-node-1" "s1-commit" "s1-show-nodes"
# remove a different node from 2 transactions, should be ok
permutation "s1-add-node-1" "s1-add-node-2" "s1-begin" "s1-remove-node-1" "s2-remove-node-2" "s1-commit" "s1-show-nodes"
# remove the same node from 2 transactions, should be ok (idempotent)
permutation "s1-add-node-1" "s1-begin" "s1-remove-node-1" "s2-remove-node-1" "s1-commit" "s1-show-nodes"

# activate an active node from 2 transactions, should be ok
permutation "s1-add-node-1" "s1-begin" "s1-activate-node-1" "s2-activate-node-1" "s1-commit" "s1-show-nodes"
# disable an active node from 2 transactions, should be ok
permutation "s1-add-node-1" "s1-begin" "s1-disable-node-1" "s2-disable-node-1" "s1-commit" "s1-show-nodes"

# activate an inactive node from 2 transactions, should be ok
permutation "s1-add-inactive-1" "s1-begin" "s1-activate-node-1" "s2-activate-node-1" "s1-commit" "s1-show-nodes"
# disable an inactive node from 2 transactions, should be ok
permutation "s1-add-inactive-1" "s1-begin" "s1-disable-node-1" "s2-disable-node-1" "s1-commit" "s1-show-nodes"

# disable and activate an active node from 2 transactions, should be ok
permutation "s1-add-node-1" "s1-begin" "s1-disable-node-1" "s2-activate-node-1" "s1-commit" "s1-show-nodes"
# activate and disable an active node node from 2 transactions, should be ok
permutation "s1-add-node-1" "s1-begin" "s1-activate-node-1" "s2-disable-node-1" "s1-commit" "s1-show-nodes"

# disable and activate an inactive node from 2 transactions, should be ok
permutation "s1-add-inactive-1" "s1-begin" "s1-disable-node-1" "s2-activate-node-1" "s1-commit" "s1-show-nodes"
# activate and disable an inactive node node from 2 transactions, should be ok
permutation "s1-add-inactive-1" "s1-begin" "s1-activate-node-1" "s2-disable-node-1" "s1-commit" "s1-show-nodes"
