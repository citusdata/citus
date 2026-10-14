setup
{
    SELECT 1 FROM citus_add_node('localhost', 57636, groupid => 0);
    INSERT INTO pg_dist_transaction(groupid, gid)
    VALUES (0, 'citus_0_node_removal_recovery');
}

teardown
{
    SELECT citus_remove_node(nodename, nodeport)
    FROM pg_dist_node WHERE groupid = 0;
    DELETE FROM pg_dist_transaction WHERE gid = 'citus_0_node_removal_recovery';
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-recover"
{
    SELECT recover_prepared_transactions();
}

step "s1-remove-coordinator"
{
    SELECT citus_remove_node('localhost', 57636);
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

step "s2-remove-coordinator"
{
    SELECT citus_remove_node('localhost', 57636);
}

step "s2-set-coordinator-property"
{
    SELECT citus_set_node_property('localhost', 57636, 'shouldhaveshards', false);
}

step "s2-commit"
{
    COMMIT;
}

// Both paths delete recovery records, so serialize them in either order.
permutation "s1-begin" "s1-recover" "s2-remove-coordinator" "s1-commit"
permutation "s2-begin" "s2-remove-coordinator" "s1-recover" "s2-commit"

// Removal must take the recovery lock after the pg_dist_node lock, like other
// node operations, so a transaction already holding pg_dist_node can remove a
// node without deadlocking against a removal waiting on pg_dist_node.
permutation "s2-begin" "s2-set-coordinator-property" "s1-remove-coordinator" "s2-remove-coordinator" "s2-commit"
