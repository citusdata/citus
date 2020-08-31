setup
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);

    CREATE TABLE t1(a int);
    SELECT create_distributed_table('t1','a');
}

teardown
{
    DROP TABLE t1;

    -- remove the nodes again
    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"
step "s1-begin" { BEGIN; }
step "s1-insert" { INSERT INTO t1 SELECT generate_series(1, 100); }
step "s1-abort" { ABORT; }

session "s2"
step "s2-begin" { BEGIN; }
step "s2-update-node-1" {
    -- update a specific node by address
    SELECT master_update_node(nodeid, 'localhost', nodeport + 10)
      FROM pg_dist_node
     WHERE nodename = 'localhost'
       AND nodeport = 57637;
}
step "s2-update-node-1-force" {
    -- update a specific node by address (force)
    SELECT master_update_node(nodeid, 'localhost', nodeport + 10, force => true, lock_cooldown => 100)
      FROM pg_dist_node
     WHERE nodename = 'localhost'
       AND nodeport = 57637;
}
step "s2-abort" { ABORT; }

permutation "s1-begin" "s1-insert" "s2-begin" "s2-update-node-1" "s1-abort" "s2-abort"
permutation "s1-begin" "s1-insert" "s2-begin" "s2-update-node-1-force" "s2-abort" "s1-abort"
