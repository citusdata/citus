session "s1"
step "s1a"
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);
    SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodeid;
}

permutation "s1a"
