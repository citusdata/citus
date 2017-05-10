session "s1"
step "s1a"
{
    SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57637);
    SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);
}

permutation "s1a"
