session "s1"
step "s1a"
{
    SELECT master_add_node('localhost', 57637);
    SELECT master_add_node('localhost', 57638);
}

permutation "s1a"
