setup
{
	SELECT 1;
}

teardown
{
	SELECT 1 FROM citus_remove_node('localhost', 57636);
}

session "s1"

step "s1-begin"
{
    SELECT 1;
}

permutation "s1-begin"
