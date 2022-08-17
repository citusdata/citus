setup
{
    SELECT 1 FROM citus_set_coordinator_host('localhost', 57636);

}

teardown
{
	SELECT 1;
}

session "s1"

step "s1-begin"
{
    SELECT 1;
}

permutation "s1-begin"
