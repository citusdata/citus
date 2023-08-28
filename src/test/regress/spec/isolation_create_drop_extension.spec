setup
{
	SELECT citus_internal.restore_isolation_tester_func();
	DROP EXTENSION citus;
	SELECT 1;
}

teardown
{
	SELECT citus_internal.replace_isolation_tester_func();
	SELECT 1 FROM master_add_node('localhost', 57637);
}

session "s1"

step "s1-create"
{
	CREATE EXTENSION citus;
	SELECT 1;
}

session "s2"

step "s2-check"
{
	 SELECT 1 FROM citus_version();
}

permutation "s1-create" "s2-check"
