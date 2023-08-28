setup
{
	SELECT citus_internal.restore_isolation_tester_func();
	DROP EXTENSION citus;
}

teardown
{
	SELECT citus_internal.replace_isolation_tester_func();

	// Restore a node for later tests to succeed.
	SELECT 1 FROM master_add_node('localhost', 57637);
}

session "s1"

step "s1-create"
{
	CREATE EXTENSION citus;
}

session "s2"

step "s2-check"
{
	 SELECT 1 FROM citus_version();
}

// session 1 creates the extension, s2 checks if it exists.
permutation "s1-create" "s2-check"
