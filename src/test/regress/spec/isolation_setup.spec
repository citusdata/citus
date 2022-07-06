session "s1"
step "setup"
{
	-- Replace pg_isolation_test_session_is_blocked so that all isolation tests are run with Citus implementation.
	--
	-- Vanilla PG only checks for local blocks, whereas citus implementation also checks worker jobs in distributed
	-- transactions.
	--
	-- We have some tests that do not produce deterministic outputs when we use the Citus UDFs. They restore this
	-- function in the setup phase and replace it again on the teardown phase so that the remainder of the tests can
	-- keep using the Citus alternatives. Those tests should never be run concurrently with other isolation tests.
	SELECT citus_internal.replace_isolation_tester_func();
}

permutation "setup"
