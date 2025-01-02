// The code we care about is in the setup stage, because it needs to be executed outside of a transaction.
// For the setup stage to be executed we need at least one permutation though.
// Therefore, we added s1a as a non-functional step, just make the setup step to work.
setup
{
	SELECT bool_and(result='ALTER SYSTEM') AS all_ok FROM  run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
	SELECT bool_and(result='t') AS all_ok FROM run_command_on_all_nodes('SELECT pg_reload_conf()');
}

session "s1"
step "s1a"
{
	SELECT 1;
}

permutation "s1a"
