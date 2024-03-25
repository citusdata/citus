setup
{
	SELECT result from run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
	SELECT result from run_command_on_all_nodes('SELECT pg_reload_conf()');
}

session "s1"
step "s1-begin"
{
	BEGIN;
}

step "s1-commit"
{
	COMMIT;
}

permutation "s1-begin" "s1-commit"
