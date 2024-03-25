setup
{
	SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
	SELECT run_command_on_workers('SELECT pg_reload_conf()');
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
