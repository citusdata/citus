session "s1"
step "s1a"
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);
	SELECT result from run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
	SELECT result from run_command_on_all_nodes('SELECT pg_reload_conf()');
}

permutation "s1a"
