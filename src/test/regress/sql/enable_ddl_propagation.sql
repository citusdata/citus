SELECT result from  run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
SELECT result from  run_command_on_all_nodes('SELECT pg_reload_conf()');
