SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
SELECT run_command_on_workers('SELECT pg_reload_conf()');
