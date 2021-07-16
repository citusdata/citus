SELECT success FROM run_command_on_workers('ALTER SYSTEM SET citus.enable_ddl_propagation TO OFF');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

