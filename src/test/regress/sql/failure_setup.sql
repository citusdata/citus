SELECT citus.mitmproxy('conn.allow()');

-- add the workers
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_proxy_port);  -- an mitmproxy which forwards to the second worker

SELECT result from  run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_ddl_propagation TO ON');
SELECT result from  run_command_on_all_nodes('SELECT pg_reload_conf()');
