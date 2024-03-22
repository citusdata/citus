ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

\c - - - :worker_1_port
ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

\c - - - :worker_2_port
ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

\c - - - :master_port
SELECT citus.mitmproxy('conn.allow()');

-- add the workers
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_proxy_port);  -- an mitmproxy which forwards to the second worker
