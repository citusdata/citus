SELECT citus.mitmproxy('conn.allow()');

-- add the workers
SELECT master_add_node('localhost', :worker_1_port);  -- the second worker
SELECT master_add_node('localhost', :worker_2_proxy_port);  -- the first worker, behind a mitmproxy
