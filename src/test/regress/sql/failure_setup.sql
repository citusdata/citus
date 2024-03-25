SELECT citus.mitmproxy('conn.allow()');

-- add the workers
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_proxy_port);  -- an mitmproxy which forwards to the second worker
