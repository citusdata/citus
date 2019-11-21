--
-- failure_add_disable_node tests master_add_node, master_remove_node
-- master_activate_node for failures.
-- master_disable_node and master_add_inactive_node can not be
-- tested as they don't create network activity
--

SELECT citus.mitmproxy('conn.allow()');

SET citus.next_shard_id TO 200000;

-- verify we have all worker nodes present
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

-- verify there are no tables that could prevent add/remove node operations
SELECT * FROM pg_dist_partition;

CREATE SCHEMA add_remove_node;
SET SEARCH_PATH=add_remove_node;
CREATE TABLE user_table(user_id int, user_name text);
SELECT create_reference_table('user_table');

CREATE TABLE event_table(user_id int, event_id int, event_name text);
SELECT create_distributed_table('event_table', 'user_id');

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

SELECT master_disable_node('localhost', :worker_2_proxy_port);

SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- fail activate node by failing reference table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');

SELECT master_activate_node('localhost', :worker_2_proxy_port);

SELECT citus.mitmproxy('conn.allow()');

-- verify node is not activated
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- fail create schema command
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SCHEMA").kill()');

SELECT master_activate_node('localhost', :worker_2_proxy_port);

-- verify node is not activated
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- fail activate node by failing reference table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").cancel(' || pg_backend_pid() || ')');

SELECT master_activate_node('localhost', :worker_2_proxy_port);

-- verify node is not activated
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

SELECT citus.mitmproxy('conn.allow()');

-- master_remove_node fails when there are shards on that worker
SELECT master_remove_node('localhost', :worker_2_proxy_port);

-- drop event table and re-run remove
DROP TABLE event_table;
SELECT master_remove_node('localhost', :worker_2_proxy_port);

-- verify node is removed
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- test master_add_inactive_node
-- it does not create any network activity therefore can not
-- be injected failure through network
SELECT master_add_inactive_node('localhost', :worker_2_proxy_port);

SELECT master_remove_node('localhost', :worker_2_proxy_port);

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- test master_add_node replicated a reference table
-- to newly added node.
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');

SELECT master_add_node('localhost', :worker_2_proxy_port);

-- verify node is not added
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").cancel(' || pg_backend_pid() || ')');

SELECT master_add_node('localhost', :worker_2_proxy_port);

-- verify node is not added
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- reset cluster to original state
SELECT citus.mitmproxy('conn.allow()');
SELECT master_add_node('localhost', :worker_2_proxy_port);

-- verify node is added
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

-- fail master_add_node by failing copy out operation
SELECT master_remove_node('localhost', :worker_1_port);
SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
SELECT master_add_node('localhost', :worker_1_port);

-- verify node is not added
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT citus.mitmproxy('conn.allow()');
SELECT master_add_node('localhost', :worker_1_port);

-- verify node is added
SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

RESET SEARCH_PATH;
DROP SCHEMA add_remove_node CASCADE;
SELECT * FROM run_command_on_workers('DROP SCHEMA IF EXISTS add_remove_node CASCADE')
ORDER BY nodeport;
