--
-- failure_add_disable_node tests master_add_node, master_remove_node
-- master_activate_node for failures.
-- citus_disable_node_and_wait and master_add_inactive_node can not be
-- tested as they don't create network activity
--

SELECT citus.mitmproxy('conn.allow()');

SET citus.next_shard_id TO 200000;
SET citus.replicate_reference_tables_on_activate TO off;

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

SELECT citus_disable_node('localhost', :worker_2_proxy_port, true);
SELECT public.wait_until_metadata_sync();

SELECT * FROM master_get_active_worker_nodes()
ORDER BY 1, 2;

SELECT shardid, shardstate
FROM pg_dist_placement p JOIN pg_dist_shard s USING (shardid)
WHERE s.logicalrelid = 'user_table'::regclass
ORDER BY placementid;

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

BEGIN;
	-- master_remove_node succeeds because there are the
	-- healthy placements of the shards that exists on
	-- worker_2_proxy_port on the other worker (worker_1_port)
	-- as well
	SELECT master_remove_node('localhost', :worker_2_proxy_port);
ROLLBACK;

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
