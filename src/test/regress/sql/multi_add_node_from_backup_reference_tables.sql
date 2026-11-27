--
-- Test that citus_promote_clone_and_rebalance() gives the promoted clone only
-- the reference table placements that its primary has. Reference tables are
-- copied to a node that joins the cluster only once a command needs them
-- there, like create_distributed_table() or replicate_reference_tables(), so a
-- primary that joined after a reference table was created might not have it
-- yet, and then neither does its clone.
--
CREATE SCHEMA promotion_reference_tables;
SET search_path TO promotion_reference_tables;

-- created before the primary joins the cluster
CREATE TABLE ref (id int PRIMARY KEY);
SELECT create_reference_table('ref');
INSERT INTO ref SELECT generate_series(1, 10);
SELECT shardid AS ref_shard_id FROM pg_dist_shard WHERE logicalrelid = 'ref'::regclass \gset

SET client_min_messages TO WARNING;
SELECT master_add_node('localhost', :worker_8_port) AS primary_node_id \gset
SELECT citus_add_clone_node('localhost', :follower_worker_8_port, 'localhost', :worker_8_port) AS clone_node_id \gset
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
RESET client_min_messages;

-- neither the primary nor the clone has a placement of the reference table,
-- so the clone reads it from another node
SELECT count(*) AS placements
FROM pg_dist_placement JOIN pg_dist_node USING (groupid)
WHERE shardid = :ref_shard_id AND nodeid IN (:primary_node_id, :clone_node_id);
\c - - - :follower_worker_8_port
SELECT count(*) FROM promotion_reference_tables.ref;
\c - - - :master_port

-- replicate_reference_tables() copies it to both
SET client_min_messages TO WARNING;
SELECT replicate_reference_tables('block_writes');
RESET client_min_messages;
SELECT count(*) AS placements
FROM pg_dist_placement JOIN pg_dist_node USING (groupid)
WHERE shardid = :ref_shard_id AND nodeid IN (:primary_node_id, :clone_node_id);
\c - - - :follower_worker_8_port
SELECT count(*) FROM promotion_reference_tables.ref;
\c - - - :master_port

-- cleanup
SET client_min_messages TO WARNING;
DROP SCHEMA promotion_reference_tables CASCADE;
RESET client_min_messages;
