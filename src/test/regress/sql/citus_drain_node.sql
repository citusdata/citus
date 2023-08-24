CREATE SCHEMA citus_drain_node;
SET search_path TO citus_drain_node;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 974653;

SET client_min_messages TO ERROR;

SELECT * FROM citus_set_coordinator_host('localhost', :master_port);
SELECT * FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE TABLE test (x INT, y INT);
SELECT create_distributed_table('test','x');

CALL citus_cleanup_orphaned_resources();

SELECT nodename, nodeport, COUNT(*)
  FROM pg_dist_placement AS placement,
       pg_dist_node AS node
 WHERE placement.groupid = node.groupid
   AND node.noderole = 'primary' GROUP BY nodename, nodeport ORDER BY 1,2;

SELECT * FROM citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
SELECT * from citus_drain_node('localhost', :worker_1_port, shard_transfer_mode :='force_logical');

CALL citus_cleanup_orphaned_resources();

SELECT nodename, nodeport, COUNT(*)
  FROM pg_dist_placement AS placement,
       pg_dist_node AS node
 WHERE placement.groupid = node.groupid
   AND node.noderole = 'primary' GROUP BY nodename, nodeport ORDER BY 1,2;

SELECT * FROM citus_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);
SELECT * FROM citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

SELECT * FROM rebalance_table_shards(shard_transfer_mode :='force_logical');

CALL citus_cleanup_orphaned_resources();

SELECT nodename, nodeport, COUNT(*)
  FROM pg_dist_placement AS placement,
       pg_dist_node AS node
 WHERE placement.groupid = node.groupid
   AND node.noderole = 'primary' GROUP BY nodename, nodeport ORDER BY 1,2;

SELECT * FROM citus_set_node_property('localhost', :master_port, 'shouldhaveshards', false);

SELECT * FROM rebalance_table_shards(shard_transfer_mode :='force_logical');

CALL citus_cleanup_orphaned_resources();

SELECT nodename, nodeport, COUNT(*)
  FROM pg_dist_placement AS placement,
       pg_dist_node AS node
 WHERE placement.groupid = node.groupid
   AND node.noderole = 'primary' GROUP BY nodename, nodeport ORDER BY 1,2;

RESET search_path;

SET client_min_messages TO WARNING;
DROP SCHEMA citus_drain_node CASCADE;
