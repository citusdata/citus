-- Negative test cases for citus_split_shard_by_split_points UDF.

CREATE SCHEMA citus_split_shard_by_split_points_negative;
SET search_path TO citus_split_shard_by_split_points_negative;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 1;

CREATE TABLE table_to_split (id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second (id bigserial PRIMARY KEY, value char);
-- Shard1     | -2147483648   | -1073741825
-- Shard2     | -1073741824   | -1
-- Shard3     | 0             | 1073741823
-- Shard4     | 1073741824    | 2147483647
SELECT create_distributed_table('table_to_split','id');
--SELECT create_distributed_table('table_second', 'id', colocate_with => 'table_to_split');

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

SELECT * FROM citus_shards;
SELECT * FROM pg_dist_shard;

SET client_min_messages TO LOG;
SET citus.log_remote_commands TO on;

CREATE OR REPLACE VIEW show_catalog AS SELECT n.nspname as "Schema",
  c.relname as "Name",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- UDF fails for range partitioned tables.
\c - - - :master_port
SET citus.log_remote_commands TO on;
SET citus.next_shard_id TO 100;
SET search_path TO citus_split_shard_by_split_points_negative;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

SELECT citus_split_shard_by_split_points(
	1,
	ARRAY['0'],
	ARRAY[:worker_2_node, :worker_2_node],
	'force_logical');
-- On worker2, we want child shard 2 and dummy shard 1  --
-- on worker1, we want child shard 3 and 1 and dummy shard 2  --

\c - - - :worker_2_port
SET search_path TO citus_split_shard_by_split_points_negative;
SELECT * FROM show_catalog;
SELECT * FROM pg_subscription;

\c - - - :worker_1_port
SET search_path TO citus_split_shard_by_split_points_negative;
SELECT * FROM show_catalog;
SELECT * FROM pg_publication;
SELECT * FROM pg_subscription;
SELECT slot_name FROM pg_replication_slots;
