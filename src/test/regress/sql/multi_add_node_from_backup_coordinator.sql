--
-- Test cloning the COORDINATOR node.
--
-- A clone of the coordinator is a byte-for-byte physical replica, so it also
-- carries the coordinator's Citus local table shard data. Citus local tables
-- live ONLY on the coordinator: the promotion shard-split relocates distributed
-- shards and replicates reference tables, but it must leave Citus local tables'
-- metadata placement on the coordinator and clean up the orphaned physical shard
-- copy that the clone carries. This test locks that behavior in (plus the basic
-- invariants: the clone becomes a worker, and the coordinator keeps serving the
-- Citus local table).
--

-- Put the coordinator in the metadata and let it own shards, so it has
-- distributed data to split to a clone and so Citus local tables can be created.
SET client_min_messages TO WARNING;
SELECT citus_set_coordinator_host('localhost', :master_port);
SELECT citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);
SET client_min_messages TO DEFAULT;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- Distributed table: with the coordinator holding shards, some shards land on
-- group 0 and are eligible to move to the clone.
CREATE TABLE coord_clone_dist (id bigserial, payload text);
SELECT create_distributed_table('coord_clone_dist', 'id');
INSERT INTO coord_clone_dist (payload) SELECT 'seed' FROM generate_series(1, 40);

-- Reference table.
CREATE TABLE coord_clone_ref (id bigserial, payload text);
SELECT create_reference_table('coord_clone_ref');
INSERT INTO coord_clone_ref (payload) SELECT 'seed' FROM generate_series(1, 10);

-- Citus local table: its single shard lives only on the coordinator.
CREATE TABLE coord_clone_local (id bigserial, payload text);
SELECT citus_add_local_table_to_metadata('coord_clone_local');
INSERT INTO coord_clone_local (payload) SELECT 'seed' FROM generate_series(1, 10);

-- Remember the Citus local shard id so we can look for its physical copy later.
SELECT shardid AS local_shardid
FROM pg_dist_shard WHERE logicalrelid = 'coord_clone_local'::regclass \gset

-- Clone the coordinator (master-follower streams from it) and promote it. Force
-- the deferred cleanup so the orphaned-copy drop is observable deterministically.
SET client_min_messages TO WARNING;
SELECT citus_add_clone_node('localhost', :follower_master_port, 'localhost', :master_port) AS clone_node_id \gset
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
CALL citus_cleanup_orphaned_resources();
SET client_min_messages TO DEFAULT;

-- Metadata: the Citus local placement must still be on the coordinator (group 0),
-- NOT moved to the clone.
SELECT p.groupid AS local_placement_group
FROM pg_dist_placement p
WHERE p.shardid = :local_shardid;

-- On the promoted clone: the Citus local SHELL table remains (like any worker),
-- but its orphaned shard-data copy must have been cleaned up.
\c - - - :follower_master_port
SELECT
    to_regclass('coord_clone_local') IS NOT NULL AS shell_present,
    to_regclass('coord_clone_local_' || :'local_shardid') IS NULL AS shard_copy_removed;

-- The clone is a worker now, not the coordinator.
SELECT citus_is_coordinator() AS clone_is_coordinator;

-- The Citus local table is still fully queryable from the promoted clone: the
-- shell routes to the coordinator placement, so despite the orphaned shard copy
-- being dropped, reads return the correct rows.
SELECT count(*) AS local_rows_from_clone,
       min(id)  AS min_id_from_clone,
       max(id)  AS max_id_from_clone
FROM coord_clone_local;

\c - - - :master_port
-- The coordinator still serves the Citus local table correctly.
SELECT count(*) AS local_rows FROM coord_clone_local;

-- cleanup
DROP TABLE coord_clone_dist;
DROP TABLE coord_clone_ref;
DROP TABLE coord_clone_local;
SET client_min_messages TO WARNING;
SELECT citus_remove_node('localhost', :master_port);
SET client_min_messages TO DEFAULT;
SET citus.shard_count TO DEFAULT;
SET citus.shard_replication_factor TO DEFAULT;
