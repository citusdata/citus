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
-- Citus local table). It also covers the resources that earlier operations left
-- on the coordinator for cleanup, which the clone would copy as well.
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

-- A shard move leaves the old placements of the shards that it moved on the
-- coordinator for deferred cleanup. The clone would copy them.
CREATE TABLE coord_clone_moved (id int);
SELECT create_distributed_table('coord_clone_moved', 'id', colocate_with => 'none');
CREATE TABLE coord_clone_moved_in_use (id int);
SELECT create_distributed_table('coord_clone_moved_in_use', 'id', colocate_with => 'coord_clone_moved');
SELECT min(shardid) AS moved_shardid
FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
WHERE logicalrelid = 'coord_clone_moved'::regclass AND groupid = 0 \gset
SELECT shardid AS in_use_shardid
FROM pg_dist_shard
WHERE logicalrelid = 'coord_clone_moved_in_use'::regclass
  AND shardminvalue = (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = :moved_shardid) \gset
SELECT nodeport AS move_target_port
FROM pg_dist_node
WHERE groupid <> 0 AND noderole = 'primary' AND isactive AND shouldhaveshards
ORDER BY nodeid LIMIT 1 \gset
SELECT citus_move_shard_placement(:moved_shardid, 'localhost', :master_port,
                                  'localhost', :move_target_port, 'block_writes');
SELECT format('coord_clone_moved_%s', :moved_shardid) AS moved_shard,
       format('coord_clone_moved_in_use_%s', :in_use_shardid) AS in_use_shard \gset

-- The clone also copies coordinator-only state that it must not act on as a
-- worker, e.g. the cleanup record of the old placement that is in use during the
-- promotion below, background jobs, and the server id.
INSERT INTO pg_dist_background_job (job_type, description, state)
VALUES ('test_job', 'copied to the clone', 'finished') RETURNING job_id AS copied_job_id \gset
SELECT metadata->>'server_id' AS coordinator_server_id FROM pg_dist_node_metadata \gset

-- Clone the coordinator (master-follower streams from it) and promote it.
SET client_min_messages TO WARNING;
SELECT citus_add_clone_node('localhost', :follower_master_port, 'localhost', :master_port) AS clone_node_id \gset

-- A write to a shard on the coordinator runs locally, without a remote
-- transaction. If it preceded the promotion in the same transaction, it would
-- only commit after it, and its shard could move to the clone without it.
SELECT min(g) AS coordinator_key
FROM generate_series(1000, 2000) g
JOIN pg_dist_shard_placement p ON p.shardid = get_shard_id_for_distribution_column('coord_clone_dist', g)
WHERE p.nodeport = :master_port \gset
BEGIN;
INSERT INTO coord_clone_dist VALUES (:coordinator_key, 'before the promotion');
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
ROLLBACK;

-- The promotion cleans up the old placements in a separate transaction before
-- the clone stops replicating, so it cannot drop the one that its own
-- transaction keeps in use. Unlike the write above, the ACCESS SHARE lock does
-- not give the transaction a transaction id, so the promotion accepts it.
BEGIN;
LOCK TABLE :"in_use_shard" IN ACCESS SHARE MODE;
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
COMMIT;
SET client_min_messages TO DEFAULT;

-- The clone did not copy the old placement that was dropped, and the promotion
-- added a cleanup record for its copy of the one in use.
SELECT groupid AS clone_group_id FROM pg_dist_node WHERE nodeid = :clone_node_id \gset
SELECT object_name = 'public.' || :'in_use_shard' AS in_use,
       node_group_id = :clone_group_id AS on_clone
FROM pg_dist_cleanup
WHERE object_name IN ('public.' || :'moved_shard', 'public.' || :'in_use_shard')
ORDER BY 1, 2;
SELECT to_regclass(:'moved_shard') IS NULL AS moved_removed,
       to_regclass(:'in_use_shard') IS NOT NULL AS in_use_present;
\c - - - :follower_master_port
SELECT to_regclass(:'moved_shard') IS NULL AS moved_not_copied,
       to_regclass(:'in_use_shard') IS NOT NULL AS in_use_copied;
\c - - - :master_port

-- Force the deferred cleanup so the drops of the orphaned copies are
-- observable deterministically.
SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
SET client_min_messages TO DEFAULT;

SELECT count(*) AS old_placement_cleanup_records
FROM pg_dist_cleanup
WHERE object_name IN ('public.' || :'moved_shard', 'public.' || :'in_use_shard');
SELECT to_regclass(:'in_use_shard') IS NULL AS in_use_removed;

-- Metadata: the Citus local placement must still be on the coordinator (group 0),
-- NOT moved to the clone.
SELECT p.groupid AS local_placement_group
FROM pg_dist_placement p
WHERE p.shardid = :local_shardid;

-- On the promoted clone: the Citus local SHELL table remains (like any worker),
-- but its orphaned shard-data copy must have been cleaned up, and so must the
-- copy of the old placement that was in use.
\c - - - :follower_master_port
SELECT
    to_regclass('coord_clone_local') IS NOT NULL AS shell_present,
    to_regclass('coord_clone_local_' || :'local_shardid') IS NULL AS shard_copy_removed,
    to_regclass(:'in_use_shard') IS NULL AS in_use_copy_removed;

-- The clone is a worker now, not the coordinator.
SELECT citus_is_coordinator() AS clone_is_coordinator;

-- ... without the coordinator's cleanup records, background jobs and server id.
SELECT (SELECT count(*) FROM pg_dist_cleanup) AS clone_cleanup_records,
       (SELECT count(*) FROM pg_dist_background_job) AS clone_background_jobs,
       metadata->>'server_id' <> :'coordinator_server_id' AS clone_has_own_server_id
FROM pg_dist_node_metadata;

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
DELETE FROM pg_dist_background_job WHERE job_id = :copied_job_id;
DROP TABLE coord_clone_dist;
DROP TABLE coord_clone_ref;
DROP TABLE coord_clone_local;
DROP TABLE coord_clone_moved;
DROP TABLE coord_clone_moved_in_use;
SET client_min_messages TO WARNING;
SELECT citus_remove_node('localhost', :master_port);
SET client_min_messages TO DEFAULT;
SET citus.shard_count TO DEFAULT;
SET citus.shard_replication_factor TO DEFAULT;
